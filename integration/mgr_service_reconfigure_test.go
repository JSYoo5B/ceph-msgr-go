package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

type mgrServiceConfigValue struct {
	Effective string `json:"effective"`
	Override  string `json:"override,omitempty"`
	Present   bool   `json:"override_present"`
}

type mgrServiceConfigNative struct {
	Manager  string                           `json:"manager"`
	GID      uint64                           `json:"gid"`
	Epoch    uint32                           `json:"epoch"`
	Baseline map[string]mgrServiceConfigValue `json:"baseline"`
	Current  map[string]mgrServiceConfigValue `json:"current"`
	Restored bool                             `json:"restored"`
}

type mgrServiceConfigSample struct {
	LastBeacon string  `json:"last_beacon"`
	Elapsed    float64 `json:"elapsed_seconds"`
	DumpEpoch  uint32  `json:"dump_epoch"`
}

type mgrServiceConfigProbe struct {
	Period     int    `json:"period_seconds"`
	Threshold  uint32 `json:"threshold"`
	Removed    bool   `json:"removed"`
	GID        uint64 `json:"gid"`
	StartEpoch uint32 `json:"start_epoch"`
}

type mgrServiceConfigPhase struct {
	Name              string                           `json:"name"`
	Period            int                              `json:"period_seconds"`
	Threshold         uint32                           `json:"threshold"`
	Native            map[string]mgrServiceConfigValue `json:"native"`
	Samples           []mgrServiceConfigSample         `json:"samples"`
	ExistingPeriod    int                              `json:"existing_period_seconds"`
	ExistingThreshold uint32                           `json:"existing_threshold"`
	NewSession        *mgrServiceConfigProbe           `json:"new_session,omitempty"`
}

func mgrServiceNativeConfiguration(t *testing.T, ctx context.Context, action string) mgrServiceConfigNative {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	call, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	if err := testcluster.ControlDaemon(call, dir, "verify", "mgr-service-config", action); err != nil {
		t.Fatal("native owned MGR configuration", action, err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "mgr-service-config-native.json"))
	var result mgrServiceConfigNative
	if err != nil || len(data) > 64<<10 || json.Unmarshal(data, &result) != nil || result.Manager == "" || result.GID == 0 || result.Epoch == 0 || len(result.Baseline) != 3 || len(result.Current) != 3 {
		t.Fatal("native owned MGR configuration receipt is incomplete", action)
	}
	return result
}

func waitMgrServiceConfigure(t *testing.T, ctx context.Context, svc *cephmsgr.MgrService, period int, threshold uint32) {
	t.Helper()
	wait, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		cfg := svc.Configure()
		if cfg.StatsPeriod == time.Duration(period)*time.Second && cfg.StatsThreshold == threshold && len(cfg.Extension) == 0 {
			return
		}
		select {
		case <-wait.Done():
			t.Fatal("MGR service did not receive the current native configuration", cfg)
		case <-svc.Done():
			t.Fatal("MGR service ended during native reconfiguration")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func testMgrServiceConfiguration(t *testing.T, ctx context.Context, c *cephmsgr.Client, svc *cephmsgr.MgrService, service, daemon string, metadata, status, task map[string]string) {
	t.Helper()
	receipt := struct {
		Manager    string                           `json:"manager"`
		ManagerGID uint64                           `json:"manager_gid"`
		ClientGID  uint64                           `json:"client_gid"`
		StartEpoch uint32                           `json:"start_epoch"`
		Baseline   map[string]mgrServiceConfigValue `json:"baseline"`
		Phases     []mgrServiceConfigPhase          `json:"phases"`
		Restored   bool                             `json:"restored"`
	}{Phases: []mgrServiceConfigPhase{}}
	defer func() {
		data, err := json.MarshalIndent(receipt, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "mgr-service-configuration.json"), append(data, '\n'))
		}
		if err != nil {
			t.Error("preserve MGR service configuration evidence", err)
		}
	}()
	native := mgrServiceNativeConfiguration(t, ctx, "begin")
	receipt.Manager, receipt.ManagerGID, receipt.Baseline = native.Manager, native.GID, native.Baseline
	// This independent cleanup is registered before the first configuration
	// mutation. The fixture also restores on teardown if the worker fails.
	defer func() {
		restore, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		restored := mgrServiceNativeConfiguration(t, restore, "restore")
		receipt.Restored = restored.Restored && reflect.DeepEqual(restored.Current, receipt.Baseline)
		if !receipt.Restored {
			t.Error("native MGR effective values or override presence were not restored")
		}
	}()
	if state := c.Snapshot(); !state.Manager.Available || state.Manager.Name != native.Manager || state.Manager.GlobalID != native.GID {
		t.Fatal("subscribed active manager does not match the owned native manager")
	}
	mgrServiceNativeConfiguration(t, ctx, "autotune-off")
	native = mgrServiceNativeConfiguration(t, ctx, "one-second")
	initial := svc.Configure()
	waitMgrServiceConfigure(t, ctx, svc, 1, initial.StatsThreshold)

	lastBeacon := ""
	started := time.Now()
	observe := func(phase *mgrServiceConfigPhase) {
		t.Helper()
		observed := nativePool(t, ctx, "servicewatch", map[string]any{
			"service": service, "daemon": daemon, "expect": "present", "wait_seconds": 15,
			"metadata": metadata, "status": status, "task_status": task,
			"exact_status": true, "after_beacon": lastBeacon,
		})
		var entry struct {
			GID        uint64            `json:"gid"`
			StartEpoch uint32            `json:"start_epoch"`
			TaskStatus map[string]string `json:"task_status"`
		}
		var serviceStatus struct {
			Status     map[string]string `json:"status"`
			LastBeacon string            `json:"last_beacon"`
		}
		if !observed.ServiceMatched || json.Unmarshal(observed.ServiceEntry, &entry) != nil || json.Unmarshal(observed.ServiceStatusEntry, &serviceStatus) != nil ||
			entry.GID != c.Snapshot().GlobalID || entry.StartEpoch == 0 || entry.StartEpoch > observed.ServiceDumpEpoch || !reflect.DeepEqual(entry.TaskStatus, task) ||
			!reflect.DeepEqual(serviceStatus.Status, status) || serviceStatus.LastBeacon == "" || lastBeacon != "" && serviceStatus.LastBeacon <= lastBeacon {
			t.Fatal("native beacon observation changed caller fields or lacks a newer beacon", phase.Name)
		}
		if receipt.ClientGID != 0 && (receipt.ClientGID != entry.GID || receipt.StartEpoch != entry.StartEpoch) {
			t.Fatal("reconfiguration changed the native service registration identity")
		}
		receipt.ClientGID, receipt.StartEpoch, lastBeacon = entry.GID, entry.StartEpoch, serviceStatus.LastBeacon
		state := c.Snapshot()
		if !state.Manager.Available || state.Manager.Name != receipt.Manager || state.Manager.GlobalID != receipt.ManagerGID {
			t.Fatal("reconfiguration changed the active manager identity")
		}
		phase.Samples = append(phase.Samples, mgrServiceConfigSample{lastBeacon, time.Since(started).Seconds(), observed.ServiceDumpEpoch})
	}
	for _, spec := range []struct {
		action    string
		period    int
		threshold uint32
	}{{"initial-one-second", 1, initial.StatsThreshold}, {"two-seconds", 2, initial.StatsThreshold}, {"threshold-only", 2, 6}, {"one-second-again", 1, 6}} {
		if spec.action != "initial-one-second" {
			native = mgrServiceNativeConfiguration(t, ctx, spec.action)
		}
		if native.Manager != receipt.Manager || native.GID != receipt.ManagerGID || native.Current["mgr_stats_period_autotune"].Effective != "false" {
			t.Fatal("native configuration phase lost its manager identity or disabled autotuning")
		}
		// Pinned handle_open excludes TYPE_CLIENT service sessions from
		// daemon_connections. handle_conf_change broadcasts only to that set,
		// so this existing registration retains its original configuration.
		// This native boundary is distinct from synthetic receive-path tests.
		phase := mgrServiceConfigPhase{Name: spec.action, Period: spec.period, Threshold: spec.threshold, Native: native.Current, Samples: []mgrServiceConfigSample{},
			ExistingPeriod: int(initial.StatsPeriod / time.Second), ExistingThreshold: initial.StatsThreshold}
		// These are strictly progressing sampled native last_beacon values.
		// They are not transmission counts or exact scheduler timing bounds.
		for range 3 {
			observe(&phase)
			if got := svc.Configure(); got.StatsPeriod != initial.StatsPeriod || got.StatsThreshold != initial.StatsThreshold || len(got.Extension) != 0 {
				t.Fatal("existing CLIENT service configuration changed despite the pinned broadcast boundary", got)
			}
		}
		if spec.action != "initial-one-second" {
			// A distinct explicit registration proves that the same native MGR
			// now sends the changed values in its initial configure push. The
			// original handle is retained and never re-registered implicitly.
			probeName := daemon + "-" + spec.action
			probe, err := c.OpenMgrService(ctx, cephmsgr.MgrServiceOptions{Service: service, Daemon: probeName,
				Metadata: map[string]string{"probe": spec.action}, Status: map[string]string{"state": "configure-probe"}, DisableBeacon: true})
			if err != nil {
				t.Fatal("open distinct configuration probe", err)
			}
			func() {
				defer func() {
					stop, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					probe.Close(stop)
				}()
				waitMgrServiceConfigure(t, ctx, probe, spec.period, spec.threshold)
				visible := serviceWatch(t, ctx, service, probeName, "present", map[string]string{"probe": spec.action}, map[string]string{"state": "configure-probe"}, nil)
				var entry struct {
					GID        uint64 `json:"gid"`
					StartEpoch uint32 `json:"start_epoch"`
				}
				if !visible.ServiceMatched || json.Unmarshal(visible.ServiceEntry, &entry) != nil || entry.GID != receipt.ClientGID || entry.StartEpoch == 0 || entry.StartEpoch > visible.ServiceDumpEpoch {
					t.Fatal("distinct configuration probe was not observed natively with the expected identity")
				}
				if err := probe.Close(ctx); err != nil {
					t.Fatal("acknowledged configuration probe close", err)
				}
				removed := serviceWatch(t, ctx, service, probeName, "absent", nil, nil, nil).ServiceMatched
				if !removed {
					t.Fatal("configuration probe removal not observed natively")
				}
				phase.NewSession = &mgrServiceConfigProbe{spec.period, spec.threshold, removed, entry.GID, entry.StartEpoch}
			}()
		}
		receipt.Phases = append(receipt.Phases, phase)
	}
	t.Logf("CLIENT MGR configuration boundary: manager %s/%d, client %d, service start epoch %d, four phases with three native beacon samples each", receipt.Manager, receipt.ManagerGID, receipt.ClientGID, receipt.StartEpoch)
}
