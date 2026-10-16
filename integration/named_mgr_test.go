package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

type namedMgrNativeMap struct {
	Epoch     uint32 `json:"epoch"`
	Name      string `json:"active_name"`
	GID       uint64 `json:"active_gid"`
	Available bool   `json:"available"`
}

type namedMgrNativeReceipt struct {
	Phase   string            `json:"phase"`
	Before  namedMgrNativeMap `json:"before"`
	After   namedMgrNativeMap `json:"after"`
	Version tellVersion       `json:"version"`
}

type namedMgrGoObservation struct {
	Action      string            `json:"action"`
	Target      string            `json:"target"`
	Result      cephmsgr.Result   `json:"result"`
	Error       string            `json:"error,omitempty"`
	Unknown     bool              `json:"outcome_unknown"`
	ClientID    uint64            `json:"client_id"`
	FSID        string            `json:"fsid"`
	Manager     namedMgrNativeMap `json:"manager"`
	Endpoint    string            `json:"endpoint"`
	AuthExpires time.Time         `json:"auth_expires"`
	MgrExpires  time.Time         `json:"mgr_expires"`
	Closed      bool              `json:"closed"`
}

func namedMgrNativeObserve(t *testing.T, ctx context.Context, control, phase string) namedMgrNativeReceipt {
	t.Helper()
	if err := testcluster.RequestControl(ctx, control, "observe-named-mgr", "named-mgr-observed", phase); err != nil {
		t.Fatal("native named MGR observation", err)
	}
	data, err := os.ReadFile(filepath.Join(control, "named-mgr-oracle-"+phase+".json"))
	var receipt namedMgrNativeReceipt
	if err != nil || len(data) > 1<<20 || json.Unmarshal(data, &receipt) != nil || receipt.Phase != phase || !receipt.Before.Available || !receipt.After.Available || receipt.Before.GID == 0 || receipt.Before.GID != receipt.After.GID || receipt.Before.Name != receipt.After.Name || receipt.Before.Epoch == 0 || receipt.After.Epoch < receipt.Before.Epoch || receipt.Version.Release != "tentacle" || !strings.HasPrefix(receipt.Version.Version, "20.2.") {
		t.Fatal("invalid independent native named MGR receipt", err, receipt)
	}
	return receipt
}

func TestCephNamedManagerTellIntegration(t *testing.T) {
	control := ordinaryLogFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	prepareNativeManagerFailover(t, ctx, control)
	before := namedMgrNativeObserve(t, ctx, control, "before")
	c, err := cephmsgr.Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	var observations []namedMgrGoObservation
	record := func(action, target string, result cephmsgr.Result, err error, s cephmsgr.State) {
		item := namedMgrGoObservation{Action: action, Target: target, Result: result, ClientID: s.GlobalID, FSID: s.FSID, Manager: namedMgrNativeMap{s.Manager.MapEpoch, s.Manager.Name, s.Manager.GlobalID, s.Manager.Available}, Endpoint: s.Manager.Endpoint, AuthExpires: s.AuthTicket.Expires, MgrExpires: s.MgrTicket.Expires, Closed: s.Closed}
		var unknown *cephmsgr.OutcomeUnknownError
		item.Unknown = errors.As(err, &unknown)
		if err != nil {
			item.Error = err.Error()
		}
		observations = append(observations, item)
	}
	t.Cleanup(func() {
		data, err := json.MarshalIndent(observations, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(control, "named-mgr-results.json"), append(data, '\n'))
		}
		if err != nil {
			t.Error("preserve named MGR public results", err)
		}
	})
	state := waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
		return s.Manager.Available && s.Manager.MapEpoch >= before.After.Epoch && s.Manager.Name == before.After.Name && s.Manager.GlobalID == before.After.GID
	})
	name := state.Manager.Name
	record("initial-map", name, cephmsgr.Result{}, nil, state)
	version := cephmsgr.Command{JSON: []byte(`{"prefix":"version","format":"json"}`)}
	check := func(t *testing.T, name string, want tellVersion) {
		t.Helper()
		result, err := c.MgrTellTo(ctx, name, version)
		record("named-version", name, result, err, c.Snapshot())
		var got tellVersion
		if err != nil || result.Code != 0 || json.Unmarshal(result.Data, &got) != nil || got != want {
			t.Fatal("named MGR/native version mismatch", result, err, got, want)
		}
		s := c.Snapshot()
		_, role, err := testcluster.SplitPeer(s.Manager.Endpoint)
		if err != nil || role != "mgr."+name || s.Manager.Name != name || !s.Manager.Ready || s.GlobalID == 0 || s.AuthRejection != nil {
			t.Fatal("named MGR target/identity", s, err)
		}
	}
	t.Run("exact_active_native", func(t *testing.T) { check(t, name, before.Version) })
	t.Run("nonactive_names_refused_locally", func(t *testing.T) {
		for _, n := range []string{"__missing_named_mgr__", "mgr." + name, "*", "0"} {
			result, err := c.MgrTellTo(ctx, n, version)
			record("nonactive-name-refusal", n, result, err, c.Snapshot())
			var unknown *cephmsgr.OutcomeUnknownError
			if !errors.Is(err, cephmsgr.ErrManagerNotFound) || errors.As(err, &unknown) {
				t.Fatal(n, err)
			}
		}
		for _, standby := range state.Manager.Standbys {
			result, err := c.MgrTellTo(ctx, standby.Name, version)
			record("standby-refusal", standby.Name, result, err, c.Snapshot())
			if !errors.Is(err, cephmsgr.ErrManagerNotActive) {
				t.Fatal("standby substituted active", standby.Name, err)
			}
		}
	})
	t.Run("signed_unknown_command_and_readonly_denial", func(t *testing.T) {
		result, err := c.MgrTellTo(ctx, name, cephmsgr.Command{JSON: []byte(`{"prefix":"__ceph_msgr_missing_tell__","format":"json"}`)})
		record("unknown-command", name, result, err, c.Snapshot())
		var server *cephmsgr.CommandError
		if result.Code != -22 || !errors.As(err, &server) || server.Message != result.Message {
			t.Fatal(result, err)
		}
		options := integrationOptions(t)
		key, err := os.ReadFile(filepath.Join(control, "readonly.key"))
		if err != nil {
			t.Fatal(err)
		}
		options.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(key)))
		if err != nil {
			t.Fatal(err)
		}
		options.Identity = "client.readonly"
		readonly, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		defer readonly.Close()
		result, err = readonly.MgrTellTo(ctx, name, version)
		record("readonly-command-denial", name, result, err, readonly.Snapshot())
		var auth *cephmsgr.AuthenticationError
		var unknown *cephmsgr.OutcomeUnknownError
		if result.Code != -13 || !errors.As(err, &server) || errors.As(err, &auth) || errors.As(err, &unknown) || server.Message != result.Message {
			t.Fatal(result, err)
		}
		if err := fixtureRecoveryRead(ctx, readonly, true); err != nil {
			t.Fatal("ordinary command after named denial", err)
		}
	})
	t.Run("mixed_shared_session_calls", func(t *testing.T) {
		var wg sync.WaitGroup
		type answer struct {
			result cephmsgr.Result
			err    error
			tell   bool
		}
		answers := make(chan answer, 3)
		for i, call := range []func(context.Context, cephmsgr.Command) (cephmsgr.Result, error){c.MgrCommand, c.MgrTell, func(ctx context.Context, cmd cephmsgr.Command) (cephmsgr.Result, error) {
			return c.MgrTellTo(ctx, name, cmd)
		}} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				command := version
				if i == 0 {
					command = cephmsgr.Command{JSON: []byte(`{"prefix":"pg stat","format":"json"}`)}
				}
				result, err := call(ctx, command)
				answers <- answer{result, err, i != 0}
			}()
		}
		wg.Wait()
		for range 3 {
			got := <-answers
			record("mixed-command-result", name, got.result, got.err, c.Snapshot())
			if got.err != nil || got.result.Code != 0 || !json.Valid(got.result.Data) {
				t.Fatal("mixed named/ordinary MGR result", got.result, got.err)
			}
			if got.tell {
				var v tellVersion
				if json.Unmarshal(got.result.Data, &v) != nil || v != before.Version {
					t.Fatal("mixed named tell/native result", v)
				}
			}
		}
	})
	t.Run("ticket_renewal_keeps_selected_daemon", func(t *testing.T) {
		baseline := c.Snapshot()
		record("before-ticket-renewal", name, cephmsgr.Result{}, nil, baseline)
		renewed := waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
			return s.AuthTicket.Expires.After(baseline.AuthTicket.Expires) && s.MgrTicket.Expires.After(baseline.MgrTicket.Expires) && s.Monitor.Ready
		})
		record("after-ticket-renewal", name, cephmsgr.Result{}, nil, renewed)
		if renewed.GlobalID != baseline.GlobalID || renewed.Manager.Name != name || renewed.Manager.GlobalID != baseline.Manager.GlobalID {
			t.Fatal("ticket renewal changed selected identity", baseline, renewed)
		}
		check(t, name, before.Version)
	})
	t.Run("failover_requires_explicit_new_name", func(t *testing.T) {
		mutation, _ := json.Marshal(map[string]string{"prefix": "mgr fail", "who": name})
		if result, err := c.MonCommand(ctx, cephmsgr.Command{JSON: mutation}); err != nil || result.Code != 0 {
			t.Fatal("once-only MGR fail", result, err)
		}
		waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
			return s.Manager.Available && s.Manager.Name != name && s.Manager.GlobalID != before.After.GID
		})
		after := namedMgrNativeObserve(t, ctx, control, "after")
		if after.After.Name == name || after.After.GID == before.After.GID {
			t.Fatal("native map did not change named MGR", after)
		}
		waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
			return s.Manager.MapEpoch >= after.After.Epoch && s.Manager.Name == after.After.Name && s.Manager.GlobalID == after.After.GID
		})
		_, err := c.MgrTellTo(ctx, name, version)
		record("old-name-after-failover", name, cephmsgr.Result{}, err, c.Snapshot())
		var unknown *cephmsgr.OutcomeUnknownError
		if (!errors.Is(err, cephmsgr.ErrManagerNotFound) && !errors.Is(err, cephmsgr.ErrManagerNotActive)) || errors.As(err, &unknown) {
			t.Fatal("old named target substituted replacement", err)
		}
		check(t, after.After.Name, after.Version)
		if err := fixtureRecoveryRead(ctx, c, false); err != nil {
			t.Fatal(err)
		}
		if err := fixtureRecoveryRead(ctx, c, true); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("cancel_before_send_and_close", func(t *testing.T) {
		before, stop := context.WithCancel(ctx)
		stop()
		if _, err := c.MgrTellTo(before, name, version); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		record("client-closed", name, cephmsgr.Result{}, nil, c.Snapshot())
		if _, err := c.MgrTellTo(ctx, name, version); !errors.Is(err, cephmsgr.ErrClosed) {
			t.Fatal(err)
		}
	})
}
