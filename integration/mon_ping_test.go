package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

type monPingProbe struct{}

type monPingObservation struct {
	Health json.RawMessage `json:"health"`
	Status struct {
		Name  string `json:"name"`
		Rank  uint32 `json:"rank"`
		State string `json:"state"`
		Map   struct {
			FSID  string `json:"fsid"`
			Epoch uint32 `json:"epoch"`
			Mons  []struct {
				Name string `json:"name"`
				Rank uint32 `json:"rank"`
			} `json:"mons"`
		} `json:"monmap"`
	} `json:"mon_status"`
}

func TestCephMonPingIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MON_PING") != "1" {
		t.Skip("requires the explicit native MON ping fixture")
	}
	control := ordinaryLogFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	options := integrationOptions(t)
	options.Monitors = options.Monitors[:1]
	dial := options.DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	var mu sync.Mutex
	var peers []string
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if marked, ok := ctx.Value(monPingProbe{}).(string); ok {
			_, name, err := testcluster.SplitPeer(endpoint)
			if err != nil {
				return nil, err
			}
			mu.Lock()
			peers = append(peers, name)
			mu.Unlock()
			if marked == "fail" {
				return nil, io.EOF // A selected endpoint failure, before any send.
			}
		}
		return dial(ctx, network, endpoint)
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := fixtureRecoveryRead(ctx, c, true); err != nil {
		t.Fatal(err)
	}
	initial := c.Snapshot()
	if len(initial.Monitor.Members) != 3 || initial.GlobalID == 0 || !initial.Monitor.Ready || !initial.Manager.Ready {
		t.Fatal("expected admitted primary MON/MGR and three MON members", initial)
	}
	stream, err := c.WatchLogs(ctx, cephmsgr.LogOptions{StartVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stream.Close() })
	oracles := make(map[string]monPingObservation)
	decode := func(raw []byte) (monPingObservation, error) {
		var value monPingObservation
		if len(raw) == 0 || len(raw) > 1<<20 || json.Unmarshal(raw, &value) != nil || len(value.Health) == 0 || value.Status.Map.FSID != initial.FSID || value.Status.Map.Epoch == 0 || len(value.Status.Map.Mons) != 3 {
			return value, errors.New("ping response is missing its native health/identity metadata")
		}
		for i, member := range value.Status.Map.Mons {
			if member.Rank != uint32(i) || member.Name != initial.Monitor.Members[i].Name {
				return value, errors.New("ping membership differs from the admitted cluster")
			}
		}
		return value, nil
	}
	var observations []map[string]any
	check := func(t *testing.T, name string, raw []byte) {
		t.Helper()
		got, err := decode(raw)
		want := oracles[name]
		if err != nil || got.Status.Name != name || got.Status.Rank != want.Status.Rank || got.Status.Map.Epoch < want.Status.Map.Epoch || (got.Status.State != "leader" && got.Status.State != "peon") {
			t.Fatal("Go MPing disagrees with independent native ping", name, got, err)
		}
		observations = append(observations, map[string]any{"target": name, "raw": raw, "json": json.RawMessage(raw), "sha256": fmt.Sprintf("%x", sha256.Sum256(raw))})
	}
	if !t.Run("each_named_monitor_matches_native_ping", func(t *testing.T) {
		if len(initial.Monitor.Members) != 3 {
			t.Fatal("expected three admitted MONs", initial)
		}
		for _, member := range initial.Monitor.Members {
			raw, err := os.ReadFile(filepath.Join(control, "mon-ping-native-"+member.Name+".json"))
			if err != nil {
				t.Fatal(err)
			}
			oracle, err := decode(raw)
			if err != nil || oracle.Status.Name != member.Name || oracle.Status.Rank != member.Rank {
				t.Fatal("native ping target identity", member, oracle, err)
			}
			oracles[member.Name] = oracle
			raw, err = c.PingMonitor(context.WithValue(ctx, monPingProbe{}, "ok"), member.Name)
			if err != nil {
				t.Fatal("Go MPing", member.Name, err)
			}
			check(t, member.Name, raw)
		}
	}) {
		return
	}
	t.Run("parallel_pings_and_primary_commands", func(t *testing.T) {
		type response struct {
			name string
			raw  []byte
			err  error
		}
		results := make(chan response, 4)
		for _, name := range []string{"a", "b"} {
			go func() {
				raw, err := c.PingMonitor(context.WithValue(ctx, monPingProbe{}, "ok"), name)
				results <- response{name, raw, err}
			}()
		}
		for _, mgr := range []bool{false, true} {
			go func() {
				call, prefix, name := c.MonCommand, "fsid", "mon-command"
				if mgr {
					call, prefix, name = c.MgrCommand, "pg stat", "mgr-command"
				}
				r, err := call(ctx, cephmsgr.Command{JSON: json.RawMessage(fmt.Sprintf(`{"prefix":%q,"format":"json"}`, prefix))})
				if err == nil && (r.Code != 0 || !json.Valid(r.Data)) {
					err = fmt.Errorf("command result code %d", r.Code)
				}
				results <- response{name, r.Data, err}
			}()
		}
		for range cap(results) {
			select {
			case r := <-results:
				if r.err != nil {
					t.Fatal(r.name, r.err)
				}
				if r.name == "a" || r.name == "b" {
					check(t, r.name, r.raw)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	})
	t.Run("target_failure_has_no_fallback", func(t *testing.T) {
		mu.Lock()
		before := len(peers)
		mu.Unlock()
		_, err := c.PingMonitor(context.WithValue(ctx, monPingProbe{}, "fail"), "b")
		var unknown *cephmsgr.OutcomeUnknownError
		mu.Lock()
		attempted := append([]string(nil), peers[before:]...)
		mu.Unlock()
		if !errors.Is(err, io.EOF) || errors.As(err, &unknown) || !reflect.DeepEqual(attempted, []string{"mon.b"}) {
			t.Fatal("setup failure retried/fell back or became uncertain", attempted, err)
		}
		for _, name := range []string{"", "mon.a", "absent"} {
			if _, err := c.PingMonitor(ctx, name); !errors.Is(err, cephmsgr.ErrMonitorNotFound) {
				t.Fatal(name, err)
			}
		}
		cancelled, stop := context.WithCancel(ctx)
		stop()
		if _, err := c.PingMonitor(cancelled, "c"); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		raw, err := c.PingMonitor(context.WithValue(ctx, monPingProbe{}, "ok"), "c")
		if err != nil {
			t.Fatal("explicit next target", err)
		}
		check(t, "c", raw)
	})
	t.Run("command_restricted_identity_can_ping", func(t *testing.T) {
		restricted := integrationOptions(t)
		rawKey, err := os.ReadFile(filepath.Join(control, "configwatch.key"))
		if err != nil {
			t.Fatal(err)
		}
		restricted.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(rawKey)))
		if err != nil {
			t.Fatal(err)
		}
		restricted.Identity = "client.configwatch"
		other, err := cephmsgr.Dial(ctx, restricted)
		if err != nil {
			t.Fatal(err)
		}
		defer other.Close()
		raw, err := other.PingMonitor(ctx, "b")
		if err != nil {
			t.Fatal("ping unnecessarily required command capabilities", err)
		}
		check(t, "b", raw)
	})
	t.Run("primary_identity_and_subscription_survive", func(t *testing.T) {
		sentinel := fmt.Sprintf("ceph-msgr-ping-%d", time.Now().UnixNano())
		publishLogOnce(t, ctx, c, sentinel)
		waitLogEntry(t, ctx, stream, sentinel)
		after := c.Snapshot()
		if after.GlobalID != initial.GlobalID || after.FSID != initial.FSID || after.Monitor.Endpoint != initial.Monitor.Endpoint || after.Manager.Endpoint != initial.Manager.Endpoint || after.Manager.GlobalID != initial.Manager.GlobalID || !after.Monitor.Ready || !after.Manager.Ready || after.AuthRejection != nil {
			t.Fatal("ping changed primary sessions", after)
		}
	})
	if t.Failed() {
		return
	}
	mu.Lock()
	privatePeers := append([]string(nil), peers...)
	mu.Unlock()
	receipt := map[string]any{"source_commit": "7f793731f1b39eb4f465e960113d2363c311b964", "initial_global_id": initial.GlobalID, "fsid": initial.FSID, "observations": observations, "private_dials": privatePeers, "primary_global_id_preserved": c.Snapshot().GlobalID == initial.GlobalID, "subscription_survived": true}
	encoded, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(control, "mon-ping-results.json"), append(encoded, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
