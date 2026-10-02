package cephmsgr

import (
	"encoding/json"
	"net"
	"os"
	"slices"
	"testing"
	"time"
)

func TestCephSnapshotIntegration(t *testing.T) {
	c, ctx := fixtureClient(t, time.Minute)
	initial := waitClientState(t, c, ctx, func(s State) bool { return s.Manager.Available })
	if initial.Closed || !initial.Monitor.Ready || initial.Manager.Ready || initial.GlobalID == 0 || initial.AuthRejection != nil {
		t.Fatal("incorrect initial public state", initial)
	}
	for i := 0; i < 10; i++ {
		if c.Snapshot().Manager.Ready {
			t.Fatal("local snapshot opened a lazy MGR connection")
		}
	}
	observer, _ := fixtureClient(t, time.Minute)
	result, err := observer.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status","format":"json"}`)})
	var status struct {
		FSID string `json:"fsid"`
	}
	if err != nil || json.Unmarshal(result.Data, &status) != nil || status.FSID != initial.FSID {
		t.Fatal("independent status disagrees with snapshot FSID", err, initial.FSID)
	}
	checkManagerMap := func() State {
		t.Helper()
		result, err := observer.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"mgr dump","format":"json"}`)})
		var server struct {
			Epoch     uint32 `json:"epoch"`
			GlobalID  uint64 `json:"active_gid"`
			Name      string `json:"active_name"`
			Available bool   `json:"available"`
		}
		if err != nil || json.Unmarshal(result.Data, &server) != nil || server.GlobalID == 0 || server.Epoch == 0 {
			t.Fatal("independent MgrMap query", err)
		}
		return waitClientState(t, c, ctx, func(s State) bool {
			return s.Manager.MapEpoch >= server.Epoch && s.Manager.GlobalID == server.GlobalID && s.Manager.Name == server.Name && s.Manager.Available == server.Available
		})
	}
	checkManagerMap()
	query := Command{JSON: []byte(`{"prefix":"pg stat","format":"json"}`)}
	if _, err := c.MgrCommand(ctx, query); err != nil {
		t.Fatal(err)
	}
	state := c.Snapshot()
	if !state.Manager.Ready || !slices.Contains(state.Manager.Endpoints, state.Manager.Endpoint) || !slices.Contains(state.Monitor.Endpoints, state.Monitor.Endpoint) {
		t.Fatal("ready session endpoints disagree with authenticated maps", state)
	}
	state.Monitor.Endpoints[0], state.Manager.Endpoints[0] = "caller-owned", "caller-owned"
	state = c.Snapshot()
	if slices.Contains(state.Monitor.Endpoints, "caller-owned") || slices.Contains(state.Manager.Endpoints, "caller-owned") {
		t.Fatal("snapshot mutation affected the real client")
	}
	renewed := waitClientState(t, c, ctx, func(s State) bool {
		return s.AuthTicket.Expires.After(initial.AuthTicket.Expires) && s.MgrTicket.Expires.After(initial.MgrTicket.Expires)
	})
	if renewed.GlobalID != initial.GlobalID || renewed.AuthRejection != nil || !renewed.Monitor.Ready {
		t.Fatal("genuine ticket renewal lost authenticated identity", renewed)
	}
	_, port, _ := net.SplitHostPort(renewed.Monitor.Endpoint)
	name := map[string]string{"33300": "a", "33301": "b", "33302": "c"}[port]
	if name == "" {
		t.Fatal("unrecognized fixture MON", renewed.Monitor.Endpoint)
	}
	if err := restartFixtureMonitor(ctx, os.Getenv("CEPH_MSGR_CONTROL_DIR"), name); err != nil {
		t.Fatal(err)
	}
	waitClientState(t, c, ctx, func(s State) bool {
		return s.Monitor.Ready && s.AuthTicket.Expires.After(renewed.AuthTicket.Expires)
	})
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status","format":"json"}`)}); err != nil {
		t.Fatal("MON query after real restart", err)
	}
	state = c.Snapshot()
	if !state.Monitor.Ready || state.AuthRejection != nil || state.GlobalID != initial.GlobalID {
		t.Fatal("MON recovery state lost admission or identity", state)
	}
	oldManager := state.Manager
	encoded, _ := json.Marshal(map[string]string{"prefix": "mgr fail", "who": oldManager.Name})
	if _, err := observer.MonCommand(ctx, Command{JSON: encoded}); err != nil {
		t.Fatal(err)
	}
	state = waitClientState(t, c, ctx, func(s State) bool {
		return s.Manager.Available && s.Manager.GlobalID != oldManager.GlobalID
	})
	if state.Manager.Ready || state.Manager.MapEpoch <= oldManager.MapEpoch {
		t.Fatal("newly advertised MGR was reported as an established session", state)
	}
	checkManagerMap()
	if _, err := c.MgrCommand(ctx, query); err != nil {
		t.Fatal("new MGR query", err)
	}
	if !c.Snapshot().Manager.Ready {
		t.Fatal("new authenticated MGR session was not ready")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	final := c.Snapshot()
	if !final.Closed || final.Monitor.Ready || final.Manager.Ready || final.FSID != initial.FSID || final.GlobalID != initial.GlobalID || final.Manager.GlobalID != state.Manager.GlobalID {
		t.Fatal("closed public state lost known metadata or retained readiness", final)
	}
	t.Logf("public snapshots matched independent FSID/MgrMap queries; renewal, MON restart, MGR takeover, lazy setup and Close preserved state; client-id=%d MGR-id=%d", final.GlobalID, final.Manager.GlobalID)
}
