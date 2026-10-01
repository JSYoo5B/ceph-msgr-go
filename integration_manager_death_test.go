package cephmsgr

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
)

func TestCephNaturalManagerFailoverIntegration(t *testing.T) {
	c, ctx := fixtureClient(t, 90*time.Second)
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	command := Command{JSON: []byte(`{"prefix":"pg stat","format":"json"}`)}
	if _, err := c.MgrCommand(ctx, command); err != nil {
		t.Fatal("establish MGR", err)
	}
	c.mu.Lock()
	old, name, id := c.mgr, c.mgrMap.Name, c.mgrMap.GlobalID
	c.mu.Unlock()
	ticket := c.snapshotAuth().Tickets[cephx.ServiceAuth]
	started := time.Now()
	if err := controlFixtureDaemon(ctx, control, "stop", "mgr", name); err != nil {
		t.Fatal("stop active MGR process", err)
	}
	defer func() {
		restart, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := controlFixtureDaemon(restart, control, "start", "mgr", name); err != nil {
			t.Error("restart stopped fixture MGR", err)
			return
		}
		// Confirm that the replacement process authenticates and rejoins as
		// standby before later fixture tests run. No mgr fail command is used.
		for {
			result, err := c.MonCommand(restart, Command{JSON: []byte(`{"prefix":"mgr dump","format":"json"}`)})
			if err != nil {
				t.Error("read MGR state after restart", err)
				return
			}
			var state struct {
				Standbys []struct {
					Name string `json:"name"`
					ID   uint64 `json:"gid"`
				} `json:"standbys"`
			}
			if err := json.Unmarshal(result.Data, &state); err != nil {
				t.Error("decode restarted MGR state", err)
				return
			}
			for _, standby := range state.Standbys {
				if standby.Name == name && standby.ID != 0 && standby.ID != id {
					return
				}
			}
			select {
			case <-restart.Done():
				t.Error("restarted MGR did not rejoin as a new standby", restart.Err())
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	select {
	case <-old.Done():
	case <-ctx.Done():
		t.Fatal("terminated MGR left the old session open", ctx.Err())
	}
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status","format":"json"}`)}); err != nil {
		t.Fatal("MON unavailable while waiting for automatic MGR takeover", err)
	}
	// Keep Ceph's default beacon grace. Promotion must be a daemon/monitor
	// decision, observed through the authenticated MgrMap subscription.
	for {
		c.mu.Lock()
		changed := c.mgrMap.Available && c.mgrMap.GlobalID != id && c.mgrMap.Name != name
		signal := c.changed
		c.mu.Unlock()
		if changed {
			break
		}
		select {
		case <-signal:
		case <-ctx.Done():
			t.Fatal("Ceph did not automatically promote standby", ctx.Err())
		}
	}
	result, err := c.MgrCommand(ctx, command)
	if err != nil || !json.Valid(result.Data) {
		t.Fatal("command on automatically promoted MGR", err)
	}
	if !c.snapshotAuth().Tickets[cephx.ServiceAuth].Expires.After(ticket.Expires) {
		t.Fatal("client did not renew authentication while waiting for daemon takeover")
	}
	c.mu.Lock()
	fresh, active := c.mgr, c.mgrMap.Name
	c.mu.Unlock()
	if fresh == old {
		t.Fatal("terminated MGR session was reused")
	}
	t.Logf("terminated MGR=%s automatically replaced by %s in %s; renewed authentication and MGR command passed", name, active, time.Since(started).Round(time.Millisecond))
}
