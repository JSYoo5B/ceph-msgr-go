package integration_test

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestCephMonitorMembersIntegration(t *testing.T) {
	control := ordinaryLogFixture(t)
	data, err := os.ReadFile(filepath.Join(control, "named-mon-b.json"))
	var oracle struct {
		Map struct {
			FSID  string `json:"fsid"`
			Epoch uint32 `json:"epoch"`
			Mons  []struct {
				Name string `json:"name"`
				Rank uint32 `json:"rank"`
			} `json:"mons"`
		} `json:"monmap"`
	}
	if err != nil || len(data) > 64<<10 || json.Unmarshal(data, &oracle) != nil || oracle.Map.FSID == "" || oracle.Map.Epoch == 0 || len(oracle.Map.Mons) != 3 {
		t.Fatal("independent native MON membership oracle", err)
	}
	expected := make([]cephmsgr.MonitorMember, len(oracle.Map.Mons))
	for i, member := range oracle.Map.Mons {
		if member.Name == "" || member.Rank != uint32(i) {
			t.Fatal("native fixture did not provide rank-ordered MON names", oracle.Map.Mons)
		}
		expected[i] = cephmsgr.MonitorMember{Name: member.Name, Rank: member.Rank}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	options := integrationOptions(t)
	options.Monitors = options.Monitors[:1]
	dial := options.DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	var dials atomic.Uint32
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		dials.Add(1)
		return dial(ctx, network, endpoint)
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	initial := c.Snapshot()
	if !initial.Monitor.Ready || initial.Manager.Ready || initial.FSID != oracle.Map.FSID || initial.Monitor.MapEpoch < oracle.Map.Epoch || !reflect.DeepEqual(initial.Monitor.Members, expected) {
		t.Fatal("admitted MON members disagree with the independent native map", initial)
	}
	before := dials.Load()
	for range 100 {
		state := c.Snapshot()
		if !reflect.DeepEqual(state.Monitor.Members, expected) || state.Manager.Ready {
			t.Fatal("local membership snapshot lost its map or opened a lazy MGR", state)
		}
		state.Monitor.Members[0] = cephmsgr.MonitorMember{Name: "caller-owned", Rank: 99}
		state.Monitor.Members[1].Name = "changed"
		state.Monitor.Members[2].Rank = 42
	}
	if dials.Load() != before || !reflect.DeepEqual(c.Snapshot().Monitor.Members, expected) {
		t.Fatal("snapshot mutation affected the client or membership discovery dialed")
	}
	// Select the target directly from the public snapshot, without asking the
	// Go client to execute or parse a separate MON command for its membership.
	member := c.Snapshot().Monitor.Members[1]
	result, err := c.MonTellTo(ctx, member.Name, cephmsgr.Command{JSON: []byte(`{"prefix":"mon_status","format":"json"}`)})
	var status namedMonStatus
	if err != nil || json.Unmarshal(result.Data, &status) != nil || result.Code != 0 || status.Name != member.Name || status.Rank != member.Rank || status.Map.FSID != initial.FSID {
		t.Fatal("snapshot-selected named MON disagrees with native membership", member, result.Code, err, status)
	}
	renewed := waitClientState(t, c, ctx, func(state cephmsgr.State) bool {
		return state.Monitor.Ready && state.AuthTicket.Expires.After(initial.AuthTicket.Expires) && state.MgrTicket.Expires.After(initial.MgrTicket.Expires)
	})
	if !reflect.DeepEqual(renewed.Monitor.Members, expected) || renewed.GlobalID != initial.GlobalID || renewed.Manager.Ready || renewed.AuthRejection != nil {
		t.Fatal("renewal or private named Tell changed public MON membership", renewed)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	final := c.Snapshot()
	if !final.Closed || final.Monitor.Ready || final.FSID != initial.FSID || final.GlobalID != initial.GlobalID || !reflect.DeepEqual(final.Monitor.Members, expected) {
		t.Fatal("Close lost last-known authenticated MON membership", final)
	}
	t.Log("native rank/name oracle, local mutation-isolated member discovery, snapshot-selected named Tell, ticket renewal, lazy MGR preservation and Close passed")
}
