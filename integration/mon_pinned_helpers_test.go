package integration_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

type pinnedMonitorClient struct {
	*cephmsgr.Client
	Seed   string
	Leader bool
}

// pinnedMonitorClients dials one client per monitor seed, each seeded only
// with that monitor, and marks the quorum leader. It skips with fewer than
// two monitors. Clients close with the test.
func pinnedMonitorClients(t *testing.T, ctx context.Context, admin *cephmsgr.Client) []pinnedMonitorClient {
	t.Helper()
	quorum, err := admin.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"quorum_status","format":"json"}`)})
	var status struct {
		Leader string `json:"quorum_leader_name"`
		Map    struct {
			Mons []struct {
				Name  string `json:"name"`
				Addrs struct {
					Vec []struct{ Type, Addr string } `json:"addrvec"`
				} `json:"public_addrs"`
			} `json:"mons"`
		} `json:"monmap"`
	}
	if err != nil || json.Unmarshal(quorum.Data, &status) != nil || status.Leader == "" {
		t.Fatal("quorum status", err)
	}
	var leaderAddr string
	for _, m := range status.Map.Mons {
		for _, a := range m.Addrs.Vec {
			if m.Name == status.Leader && a.Type == "v2" {
				leaderAddr = a.Addr
			}
		}
	}
	seeds := integrationOptions(t).Monitors
	if len(seeds) < 2 || leaderAddr == "" {
		t.Skip("needs at least two monitors and a known leader address")
	}
	var clients []pinnedMonitorClient
	for _, seed := range seeds {
		options := integrationOptions(t)
		options.Monitors = []string{seed}
		options.EnableOSD = true
		if isReplicatedPGFixtureProfile() {
			options.OSDProfile = replicatedPGFixtureOSDProfile(t)
		}
		c, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		clients = append(clients, pinnedMonitorClient{c, seed, strings.Contains(c.Snapshot().Monitor.Endpoint, leaderAddr)})
	}
	return clients
}
