package integration_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

type pinnedMonitorClient struct {
	*cephmsgr.Client
	Seed   string
	Leader bool
}

// Leader-election cases deliberately allow a registered request to outlive
// the ordinary 10 s retired-MON drain window. This existing caller option also
// bounds endpoint setup; it changes no product default or ticket lifetime.
const leaderStopMONDrainBudget = 60 * time.Second

func leaderStopMonitorOptions(options *cephmsgr.Options) {
	options.ConnectTimeout = leaderStopMONDrainBudget
}

func assertLeaderStopClientIdentity(t *testing.T, c *cephmsgr.Client, before cephmsgr.State, phase string) {
	t.Helper()
	after := c.Snapshot()
	t.Logf("%s: client GID %d -> %d, MON endpoint %s -> %s, ready=%t, AUTH expiry=%s, OSD expiry=%s, rejection=%v",
		phase, before.GlobalID, after.GlobalID, before.Monitor.Endpoint, after.Monitor.Endpoint, after.Monitor.Ready,
		after.AuthTicket.Expires.UTC().Format(time.RFC3339Nano), after.OSDTicket.Expires.UTC().Format(time.RFC3339Nano), after.AuthRejection)
	if before.GlobalID == 0 || after.GlobalID != before.GlobalID || after.FSID != before.FSID || after.AuthRejection != nil {
		t.Fatal("leader-election subject lost its original authenticated identity", phase)
	}
}

// pinnedMonitorClients dials one client per monitor seed, each seeded only
// with that monitor, and marks the quorum leader. It skips with fewer than
// two monitors. Clients close with the test.
func pinnedMonitorClients(t *testing.T, ctx context.Context, admin *cephmsgr.Client, configure ...func(*cephmsgr.Options)) []pinnedMonitorClient {
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
		for _, apply := range configure {
			apply(&options)
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
