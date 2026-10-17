package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// OSD sessions authenticate with the OSD service ticket and do not pass
// through the MON. While the client's MON is the stopped leader and the
// quorum re-elects, object writes on an open OSD connection keep completing;
// the client's MON session recovers on its own, and native bytes match every
// acknowledged write.
func TestCephOSDIOAcrossMonLeaderStopIntegration(t *testing.T) {
	fixture, request := osdFixture, osdObjectRequest
	if isReplicatedPGFixtureProfile() {
		fixture, request = replicatedPGFixture, replicatedPGRequest
	}
	admin, _, base, _ := fixture(t)
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	requireUsableFixture(t, control)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	prepared := false
	t.Cleanup(func() {
		if !prepared {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 60*time.Second)
		defer stop()
		if result := nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}); !result.Cleanup.Removed {
			t.Error("owned object was not removed", result.Cleanup)
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("mon-outage")}, Reserve: 1})
	prepared = true
	name := owned.Objects[1]
	route := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement

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
	if err != nil || json.Unmarshal(quorum.Data, &status) != nil || len(status.Map.Mons) < 3 {
		t.Skip("needs a quorum of at least three monitors", err)
	}
	var leaderSeed string
	for _, m := range status.Map.Mons {
		for _, a := range m.Addrs.Vec {
			if m.Name == status.Leader && a.Type == "v2" {
				for _, seed := range integrationOptions(t).Monitors {
					if strings.Contains(seed, a.Addr) {
						leaderSeed = seed
					}
				}
			}
		}
	}
	if leaderSeed == "" {
		t.Fatal("leader seed address not found", status.Leader)
	}
	// The client's MON session is on the leader, so stopping it takes that
	// session down while the OSD connection stays open.
	options := integrationOptions(t)
	options.Monitors = []string{leaderSeed}
	options.EnableOSD = true
	if isReplicatedPGFixtureProfile() {
		options.OSDProfile = replicatedPGFixtureOSDProfile(t)
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	o, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: route.ID, Address: route.Address})
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	monState := admin.Snapshot().Monitor
	before := c.Snapshot().Monitor.Endpoint

	var expected []byte
	var writes atomic.Int64
	stopWrites := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stopWrites:
				writerDone <- nil
				return
			default:
			}
			b := []byte{byte(i), byte(i >> 8), 'w'}
			r := request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(b)), Data: b})
			r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700005000}
			call, done := context.WithTimeout(ctx, 10*time.Second)
			_, err := o.Request(call, r)
			done()
			if err != nil {
				writerDone <- err
				return
			}
			expected = append(expected, b...)
			writes.Add(1)
			time.Sleep(20 * time.Millisecond)
		}
	}()
	time.Sleep(2 * time.Second)
	if err := testcluster.ControlDaemon(ctx, control, "stop", "mon", status.Leader); err != nil {
		t.Fatal("stop leader", err)
	}
	stopped := true
	defer func() {
		if !stopped {
			return
		}
		restore, stop := context.WithTimeout(context.Background(), 180*time.Second)
		defer stop()
		if err := testcluster.ControlDaemon(restore, control, "start", "mon", status.Leader); err != nil {
			markFixtureUnusable(t, control, "stopped leader MON did not restart")
			return
		}
		if _, err := settleNativeMonitorElection(restore, admin, control, "restarted", monState, monState.Members); err != nil {
			markFixtureUnusable(t, control, "election did not settle after the leader restart")
		}
	}()
	atStop := writes.Load()
	// The MON session recovers to another member while writes continue.
	recovered := waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
		return s.Monitor.Ready && s.Monitor.Endpoint != before
	})
	if _, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"status","format":"json"}`)}); err != nil {
		t.Fatal("MON command after recovery", err)
	}
	time.Sleep(3 * time.Second)
	duringOutage := writes.Load() - atStop
	if err := testcluster.ControlDaemon(ctx, control, "start", "mon", status.Leader); err != nil {
		t.Fatal("restart leader", err)
	}
	stopped = false
	waitNativeMonitorElection(t, ctx, admin, control, "restarted", monState, monState.Members)
	time.Sleep(time.Second)
	close(stopWrites)
	if err := <-writerDone; err != nil {
		t.Fatal("a write failed across the MON outage", err)
	}
	if duringOutage < 10 {
		t.Fatalf("too few writes while the leader was down: %d", duringOutage)
	}
	state := nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: name}).State
	if !bytes.Equal(state.Data, expected) {
		t.Fatalf("native bytes differ: %d vs %d expected", len(state.Data), len(expected))
	}
	t.Logf("%d writes, %d after the leader stopped; MON session %s -> %s; native bytes matched", writes.Load(), duringOutage, before, recovered.Monitor.Endpoint)
}
