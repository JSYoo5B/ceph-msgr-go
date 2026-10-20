package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// The destination primary performs COPY_FROM by reading the source through
// its own internal client. With the source's primary frozen (docker pause),
// that read cannot finish until the cluster marks the frozen OSD down and the
// source PG gets a new primary; the destination OSD then resends its read and
// completes the copy. The caller only waits: one request, one ONDISK reply,
// and the destination equals the source natively.
func TestCephOSDCopyFromSourceFailoverIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("source failover needs three BlueStore replicas")
	}
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	pools := createOwnedPools(t, ctx, base, 1, 2)
	var source, destination string
	var srcRoute, dstRoute osdFixtureOracle
	for i := 0; ; i++ {
		if i == 40 {
			t.Skip("no source/destination pair with different primaries")
		}
		source, destination = fmt.Sprintf("copy-failover-src-%d", i), fmt.Sprintf("copy-failover-dst-%d", i)
		srcRoute, dstRoute = poolPlace(t, ctx, 1, source, ""), poolPlace(t, ctx, 2, destination, "")
		if srcRoute.ID != dstRoute.ID {
			break
		}
	}
	data := bytes.Repeat([]byte{0, 0xff, 'c', 'f'}, 16384)
	attrs := []nativeXattrEntry{{Name: []byte("copy-failover"), Value: []byte{1, 2, 3}}}
	seeded := poolSeed(t, ctx, 1, source, "", data, attrs)
	o := mutationConnection(t, c, ctx, dstRoute)
	defer o.Close()

	frozen := strconv.Itoa(int(srcRoute.ID))
	if err := testcluster.ControlDaemon(ctx, dir, "pause", "osd", frozen); err != nil {
		t.Fatal("pause source primary", err)
	}
	paused := true
	defer func() {
		// A daemon frozen past its heartbeat grace is restarted rather than
		// thawed in place: resume, SIGKILL, start, then wait for clean PGs.
		restore, stop := context.WithTimeout(context.Background(), 240*time.Second)
		defer stop()
		if paused {
			if err := testcluster.ControlDaemon(restore, dir, "resume", "osd", frozen); err != nil {
				t.Error("resume", err)
				return
			}
		}
		for _, action := range []string{"kill", "start"} {
			if err := testcluster.ControlDaemon(restore, dir, action, "osd", frozen); err != nil {
				t.Error(action, err)
				return
			}
		}
		waitOSDFixtureClean(t, restore, base)
		fresh := nativeMutation(t, restore, map[string]any{"compare": false, "placement": true}).Placement
		publishReplicaOSDRouting(t, dir, adoptMutationPlacement(t, base, fresh))
	}()
	started := time.Now()
	r := pools.request(dstRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDCopyFrom2, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: pools.ids[1], SourceName: source}})
	r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700006000}
	call, stop := context.WithTimeout(ctx, 150*time.Second)
	reply, err := o.Request(call, r)
	stop()
	elapsed := time.Since(started)
	if err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("copy while the source primary was frozen", elapsed, err)
	}
	moved := poolPlace(t, ctx, 1, source, "")
	if err := testcluster.ControlDaemon(ctx, dir, "resume", "osd", frozen); err != nil {
		t.Fatal("resume", err)
	}
	paused = false
	after := poolObserve(t, ctx, 2, destination, "", "")
	if after.Code != 0 || !bytes.Equal(after.Data, data) || !reflect.DeepEqual(attrNames(after), attrNames(seeded)) || reply.UserVersion != after.Version {
		t.Fatalf("destination differs from the source: code=%d %d bytes", after.Code, len(after.Data))
	}
	t.Logf("copy completed in %s with osd.%s frozen; source primary osd.%d -> osd.%d (epoch %d -> %d), destination primary osd.%d", elapsed.Round(100*time.Millisecond), frozen, srcRoute.ID, moved.ID, srcRoute.Epoch, moved.Epoch, dstRoute.ID)
	if elapsed < 5*time.Second || moved.ID == srcRoute.ID {
		t.Fatalf("the copy did not wait for a source failover (%s, primary osd.%d)", elapsed, moved.ID)
	}
}

// The destination primary is SIGKILLed while it performs a COPY_FROM: the
// source primary is frozen first, so the copy is still waiting on its source
// read when the destination daemon dies. The caller gets an unknown outcome
// and nothing is resent. After both OSDs are back and the PGs are clean the
// destination is either absent or an exact copy, never partial, and it does
// not change afterwards.
func TestCephOSDCopyFromDestinationCrashIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("destination crash needs three BlueStore replicas")
	}
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 360*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	pools := createOwnedPools(t, ctx, base, 1, 2)
	var source, destination string
	var srcRoute, dstRoute osdFixtureOracle
	for i := 0; ; i++ {
		if i == 40 {
			t.Skip("no source/destination pair with different primaries")
		}
		source, destination = fmt.Sprintf("copy-crash-src-%d", i), fmt.Sprintf("copy-crash-dst-%d", i)
		srcRoute, dstRoute = poolPlace(t, ctx, 1, source, ""), poolPlace(t, ctx, 2, destination, "")
		if srcRoute.ID != dstRoute.ID {
			break
		}
	}
	data := bytes.Repeat([]byte{0, 0xff, 'd', 'c'}, 16384)
	attrs := []nativeXattrEntry{{Name: []byte("copy-crash"), Value: []byte{4, 5, 6}}}
	seeded := poolSeed(t, ctx, 1, source, "", data, attrs)
	o := mutationConnection(t, c, ctx, dstRoute)
	defer o.Close()

	frozen, killed := strconv.Itoa(int(srcRoute.ID)), strconv.Itoa(int(dstRoute.ID))
	if err := testcluster.ControlDaemon(ctx, dir, "pause", "osd", frozen); err != nil {
		t.Fatal("pause source primary", err)
	}
	paused, down := true, false
	restored := false
	restore := func() {
		if restored {
			return
		}
		restored = true
		recover, stop := context.WithTimeout(context.Background(), 240*time.Second)
		defer stop()
		if paused {
			if err := testcluster.ControlDaemon(recover, dir, "resume", "osd", frozen); err != nil {
				t.Error("resume", err)
				return
			}
			paused = false
		}
		// The frozen daemon is restarted rather than thawed in place (see the
		// source failover test); the killed one is started again.
		for _, action := range []string{"kill", "start"} {
			if err := testcluster.ControlDaemon(recover, dir, action, "osd", frozen); err != nil {
				t.Error(action, err)
				return
			}
		}
		if down {
			if err := testcluster.ControlDaemon(recover, dir, "start", "osd", killed); err != nil {
				t.Error("start killed destination primary", err)
				return
			}
			down = false
		}
		waitOSDFixtureClean(t, recover, base)
		fresh := nativeMutation(t, recover, map[string]any{"compare": false, "placement": true}).Placement
		publishReplicaOSDRouting(t, dir, adoptMutationPlacement(t, base, fresh))
	}
	defer restore()

	r := pools.request(dstRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDCopyFrom2, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: pools.ids[1], SourceName: source}})
	r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700006100}
	type outcome struct {
		reply cephmsgr.OSDReply
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		call, stop := context.WithTimeout(ctx, 150*time.Second)
		defer stop()
		reply, err := o.Request(call, r)
		done <- outcome{reply, err}
	}()
	select {
	case result := <-done:
		t.Fatal("the copy finished while its source primary was frozen", result.reply.Code, result.err)
	case <-time.After(3 * time.Second):
	}
	if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", killed); err != nil {
		t.Fatal("kill destination primary", err)
	}
	down = true
	var crash struct {
		ExitCode *int `json:"exit_code"`
	}
	if data, err := os.ReadFile(filepath.Join(dir, "osd-crash-state.json")); err != nil || json.Unmarshal(data, &crash) != nil || crash.ExitCode == nil || *crash.ExitCode != 137 {
		t.Fatal("expected SIGKILL evidence", err)
	}
	result := <-done
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(result.err, &unknown) {
		t.Fatal("a copy whose destination primary died must be an unknown outcome", result.reply.Code, result.err)
	}
	t.Logf("copy ended unknown after osd.%s was killed: %v (transaction %d, epoch %d)", killed, result.err, result.reply.Submission.Transaction, result.reply.Submission.MapEpoch)

	restore()
	after := poolObserve(t, ctx, 2, destination, "", "")
	copied := after.Code == 0
	if copied && (!bytes.Equal(after.Data, data) || !reflect.DeepEqual(attrNames(after), attrNames(seeded))) {
		t.Fatalf("partial destination after the crash: %d bytes, attrs %v", len(after.Data), attrNames(after))
	}
	if !copied && after.Code != -2 {
		t.Fatal("destination observation", after.Code)
	}
	time.Sleep(5 * time.Second)
	again := poolObserve(t, ctx, 2, destination, "", "")
	if again.Code != after.Code || again.Version != after.Version {
		t.Fatalf("destination changed without a request: code %d -> %d, version %d -> %d", after.Code, again.Code, after.Version, again.Version)
	}
	t.Logf("destination after recovery: copied=%t (code %d, version %d); source primary osd.%s, destination primary osd.%s", copied, after.Code, after.Version, frozen, killed)
}
