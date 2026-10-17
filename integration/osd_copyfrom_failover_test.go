package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
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
