package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// A committed WRITESAME reply survives SIGKILL of the object's primary: the two
// remaining BlueStore replicas hold identical bytes, version and mtime, and a
// caller-targeted read on the replacement primary sees them without any replay.
func TestCephOSDWriteSameFailoverIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("requires three sparse-file BlueStore replicas")
	}
	// The shared restoration helper is legacy-v6 only, so no PG profile runs here.
	request := osdObjectRequest
	c, _, base, original := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	baseline := bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7, 8}, 4)
	attrs := []nativeXattrEntry{{Name: []byte("failover-attr"), Value: []byte{0xff, 0, 'F'}}}
	prepared := false
	t.Cleanup(func() {
		if !prepared {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
		defer stop()
		if result := nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}); !result.Cleanup.Removed {
			t.Error("owned writesame object was not removed", result.Cleanup)
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline, Attributes: attrs}})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 1 {
		t.Fatal("native writesame seed differs", owned.SeedCode, owned.Objects)
	}
	subject := owned.Objects[0]
	route := func() osdFixtureOracle {
		r := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": subject, "namespace": ""}).Placement
		if r.Name != subject || r.Pool != base.Pool || r.ID < 0 || r.Address == "" {
			t.Fatal("native subject placement differs", r)
		}
		return r
	}
	target := route()
	if len(target.Acting) != 3 {
		t.Fatalf("expected a healthy three-replica placement: %+v", target)
	}
	primary := target.ID
	name := strconv.FormatInt(int64(primary), 10)
	stopped := false
	defer func() { recoverMutationFixture(t, c, base, original, name, stopped) }()

	o := mutationConnection(t, c, ctx, target)
	defer o.Close()
	pattern := []byte{0, 255, 'x'}
	op := cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteSame, Offset: 2, Length: 12, Data: pattern}
	req := request(target, op)
	req.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000400, Nanoseconds: 141421356}
	reply, err := o.Request(ctx, req)
	if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("ONDISK writesame before primary termination", err)
	}
	want := bytes.Clone(baseline)
	for i := 0; i < 12; i++ {
		want[2+i] = pattern[i%len(pattern)]
	}
	observe := func() omapWriteState {
		return nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: subject}).State
	}
	before := observe()
	if before.Code != 0 || !bytes.Equal(before.Data, want) || before.Version != reply.UserVersion || before.Seconds != int64(req.ModificationTime.Seconds) || before.Nanos != int64(req.ModificationTime.Nanoseconds) {
		t.Fatalf("native bytes/version/mtime before termination differ: %x version=%d", before.Data, before.Version)
	}
	if err = testcluster.ControlDaemon(ctx, dir, "kill", "osd", name); err != nil {
		t.Fatal("force primary termination", err)
	}
	stopped = true
	if err = o.Close(); err != nil {
		t.Fatal("close killed primary connection", err)
	}
	crashData, err := os.ReadFile(filepath.Join(dir, "osd-crash-state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var crash struct {
		Daemon    string `json:"daemon"`
		ExitCode  *int   `json:"exit_code"`
		OOMKilled *bool  `json:"oom_killed"`
	}
	if err = json.Unmarshal(crashData, &crash); err != nil {
		t.Fatal(err)
	}
	if crash.Daemon != "osd."+name || crash.ExitCode == nil || *crash.ExitCode != 137 || crash.OOMKilled == nil || *crash.OOMKilled {
		t.Fatalf("expected primary SIGKILL without OOM: %+v", crash)
	}
	after := observe()
	if after.Code != 0 || after.Version != before.Version || after.Seconds != before.Seconds || after.Nanos != before.Nanos || !bytes.Equal(after.Data, want) || !reflect.DeepEqual(after.Attributes, attrs) {
		t.Fatalf("surviving replicas changed the committed writesame: %x version=%d", after.Data, after.Version)
	}
	fresh := route()
	if fresh.Epoch <= target.Epoch || fresh.ID == primary || len(fresh.Acting) != 2 {
		t.Fatalf("expected a refreshed two-replica placement without the killed primary: %+v", fresh)
	}
	o = mutationConnection(t, c, ctx, fresh)
	defer o.Close()
	read, err := o.Request(ctx, request(fresh, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(want) + 8)}))
	if err != nil || read.Transaction <= reply.Transaction || read.UserVersion != before.Version || !bytes.Equal(read.Operations[0].Data, want) {
		t.Fatal("replacement primary lost committed bytes/version or identity", err)
	}
	if again := observe(); again.Version != before.Version || !bytes.Equal(again.Data, want) {
		t.Fatal("recovery read changed native state; the writesame must not be replayed")
	}
	t.Log("ONDISK WRITESAME survived primary SIGKILL with two BlueStore replicas; native and caller-targeted Go reads matched bytes, version and mtime without a replay")
}
