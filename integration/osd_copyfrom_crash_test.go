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

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// An ONDISK COPY_FROM2 reply means the copied bytes, xattrs and OMAP are
// committed: they survive SIGKILL and restart of a BlueStore OSD. This covers
// daemon termination only, not host power loss or device durability.
func TestCephOSDCopyFromCrashIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("forced daemon termination persistence requires the sparse-file BlueStore fixture")
	}
	c, _, base, original := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	baseline := []byte{0, 0xff, 'c', 'r', 'a', 's', 'h', 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	attrs := []nativeXattrEntry{{Name: []byte("crash-attr"), Value: []byte{0xff, 0, 'C'}}}
	model := map[string][]byte{"k1": []byte("v1"), "\x00k2": {0xff}}
	inventory := [][]byte{[]byte(""), []byte("k1"), []byte("\x00k2")}
	prepared := false
	t.Cleanup(func() {
		if !prepared {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
		defer stop()
		if result := nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}); !result.Cleanup.Removed {
			t.Error("owned copy-from objects were not removed", result.Cleanup)
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline, Attributes: attrs, Entries: omapWriteEntries(model)}, Reserve: 1})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 2 {
		t.Fatal("native copy source seed differs", owned.SeedCode, owned.Objects)
	}
	src, dst := owned.Objects[0], owned.Objects[1]
	name := strconv.FormatInt(int64(base.ID), 10)
	stopped := false
	defer func() { recoverMutationFixture(t, c, base, original, name, stopped) }()

	route := func() osdFixtureOracle {
		r := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": dst, "namespace": ""}).Placement
		if r.Name != dst || r.Pool != base.Pool || r.ID < 0 || r.Address == "" {
			t.Fatal("native destination placement differs", r)
		}
		return r
	}
	target := route()
	o := mutationConnection(t, c, ctx, target)
	defer o.Close()
	request := osdObjectRequest(target, cephmsgr.OSDOperation{Code: cephmsgr.OSDCopyFrom2, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: base.Pool, SourceName: src}})
	request.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000300, Nanoseconds: 161803398}
	reply, err := o.Request(ctx, request)
	if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("ONDISK copy before forced termination", err)
	}
	observe := func() omapWriteState {
		return nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: dst, InventoryKeys: inventory}).State
	}
	before := observe()
	if before.Code != 0 || !bytes.Equal(before.Data, baseline) || !reflect.DeepEqual(before.Attributes, attrs) || !equalOmapModel(omapWriteInventory(t, before), model) || before.Version != reply.UserVersion {
		t.Fatal("native destination before the crash differs from the source", before.Code, before.Version, reply.UserVersion)
	}
	if err = testcluster.ControlDaemon(ctx, dir, "kill", "osd", name); err != nil {
		t.Fatal("force OSD termination", err)
	}
	stopped = true
	if err = o.Close(); err != nil {
		t.Fatal("close pre-crash OSD session", err)
	}
	crashData, err := os.ReadFile(filepath.Join(dir, "osd-crash-state.json"))
	if err != nil {
		t.Fatal("forced termination evidence", err)
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
		t.Fatalf("expected SIGKILL, not orderly shutdown or OOM: %+v", crash)
	}
	if err = testcluster.ControlDaemon(ctx, dir, "start", "osd", name); err != nil {
		t.Fatal("restart forcibly terminated OSD", err)
	}
	stopped = false
	waitOSDFixtureClean(t, ctx, base)
	after := observe()
	if after.Code != 0 || after.Version != before.Version || after.Seconds != before.Seconds || after.Nanos != before.Nanos || !bytes.Equal(after.Data, baseline) || !reflect.DeepEqual(after.Attributes, attrs) || !equalOmapModel(omapWriteInventory(t, after), model) {
		t.Fatal("copied object changed across SIGKILL and restart", before.Version, after.Version)
	}
	target = route()
	o = mutationConnection(t, c, ctx, target)
	defer o.Close()
	read, err := o.Request(ctx, osdObjectRequest(target, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(baseline) + 8)}))
	if err != nil || read.Transaction <= reply.Transaction || read.UserVersion != before.Version || !bytes.Equal(read.Operations[0].Data, baseline) {
		t.Fatal("fresh explicit OSD session lost identity or bytes", err)
	}
	t.Log("ONDISK COPY_FROM2 bytes, xattr, OMAP, version and mtime survived BlueStore OSD SIGKILL; this covers daemon termination, not host power loss")
}
