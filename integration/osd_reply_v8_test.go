package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

type replyV8WatcherInventory struct {
	Source  string `json:"source_commit"`
	Object  string `json:"object"`
	Entries []struct {
		Address  string `json:"address"`
		GlobalID uint64 `json:"global_id"`
		Cookie   uint64 `json:"cookie"`
	} `json:"entries"`
}
type replyV8Case struct {
	Name            string              `json:"name"`
	Request         cephmsgr.OSDRequest `json:"request"`
	Reply           cephmsgr.OSDReply   `json:"reply"`
	Before, After   omapWriteReceipt
	WritePartMirror *omapWriteReceipt        `json:"native_write_part_mirror,omitempty"`
	Watchers        *cephmsgr.OSDReply       `json:"standalone_watchers_reply,omitempty"`
	NativeWatchers  *replyV8WatcherInventory `json:"native_watchers,omitempty"`
}

func replyV8Watchers(t *testing.T, ctx context.Context, name string) replyV8WatcherInventory {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-watchers", "observe"); err != nil {
		t.Fatal("independent native watcher inventory", err)
	}
	file, err := os.Open(filepath.Join(dir, "osd-watchers-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	if err != nil || len(data) > 64<<10 {
		t.Fatal("bounded native watcher inventory", err)
	}
	var result replyV8WatcherInventory
	if err = json.Unmarshal(data, &result); err != nil || result.Source != "7f793731f1b39eb4f465e960113d2363c311b964" || result.Object != name {
		t.Fatal("native watcher provenance or owned head differs", result, err)
	}
	return result
}

// Summaries preserve useful failure identity without dumping entire raw frames.
func replyV8Summary(reply cephmsgr.OSDReply) string {
	return fmt.Sprintf("tid=%d gid=%d code=%d flags=%#x pg=%d.%x epoch=%d user=%d legacy=%+v replay=%+v ops=%d rawFront=%d rawData=%d", reply.Transaction, reply.GlobalID, reply.Code, reply.Flags, reply.PG.Pool, reply.PG.Seed, reply.MapEpoch, reply.UserVersion, reply.LegacyVersion, reply.ReplayVersion, len(reply.Operations), len(reply.RawFront), len(reply.RawData))
}
func checkReplyV8Vector(t *testing.T, reply cephmsgr.OSDReply, request cephmsgr.OSDRequest, route osdFixtureOracle, results []int32) {
	t.Helper()
	if reply.Transaction == 0 || reply.GlobalID == 0 || reply.Submission.Transaction != reply.Transaction || reply.Submission.GlobalID != reply.GlobalID || reply.Submission.ClientIncarnation != request.ClientIncarnation || reply.Submission.MapEpoch != request.MapEpoch || reply.Submission.Attempt != 0 || !reflect.DeepEqual(reply.Submission.Placement, request.Placement) || reply.Object != route.Name || reply.PG != request.Placement.PG || reply.MapEpoch < request.MapEpoch || reply.RetryAttempt != 0 || len(reply.RawFront) == 0 || reply.Redirect.Object != "" || reply.Redirect.Pool != -1 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || len(reply.Operations) != len(request.Operations) || len(results) != len(reply.Operations) {
		t.Fatal("v8 reply completion, identity or placement differs", replyV8Summary(reply))
	}
	var joined []byte
	for i, op := range reply.Operations {
		wanted := request.Operations[i]
		if op.Code != wanted.Code || op.Flags != uint32(wanted.Flags) || op.Result != results[i] || op.PayloadLength != uint32(len(op.Data)) {
			t.Fatal("v8 packed operation/result/length differs", i, "opcode", op.Code, "flags", op.Flags, "result", op.Result, "expected", results[i], "payload", op.PayloadLength, "data length", len(op.Data))
		}
		joined = append(joined, op.Data...)
	}
	if !bytes.Equal(joined, reply.RawData) {
		t.Fatal("v8 ordered operation payload boundaries differ from raw data")
	}
}
func replyV8Omap(t *testing.T, state omapWriteState) map[string]string {
	t.Helper()
	if state.Omap == nil || state.Omap.Aggregate != 0 {
		t.Fatal("native owned OMAP inventory missing", state)
	}
	for _, op := range state.Omap.Operations {
		if op.Op == "vals" {
			if op.Result != 0 || op.More == nil || *op.More {
				t.Fatal("native OMAP inventory exceeds single bounded page", op)
			}
			values := map[string]string{}
			for _, e := range op.Entries {
				values[string(e.Key)] = string(e.Value)
			}
			return values
		}
	}
	t.Fatal("native OMAP values inventory missing")
	return nil
}

// The selected public profile rejects replies outside v8/compat2. These actual
// daemon requests supplement its independently built five reply codec vectors;
// they do not establish every opcode family or general placement support.
func TestCephOSDReplyV8Integration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "replicated-pg-reply-v8" && os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "replicated-pg-request-v9-reply-v8" {
		t.Skip("requires explicit replicated-pg-reply-v8 fixture")
	}
	c, _, base, _ := replicatedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	t.Cleanup(cancel)
	baseline := []byte{0, 255, 128, 'v', '8', '-', 0, 'r', 'e', 'p', 'l', 'y'}
	attr := nativeXattrEntry{Name: []byte("reply-v8-binary-attr"), Value: []byte{0, 255, 128, 'x', 0}}
	guardKey, changedKey := "guard\x00key", "changed\x00key"
	guardValue, changedValue := []byte{255, 0, 128, 'g'}, []byte{0, 254, 128, 's', 0}
	seed := &omapWriteSeed{Baseline: baseline, Attributes: []nativeXattrEntry{attr}, Entries: []omapNativeEntry{{Key: []byte(guardKey), Value: guardValue}}}
	keys := [][]byte{[]byte(guardKey), []byte(changedKey), []byte("missing")}
	fixtureBefore := replicatedPGNative(t, ctx, base.Name).Actual
	var fixtureAfter mutationSnapshot
	var evidence []replyV8Case
	var cleanupReceipt omapWriteReceipt
	prepared, cleaned := false, false
	t.Cleanup(func() {
		defer func() {
			data, err := json.Marshal(struct {
				Profile                     string        `json:"profile"`
				Scope                       string        `json:"scope"`
				Cases                       []replyV8Case `json:"cases"`
				FixtureBefore, FixtureAfter mutationSnapshot
				Cleanup                     omapWriteReceipt `json:"cleanup"`
				Cleaned                     bool             `json:"cleaned"`
			}{os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE"), "native C mirror reconstructs write part only; earlier mixed READ is compared with independently observed committed bytes; different objects' versions are never equated", evidence, fixtureBefore, fixtureAfter, cleanupReceipt, cleaned})
			if err == nil && len(data) <= 512<<10 {
				err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-reply-v8-results.json"), data)
			} else if err == nil {
				err = errors.New("v8 reply evidence exceeds bound")
			}
			if err != nil {
				t.Error(err)
			}
		}()
		if prepared {
			cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
			defer stop()
			cleanupReceipt = replicatedPGOwned(t, cleanup, base, "cleanup", omapWriteInput{})
			cleaned = cleanupReceipt.Cleanup.Removed
			if !cleaned {
				t.Error("native v8 reply owned head cleanup incomplete")
			}
			fixtureAfter = replicatedPGNative(t, cleanup, base.Name).Actual
			if !reflect.DeepEqual(fixtureBefore, fixtureAfter) {
				t.Error("v8 reply acceptance changed original fixture head")
			}
		}
	})
	owned := replicatedPGOwned(t, ctx, base, "prepare", omapWriteInput{Seed: seed, Reserve: 8})
	prepared = true
	if len(owned.Objects) != 9 || owned.SeedCode == nil || *owned.SeedCode != 0 {
		t.Fatal("native v8 reply UUID ownership seed differs", owned)
	}
	name := owned.Objects[0]
	observe := func() omapWriteReceipt {
		return replicatedPGOwned(t, ctx, base, "observe", omapWriteInput{Object: name, InventoryKeys: keys, ObjectInfo: true})
	}
	native := replicatedPGNative(t, ctx, name)
	route := native.Placement
	if route.Hash == route.PG || native.Actual.Code != 0 || !bytes.Equal(native.Actual.Data, baseline) {
		t.Fatal("native v8 reply actual PG/raw hash or baseline differs", native)
	}
	o := replicatedPGConnection(t, c, ctx, route)
	t.Cleanup(func() { _ = o.Close() })
	cmp := func(flags cephmsgr.OSDOperationFlags) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDCompareOmap, Flags: flags, Omap: cephmsgr.OSDOmapInput{Assertions: []cephmsgr.OSDOmapAssertion{{Key: guardKey, Value: []byte("different"), Comparison: cephmsgr.OSDOmapEqual}}}}
	}
	set := cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapValues, Omap: cephmsgr.OSDOmapInput{Entries: []cephmsgr.OSDOmapEntry{{Key: changedKey, Value: changedValue}}}}
	read := cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Offset: 1, Length: 4}
	compare := int(cephmsgr.OSDOmapEqual)
	mirror := func(flags cephmsgr.OSDOperationFlags, stamp cephmsgr.OSDTime, hint bool) omapWriteReceipt {
		ops := []omapWriteAction{omapCmpAction(flags, guardKey, []byte("different"), compare), {Op: "set", Entries: []omapNativeEntry{{Key: []byte(changedKey), Value: changedValue}}}}
		if hint {
			ops = append(ops, omapWriteAction{Op: "allochint", Flags: uint32(cephmsgr.OSDFailOK), ObjectSize: 1 << 20, WriteSize: 4096, HintFlags: uint32(cephmsgr.OSDAllocHintSequentialWrite | cephmsgr.OSDAllocHintCompressible)})
		}
		r := replicatedPGOwned(t, ctx, base, "compare", omapWriteInput{Seed: seed, Operations: ops, MTime: &omapWriteMTime{Seconds: stamp.Seconds, Nanos: stamp.Nanoseconds}, InventoryKeys: keys, ObjectInfo: true})
		if !r.Cleanup.Mirror || r.Aggregate == nil || len(r.Prvals) != 1 || r.Prvals[0].Index != 0 || r.Prvals[0].Prval != -125 || r.Object == name {
			t.Fatal("independent distinct native write-part mirror missing", r)
		}
		return r
	}
	t.Run("signed refusal preserves earlier output", func(t *testing.T) {
		before := observe()
		request := replicatedPGRequest(route, read, cmp(0), set)
		request.ClientIncarnation = 101
		request.ModificationTime = cephmsgr.OSDTime{Seconds: 1700001200, Nanoseconds: 123456789}
		nativeMirror := mirror(0, request.ModificationTime, false)
		if *nativeMirror.Aggregate != -125 || !reflect.DeepEqual(nativeMirror.Before, nativeMirror.After) {
			t.Fatal("native CMP mismatch did not preserve mirror state", nativeMirror)
		}
		reply, err := o.Request(ctx, request)
		var server *cephmsgr.OSDError
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &server) || server.Code != -125 || errors.As(err, &unknown) || reply.Code != -125 {
			t.Fatal("v8 signed aggregate refusal became unknown or changed", replyV8Summary(reply), err)
		}
		checkReplyV8Vector(t, reply, request, route, []int32{0, -125, 0})
		// A failed write still enters update_log_only for duplicate detection
		// (PrimaryLogPG.cc:8991-8998,4325,4352-4375). Its replay version is the
		// new PG-log sequence, while user/legacy version retain the current
		// object version. The final raw per-op zero does not prove execution.
		if !bytes.Equal(reply.Operations[0].Data, baseline[1:5]) || len(reply.Operations[1].Data) != 0 || len(reply.Operations[2].Data) != 0 || reply.UserVersion != before.State.Version || reply.LegacyVersion.Version != before.State.Version || reply.ReplayVersion.Version <= before.State.Version || reply.ReplayVersion.Epoch == 0 || reply.LegacyVersion.Epoch != reply.ReplayVersion.Epoch {
			t.Fatal("v8 failed write lost earlier bytes, current user version or distinct PG-log replay version", replyV8Summary(reply), "native current version", before.State.Version)
		}
		after := observe()
		if !reflect.DeepEqual(before.State, after.State) {
			t.Fatal("ordinary failed compound changed native bytes/metadata/version/mtime")
		}
		evidence = append(evidence, replyV8Case{Name: "signed refusal preserves earlier output", Request: request, Reply: reply, Before: before, After: after, WritePartMirror: &nativeMirror})
	})
	if t.Failed() {
		return
	}
	t.Run("FAILOK signed result commits metadata and hint", func(t *testing.T) {
		before := observe()
		hint := cephmsgr.OSDOperation{Code: cephmsgr.OSDSetAllocHint, Flags: cephmsgr.OSDFailOK, AllocHint: cephmsgr.OSDAllocHint{ExpectedObjectSize: 1 << 20, ExpectedWriteSize: 4096, Flags: cephmsgr.OSDAllocHintSequentialWrite | cephmsgr.OSDAllocHintCompressible}}
		request := replicatedPGRequest(route, read, cmp(cephmsgr.OSDFailOK), set, hint)
		request.ClientIncarnation = 103
		request.ModificationTime = cephmsgr.OSDTime{Seconds: 1700001201, Nanoseconds: 987654321}
		nativeMirror := mirror(cephmsgr.OSDFailOK, request.ModificationTime, true)
		if *nativeMirror.Aggregate != 0 || nativeMirror.ObjectInfo == nil {
			t.Fatal("native FAILOK write-part mirror failed", nativeMirror)
		}
		reply, err := o.Request(ctx, request)
		if err != nil || reply.Code != 0 {
			t.Fatal("v8 FAILOK vector", replyV8Summary(reply), err)
		}
		checkReplyV8Vector(t, reply, request, route, []int32{0, -125, 0, 0})
		if !bytes.Equal(reply.Operations[0].Data, baseline[1:5]) || reply.Operations[3].AllocHint != hint.AllocHint {
			t.Fatal("v8 READ payload or packed alloc-hint union differs", replyV8Summary(reply))
		}
		after := observe()
		s := after.State
		if s.Code != 0 || !bytes.Equal(s.Data, baseline) || s.Version <= before.State.Version || s.Version != reply.UserVersion || s.Seconds != int64(request.ModificationTime.Seconds) || s.Nanos != int64(request.ModificationTime.Nanoseconds) || after.ObjectInfo == nil || after.ObjectInfo.UserVersion != s.Version {
			t.Fatal("native own-head FAILOK bytes/version/mtime differ", after.State.Version, replyV8Summary(reply))
		}
		if reply.LegacyVersion.Version != s.Version || reply.ReplayVersion.Version != s.Version || reply.ReplayVersion.Epoch == 0 || reply.LegacyVersion.Epoch != reply.ReplayVersion.Epoch {
			t.Fatal("classic-pool non-watch mutation versions differ", replyV8Summary(reply))
		}
		if !reflect.DeepEqual(replyV8Omap(t, s), map[string]string{guardKey: string(guardValue), changedKey: string(changedValue)}) || !reflect.DeepEqual(replyV8Omap(t, s), replyV8Omap(t, nativeMirror.After)) || !reflect.DeepEqual(xattrInventory(s.nativeXattrSnapshot), xattrInventory(nativeMirror.After.nativeXattrSnapshot)) {
			t.Fatal("native own-head and distinct mirror metadata differ")
		}
		a, b := after.ObjectInfo, nativeMirror.ObjectInfo
		if a.ExpectedObjectSize != hint.AllocHint.ExpectedObjectSize || a.ExpectedWriteSize != hint.AllocHint.ExpectedWriteSize || a.AllocHintFlags != uint32(hint.AllocHint.Flags) || a.ExpectedObjectSize != b.ExpectedObjectSize || a.ExpectedWriteSize != b.ExpectedWriteSize || a.AllocHintFlags != b.AllocHintFlags || a.Size != uint64(len(baseline)) {
			t.Fatal("native alloc hint record differs", a, b)
		}
		evidence = append(evidence, replyV8Case{Name: "FAILOK signed result commits metadata and hint", Request: request, Reply: reply, Before: before, After: after, WritePartMirror: &nativeMirror})
	})
	if t.Failed() {
		return
	}
	t.Run("bounded metadata outputs and standalone watchers", func(t *testing.T) {
		before := observe()
		request := replicatedPGRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDGetXattr, Xattr: cephmsgr.OSDXattrInput{Name: string(attr.Name), MaxOutputBytes: 4096}}, cephmsgr.OSDOperation{Code: cephmsgr.OSDGetXattrs, Xattr: cephmsgr.OSDXattrInput{MaxOutputBytes: 4096}}, cephmsgr.OSDOperation{Code: cephmsgr.OSDGetOmapValuesByKeys, Omap: cephmsgr.OSDOmapInput{Keys: []string{changedKey, guardKey, "missing"}, MaxOutputBytes: 4096}})
		request.ClientIncarnation = 105
		reply, err := o.Request(ctx, request)
		if err != nil || reply.Code != 0 {
			t.Fatal("v8 bounded metadata vector", replyV8Summary(reply), err)
		}
		checkReplyV8Vector(t, reply, request, route, []int32{0, 0, 0})
		x := reply.Operations[0]
		if !bytes.Equal(x.Data, attr.Value) || x.Xattr.NameLength != uint32(len(attr.Name)) || x.Xattr.ValueLength != uint32(len(attr.Value)) || !reflect.DeepEqual(xattrEncodedInventory(t, reply.Operations[1].Data), xattrInventory(before.State.nativeXattrSnapshot)) {
			t.Fatal("v8 xattr union or native binary inventory differs", replyV8Summary(reply))
		}
		entries, more := decodeOmapRead(t, cephmsgr.OSDGetOmapValuesByKeys, reply.Operations[2].Data)
		got := map[string]string{}
		for _, e := range entries {
			got[string(e.Key)] = string(e.Value)
		}
		if more != nil || !reflect.DeepEqual(got, replyV8Omap(t, before.State)) {
			t.Fatal("v8 bounded OMAP raw payload differs from native inventory", entries)
		}
		if reply.UserVersion != before.State.Version || reply.ReplayVersion != (cephmsgr.OSDVersion{}) || reply.LegacyVersion.Version != before.State.Version || reply.LegacyVersion.Epoch != 0 {
			t.Fatal("v8 current read versions differ", replyV8Summary(reply))
		}
		// LIST_WATCHERS retains its standalone-only operation contract.
		watcherRequest := replicatedPGRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDListWatchers, ListWatchers: cephmsgr.OSDListWatchersInput{MaxOutputBytes: 4096}})
		watcherRequest.ClientIncarnation = 107
		watchers, err := o.Request(ctx, watcherRequest)
		if err != nil || watchers.Code != 0 {
			t.Fatal("v8 standalone watcher list", replyV8Summary(watchers), err)
		}
		checkReplyV8Vector(t, watchers, watcherRequest, route, []int32{0})
		inventory := replyV8Watchers(t, ctx, name)
		if len(watchers.Operations[0].Watchers) != 0 || len(inventory.Entries) != 0 || len(watchers.Operations[0].Data) != 10 || watchers.UserVersion != before.State.Version {
			t.Fatal("native empty watchers or current version differs", replyV8Summary(watchers), "native entries", len(inventory.Entries))
		}
		after := observe()
		if !reflect.DeepEqual(before.State, after.State) {
			t.Fatal("v8 metadata/watcher reads changed native state")
		}
		evidence = append(evidence, replyV8Case{Name: "bounded metadata outputs and standalone watchers", Request: request, Reply: reply, Before: before, After: after, Watchers: &watchers, NativeWatchers: &inventory})
	})
}
