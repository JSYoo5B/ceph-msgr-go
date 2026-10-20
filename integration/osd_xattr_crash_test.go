package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

func TestCephOSDXattrCrashIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("requires three sparse-file BlueStore replicas")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	c, _, oracle, original := osdFixture(t)
	if oracle.Replicas != 3 || oracle.MinSize != 2 || len(oracle.Acting) != 3 {
		t.Fatal("metadata crash acceptance requires healthy replicas3/min_size2 placement")
	}
	const keptName = "go-xattr-crash-kept"
	const removedName = "go-xattr-crash-removed"
	b64 := base64.StdEncoding.EncodeToString
	observation := xattrNativeInput{Exists: true, Kind: "read", Names: []string{b64([]byte(keptName)), b64([]byte(removedName))}}
	initial := nativeXattrCompare(t, ctx, oracle, observation).Actual
	if initial.Code != 0 || !bytes.Equal(initial.Data, original) {
		t.Fatal("native metadata crash baseline differs from fixture bytes")
	}
	initialInventory := xattrInventory(initial)
	for _, name := range []string{keptName, removedName} {
		if _, present := initialInventory[name]; present {
			t.Fatal("isolated crash xattr name already exists", name)
		}
	}
	set := func(name string, value []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: name}, Data: value}
	}
	remove := func(name string) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDRemoveXattr, Xattr: cephmsgr.OSDXattrInput{Name: name}}
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	primary := oracle.ID
	primaryName := strconv.FormatInt(int64(primary), 10)
	stopped := false
	defer func() {
		restore, stop := context.WithTimeout(context.Background(), 60*time.Second)
		defer stop()
		if stopped {
			if err := testcluster.ControlDaemon(restore, dir, "start", "osd", primaryName); err != nil {
				t.Error("restore crashed metadata primary", err)
				return
			}
		}
		// Only the restored three-replica PG can become clean. The surviving
		// two-replica phase below is intentionally degraded, not called clean.
		waitOSDFixtureClean(t, restore, oracle)
		fresh := nativeMutation(t, restore, map[string]any{"compare": false, "placement": true}).Placement
		oracle = adoptMutationPlacement(t, oracle, fresh)
		publishReplicaOSDRouting(t, dir, oracle)
		cleanup := mutationConnection(t, c, restore, oracle)
		defer cleanup.Close()
		// This explicit repair removes only owned attrs and restores original
		// bytes/mtime; it never replays the tested APPEND+SET+RM vector.
		r := osdObjectRequest(oracle, remove(keptName), remove(removedName), cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(original)), Data: original})
		r.ModificationTime = cephmsgr.OSDTime{Seconds: uint32(initial.Seconds), Nanoseconds: uint32(initial.Nanos)}
		if _, err := cleanup.Request(restore, r); err != nil {
			t.Error("restore metadata crash fixture bytes/owned attrs/mtime", err)
			return
		}
		if after := nativeXattrCompare(t, restore, oracle, observation).Actual; !xattrSnapshotEqual(initial, after, false) {
			t.Error("metadata crash cleanup changed preexisting bytes/attrs/mtime")
		}
	}()
	o := mutationConnection(t, c, ctx, oracle)
	defer o.Close()
	seed := osdObjectRequest(oracle, set(removedName, []byte("exists-before-crash\x00")))
	seed.ModificationTime = cephmsgr.OSDTime{Seconds: uint32(initial.Seconds), Nanoseconds: uint32(initial.Nanos)}
	if reply, err := o.Request(ctx, seed); err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("prepare existing attr to remove", err)
	}
	prepared := nativeXattrCompare(t, ctx, oracle, observation).Actual
	preparedInventory := xattrInventory(prepared)
	if prepared.Code != 0 || preparedInventory[removedName] != "exists-before-crash\x00" || !bytes.Equal(prepared.Data, original) || prepared.Seconds != initial.Seconds || prepared.Nanos != initial.Nanos || prepared.Version <= initial.Version {
		t.Fatal("native preparation did not confirm the existing attr and unchanged object bytes/mtime")
	}
	tail := []byte{255, 0, 'x', 'a', 't', 't', 'r', '-', 'c', 'r', 'a', 's', 'h', 0, 128}
	value := []byte{0, 255, 'C', 'r', 'a', 's', 'h', 0, 128, 10}
	want := append(bytes.Clone(original), tail...)
	wantInventory := xattrInventory(initial)
	wantInventory[keptName] = string(value)
	request := osdObjectRequest(oracle,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail},
		set(keptName, value), remove(removedName))
	request.ClientIncarnation = 41
	request.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000600, Nanoseconds: 271828182}
	// One non-idempotent append plus metadata mutation is submitted exactly
	// once. Independent native snapshots establish the committed full state.
	reply, err := o.Request(ctx, request)
	if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("ONDISK replicated append/SET/RM vector", err)
	}
	checkOSDMetadata(t, reply, oracle)
	if reply.Submission.ClientIncarnation != 41 || reply.Submission.Transaction != reply.Transaction || len(reply.Operations) != 3 {
		t.Fatal("metadata mutation lost local request identity/vector shape")
	}
	for i, op := range reply.Operations {
		if op.Code != request.Operations[i].Code || op.Result != 0 || op.PayloadLength != 0 || len(op.Data) != 0 {
			t.Fatal("metadata mutation did not return successful bounded per-op results", i, op)
		}
	}
	committed := nativeXattrCompare(t, ctx, oracle, observation).Actual
	if committed.Code != 0 || committed.Version != reply.UserVersion || committed.Version <= prepared.Version || committed.Seconds != int64(request.ModificationTime.Seconds) || committed.Nanos != int64(request.ModificationTime.Nanoseconds) || !bytes.Equal(committed.Data, want) || !reflect.DeepEqual(xattrInventory(committed), wantInventory) {
		t.Fatal("native before-crash bytes/attrs/version/mtime differ from ONDISK vector")
	}
	if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", primaryName); err != nil {
		t.Fatal("kill actual selected metadata primary", err)
	}
	stopped = true
	if err := o.Close(); err != nil {
		t.Fatal("close killed metadata primary session", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "osd-crash-state.json"))
	if err != nil {
		t.Fatal("metadata primary termination receipt", err)
	}
	var crash struct {
		Daemon    string `json:"daemon"`
		ID        string `json:"id"`
		ExitCode  *int   `json:"exit_code"`
		OOMKilled *bool  `json:"oom_killed"`
	}
	if err := json.Unmarshal(data, &crash); err != nil {
		t.Fatal(err)
	}
	if crash.Daemon != "osd."+primaryName || crash.ID == "" || crash.ExitCode == nil || *crash.ExitCode != 137 || crash.OOMKilled == nil || *crash.OOMKilled {
		t.Fatal("metadata crash was not proven primary SIGKILL without OOM", crash)
	}
	// Native readback waits for the surviving PG to be readable; a size3 PG
	// with two acting replicas cannot be clean before the original is restored.
	placement := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true})
	fresh := placement.Placement
	if placement.Actual.Code != 0 || !bytes.Equal(placement.Actual.Data, want) || placement.Actual.Version != committed.Version || placement.Actual.Seconds != uint32(committed.Seconds) || placement.Actual.Nanos != uint32(committed.Nanos) || fresh.ID == primary || fresh.Epoch <= oracle.Epoch {
		t.Fatal("surviving native data/identity or replacement placement differs")
	}
	remaining := map[int32]bool{}
	for _, id := range fresh.Acting {
		if id == primary || id < 0 || id > 2 || remaining[id] {
			t.Fatal("unexpected metadata survivor acting set", fresh.Acting)
		}
		remaining[id] = true
	}
	if len(remaining) != 2 || !remaining[fresh.ID] {
		t.Fatal("metadata recovery lacks the surviving two-replica primary", fresh.Acting)
	}
	oracle = adoptMutationPlacement(t, oracle, fresh)
	publishReplicaOSDRouting(t, dir, oracle)
	surviving := nativeXattrCompare(t, ctx, oracle, observation).Actual
	if !xattrSnapshotEqual(committed, surviving, true) {
		t.Fatal("native surviving replicas changed bytes/attrs/version/mtime")
	}
	o = mutationConnection(t, c, ctx, oracle)
	defer o.Close()
	read, err := o.Request(ctx, osdObjectRequest(oracle,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDStat},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(want) + 32)},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDGetXattr, Xattr: cephmsgr.OSDXattrInput{Name: keptName, MaxOutputBytes: 1024}},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDGetXattrs, Xattr: cephmsgr.OSDXattrInput{MaxOutputBytes: 64 << 10}}))
	if err != nil {
		t.Fatal("explicit surviving-primary stat/read/GET/GETALL", err)
	}
	checkOSDMetadata(t, read, oracle)
	if read.Transaction <= reply.Transaction || read.UserVersion != committed.Version || len(read.Operations) != 4 || !bytes.Equal(read.Operations[1].Data, want) || !bytes.Equal(read.Operations[2].Data, value) || !reflect.DeepEqual(xattrEncodedInventory(t, read.Operations[3].Data), wantInventory) {
		t.Fatal("Go surviving-primary bytes/metadata/version/TID differ from native state")
	}
	stat := read.Operations[0].Data
	if len(stat) != 16 || binary.LittleEndian.Uint64(stat) != uint64(len(want)) || binary.LittleEndian.Uint32(stat[8:]) != uint32(committed.Seconds) || binary.LittleEndian.Uint32(stat[12:]) != uint32(committed.Nanos) {
		t.Fatal("surviving Go stat size/nanosecond mtime differs from native stat2")
	}
	missing, err := o.Request(ctx, osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDGetXattr, Xattr: cephmsgr.OSDXattrInput{Name: removedName, MaxOutputBytes: 1024}}))
	var server *cephmsgr.OSDError
	if !errors.As(err, &server) || server.Code != -61 || missing.Code != -61 || len(missing.Operations) != 1 || missing.Operations[0].Result != -61 || len(missing.Operations[0].Data) != 0 {
		t.Fatal("removed attr reappeared or lost known ENODATA after primary crash", err)
	}
	final := nativeXattrCompare(t, ctx, oracle, observation).Actual
	if !xattrSnapshotEqual(committed, final, true) {
		t.Fatal("read-only recovery replayed append/metadata writes or changed native state")
	}
	t.Log("one ONDISK APPEND+SET(binary)+RM(existing) survived actual-primary SIGKILL with two BlueStore replicas; explicit Go stat/read/GET/GETALL and native inventory/version/mtime matched without another write. This proves daemon termination, not host/device power-loss durability.")
}
