package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

func TestCephOSDMutationCrashIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("forced daemon termination persistence requires the sparse-file BlueStore fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c, _, oracle, original := osdFixture(t)
	// Dial's context bounds bootstrap only; the client owns its background
	// lifetime. The crash/restart work therefore uses this independent deadline,
	// rather than inheriting osdFixture's shorter bootstrap/test deadline.
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	name := strconv.FormatInt(int64(oracle.ID), 10)
	stopped := false
	defer func() {
		recoverMutationFixture(t, c, oracle, original, name, stopped)
	}()

	o := mutationConnection(t, c, ctx, oracle)
	defer o.Close()
	want := bytes.Repeat([]byte{0, 255, 128, 10, 1, 0, 254, 127}, 1024)
	request := osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(want)), Data: want})
	request.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000002, Nanoseconds: 314159265}
	reply, err := o.Request(ctx, request)
	if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("ONDISK write before forced termination", err)
	}
	checkOSDMetadata(t, reply, oracle)
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
		ID        string `json:"id"`
		ExitCode  *int   `json:"exit_code"`
		OOMKilled *bool  `json:"oom_killed"`
	}
	if err = json.Unmarshal(crashData, &crash); err != nil {
		t.Fatal("decode forced termination evidence", err)
	}
	if crash.Daemon != "osd."+name || crash.ID == "" || crash.ExitCode == nil || *crash.ExitCode != 137 || crash.OOMKilled == nil || *crash.OOMKilled {
		t.Fatalf("expected SIGKILL, not orderly shutdown or OOM: %+v", crash)
	}
	if err = testcluster.ControlDaemon(ctx, dir, "start", "osd", name); err != nil {
		t.Fatal("restart forcibly terminated OSD", err)
	}
	stopped = false
	waitOSDFixtureClean(t, ctx, oracle)
	native := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true})
	fresh := native.Placement
	if native.Actual.Code != 0 || native.Actual.Version != reply.UserVersion || !bytes.Equal(native.Actual.Data, want) || native.Actual.Seconds != request.ModificationTime.Seconds || native.Actual.Nanos != request.ModificationTime.Nanoseconds || fresh.Epoch <= oracle.Epoch {
		t.Fatalf("native crash readback: code=%d bytes=%d version=%d mtime=%d.%09d placement=%+v", native.Actual.Code, len(native.Actual.Data), native.Actual.Version, native.Actual.Seconds, native.Actual.Nanos, fresh)
	}
	// Retain the original fixture size/mtime for restoration; native placement
	// supplies the current map epoch and full endpoint, including its nonce.
	oracle = adoptMutationPlacement(t, oracle, fresh)
	publishReplicaOSDRouting(t, dir, oracle)
	o = mutationConnection(t, c, ctx, oracle)
	defer o.Close()
	read, err := o.Request(ctx, osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(want))}))
	if err != nil {
		t.Fatal("Go readback after forced termination", err)
	}
	checkOSDMetadata(t, read, oracle)
	if read.Transaction <= reply.Transaction || read.UserVersion != native.Actual.Version || len(read.Operations) != 1 || !bytes.Equal(read.Operations[0].Data, native.Actual.Data) {
		t.Fatal("fresh explicit OSD session lost request identity or native bytes")
	}
	t.Log("ONDISK write and mtime survived BlueStore OSD SIGKILL (exit 137, no OOM); native and fresh Go reads matched. This covers daemon termination, not host power loss or device durability.")
}
