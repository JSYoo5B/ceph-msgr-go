package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

func TestCephOSDReplicasIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("requires three sparse-file BlueStore replicas")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	c, _, oracle, original := osdFixture(t)
	if oracle.Replicas != 3 || oracle.MinSize != 2 || len(oracle.Acting) != 3 {
		t.Fatalf("expected healthy three-replica/min_size-two placement: %+v", oracle)
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	primary := oracle.ID
	name := strconv.FormatInt(int64(primary), 10)
	stopped := false
	defer func() {
		// Restore the original daemon before obtaining caller-owned routing.
		// This also runs on failed evidence checks and uses its own deadline.
		recoverCtx, recoverCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer recoverCancel()
		if stopped {
			if err := testcluster.ControlDaemon(recoverCtx, dir, "start", "osd", name); err != nil {
				t.Error("restore killed primary", err)
				return
			}
		}
		waitOSDFixtureClean(t, recoverCtx, oracle)
		fresh := nativeMutation(t, recoverCtx, map[string]any{"compare": false, "placement": true}).Placement
		oracle.ID, oracle.Epoch, oracle.Address, oracle.Acting, oracle.Up = fresh.ID, fresh.Epoch, fresh.Address, fresh.Acting, fresh.Up
		publishReplicaOSDRouting(t, dir, oracle)
		restoreMutationObject(t, c, oracle, original)
	}()
	o := mutationConnection(t, c, ctx, oracle)
	defer o.Close()
	want := bytes.Repeat([]byte{0, 255, 128, 1, 10, 254, 0, 127}, 1024)
	request := osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(want)), Data: want})
	request.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000200, Nanoseconds: 271828182}
	reply, err := o.Request(ctx, request)
	if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("ONDISK replicated write", err)
	}
	checkOSDMetadata(t, reply, oracle)
	if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", name); err != nil {
		t.Fatal("kill original primary", err)
	}
	stopped = true
	if err := o.Close(); err != nil {
		t.Fatal("close killed primary connection", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "osd-crash-state.json"))
	if err != nil {
		t.Fatal("primary termination evidence", err)
	}
	var state struct {
		Daemon    string `json:"daemon"`
		ID        string `json:"id"`
		ExitCode  *int   `json:"exit_code"`
		OOMKilled *bool  `json:"oom_killed"`
	}
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal("decode primary termination evidence", err)
	}
	if state.Daemon != "osd."+name || state.ID == "" || state.ExitCode == nil || *state.ExitCode != 137 || state.OOMKilled == nil || *state.OOMKilled {
		t.Fatalf("expected primary SIGKILL without OOM: %+v", state)
	}
	// The independent native client waits for the surviving PG to become
	// readable. Native MON placement chooses the next explicit target; the Go
	// product performs neither CRUSH placement nor mutation replay here.
	native := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true})
	fresh := native.Placement
	if native.Actual.Code != 0 || !bytes.Equal(native.Actual.Data, want) || native.Actual.Version != reply.UserVersion || native.Actual.Seconds != request.ModificationTime.Seconds || native.Actual.Nanos != request.ModificationTime.Nanoseconds || fresh.Epoch <= oracle.Epoch || fresh.ID == primary || fresh.ID < 0 || fresh.ID > 2 || fresh.Address == "" || fresh.Pool != oracle.Pool || fresh.Name != oracle.Name || fresh.Hash != oracle.Hash {
		t.Fatalf("surviving native replicas changed data/version/mtime or lack refreshed routing: %+v", native)
	}
	remaining := map[int32]bool{}
	for _, id := range fresh.Acting {
		if id == primary || id < 0 || id > 2 || remaining[id] {
			t.Fatalf("unexpected surviving acting set: %v", fresh.Acting)
		}
		remaining[id] = true
	}
	if len(remaining) != 2 || !remaining[fresh.ID] {
		t.Fatalf("the surviving two-replica quorum is incomplete: %v", fresh.Acting)
	}
	oracle.ID, oracle.Epoch, oracle.Address, oracle.Acting, oracle.Up = fresh.ID, fresh.Epoch, fresh.Address, fresh.Acting, fresh.Up
	publishReplicaOSDRouting(t, dir, oracle)
	o = mutationConnection(t, c, ctx, oracle)
	defer o.Close()
	read, err := o.Request(ctx, osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(want))}))
	if err != nil {
		t.Fatal("explicit Go read on replacement primary", err)
	}
	checkOSDMetadata(t, read, oracle)
	if read.Transaction <= reply.Transaction || read.UserVersion != reply.UserVersion || len(read.Operations) != 1 || !bytes.Equal(read.Operations[0].Data, native.Actual.Data) {
		t.Fatal("replacement primary lost committed bytes/version or request identity")
	}
	second := nativeMutation(t, ctx, map[string]any{"compare": false})
	if second.Actual.Code != 0 || second.Actual.Version != reply.UserVersion || !bytes.Equal(second.Actual.Data, want) {
		t.Fatal("read-only recovery changed native bytes/version; the write must not be replayed")
	}
	t.Log("ONDISK binary write survived primary SIGKILL with two BlueStore replicas; independent native and caller-targeted Go reads matched bytes/version/mtime without another write")
}

func publishReplicaOSDRouting(t *testing.T, dir string, oracle osdFixtureOracle) {
	t.Helper()
	path := filepath.Join(dir, "osd-oracle.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("read fixture routing", err)
	}
	var metadata map[string]any
	if err := json.Unmarshal(data, &metadata); err != nil {
		t.Fatal("decode fixture routing", err)
	}
	metadata["osd_id"], metadata["epoch"], metadata["address"], metadata["acting"] = oracle.ID, oracle.Epoch, oracle.Address, oracle.Acting
	metadata["up"] = oracle.Up
	parts := strings.Split(strings.TrimPrefix(oracle.Address, "v2:"), "/")
	if !strings.HasPrefix(oracle.Address, "v2:") || len(parts) != 2 || parts[0] == "" {
		t.Fatal("native routing is not a full v2 endpoint with nonce", oracle.Address)
	}
	nonce, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		t.Fatal("native endpoint nonce", err)
	}
	metadata["endpoint"] = map[string]any{"type": "v2", "addr": parts[0], "nonce": nonce}
	data, err = json.Marshal(metadata)
	if err != nil {
		t.Fatal("encode native routing", err)
	}
	file, err := os.CreateTemp(dir, "osd-replicas-routing-")
	if err != nil {
		t.Fatal("stage native routing", err)
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		file.Close()
		t.Fatal("write native routing", err)
	}
	if err := file.Chmod(0644); err != nil {
		file.Close()
		t.Fatal("native routing permissions", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal("close native routing", err)
	}
	if err := os.Rename(file.Name(), path); err != nil {
		t.Fatal("publish native routing", err)
	}
}
