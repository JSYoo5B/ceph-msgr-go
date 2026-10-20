package integration_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// ISDIRTY reads the committed object_info FLAG_DIRTY (a 1-byte bool in its
// reply output, per-op result always 0); UNDIRTY clears it on commit. Any
// write — native or through the Go client — sets the flag via make_writeable,
// so the WRITE -> ISDIRTY(true) -> UNDIRTY -> ISDIRTY(false) sequence is
// verifiable on an ordinary replicated pool with no cache-tier overlay. UNDIRTY
// only flips the flag; it leaves the object bytes unchanged, which native
// observation confirms. (The cache-tier CACHE_FLUSH/EVICT/PIN and dedup
// SET_CHUNK opcodes return -EINVAL/-EOPNOTSUPP on a plain pool and are not
// implemented.)
func TestCephOSDDirtyFlagIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1)

	seed := []byte("dirty-seed\x00\xff")
	poolSeed(t, ctx, 1, "dirty-obj", "", seed, nil)
	route := poolPlace(t, ctx, 1, "dirty-obj", "")
	conn := mutationConnection(t, c, ctx, route)
	defer conn.Close()

	op := func(o cephmsgr.OSDOperation) cephmsgr.OSDReply {
		t.Helper()
		req := pools.request(route, o)
		if o.Code != cephmsgr.OSDIsdirty {
			req.ModificationTime = cephmsgr.OSDTime{Seconds: 1700008000}
		}
		reply, err := conn.Request(ctx, req)
		if err != nil || reply.Code != 0 || len(reply.Operations) != 1 || reply.Operations[0].Result != 0 {
			t.Fatalf("opcode %#x: err=%v code=%d", o.Code, err, reply.Code)
		}
		return reply
	}
	isDirty := func() bool {
		r := op(cephmsgr.OSDOperation{Code: cephmsgr.OSDIsdirty})
		if r.Operations[0].PayloadLength != 1 {
			t.Fatalf("ISDIRTY output length %d, want 1", r.Operations[0].PayloadLength)
		}
		return r.Operations[0].Dirty
	}
	undirty := func() {
		r := op(cephmsgr.OSDOperation{Code: cephmsgr.OSDUndirty})
		if r.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("UNDIRTY lacks ONDISK completion")
		}
	}

	// A native write already set FLAG_DIRTY.
	if !isDirty() {
		t.Fatal("object is not dirty after the native seed write")
	}
	undirty()
	if isDirty() {
		t.Fatal("object still dirty after UNDIRTY")
	}
	// UNDIRTY changed only the flag; the data is untouched.
	if n := poolObserve(t, ctx, 1, "dirty-obj", "", ""); n.Code != 0 || !bytes.Equal(n.Data, seed) {
		t.Fatalf("UNDIRTY altered object bytes: code %d %q", n.Code, n.Data)
	}
	// A Go write re-dirties it; UNDIRTY clears it again.
	rewrite := []byte("rewritten by the Go client")
	op(cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(rewrite)), Data: rewrite})
	if !isDirty() {
		t.Fatal("object is not dirty after a Go WRITEFULL")
	}
	undirty()
	if isDirty() {
		t.Fatal("object still dirty after the second UNDIRTY")
	}
	if n := poolObserve(t, ctx, 1, "dirty-obj", "", ""); n.Code != 0 || !bytes.Equal(n.Data, rewrite) {
		t.Fatalf("final object bytes differ: code %d %q", n.Code, n.Data)
	}
}
