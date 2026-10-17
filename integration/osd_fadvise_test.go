package integration_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// FADVISE cache hints ride on the data-path operations and change no result:
// writes and reads carrying them match the native client's view of the object.
func TestCephOSDFadviseIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1)
	const object = "fadvise-object"
	poolSeed(t, ctx, 1, object, "", []byte("seed"), nil)
	route := poolPlace(t, ctx, 1, object, "")
	o := mutationConnection(t, c, ctx, route)
	defer o.Close()
	send := func(ops ...cephmsgr.OSDOperation) (cephmsgr.OSDReply, error) {
		r := pools.request(route, ops...)
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700008000}
		if ops[0].Code == cephmsgr.OSDRead || ops[0].Code == cephmsgr.OSDCompareExtent || ops[0].Code == cephmsgr.OSDStat {
			r.ModificationTime = cephmsgr.OSDTime{}
		}
		return o.Request(ctx, r)
	}
	body := bytes.Repeat([]byte("fadvise\x00\xff"), 512)
	tail := []byte("tail")
	if _, err := send(cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Flags: cephmsgr.OSDFadviseDontNeed | cephmsgr.OSDFadviseNoCache, Length: uint64(len(body)), Data: body}); err != nil {
		t.Fatal("writefull with DONTNEED|NOCACHE", err)
	}
	if _, err := send(cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Flags: cephmsgr.OSDFadviseRandom, Length: uint64(len(tail)), Data: tail}); err != nil {
		t.Fatal("append with RANDOM", err)
	}
	want := append(bytes.Clone(body), tail...)
	if n := poolObserve(t, ctx, 1, object, "", ""); !bytes.Equal(n.Data, want) {
		t.Fatal("native bytes after hinted writes differ")
	}
	reply, err := send(
		cephmsgr.OSDOperation{Code: cephmsgr.OSDCompareExtent, Flags: cephmsgr.OSDFadviseWillNeed, Length: uint64(len(tail)), Offset: uint64(len(body)), Data: tail},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Flags: cephmsgr.OSDFadviseSequential | cephmsgr.OSDFadviseWillNeed, Length: uint64(len(want) + 16)})
	if err != nil || !bytes.Equal(reply.Operations[1].Data, want) {
		t.Fatal("hinted CMPEXT and READ", err)
	}
	t.Logf("echoed op flags: cmpext %#x, read %#x", reply.Operations[0].Flags, reply.Operations[1].Flags)
	// COPY_FROM2 carries hints for the OSD's own read of the source.
	copyRoute := poolPlace(t, ctx, 1, "fadvise-copy", "")
	co := mutationConnection(t, c, ctx, copyRoute)
	defer co.Close()
	cp := pools.request(copyRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDCopyFrom2, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: pools.ids[1], SourceName: object, SourceFadvise: cephmsgr.OSDFadviseDontNeed | cephmsgr.OSDFadviseNoCache}})
	cp.ModificationTime = cephmsgr.OSDTime{Seconds: 1700008001}
	if _, err := co.Request(ctx, cp); err != nil {
		t.Fatal("copy with source hints", err)
	}
	if n := poolObserve(t, ctx, 1, "fadvise-copy", "", ""); !bytes.Equal(n.Data, want) {
		t.Fatal("native copy differs")
	}
	for _, op := range []cephmsgr.OSDOperation{{Code: cephmsgr.OSDStat, Flags: cephmsgr.OSDFadviseNoCache}, {Code: cephmsgr.OSDCreate, Flags: cephmsgr.OSDFadviseDontNeed}} {
		if reply, err := send(op); err == nil || reply.Submission != (cephmsgr.OSDSubmission{}) {
			t.Fatal("a hint on an opcode without a data path was submitted", op.Code, err)
		}
	}
}
