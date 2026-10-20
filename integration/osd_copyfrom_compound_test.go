package integration_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// A COPY_FROM2 may lead a vector whose later operations write xattrs: the OSD
// stages the copy and then runs the whole vector in one transaction, so the
// destination holds the source's bytes and attributes as edited by the rest.
func TestCephOSDCopyFromCompoundIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1)
	data := []byte("compound copy source\x00\xff")
	poolSeed(t, ctx, 1, "compound-src", "", data, []nativeXattrEntry{{Name: []byte("keep"), Value: []byte("k")}, {Name: []byte("drop"), Value: []byte("d")}})
	route := poolPlace(t, ctx, 1, "compound-dst", "")
	o := mutationConnection(t, c, ctx, route)
	defer o.Close()
	r := pools.request(route,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDCopyFrom2, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: pools.ids[1], SourceName: "compound-src"}},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "added"}, Data: []byte{0, 'a'}},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDRemoveXattr, Xattr: cephmsgr.OSDXattrInput{Name: "drop"}})
	r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700009000}
	reply, err := o.Request(ctx, r)
	if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || len(reply.Operations) != 3 {
		t.Fatal("compound copy", err)
	}
	n := poolObserve(t, ctx, 1, "compound-dst", "", "")
	attrs := xattrInventory(n)
	if !bytes.Equal(n.Data, data) || len(attrs) != 2 || attrs["keep"] != "k" || attrs["added"] != string([]byte{0, 'a'}) || n.Version != reply.UserVersion {
		t.Fatalf("destination differs: %q %v", n.Data, attrs)
	}
	// An xattr write before the copy is refused before submission.
	bad := pools.request(route,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "x"}, Data: []byte("x")},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDCopyFrom2, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: pools.ids[1], SourceName: "compound-src"}})
	bad.ModificationTime = cephmsgr.OSDTime{Seconds: 1700009001}
	if reply, err := o.Request(ctx, bad); err == nil || reply.Submission != (cephmsgr.OSDSubmission{}) {
		t.Fatal("a copy after another operation was submitted", err)
	}
}
