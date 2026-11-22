package cephmsgr

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
)

// Synthetic MDS peers; not native evidence. The live acceptance of the
// opt-in is TestCephMDSCephFSCharmapIntegration (namespace, alternate names,
// restart) and a leaf of TestCephMDSSessionsIntegration (a filesystem that
// requires CHARMAP).

// TestCephFSCharmapOptInRequiresCephFS refuses the opt-in without the CephFS
// profile: the bit would claim entry creation on a session that cannot send
// metadata requests.
func TestCephFSCharmapOptInRequiresCephFS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	options := mockOptions(t, 20, [16]byte{1})
	options.EnableMDS, options.EnableCephFSCharmap = true, true
	if c, err := Dial(ctx, options); err == nil || err.Error() != "ceph: EnableCephFSCharmap requires EnableCephFS" {
		if c != nil {
			c.Close()
		}
		t.Fatal("charmap opt-in admitted without EnableCephFS", err)
	}
}

// TestCephFSCharmapOptInDeclaresOnlyCharmap requires the opt-in to add
// exactly bit 22 to the CephFS declaration, in the accessor and in
// REQUEST_OPEN, and the library to send a request's name and alternate name
// unchanged: it applies no charmap of its own.
func TestCephFSCharmapOptInDeclaresOnlyCharmap(t *testing.T) {
	want := MDSCephFSClientFeatures()
	want[mds.FeatureCharmap/8] |= 1 << (mds.FeatureCharmap % 8)
	if !bytes.Equal(MDSCephFSCharmapClientFeatures(), want) || MDSFeatureBitsetHas(MDSCephFSClientFeatures(), mds.FeatureCharmap) {
		t.Fatal("the opt-in bitset is not the CephFS bitset plus CHARMAP", MDSCephFSCharmapClientFeatures())
	}
	c, peers, ctx := cephfsTestClientOptions(t, func(*mdsTestPeer, int) {}, func(o *Options) { o.EnableCephFSCharmap = true })
	got := c.MDSClientFeatures()
	if !bytes.Equal(got, mds.CephFSCharmapClientFeatures) {
		t.Fatal("opt-in client reports another declaration", got)
	}
	got[2] = 0
	if !bytes.Equal(c.MDSClientFeatures(), mds.CephFSCharmapClientFeatures) || !bytes.Equal(mds.CephFSCharmapClientFeatures, want) {
		t.Fatal("the reported declaration aliases the client's own")
	}
	m, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/17"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	pending := mdsStartSession(m, ctx, MDSSessionOptions{RenewInterval: time.Hour})
	open, err := mds.DecodeClientSession(peer.next(t), 0)
	if err != nil || open.Op != mds.SessionRequestOpen || !bytes.Equal(open.SupportedFeatures, want) {
		t.Fatal("REQUEST_OPEN does not declare the opt-in bitset", open.SupportedFeatures, err)
	}
	peer.send(t, mdsSessionV7(mds.SessionOpen, 0, nil, 0, false))
	result := <-pending
	if result.err != nil {
		t.Fatal(result.err)
	}
	s := result.session
	mdsSessionNext(t, peer, mds.SessionRequestRenewCaps)
	// A name the stock client would fold and normalize, with its original
	// spelling as the alternate name; and one it would not have produced.
	for _, r := range []MDSRequest{
		{Op: MDSOpMkdir, Path: MDSFilePath{Ino: 0x10000000001, Path: "café-dir"}, AlternateName: []byte("Café-Dir")},
		{Op: MDSOpCreate, Path: MDSFilePath{Ino: 0x10000000001, Path: "Upper\xff"}},
	} {
		call, err := s.Submit(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		sent, err := mds.DecodeClientRequest(peer.next(t), 0)
		if err != nil || sent.Op != r.Op || sent.Path.Path != r.Path.Path || !bytes.Equal(sent.AlternateName, r.AlternateName) {
			t.Fatalf("the library changed the caller's name or alternate name: %q %q %v", sent.Path.Path, sent.AlternateName, err)
		}
		peer.send(t, cephfsReply(r.Op, call.Transaction(), 0, true, nil))
		if _, err := call.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
}
