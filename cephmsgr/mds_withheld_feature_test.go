package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
)

// Synthetic MDS peers; the REJECT reason follows pinned Tentacle
// Server::handle_client_session (mds/Server.cc:743-748), which formats the
// missing feature_bitset_t as "0x" plus 16 hex digits per 64-bit block
// (mds/mdstypes.cc:559-571). The live session-profile refusal is in
// integration/fixture/MDS_SESSIONS_VALIDATION.md; this is not native evidence.

// TestMDSWithheldFeatureRequirementIsRejectedExplicitly requires, for both
// session profiles and every bit of the first feature block that the profile
// does not declare, that REQUEST_OPEN keeps the exact fixed declaration, an
// MDS REJECT for that missing requirement returns *MDSSessionRejectedError
// with the raw reason and leaves no session, and the client neither widens
// its declaration nor retries the open on its own.
func TestMDSWithheldFeatureRequirementIsRejectedExplicitly(t *testing.T) {
	profiles := map[string]struct {
		declared []byte
		open     func(*testing.T) (*MDSConnection, *mdsTestPeer, context.Context)
	}{
		"session": {mds.ClientFeatures, func(t *testing.T) (*MDSConnection, *mdsTestPeer, context.Context) {
			_, m, peer, ctx, _ := mdsTestOpen(t)
			peer.sessions.Store(true)
			return m, peer, ctx
		}},
		"cephfs": {mds.CephFSClientFeatures, func(t *testing.T) (*MDSConnection, *mdsTestPeer, context.Context) {
			c, peers, ctx := cephfsTestClient(t)
			m, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/17"})
			if err != nil {
				t.Fatal(err)
			}
			return m, <-peers, ctx
		}},
	}
	for name, profile := range profiles {
		t.Run(name, func(t *testing.T) {
			m, peer, ctx := profile.open(t)
			if !bytes.Equal(m.client.MDSClientFeatures(), profile.declared) {
				t.Fatal("client reports a different declaration", m.client.MDSClientFeatures())
			}
			expectOpen := func() {
				t.Helper()
				request := peer.next(t)
				got, err := mds.DecodeClientSession(request, 0)
				if err != nil || got.Op != mds.SessionRequestOpen || !bytes.Equal(got.SupportedFeatures, profile.declared) {
					t.Fatal("REQUEST_OPEN changed its fixed feature declaration", got.Op, got.SupportedFeatures, err)
				}
			}
			withheld := 0
			for bit := uint(0); bit < 64; bit++ {
				if mds.FeatureBitsetHas(profile.declared, bit) {
					continue
				}
				withheld++
				pending := mdsStartSession(m, ctx, MDSSessionOptions{RenewInterval: time.Hour})
				expectOpen()
				reason := fmt.Sprintf("missing required features '0x%016x'", uint64(1)<<bit)
				peer.send(t, mdsSessionV7(mds.SessionReject, 0, map[string]string{"error_string": reason}, 0, false))
				got := <-pending
				var rejected *MDSSessionRejectedError
				if got.session != nil || !errors.As(got.err, &rejected) || rejected.Reason != reason || rejected.Blocklisted || rejected.Flags != 0 {
					t.Fatalf("bit %d: missing-feature REJECT was not explicit: %v", bit, got.err)
				}
			}
			if withheld == 0 {
				t.Fatal("profile declares every bit of the first block")
			}
			// The next frame is this explicit open, not a client-initiated
			// retry, and it still carries the unchanged declaration.
			pending := mdsStartSession(m, ctx, MDSSessionOptions{RenewInterval: time.Hour})
			expectOpen()
			peer.send(t, mdsSessionV7(mds.SessionOpen, 0, nil, 0, false))
			if got := <-pending; got.err != nil || got.session == nil {
				t.Fatal("open after explicit refusals", got.err)
			}
		})
	}
}
