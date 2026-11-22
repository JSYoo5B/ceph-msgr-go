package mds

import "testing"

// extensionDecision is this repository's decision for a CephFS client
// feature bit that neither profile declares.
type extensionDecision int

const (
	// baseline: at least one profile declares the bit (ClientFeatures or
	// CephFSClientFeatures); its contract is on that declaration.
	baseline extensionDecision = iota + 1
	// implement: the next stage for the bit. A new codec needs fresh native
	// vectors, and the selected profile needs operation, push and failure
	// acceptance with independent namespace or inode observation, before
	// the bit can be declared.
	implement
	// deferred: optional, with no caller contract or native oracle yet.
	deferred
	// consumerOwned: the bit promises a policy of the layer above this
	// library (permission, capability or inode policy, or result
	// interpretation). The bit stays withheld and the raw wire data stays
	// available.
	consumerOwned
)

// tentacleCephFSFeatures is the closed CephFS client feature list of Ceph
// v20.2.4 (commit 7f793731f1b39eb4f465e960113d2363c311b964):
// mds/cephfs_features.cc feature_names, indexed by bit up to
// CEPHFS_FEATURE_MAX (mds/cephfs_features.h), with this repository's decision
// on each bit's own line. Server and client paths named below are relative to
// the pinned src/. A newer pinned source with more features extends this list.
//
// Where the MDS reads a withheld bit at the pinned tag: admission
// (Server::handle_client_session REJECTs a REQUEST_OPEN lacking a
// required_client_features bit, handle_client_reconnect answers CLOSE, and
// update_required_client_features evicts open sessions); bit 11 in
// Locker::resume_stale_caps; bit 16 in handle_client_session; bit 21 when a
// root_squash grant is held; bit 22 on entry creation in a charmap directory.
// Bits 14, 17 and 23 are never read from the client's bitset: the native
// client gates them on the MDS's advertisement. Every withheld bit keeps an
// explicit refusal when a filesystem requires it; nothing is retried.
var tentacleCephFSFeatures = []struct {
	name     string
	decision extensionDecision
}{
	{"reserved", baseline},
	{"reserved", baseline},
	{"reserved", baseline},
	{"reserved", baseline},
	{"reserved", baseline},
	{"jewel", baseline},
	{"kraken", baseline},
	{"luminous", baseline},
	{"mimic", baseline},
	{"reply_encoding", baseline},
	{"reclaim_client", baseline},
	// lazy_caps_wanted: after STALE returns to OPEN the client must
	// re-announce wanted caps with an MClientCaps UPDATE; the consumer owns
	// that policy (SendCaps carries Wanted). Omission keeps eager reissue.
	{"lazy_caps_wanted", consumerOwned},
	{"multi_reconnect", baseline},
	{"deleg_ino", baseline},
	// metric_collect: MClientMetrics (type 29) is not represented and
	// REQUEST_OPEN sends an empty metric spec; telemetry values belong to the
	// I/O layer and the MDS sends no acknowledgement.
	{"metric_collect", deferred},
	{"alternate_name", baseline},
	// notify_session_state: needs a duplicate-REQUEST_OPEN path that resends
	// OPEN or FORCE_RO on an existing session; one REQUEST_OPEN is sent per
	// local session.
	{"notify_session_state", deferred},
	// op_getvxattr: CEPH_MDS_OP_GETVXATTR can travel through raw Submit; the
	// consumer picks the vxattr, checks the MDS advertisement and interprets
	// the text. No exported constant or native evidence.
	{"op_getvxattr", consumerOwned},
	{"32bits_retry_fwd", baseline},
	{"new_snaprealm_info", baseline},
	{"has_owner_uidgid", baseline},
	// client_mds_auth_caps: the client must enforce MDS auth caps before
	// buffering changes; the library enforces no grant. Without the bit a
	// root_squash session is reported as a broken client (health error, by
	// source; not observed live).
	{"client_mds_auth_caps", consumerOwned},
	// charmap: the MDS adds a directory's charmap to every REPLY_ENCODING
	// InodeStat without testing the bit, and InodeStat.DecodeOptionalMetadata
	// decodes it (TestDecodeOptionalMetadataMatchesFreshNative; the charmap
	// leaf of TestCephMDSCephFSPushIntegration reads it with a session that
	// lacks the bit). Entry creation in a charmap directory answers -EPERM
	// without the bit, which that leaf also checks. Declaring the bit needs
	// an explicit CephFS opt-in in which the caller states it validates,
	// normalizes and folds names (never a default), with a native comparison
	// of transformed names and their alternate names.
	{"charmap", implement},
	// blockdiff: CEPH_MDS_OP_FILE_BLOCKDIFF can travel through raw Submit
	// with caller-built arguments; snapshot selection, paging and block
	// mapping are the consumer's. No exported constant or native evidence.
	{"blockdiff", consumerOwned},
}

// TestClientExtensionDecisionsCoverEveryPinnedFeature binds the decision
// table to the declared bitsets: a declared bit is baseline, a withheld bit
// has a decision, the CephFS profile includes every session-profile bit, no
// declared bit lies beyond the pinned list, and codec constants named after
// a feature agree with the pinned bit.
func TestClientExtensionDecisionsCoverEveryPinnedFeature(t *testing.T) {
	for bit, feature := range tentacleCephFSFeatures {
		session, cephfs := FeatureBitsetHas(ClientFeatures, uint(bit)), FeatureBitsetHas(CephFSClientFeatures, uint(bit))
		if session && !cephfs {
			t.Errorf("bit %d (%s): the CephFS profile must include every session-profile bit", bit, feature.name)
		}
		if cephfs != (feature.decision == baseline) {
			t.Errorf("bit %d (%s): declared %v, but decision %d", bit, feature.name, cephfs, feature.decision)
		}
		if feature.decision < baseline || feature.decision > consumerOwned {
			t.Errorf("bit %d (%s): no decision", bit, feature.name)
		}
	}
	for _, profile := range [][]byte{ClientFeatures, CephFSClientFeatures} {
		for bit := uint(len(tentacleCephFSFeatures)); bit < uint(len(profile))*8; bit++ {
			if FeatureBitsetHas(profile, bit) {
				t.Errorf("declared bit %d is beyond the pinned feature list", bit)
			}
		}
	}
	for name, bit := range map[string]int{"reply_encoding": FeatureReplyEncoding, "reclaim_client": FeatureReclaimClient,
		"multi_reconnect": FeatureMultiReconnect, "deleg_ino": FeatureDelegIno, "32bits_retry_fwd": Feature32BitsRetryFwd,
		"new_snaprealm_info": FeatureNewSnaprealmInfo, "has_owner_uidgid": FeatureHasOwnerUIDGID} {
		if tentacleCephFSFeatures[bit].name != name {
			t.Errorf("Go feature constant for %s is bit %d (%s)", name, bit, tentacleCephFSFeatures[bit].name)
		}
	}
}
