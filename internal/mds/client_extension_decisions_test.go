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
	// optIn: the bit promises a policy the caller states it applies; the
	// CephFS profile declares it only with that explicit opt-in
	// (CephFSCharmapClientFeatures), never by default.
	optIn
)

// tentacleCephFSFeatures is the closed CephFS client feature list of Ceph
// v20.2.4 (commit 7f793731f1b39eb4f465e960113d2363c311b964):
// mds/cephfs_features.cc feature_names, indexed by bit up to
// CEPHFS_FEATURE_MAX (mds/cephfs_features.h), with this repository's decision
// on each bit's own line. Server and client paths named below are relative to
// the pinned src/. A newer pinned source with more features extends this list.
//
// Where the MDS reads a withheld bit at the pinned tag (re-audited for bits
// 11, 14, 16, 17, 21 and 23): admission (Server::handle_client_session
// REJECTs a REQUEST_OPEN lacking a required_client_features bit,
// handle_client_reconnect answers CLOSE, and update_required_client_features
// evicts open sessions); bit 11 in Locker::resume_stale_caps; bit 16 in
// handle_client_session; bit 21 in handle_client_session and
// handle_client_reconnect when a root_squash grant is held; bit 22 on entry
// creation in a charmap directory. Bits 14, 17 and 23 are never read from the
// client's bitset: the native client gates them on the MDS's advertisement
// (Client::collect_and_send_global_metrics, and Client::_getvxattr and
// Client::file_blockdiff through make_request's feature_needed). Every
// withheld bit keeps an explicit refusal when a filesystem requires it;
// nothing is retried. The
// reject_required_withheld_features leaf of TestCephMDSSessionsIntegration
// requires each of the six in turn and observes the native REJECT of every
// profile, whose reason names exactly that bit. The reconnect CLOSE for a
// missing bit is observed for CHARMAP (bit 22): the
// reconnect_closed_for_a_missing_required_bit leaf of
// TestCephMDSCephFSCharmapIntegration adds the requirement while no daemon
// holds rank 0, and the new incarnation answers the reconnect of a default
// CephFS session held across the restart with CLOSE. The withheld bits share
// that handler and are not repeated there.
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
	// lazy_caps_wanted: with the bit, Locker::resume_stale_caps stops at the
	// first cap that is not notable (as revoke_stale_caps does), so after
	// STALE returns to OPEN the client must re-announce wanted caps with an
	// MClientCaps UPDATE. The consumer owns that policy (SendCaps carries
	// Wanted). Omission keeps the eager reissue of every cap.
	{"lazy_caps_wanted", consumerOwned},
	{"multi_reconnect", baseline},
	{"deleg_ino", baseline},
	// metric_collect: MClientMetrics (type 29) is not represented and
	// REQUEST_OPEN sends an empty metric spec. The MDS never reads the bit:
	// MetricsHandler::handle_client_metrics accepts metrics from any session,
	// and the native client sends them only when rank 0 advertises the bit.
	// Telemetry values belong to the I/O layer and the MDS sends no
	// acknowledgement, so there is no caller contract or oracle yet.
	{"metric_collect", deferred},
	{"alternate_name", baseline},
	// notify_session_state: handle_client_session reads it from the
	// supported features of a duplicate REQUEST_OPEN on an open session and
	// then resends OPEN (and FORCE_RO on a read-only MDS). Supporting it needs
	// that duplicate-REQUEST_OPEN path; one REQUEST_OPEN is sent per local
	// session.
	{"notify_session_state", deferred},
	// op_getvxattr: Server::handle_client_getvxattr does not read the bit.
	// CEPH_MDS_OP_GETVXATTR (OpGetVxattr) travels through raw Submit with the
	// inode as the path and the vxattr name as the second path; the reply
	// extra is a v1/compat1 envelope around the text, or the result is
	// -ENODATA. The consumer picks the vxattr, checks the MDS advertisement
	// and interprets the text; the library adds no codec and no exported
	// constant. TestCephMDSCephFSConsumerRequestsIntegration sends it from a
	// session without the bit and compares seven texts and two -ENODATA
	// answers with the stock client; the charmap leaf of
	// TestCephMDSCephFSRanksIntegration does so on both ranks.
	{"op_getvxattr", consumerOwned},
	{"32bits_retry_fwd", baseline},
	{"new_snaprealm_info", baseline},
	{"has_owner_uidgid", baseline},
	// client_mds_auth_caps: the client must enforce MDS auth caps before
	// buffering changes; the library enforces no grant. Without the bit,
	// handle_client_session and handle_client_reconnect add a session holding
	// a root_squash grant to the broken root_squash clients, which the beacon
	// reports as a health error (by source; not observed live).
	{"client_mds_auth_caps", consumerOwned},
	// charmap: the MDS adds a directory's charmap to every REPLY_ENCODING
	// InodeStat without testing the bit, and InodeStat.DecodeOptionalMetadata
	// decodes it (TestDecodeOptionalMetadataMatchesFreshNative; the charmap
	// leaf of TestCephMDSCephFSPushIntegration reads it with a session that
	// lacks the bit). Entry creation in a charmap directory answers -EPERM
	// without the bit. The bit names no message format: it is the client's
	// promise to validate, normalize and fold names and send the original as
	// the alternate name, which the MDS does not check. Options
	// EnableCephFSCharmap is that explicit caller statement and declares it
	// (TestCephMDSCephFSCharmapIntegration compares the transformed names and
	// alternate names with a stock client, and reconnects across a restart
	// while the filesystem requires the bit, which the new incarnation then
	// refuses to a default CephFS session; TestCephMDSSessionsIntegration
	// admits it to a filesystem that requires the bit). The charmap leaf of
	// TestCephMDSCephFSRanksIntegration repeats the metadata, the -EPERM gate
	// and the opt-in creation on a directory whose fragment rank 1 holds.
	{"charmap", optIn},
	// blockdiff: Server::handle_client_file_blockdiff does not read the bit.
	// CEPH_MDS_OP_FILE_BLOCKDIFF (OpFileBlockdiff) travels through raw Submit
	// with the scan index and page size in the arguments and the two
	// snapshot files as paths; the reply extra is BlockDiff (rval, next scan
	// index, extents). Snapshot selection, paging and block mapping are the
	// consumer's; the library adds no codec and no exported constant.
	// TestCephMDSCephFSConsumerRequestsIntegration pages a diff from a
	// session without the bit and compares every page with the stock
	// client's ceph_file_blockdiff.
	{"blockdiff", consumerOwned},
}

// TestClientExtensionDecisionsCoverEveryPinnedFeature binds the decision
// table to the declared bitsets: a declared bit is baseline, a withheld bit
// has a decision, the CephFS profile includes every session-profile bit, the
// opt-in profile adds exactly the opt-in bits to the CephFS profile, no
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
		if charmap := FeatureBitsetHas(CephFSCharmapClientFeatures, uint(bit)); charmap != (cephfs || feature.decision == optIn) {
			t.Errorf("bit %d (%s): the opt-in profile must be the CephFS profile plus the opt-in bits, declared %v", bit, feature.name, charmap)
		}
		if feature.decision < baseline || feature.decision > optIn {
			t.Errorf("bit %d (%s): no decision", bit, feature.name)
		}
	}
	for _, profile := range [][]byte{ClientFeatures, CephFSClientFeatures, CephFSCharmapClientFeatures} {
		for bit := uint(len(tentacleCephFSFeatures)); bit < uint(len(profile))*8; bit++ {
			if FeatureBitsetHas(profile, bit) {
				t.Errorf("declared bit %d is beyond the pinned feature list", bit)
			}
		}
	}
	for name, bit := range map[string]int{"reply_encoding": FeatureReplyEncoding, "reclaim_client": FeatureReclaimClient,
		"multi_reconnect": FeatureMultiReconnect, "deleg_ino": FeatureDelegIno, "32bits_retry_fwd": Feature32BitsRetryFwd,
		"new_snaprealm_info": FeatureNewSnaprealmInfo, "has_owner_uidgid": FeatureHasOwnerUIDGID, "charmap": FeatureCharmap} {
		if tentacleCephFSFeatures[bit].name != name {
			t.Errorf("Go feature constant for %s is bit %d (%s)", name, bit, tentacleCephFSFeatures[bit].name)
		}
	}
}
