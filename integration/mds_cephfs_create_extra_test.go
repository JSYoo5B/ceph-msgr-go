package integration_test

import "github.com/JSYoo5B/ceph-msgr-go/cephmsgr"

// cephfsCreatedIno reads the created inode from the extra blob of a CREATE or
// completed-request reply on an unmodified fixture. The CephFS profile
// declares DELEG_INO and the fixtures keep mds_allow_async_dirops at its
// default, so the MDS answers with openc_response_t; ok is false for the bare
// 8-byte form and for anything that does not decode. The bare form is covered
// where the option is turned off (TestCephMDSCephFSCompletionIntegration,
// delegated_inode_reply_forms). Consumer-side interpretation; not product API.
func cephfsCreatedIno(extra []byte) (ino uint64, ok bool) {
	created, err := cephmsgr.MDSReply{Extra: extra}.DecodeCreateExtra()
	return created.Ino, err == nil && created.Delegation
}

// cephfsCreated reports whether extra is the delegated form naming ino.
func cephfsCreated(extra []byte, ino uint64) bool {
	got, ok := cephfsCreatedIno(extra)
	return ok && got == ino
}
