package osd

import "github.com/jsyoo5b/ceph-msgr-go/internal/msgr"

// EncodePGListRequestV9 encodes dedicated head-only PGNLS in untraced MOSDOp
// v9/compat3. Pinned Ceph 20.2.4 commit 7f793731f1b39eb4f465e960113d2363c311b964:
// MOSDOp.h:398-427, common/tracer.h !HAVE_JAEGER and Objecter.h::pg_read.
// LGPL-2.1 source; independently written. Actual PG and raw hash remain the
// caller's explicit PG seed; empty oid retains the explicit locator hash.
func EncodePGListRequestV9(r PGListRequest, features uint64, limit uint32) (msgr.MessageData, error) {
	if !v9RequestFeatures(features) {
		return msgr.MessageData{}, ErrFeatures
	}
	return encodeReplicatedPGList(r, features, limit, 9)
}
