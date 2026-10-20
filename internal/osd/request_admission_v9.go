package osd

import "github.com/JSYoo5B/ceph-msgr-go/internal/msgr"

// DecodeRequestV9AdmissionTarget projects only trusted, locally encoded
// untraced v9/compat3 messages for the outgoing backoff gate. It rejects valid
// nonempty OTEL contexts rather than interpreting trace or accepting another
// wire profile. Native source: pinned Tentacle MOSDOp.h:398-445 and
// common/tracer.h !HAVE_JAEGER, commit
// 7f793731f1b39eb4f465e960113d2363c311b964 (LGPL-2.1, no code copied).
func DecodeRequestV9AdmissionTarget(m msgr.MessageData, limit uint32) (RequestAdmissionTarget, error) {
	return decodeReplicatedAdmissionTarget(m, limit, 9)
}
