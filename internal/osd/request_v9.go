package osd

import (
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// V9Request is the explicitly routed, untraced replicated MOSDOp request.
// Only native NO_SHARD (-1) is admitted. Trace generation, nonempty OpenTelemetry
// context input and erasure-coded requests are outside this codec's contract.
type V9Request struct {
	Request
	PG    PG
	Shard int8
}

const emptyOTELBytes uint64 = 7

// EncodeRequestV9 encodes pinned Ceph Tentacle 20.2.4 commit
// 7f793731f1b39eb4f465e960113d2363c311b964 MOSDOp.h:398-427 and
// common/tracer.h's official !HAVE_JAEGER branch: v1/compat1, body length 1,
// is_valid=false. This is an untraced wire encoding, not an OTEL SDK or tracing
// workflow. The existing three-u64 trace is also empty. Source is LGPL-2.1;
// this Go implementation copies no native code and requires no native library.
// The full SERVER_SQUID incarnation mask is mandatory. No routing, retry,
// downgrade or protocol-feature advertisement is performed by this encoder.
func EncodeRequestV9(r V9Request, features uint64, limit uint32) (msgr.MessageData, error) {
	if !v9RequestFeatures(features) {
		return msgr.MessageData{}, ErrFeatures
	}
	if r.Shard != -1 {
		return msgr.MessageData{}, ErrRequest
	}
	return encodeReplicatedRequest(replicatedRequest(r), features, limit, 9)
}

func v9RequestFeatures(features uint64) bool {
	const required = featureObjectLocator | featurePGID64 | featureNewOSDOp | featureResendOnSplitMask | featureServerSquidMask
	return features&required == required
}

func encodeEmptyOTEL(e *wire.Encoder) {
	e.Struct(1, 1, []byte{0})
}
