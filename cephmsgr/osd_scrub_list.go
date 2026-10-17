package cephmsgr

import (
	"context"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
)

// OSDInconsistentRequest asks a PG's primary for one page of the scrub errors
// it retained from the PG's last scrub (SCRUBLS, as librados
// get_inconsistent_objects and get_inconsistent_snapsets send). Interval zero
// accepts the current interval; a nonzero Interval no longer current is
// refused with -EAGAIN, so pages of one listing can pin the interval of the
// first. StartAfter is the last object of the previous page (zero for the
// first). MaxReturn bounds the values; MaxOutputBytes the reply payload.
type OSDInconsistentRequest struct {
	ClientIncarnation, MapEpoch uint32
	PG                          OSDPGListTarget
	Interval                    uint32
	Snapsets                    bool
	StartAfter                  OSDScrubObject
	MaxReturn                   uint64
	MaxOutputBytes              uint32
}

// OSDScrubObject names an object in a scrub listing.
type OSDScrubObject struct {
	Name, Namespace string
	Snap            uint64
}

// OSDInconsistentReply carries the PG interval the errors belong to and each
// value as the raw encoded inconsistent object or snapset wrapper
// (common/scrub_types.h); interpreting them belongs to the caller. A PG the
// primary has no scrub results for is refused with -ENOENT.
type OSDInconsistentReply struct {
	OSDReply
	Interval uint32
	Values   [][]byte
}

// ListInconsistent sends exactly one SCRUBLS to the explicit primary. It is a
// read: nothing is scrubbed, repaired or retried.
func (o *OSDConnection) ListInconsistent(ctx context.Context, request OSDInconsistentRequest) (OSDInconsistentReply, error) {
	var result OSDInconsistentReply
	input := osd.ScrubListRequest{ClientInc: request.ClientIncarnation, MapEpoch: request.MapEpoch, PG: osd.PG{Pool: request.PG.Pool, Seed: request.PG.Seed},
		Erasure: request.PG.Erasure, Shard: primaryFirstShard(request.PG.Shard, request.PG.PrimaryFirst), Interval: request.Interval, Snapsets: request.Snapsets,
		StartAfter: osd.ScrubObject{Name: request.StartAfter.Name, Namespace: request.StartAfter.Namespace, Snap: request.StartAfter.Snap}, MaxReturn: request.MaxReturn, MaxOutputBytes: request.MaxOutputBytes}
	encode := func(v9, v8 bool) (msgr.MessageData, error) {
		switch {
		case v9:
			return osd.EncodeScrubListRequestV9(input, o.features, o.client.options.MaxFrameSize)
		case v8:
			return osd.EncodeScrubListRequestV8(input, o.features, o.client.options.MaxFrameSize)
		}
		return osd.EncodeScrubListRequest(input, o.features, o.client.options.MaxFrameSize)
	}
	validate := func(decoded osd.Reply, redirect bool) error {
		return osd.ValidateScrubListReply(input, decoded, redirect)
	}
	reply, decoded, err := o.pgOperation(ctx, request.PG, request.ClientIncarnation, request.MapEpoch, "scrub listing", encode, validate)
	result.OSDReply = reply
	if err != nil {
		return result, err
	}
	page, err := osd.DecodeScrubList(decoded.Operations[0].Data, request.MaxReturn)
	if err != nil {
		return result, err
	}
	result.Interval, result.Values = page.Interval, page.Values
	return result, nil
}
