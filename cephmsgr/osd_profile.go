package cephmsgr

import (
	"fmt"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
)

func validateOSDProfile(options Options) error {
	if !options.OSDProfile.valid() {
		return fmt.Errorf("%w: unknown OSD profile", ErrUnsupportedFeatures)
	}
	if (options.OSDProfile.replicatedPG() || options.EnableOSDCachePools) && !options.EnableOSD {
		return ErrOSDDisabled
	}
	return nil
}

// OSDProfile selects one implemented wire contract, never arbitrary features.
type OSDProfile uint8

const (
	// OSDProfileObjectHash uses v6 requests with a caller-supplied raw hash.
	OSDProfileObjectHash OSDProfile = iota
	// OSDProfileReplicatedPG uses v8 requests and v6 replies with an explicit
	// actual PG, HASHPSPOOL and bounded backoff. It does not implement CRUSH or routing.
	OSDProfileReplicatedPG
	// OSDProfileReplicatedPGReplyV8 uses the same explicit replicated placement
	// and v8 requests, with strict v8/compat2 replies. It additionally requires
	// NEW_OSDOPREPLY_ENCODING before Dial; it does not select placement, follow
	// redirects, replay requests or establish general CRUSH/EC compatibility.
	OSDProfileReplicatedPGReplyV8
	// OSDProfileReplicatedPGRequestV9ReplyV8 uses untraced v9/compat3 requests
	// and strict v8/compat2 replies with explicit replicated placement. The
	// empty OTEL envelope is supported; nonempty trace input, CRUSH, erasure
	// coding, automatic routing and uncertain-request replay are not provided.
	OSDProfileReplicatedPGRequestV9ReplyV8
)

// OSDPlacement is selected by the caller from its applied map.
// PG.Seed is the actual folded PG, distinct from Object.Hash. The connection
// primary must match Primary. A replicated pool's requests use NO_SHARD (-1).
// For an erasure-coded pool set Erasure and Shard to the shard the connection
// target holds in the PG's acting set (its position there), which the OSD
// requires in spg_t; the caller derives it from its own map, as for Primary.
// For an optimized pool whose PG has a pg_temp also set PrimaryFirst.
type OSDPlacement struct {
	PG      OSDPG
	Primary int32
	Erasure bool
	Shard   uint8
	// PrimaryFirst is set exactly when the pool has allow_ec_optimizations and
	// the caller's map holds a pg_temp for this PG.
	PrimaryFirst *OSDPrimaryFirst
}

// OSDPrimaryFirst carries the two pool facts with which an OSD reinterprets
// the erasure-coded shard sent by this client. OSD sessions here do not
// declare SERVER_TENTACLE, so for an optimized pool (allow_ec_optimizations)
// whose PG has a pg_temp the OSD reads spg_t's shard in the pre-Tentacle
// numbering, in which pg_temp lists the primary-eligible shards first, and
// converts it back with OSDMap::pgtemp_undo_primaryfirst (Ceph 20.2.4
// osd/OSD.cc ms_fast_dispatch and dispatch_session_waiting). With it set the
// client encodes the actual Shard in that numbering
// (OSDMap::pgtemp_primaryfirst), so the OSD resolves the shard the caller
// named. Without it a non-zero shard reaches another shard's PG, which
// answers nothing.
type OSDPrimaryFirst struct {
	// PoolSize is the pool's size (k+m) and NonprimaryShards the number of
	// its nonprimary_shards (k-1 for a plugin without a chunk remapping).
	PoolSize, NonprimaryShards uint8
}

// wireShard is the spg_t shard: NO_SHARD for replicated placement.
func (p *OSDPlacement) wireShard() int8 {
	if p == nil || !p.Erasure {
		return -1
	}
	return int8(primaryFirstShard(p.Shard, p.PrimaryFirst))
}

func primaryFirstShard(shard uint8, f *OSDPrimaryFirst) uint8 {
	if f == nil || shard == 0 {
		return shard
	}
	parity := f.PoolSize - f.NonprimaryShards - 1
	if shard >= f.PoolSize-parity {
		return shard + parity + 1 - f.PoolSize
	}
	return shard + parity
}

func validShardPlacement(erasure bool, shard uint8, f *OSDPrimaryFirst) bool {
	if shard > 127 || !erasure && (shard != 0 || f != nil) {
		return false
	}
	return f == nil || f.PoolSize >= 2 && f.PoolSize <= 128 && f.NonprimaryShards < f.PoolSize && shard < f.PoolSize
}

func cloneOSDPrimaryFirst(f *OSDPrimaryFirst) *OSDPrimaryFirst {
	if f == nil {
		return nil
	}
	copy := *f
	return &copy
}

func cloneOSDPlacement(p *OSDPlacement) *OSDPlacement {
	if p == nil {
		return nil
	}
	copy := *p
	copy.PrimaryFirst = cloneOSDPrimaryFirst(p.PrimaryFirst)
	return &copy
}

func (o *OSDConnection) validatePlacement(request OSDRequest) error {
	if o.client.options.OSDProfile == OSDProfileObjectHash {
		if request.Placement != nil {
			return fmt.Errorf("%w: actual PG requires the replicated PG profile", osd.ErrRequest)
		}
		return nil
	}
	p := request.Placement
	if p != nil && !validShardPlacement(p.Erasure, p.Shard, p.PrimaryFirst) {
		return fmt.Errorf("%w: a shard is meaningful only for erasure-coded placement", osd.ErrRequest)
	}
	if !o.client.options.OSDProfile.replicatedPG() || p == nil || request.Object.Pool < 0 || p.PG.Pool != uint64(request.Object.Pool) || p.Primary != o.target.ID && !request.BalanceReads && !request.LocalizeReads {
		return fmt.Errorf("%w: replicated request requires its explicit PG and connection primary", osd.ErrRequest)
	}
	return nil
}

// These fixed predicates describe the replicated wire contracts. Keep shared
// placement, control and request capabilities independent of reply selection.
func (p OSDProfile) valid() bool { return p == OSDProfileObjectHash || p.replicatedPG() }
func (p OSDProfile) replicatedPG() bool {
	return p == OSDProfileReplicatedPG || p == OSDProfileReplicatedPGReplyV8 || p == OSDProfileReplicatedPGRequestV9ReplyV8
}
func (p OSDProfile) replyV8() bool   { return p == OSDProfileReplicatedPGReplyV8 || p.requestV9() }
func (p OSDProfile) requestV9() bool { return p == OSDProfileReplicatedPGRequestV9ReplyV8 }

func (o *OSDConnection) decodeRequestAdmissionTarget(m msgr.MessageData) (osd.RequestAdmissionTarget, error) {
	switch o.client.options.OSDProfile {
	case OSDProfileReplicatedPGRequestV9ReplyV8:
		return osd.DecodeRequestV9AdmissionTarget(m, o.client.options.MaxFrameSize)
	case OSDProfileReplicatedPG, OSDProfileReplicatedPGReplyV8:
		return osd.DecodeRequestV8AdmissionTarget(m, o.client.options.MaxFrameSize)
	default:
		return osd.RequestAdmissionTarget{}, osd.ErrFeatures
	}
}

// decodeReply selects only the pre-Dial profile's strict envelope. Header
// inspection never changes the selected codec or permits protocol fallback.
func (o *OSDConnection) decodeReply(m msgr.MessageData) (osd.Reply, error) {
	profile := o.client.options.OSDProfile
	if !profile.valid() {
		return osd.Reply{}, fmt.Errorf("%w: unknown OSD profile", osd.ErrFeatures)
	}
	if profile.replyV8() {
		if o.features&(1<<58) == 0 {
			return osd.Reply{}, fmt.Errorf("%w: v8 replies require negotiated NEW_OSDOPREPLY_ENCODING", osd.ErrFeatures)
		}
		return osd.DecodeReplyV8(m, o.client.options.MaxFrameSize)
	}
	return osd.DecodeReply(m, o.client.options.MaxFrameSize)
}
