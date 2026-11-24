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
// It is chosen before Dial, applies to the client's MON and OSD sessions and
// cannot change on an existing client; an unknown value fails locally, and a
// replicated profile without EnableOSD fails before network activity.
//
// The replicated profiles add RESEND_ON_SPLIT (the incarnation-2 mask
// 1<<21|1<<57 it shares with RADOS_BACKOFF, OSDMAP_PG_UPMAP,
// CRUSH_CHOOSE_ARGS and SERVER_LUMINOUS), OSDHASHPSPOOL (30) and
// NEW_OSDOP_ENCODING (56) to the OSD sessions' supported and required
// features, and HASHPSPOOL to MON admission; the MON-only CRUSH admission
// bits are never copied to OSD sessions. A peer that requires a missing bit
// fails negotiation, without a weaker retry. The ReplyV8 profile also
// requires NEW_OSDOPREPLY_ENCODING (58), which shares its bit with
// CRUSH_TUNABLES5, and the v9 profile also requires the SERVER_SQUID mask
// 1<<49|1<<57 (ceph/ceph/blob/v20.2.4/src/include/ceph_features.h). These
// bits select encodings only: no profile evaluates CRUSH, applies maps,
// follows redirects or handles PG splits. A request older than the PG's
// current primary interval, split or force-resend epoch can be dropped by the
// OSD without a reply (osd/PG.cc), and the client never replays it. Native
// acceptance covers legacy CRUSH with HASHPSPOOL; default optimal CRUSH, MSR,
// upmap routing and general pool topologies are not established.
//
// Every profile accepts exactly one reply version (v6/compat2, or v8/compat2
// for the ReplyV8 profiles); the received header never selects another
// decoder. The v8 and v9 replies carry no reqid, client incarnation or shard,
// so OSDReply.Submission keeps those locally and validation checks object,
// pool, actual PG, operation vector and retry attempt.
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
	// request carries the canonical empty OTEL envelope (version 1, compat 1,
	// length 1, is_valid false), as a native build without Jaeger sends it;
	// nonempty trace input, CRUSH, erasure coding, automatic routing and
	// uncertain-request replay are not provided.
	OSDProfileReplicatedPGRequestV9ReplyV8
)

// OSDPlacement is selected by the caller from its applied map.
// PG.Seed is the actual folded PG, distinct from Object.Hash. The connection
// primary must match Primary. A replicated pool's requests use NO_SHARD (-1).
// For an erasure-coded pool set Erasure and Shard to the shard the connection
// target holds in the PG's acting set (its position there), which the OSD
// requires in spg_t; the caller derives it from its own map, as for Primary.
// For an optimized pool whose PG has a pg_temp also set PrimaryFirst.
// A request naming NO_SHARD or another shard on an EC PG is treated as
// misdirected and never answered; after a primary change the new primary
// holds a different shard, so derive both from the new map. Operations travel
// unchanged and the pool decides what it supports (an EC pool refuses OMAP
// writes with -95 EOPNOTSUPP). One shape is refused before submission with
// ErrUnsupportedOSDOperation for an erasure-coded placement: a vector that
// combines an extent read (READ, SPARSE_READ, CHECKSUM, CMPEXT) with an
// operation that returns output (STAT, GETXATTR, GETXATTRS, the OMAP reads,
// CALL), because the pinned EC primary runs such a vector again after the
// read and, depending on the order, returns that output once per run and
// runs a CALL's method each time. BalanceReads and LocalizeReads are refused
// the same way: the pinned native client sends such a read to a replica only
// on a replicated pool (osdc/Objecter.cc _calc_target), while the OSD would
// serve it on any shard that is not the primary, a path this client keeps
// away from. The
// object-hash profile sends no shard and the OSD maps the raw hash to the
// primary shard itself; it names no placement, so the client cannot refuse
// either shape there.
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
	if p != nil && p.Erasure && (request.BalanceReads || request.LocalizeReads) {
		return fmt.Errorf("%w: BalanceReads and LocalizeReads are refused for an erasure-coded placement", ErrUnsupportedOSDOperation)
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
