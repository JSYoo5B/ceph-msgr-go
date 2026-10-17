package cephmsgr

import (
	"fmt"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
)

func validateOSDProfile(options Options) error {
	if !options.OSDProfile.valid() {
		return fmt.Errorf("%w: unknown OSD profile", ErrUnsupportedFeatures)
	}
	if options.OSDProfile.replicatedPG() && !options.EnableOSD {
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

// OSDReplicatedPlacement is selected by the caller from its applied map.
// PG.Seed is the actual folded PG, distinct from Object.Hash. The connection
// primary must match Primary. Replicated requests always use NO_SHARD (-1).
type OSDReplicatedPlacement struct {
	PG      OSDPG
	Primary int32
}

func cloneOSDPlacement(p *OSDReplicatedPlacement) *OSDReplicatedPlacement {
	if p == nil {
		return nil
	}
	copy := *p
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
