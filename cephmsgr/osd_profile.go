package cephmsgr

import (
	"fmt"

	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
)

func validateOSDProfile(options Options) error {
	if options.OSDProfile != OSDProfileObjectHash && options.OSDProfile != OSDProfileReplicatedPG {
		return fmt.Errorf("%w: unknown OSD profile", ErrUnsupportedFeatures)
	}
	if options.OSDProfile == OSDProfileReplicatedPG && !options.EnableOSD {
		return ErrOSDDisabled
	}
	return nil
}

// OSDProfile selects one implemented wire contract, never arbitrary features.
type OSDProfile uint8

const (
	// OSDProfileObjectHash uses v6 requests with a caller-supplied raw hash.
	OSDProfileObjectHash OSDProfile = iota
	// OSDProfileReplicatedPG uses v8 requests with an explicit actual PG,
	// HASHPSPOOL and bounded backoff. It does not implement CRUSH or routing.
	OSDProfileReplicatedPG
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
	if o.client.options.OSDProfile != OSDProfileReplicatedPG || p == nil || request.Object.Pool < 0 || p.PG.Pool != uint64(request.Object.Pool) || p.Primary != o.target.ID {
		return fmt.Errorf("%w: replicated request requires its explicit PG and connection primary", osd.ErrRequest)
	}
	return nil
}
