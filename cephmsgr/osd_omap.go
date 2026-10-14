package cephmsgr

import "github.com/jsyoo5b/ceph-msgr-go/internal/osd"

const (
	OSDGetOmapKeys         OSDOpcode = OSDOpcode(osd.OmapGetKeys)
	OSDGetOmapValues       OSDOpcode = OSDOpcode(osd.OmapGetValues)
	OSDGetOmapValuesByKeys OSDOpcode = OSDOpcode(osd.OmapGetValuesByKeys)
)

// OSDOmapInput selects one raw OMAP read page or one explicit key set.
// StartAfter, Prefix and Keys contain length-aware wire bytes; no text
// normalization occurs. Page reads start strictly after StartAfter and
// MaxEntries may be zero. GetOmapKeys accepts no Prefix. ByKeys accepts only
// Keys; duplicate keys are rejected and encoding sorts a copy of the slice.
// Empty values and omitted keys remain distinct in the native output.
//
// MaxOutputBytes is a positive local receipt budget, not a remote byte limit.
// Page output is a native set/map followed by a bool more byte; ByKeys output
// is only a native map. Operation Data and RawData preserve those bytes.
// Callers interpret outputs and choose subsequent pages. There is no automatic
// pagination or consistency guarantee across separate requests.
type OSDOmapInput struct {
	StartAfter, Prefix string
	MaxEntries         uint64
	Keys               []string
	MaxOutputBytes     uint32
}

func omapWireInput(input OSDOmapInput) osd.OmapInput {
	return osd.OmapInput{StartAfter: input.StartAfter, Prefix: input.Prefix,
		MaxEntries: input.MaxEntries, Keys: input.Keys, MaxOutputBytes: input.MaxOutputBytes}
}
