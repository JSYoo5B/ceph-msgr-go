package cephmsgr

import "github.com/jsyoo5b/ceph-msgr-go/internal/osd"

// OSDMapExt reads logical allocated extents in Offset/Length. Length must be
// nonzero and Offset+Length must not overflow. This bounded subset permits a
// standalone read at ReadSnapshot (zero selects the head), without operation
// flags or a write SnapContext. It reports the store's logical
// allocation map; holes, physical locations and bytes require interpretation
// by the calling layer. Erasure-coded pools are unsupported.
const OSDMapExt OSDOpcode = OSDOpcode(osd.MapExt)

// OSDMapExtInput bounds accepted output, including the four-byte extent count.
// The budget is local; it is not a server-side limit or a pagination size.
type OSDMapExtInput struct{ MaxOutputBytes uint32 }

func publicMapExt(data []byte) []OSDExtent {
	extents, err := osd.DecodeMapExt(data)
	if err != nil {
		return nil
	}
	result := make([]OSDExtent, len(extents))
	for i, e := range extents {
		result[i] = OSDExtent{Offset: e.Offset, Length: e.Length}
	}
	return result
}
