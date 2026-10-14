package cephmsgr

import "github.com/jsyoo5b/ceph-msgr-go/internal/osd"

const (
	OSDGetOmapKeys         OSDOpcode = OSDOpcode(osd.OmapGetKeys)
	OSDGetOmapValues       OSDOpcode = OSDOpcode(osd.OmapGetValues)
	OSDGetOmapHeader       OSDOpcode = OSDOpcode(osd.OmapGetHeader)
	OSDGetOmapValuesByKeys OSDOpcode = OSDOpcode(osd.OmapGetValuesByKeys)
	OSDSetOmapValues       OSDOpcode = OSDOpcode(osd.OmapSetValues)
	OSDSetOmapHeader       OSDOpcode = OSDOpcode(osd.OmapSetHeader)
	OSDClearOmap           OSDOpcode = OSDOpcode(osd.OmapClear)
	OSDRemoveOmapKeys      OSDOpcode = OSDOpcode(osd.OmapRemoveKeys)
)

// OSDOmapEntry is one SetOmapValues key/value pair. Key holds length-aware
// wire bytes. A nil or empty Value stores an empty value; it never removes
// the key.
type OSDOmapEntry struct {
	Key   string
	Value []byte
}

// OSDOmapInput selects one raw OMAP read page, one explicit key set, one
// header read or one OMAP mutation. Each opcode admits only its own fields;
// StartAfter, Prefix, Keys and entry keys contain length-aware wire bytes and
// no text normalization occurs.
//
// Page reads start strictly after StartAfter and MaxEntries may be zero.
// GetOmapKeys accepts no Prefix. GetOmapValuesByKeys accepts only Keys.
// RemoveOmapKeys accepts only Keys; SetOmapValues accepts only Entries;
// SetOmapHeader accepts only Header; GetOmapHeader and ClearOmap accept no
// input. Duplicate keys are rejected and encoding sorts copies of Keys and
// Entries in native byte order. For the opcode's own field, nil and empty
// Keys, Entries and Header are explicit empty inputs: an empty header is not
// ClearOmap, and an empty SetOmapValues or RemoveOmapKeys is still transmitted
// as a mutation. Any other opcode rejects even an empty non-nil slice.
// ClearOmap removes the server's header as well as its values.
//
// MaxOutputBytes is a positive local receipt budget for the four reads and
// zero for mutations; it is not a remote byte limit. Page output is a native
// set/map followed by a bool more byte; ByKeys output is only a native map;
// header output is the raw header bytes. Operation Data and RawData preserve
// those bytes. Callers interpret outputs and choose subsequent pages. There is
// no automatic pagination or consistency guarantee across separate requests.
// An OMAP read after any OMAP mutation or Delete in the same vector, including
// FAILOK, is rejected with ErrUnsupportedOSDOperation. In a vector containing a
// mutation, Tentacle refuses any per-op output above its configured
// osd_max_write_op_reply_len (64 bytes by default) with -EOVERFLOW and does not
// commit the transaction; the client returns that server code unchanged.
type OSDOmapInput struct {
	StartAfter, Prefix string
	MaxEntries         uint64
	Keys               []string
	Entries            []OSDOmapEntry
	Header             []byte
	MaxOutputBytes     uint32
}

// omapWireInput copies only entry headers; key and value bytes are copied by
// the encoder after the complete request has been bounded.
func omapWireInput(input OSDOmapInput) osd.OmapInput {
	result := osd.OmapInput{StartAfter: input.StartAfter, Prefix: input.Prefix,
		MaxEntries: input.MaxEntries, Keys: input.Keys, Header: input.Header, MaxOutputBytes: input.MaxOutputBytes}
	if input.Entries != nil {
		result.Entries = make([]osd.OmapEntry, len(input.Entries))
		for i, entry := range input.Entries {
			result.Entries[i] = osd.OmapEntry{Key: entry.Key, Value: entry.Value}
		}
	}
	return result
}
