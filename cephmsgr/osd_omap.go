package cephmsgr

import "github.com/jsyoo5b/ceph-msgr-go/internal/osd"

const (
	OSDGetOmapKeys         OSDOpcode = OSDOpcode(osd.OmapGetKeys)
	OSDGetOmapValues       OSDOpcode = OSDOpcode(osd.OmapGetValues)
	OSDGetOmapHeader       OSDOpcode = OSDOpcode(osd.OmapGetHeader)
	OSDGetOmapValuesByKeys OSDOpcode = OSDOpcode(osd.OmapGetValuesByKeys)
	OSDCompareOmap         OSDOpcode = OSDOpcode(osd.OmapCompare)
	OSDSetOmapValues       OSDOpcode = OSDOpcode(osd.OmapSetValues)
	OSDSetOmapHeader       OSDOpcode = OSDOpcode(osd.OmapSetHeader)
	OSDClearOmap           OSDOpcode = OSDOpcode(osd.OmapClear)
	OSDRemoveOmapKeys      OSDOpcode = OSDOpcode(osd.OmapRemoveKeys)
	OSDRemoveOmapKeyRange  OSDOpcode = OSDOpcode(osd.OmapRemoveKeyRange)
)

// OSDOmapEntry is one SetOmapValues key/value pair. Key holds length-aware
// wire bytes. A nil or empty Value stores an empty value; it never removes
// the key.
type OSDOmapEntry struct {
	Key   string
	Value []byte
}

// OSDOmapComparison is a native OMAP_CMP identifier. Unlike
// OSDXattrComparison, the stored value is the LEFT operand: OSDOmapLess
// asserts stored < Value. Ordering follows the pinned server's bufferlist
// comparison, which compares bytes as signed char (Tentacle builds with
// -fsigned-char), then length; it is not bytes.Compare order.
type OSDOmapComparison int32

const (
	OSDOmapEqual   OSDOmapComparison = OSDOmapComparison(osd.OmapCompareEqual)
	OSDOmapGreater OSDOmapComparison = OSDOmapComparison(osd.OmapCompareGreater)
	OSDOmapLess    OSDOmapComparison = OSDOmapComparison(osd.OmapCompareLess)
)

// OSDOmapAssertion is one CompareOmap assertion. Key holds length-aware wire
// bytes; a missing key is compared as an empty value by the server.
type OSDOmapAssertion struct {
	Key        string
	Value      []byte
	Comparison OSDOmapComparison
}

// OSDOmapInput selects one raw OMAP read page, one explicit key set, one
// header read, one comparison set or one OMAP mutation. Each opcode admits only its own fields;
// StartAfter, Prefix, Keys and entry keys contain length-aware wire bytes and
// no text normalization occurs.
//
// Page reads start strictly after StartAfter and MaxEntries may be zero.
// GetOmapKeys accepts no Prefix. GetOmapValuesByKeys accepts only Keys.
// RemoveOmapKeys accepts only Keys; SetOmapValues accepts only Entries;
// SetOmapHeader accepts only Header; CompareOmap accepts only Assertions;
// RemoveOmapKeyRange accepts only RangeBegin (inclusive) and RangeEnd
// (exclusive) byte strings, and refuses a reversed range before transmission;
// GetOmapHeader and ClearOmap accept no input. Duplicate keys are rejected and
// encoding sorts copies of Keys, Entries and Assertions in native byte order. For the opcode's own field, nil and empty
// Keys, Entries, Assertions and Header are explicit empty inputs: an empty
// assertion set only checks object existence, an empty header is not
// ClearOmap, and an empty SetOmapValues or RemoveOmapKeys is still transmitted
// as a mutation. Any other opcode rejects even an empty non-nil slice.
// ClearOmap removes the server's header as well as its values.
//
// The server evaluates assertions in that key order and stops at the first
// failure with -ECANCELED; a missing object is -ENOENT. Only OSDOmapEqual,
// OSDOmapGreater and OSDOmapLess are sent; other identifiers are rejected
// with ErrUnsupportedOSDOperation. CompareOmap returns no output.
//
// MaxOutputBytes is a positive local receipt budget for the four reads and
// zero for comparisons and mutations; it is not a remote byte limit. Page output is a native
// set/map followed by a bool more byte; ByKeys output is only a native map;
// header output is the raw header bytes. Operation Data and RawData preserve
// those bytes. Callers interpret outputs and choose subsequent pages. There is
// no automatic pagination or consistency guarantee across separate requests.
// An OMAP read or comparison after any OMAP mutation or Delete in the same
// vector, including FAILOK, is rejected with ErrUnsupportedOSDOperation;
// compare-before-write is supported. In a vector containing a
// mutation, Tentacle refuses any per-op output above its configured
// osd_max_write_op_reply_len (64 bytes by default) with -EOVERFLOW and does not
// commit the transaction; the client returns that server code unchanged.
type OSDOmapInput struct {
	StartAfter, Prefix   string
	MaxEntries           uint64
	Keys                 []string
	Entries              []OSDOmapEntry
	Assertions           []OSDOmapAssertion
	Header               []byte
	RangeBegin, RangeEnd string
	MaxOutputBytes       uint32
}

// omapWireInput copies only entry/assertion headers; key and value bytes are copied by
// the encoder after the complete request has been bounded.
func omapWireInput(input OSDOmapInput) osd.OmapInput {
	result := osd.OmapInput{StartAfter: input.StartAfter, Prefix: input.Prefix,
		MaxEntries: input.MaxEntries, Keys: input.Keys, Header: input.Header,
		RangeBegin: input.RangeBegin, RangeEnd: input.RangeEnd, MaxOutputBytes: input.MaxOutputBytes}
	if input.Entries != nil {
		result.Entries = make([]osd.OmapEntry, len(input.Entries))
		for i, entry := range input.Entries {
			result.Entries[i] = osd.OmapEntry{Key: entry.Key, Value: entry.Value}
		}
	}
	if input.Assertions != nil {
		result.Assertions = make([]osd.OmapAssertion, len(input.Assertions))
		for i, assertion := range input.Assertions {
			result.Assertions[i] = osd.OmapAssertion{Key: assertion.Key, Value: assertion.Value, Comparison: int32(assertion.Comparison)}
		}
	}
	return result
}
