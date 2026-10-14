package osd

import (
	"errors"
	"math"
	"sort"
	"strings"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// OmapInput admits one bounded read page or one explicit key set. Strings are
// length-aware wire bytes, including empty, NUL and non-UTF-8 strings. The local
// output budget does not become a remote byte-limit argument or pagination loop.
type OmapInput struct {
	StartAfter, Prefix string
	MaxEntries         uint64
	Keys               []string
	MaxOutputBytes     uint32
}

func IsOmapOperation(code uint16) bool {
	return code == OmapGetKeys || code == OmapGetValues || code == OmapGetValuesByKeys
}

func omapInputEmpty(input OmapInput) bool {
	return input.StartAfter == "" && input.Prefix == "" && input.MaxEntries == 0 && input.Keys == nil && input.MaxOutputBytes == 0
}

// Bound every encoded field before sorting or allocating an encoded buffer.
func omapInputBytes(op Operation, limit uint32) (uint64, error) {
	if op.Offset != 0 || op.Length != 0 || op.Version != 0 || len(op.Data) != 0 || op.Xattr != (XattrInput{}) {
		return 0, ErrRequest
	}
	input := op.Omap
	minimum := uint32(5)
	if op.Code == OmapGetValuesByKeys {
		minimum = 4
	}
	if input.MaxOutputBytes < minimum {
		return 0, ErrRequest
	}
	if input.MaxOutputBytes > limit {
		return 0, wire.ErrLimit
	}
	switch op.Code {
	case OmapGetKeys, OmapGetValues:
		if input.Keys != nil || op.Code == OmapGetKeys && input.Prefix != "" {
			return 0, ErrRequest
		}
		if uint64(len(input.StartAfter)) > math.MaxUint32 || uint64(len(input.Prefix)) > math.MaxUint32 {
			return 0, wire.ErrLimit
		}
		if op.Code == OmapGetKeys {
			return boundedBytes(limit, 12, uint64(len(input.StartAfter)))
		}
		return boundedBytes(limit, 16, uint64(len(input.StartAfter)), uint64(len(input.Prefix)))
	case OmapGetValuesByKeys:
		if input.StartAfter != "" || input.Prefix != "" || input.MaxEntries != 0 {
			return 0, ErrRequest
		}
		if uint64(len(input.Keys)) > math.MaxUint32 {
			return 0, wire.ErrLimit
		}
		total, err := boundedBytes(limit, 4, uint64(len(input.Keys))*4)
		if err != nil {
			return 0, err
		}
		for _, key := range input.Keys {
			if uint64(len(key)) > math.MaxUint32 {
				return 0, wire.ErrLimit
			}
			total, err = boundedBytes(limit, total, uint64(len(key)))
			if err != nil {
				return 0, err
			}
		}
		return total, nil
	}
	return 0, ErrOperation
}

func canonicalOmapKeys(keys []string) ([]string, error) {
	ordered := append([]string(nil), keys...)
	sort.Strings(ordered)
	for i := 1; i < len(ordered); i++ {
		if ordered[i] == ordered[i-1] {
			return nil, ErrRequest
		}
	}
	return ordered, nil
}

func validateOmapOperation(op Operation, limit uint32) (uint64, uint64, error) {
	size, err := omapInputBytes(op, limit)
	if err != nil {
		return 0, 0, err
	}
	if op.Code == OmapGetValuesByKeys {
		if _, err := canonicalOmapKeys(op.Omap.Keys); err != nil {
			return 0, 0, err
		}
	}
	return FlagRead, size, nil
}

// Native Objecter.h 953/992/1065 derives the extent length from this encoding.
// Use the correct std::string GETVALS overload's start/count/prefix ordering.
func encodeOmapInput(op Operation) []byte {
	e := wire.Encoder{}
	if op.Code == OmapGetValuesByKeys {
		keys, _ := canonicalOmapKeys(op.Omap.Keys) // already admitted before allocation
		e.U32(uint32(len(keys)))
		for _, key := range keys {
			e.String(key)
		}
	} else {
		e.String(op.Omap.StartAfter)
		e.U64(op.Omap.MaxEntries)
		if op.Code == OmapGetValues {
			e.String(op.Omap.Prefix)
		}
	}
	return e.Data
}

// ValidateOmapReply preserves raw output, validating only its bounded wire
// framing and submitted read constraints. A failed/unexecuted op need not have
// a successful collection payload. Identity, signed results and completion are
// independently validated by the session's caller.
func ValidateOmapReply(requested Operation, reply OperationResult, success bool) error {
	if !IsOmapOperation(requested.Code) {
		return ErrOperation
	}
	inputBytes, err := omapInputBytes(requested, math.MaxUint32)
	if err != nil {
		return err
	}
	if reply.Code != requested.Code || reply.Offset != 0 || reply.Length != inputBytes || reply.TruncateSize != 0 || reply.TruncateSequence != 0 || reply.Xattr != (XattrResult{}) {
		return errors.New("OSD OMAP reply union differs from submitted operation")
	}
	if reply.PayloadLength > requested.Omap.MaxOutputBytes {
		return wire.ErrLimit
	}
	if uint64(len(reply.Data)) != uint64(reply.PayloadLength) {
		return errors.New("OSD OMAP output length differs from reply payload")
	}
	if !success {
		return nil
	}
	minimum, itemMinimum := uint32(5), uint32(4)
	maxCount := requested.Omap.MaxEntries
	var keys []string
	if requested.Code == OmapGetValuesByKeys {
		minimum, itemMinimum = 4, 8
		keys, err = canonicalOmapKeys(requested.Omap.Keys)
		if err != nil {
			return err
		}
		maxCount = uint64(len(keys))
	} else if requested.Code == OmapGetValues {
		itemMinimum = 8
	}
	if reply.PayloadLength < minimum {
		return errors.New("OSD OMAP output lacks required collection framing")
	}
	if budgetCount := uint64(requested.Omap.MaxOutputBytes / itemMinimum); maxCount > budgetCount {
		maxCount = budgetCount
	}
	d := wire.NewDecoderLimit(reply.Data, requested.Omap.MaxOutputBytes)
	count := d.Count(itemMinimum, uint32(maxCount))
	previous := ""
	for i := 0; i < count && d.Err() == nil; i++ {
		key := d.String()
		if i > 0 && key <= previous {
			d.Fail(errors.New("OSD OMAP output keys are not strictly ordered"))
		}
		if requested.Code == OmapGetValuesByKeys {
			index := sort.SearchStrings(keys, key)
			if index == len(keys) || keys[index] != key {
				d.Fail(errors.New("OSD OMAP output contains an unrequested key"))
			}
		} else if key <= requested.Omap.StartAfter || requested.Code == OmapGetValues && !strings.HasPrefix(key, requested.Omap.Prefix) {
			d.Fail(errors.New("OSD OMAP output differs from submitted cursor/prefix"))
		}
		previous = key
		if requested.Code != OmapGetKeys {
			_ = d.Bytes()
		}
	}
	if requested.Code != OmapGetValuesByKeys {
		_ = d.Bool()
	}
	return d.Done()
}
