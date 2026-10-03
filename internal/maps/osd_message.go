package maps

import (
	"bytes"
	"fmt"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

const osdMapMessage uint16 = 41

// MaxOSDMapBlobs bounds the aggregate number of full and incremental map
// entries in one message, independently of the total byte limit.
const MaxOSDMapBlobs = 4096

// OSDBlob retains one map's container epoch and opaque native encoding.
// Data borrows from the returned OSDMessage's owned RawFront.
type OSDBlob struct {
	Epoch uint32
	Data  []byte
}

// OSDMessage preserves a MOSDMap envelope without decoding or applying its
// nested OSDMap/Incremental objects. Each container is in ascending epoch order.
type OSDMessage struct {
	FSID [16]byte
	// OldestMap is Tentacle's cluster_osdmap_trim_lower_bound, not a promise
	// that the message contains the sender's entire retained history.
	// Both bounds are absent (zero) in the negotiated version-1 envelope.
	OldestMap, NewestMap uint32
	FullMaps             []OSDBlob
	IncrementalMaps      []OSDBlob
	RawFront             []byte
}

// DecodeOSDMessage reads the v1 and v4 envelopes selected by the fixed Tentacle
// encoder for this client's current features. Supporting the v1 wire branch
// does not add support for Ceph release families earlier than Tentacle.
// Reference: Ceph 20.2.4, commit 7f793731f1b39eb4f465e960113d2363c311b964,
// src/messages/MOSDMap.h. Its LGPL-2.1 source is not copied here.
func DecodeOSDMessage(m msgr.MessageData, limit uint32) (OSDMessage, error) {
	var result OSDMessage
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if uint64(41)+uint64(len(m.Front))+uint64(len(m.Middle))+uint64(len(m.Data)) > uint64(limit) {
		return result, wire.ErrLimit
	}
	if m.Type != osdMapMessage || len(m.Middle) != 0 || len(m.Data) != 0 {
		return result, msgr.ErrFrame
	}
	result.RawFront = bytes.Clone(m.Front)
	if m.Version != 1 && m.Version != 4 || m.Version == 1 && m.CompatVersion != 1 || m.Version == 4 && m.CompatVersion != 3 {
		return result, wire.ErrVersion
	}
	d := wire.NewDecoderLimit(result.RawFront, limit)
	copy(result.FSID[:], d.Raw(16))
	result.IncrementalMaps = decodeOSDBlobs(d, MaxOSDMapBlobs)
	result.FullMaps = decodeOSDBlobs(d, MaxOSDMapBlobs-uint32(len(result.IncrementalMaps)))
	if m.Version == 4 {
		result.OldestMap, result.NewestMap = d.U32(), d.U32()
		// The obsolete gap_removed_snaps map is always encoded as an empty
		// map by Tentacle. Preserve its count in RawFront; never skip a
		// nonempty payload whose snapshot schema this codec does not support.
		if d.U32() != 0 {
			d.Fail(fmt.Errorf("%w: nonempty OSD map removed-snapshot payload", wire.ErrVersion))
		}
		if result.OldestMap > result.NewestMap {
			d.Fail(msgr.ErrFrame)
		}
	}
	if result.FSID == [16]byte{} {
		d.Fail(msgr.ErrFrame)
	}
	return result, d.Done()
}

func decodeOSDBlobs(d *wire.Decoder, maximum uint32) []OSDBlob {
	n := d.Count(8, maximum) // epoch + byte-length prefix, before allocation.
	if n == 0 {
		return nil
	}
	result := make([]OSDBlob, 0, n)
	var previous uint32
	for i := 0; i < n && d.Err() == nil; i++ {
		blob := OSDBlob{Epoch: d.U32(), Data: d.Bytes()}
		if blob.Epoch == 0 || blob.Epoch <= previous || len(blob.Data) == 0 {
			d.Fail(msgr.ErrFrame)
		}
		if d.Err() != nil {
			break
		}
		result = append(result, blob)
		previous = blob.Epoch
	}
	return result
}
