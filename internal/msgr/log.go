package msgr

import (
	"fmt"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

const LogMessage uint16 = 52

// LogBatch is a MON log-service update. Version is the service cursor, distinct
// from the source-specific Sequence carried by each entry.
type LogBatch struct {
	Version uint64
	FSID    [16]byte
	Entries []LogEntry
}

// LogEntry preserves Tentacle's source identity, raw timestamp and priority.
// NameID is the EntityName identifier, without an invented type-name prefix.
// RankNumber is signed because Ceph uses negative ranks for unknown entities.
type LogEntry struct {
	NameType             uint32
	NameID               string
	RankType             uint8
	RankNumber           int64
	Addresses            []Address
	Seconds, Nanoseconds uint32
	Sequence             uint64
	Priority             uint16
	Message, Channel     string
}

// DecodeLog decodes a bounded Tentacle MLog front. Our negotiated
// SERVER_NAUTILUS feature selects LogEntry v5; older entry layouts are not
// implemented. Newer compatible envelopes may append fields. A batch contains
// at most 65536 entries; malformed input returns no partially decoded entries.
func DecodeLog(m MessageData, limit uint32) (batch LogBatch, err error) {
	defer func() {
		if err != nil {
			batch = LogBatch{}
			err = fmt.Errorf("%w: invalid MON log: %w", ErrFrame, err)
		}
	}()
	if m.Type != LogMessage {
		return batch, ErrFrame
	}
	if m.CompatVersion > 1 {
		return batch, wire.ErrVersion
	}
	d := wire.NewDecoderLimit(m.Front, limit)
	batch.Version = d.U64()
	d.U16() // Deprecated session monitor.
	d.U64() // Deprecated session monitor transaction.
	copy(batch.FSID[:], d.Raw(16))
	// An empty v5 entry occupies 54 bytes including its versioned envelope.
	// Validate both count and remaining bytes before allocating the entry list.
	n := d.Count(54, 65536)
	batch.Entries = make([]LogEntry, 0, n)
	for i := 0; i < n && d.Err() == nil; i++ {
		version, p := d.Struct(5)
		if version < 5 {
			p.Fail(wire.ErrVersion)
		}
		entry := LogEntry{
			NameType: p.U32(), NameID: p.String(),
			RankType: p.U8(), RankNumber: int64(p.U64()),
		}
		entry.Addresses = DecodeAddresses(p)
		entry.Seconds, entry.Nanoseconds = p.U32(), p.U32()
		entry.Sequence = p.U64()
		entry.Priority = p.U16()
		entry.Message, entry.Channel = p.String(), p.String()
		if p.Err() != nil {
			d.Fail(p.Err())
			break
		}
		// Ceph's versioned envelope permits appended fields; the parent has
		// already consumed its exact length, so they cannot swallow a sibling.
		batch.Entries = append(batch.Entries, entry)
	}
	err = d.Done()
	return batch, err
}
