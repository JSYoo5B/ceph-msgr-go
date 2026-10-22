package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// TestCephRBDJournalIntegration checks that the explicit OSD API carries
// librbd's image journal, against a fourth image created by native librbd in
// the RBD base fixture's owned pool with the exclusive-lock and journaling
// features. Go reads the journal header with cls_journal methods in the
// vectors librbd's JournalMetadata and the rbd CLI send and decodes the
// registered clients, commit positions and tags; it reads the journal data
// objects and decodes the Entry framing (preamble, version, entry and tag
// tids, payload, CRC-32C) and librbd's EventEntry payloads (AioWrite with its
// data, AioDiscard); it registers its own journal client with
// MirrorPeerClientMeta data and commits positions after each native batch as
// a mirror-like consumer would; it watches the journal header while native
// librbd writes and acknowledges librbd's header-update notifies; and it
// takes the image's exclusive lock in librbd's managed-lock format, allocates
// a local journal tag and appends two AioWrite entries in librbd's format,
// one with `journal.append` (librbd's form when require_osd_release is
// octopus or later) and one with `journal.guard_append` then APPEND (its
// older compat form). Native librados `exec`, librados reads, `rbd journal
// info`, `status` and `inspect` and a native librbd journal replay on lock
// acquisition observe each step independently. Finally, in one bounded
// concurrent session, native librbd keeps the image open read-write while
// the exclusive lock goes native -> Go -> native through librbd's own lock
// notifications: Go appends three entries under its own tag (two submitted
// from concurrent goroutines; their wire overlap is not observed) while
// native aio writes wait for the lock, applies them to the data objects and commits the image
// client's position, and native librbd then appends its queued writes under
// a new tag. See integration/fixture/RBD_BASE_FIXTURE.md,
// RBD_JOURNAL_VALIDATION.md and RBD_JOURNAL_CONCURRENT_VALIDATION.md.
//
// Everything journal-specific below is test-only consumer logic: the
// cls_journal encodings, the Entry framing, the librbd EventEntry,
// ClientData and TagData encodings, the commit-position rule, the tag
// allocation and the append protocol. The product carries raw class names,
// inputs and outputs, raw notify payloads and acknowledgements and raw object
// bytes only. Encodings restate the pinned cls/journal/cls_journal.cc,
// cls/journal/cls_journal_client.cc, cls/journal/cls_journal_types.cc,
// journal/Entry.cc, journal/JournalMetadata.cc, journal/JournalRecorder.cc,
// journal/ObjectRecorder.cc, journal/Journaler.cc, librbd/Journal.cc,
// librbd/journal/CreateRequest.cc, librbd/journal/Types.{h,cc},
// tools/rbd/action/Journal.cc and include/utime.h (LGPL-2.1 references; no
// code copied).

const (
	// journal::Entry::preamble and its only version.
	rbdJournalPreamble     = 0x3141592653589793
	rbdJournalEntryVersion = 1
	// librbd::journal::EventType.
	rbdJournalEventDiscard = 0
	rbdJournalEventWrite   = 1
	rbdJournalEventFlush   = 2
	// librbd::journal::ClientMetaType.
	rbdJournalImageClientMeta = 0
	rbdJournalMirrorPeerMeta  = 1
	// MIRROR_PEER_STATE_REPLAYING.
	rbdJournalMirrorReplaying = 1
	// cls::journal::JOURNAL_MAX_RETURN.
	rbdJournalMaxReturn = 256
	// RBD_FEATURE_LAYERING | EXCLUSIVE_LOCK | JOURNALING.
	rbdJournalImageFeatures = 1 | 4 | 64
	// rbd_journal_order and rbd_journal_splay_width defaults.
	rbdJournalOrder = 24
	rbdJournalSplay = 4
	// The Go journal-header and image-header watches ping at least this
	// often (their timeout is 30 s).
	rbdJournalPingInterval = 5 * time.Second
)

type nativeRBDJournalEvent struct {
	Kind   string `json:"kind"`
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
}

type nativeRBDJournalPlan struct {
	Pool         string                  `json:"pool"`
	BaseImage    string                  `json:"base_image"`
	Image        string                  `json:"image"`
	Order        int                     `json:"order"`
	Size         uint64                  `json:"size"`
	Features     uint64                  `json:"features"`
	OracleClient string                  `json:"oracle_client"`
	GoClient     string                  `json:"go_client"`
	SetupEvents  []nativeRBDJournalEvent `json:"setup_events"`
	WriteEvents  []nativeRBDJournalEvent `json:"write_events"`
	GoWrites     []nativeRBDExtent       `json:"go_writes"`

	ConcurrentOwnerEvents []nativeRBDJournalEvent `json:"concurrent_owner_events"`
	GoConcurrentWrites    []nativeRBDExtent       `json:"go_concurrent_writes"`
	ConcurrentEvents      []nativeRBDJournalEvent `json:"concurrent_events"`
}

type nativeRBDJournalApplied struct {
	Phase      string `json:"phase"`
	Kind       string `json:"kind"`
	Offset     uint64 `json:"offset"`
	Length     uint64 `json:"length"`
	SHA256     string `json:"sha256"`
	StartedNS  int64  `json:"started_ns"`
	FinishedNS int64  `json:"finished_ns"`
}

// nativeRBDJournalPhase is one phase the concurrent action publishes in
// rbd-journal-state.json and returns in its output.
type nativeRBDJournalPhase struct {
	Phase   string `json:"phase"`
	TimeNS  int64  `json:"time_ns"`
	IsOwner bool   `json:"is_owner"`
	Owners  []struct {
		Mode  int    `json:"mode"`
		Owner string `json:"owner"`
	} `json:"owners"`
	Watchers        []nativeRBDWatcher        `json:"watchers"`
	WaitNS          int64                     `json:"wait_ns"`
	Events          []nativeRBDJournalApplied `json:"events"`
	AioReturnValues []int64                   `json:"aio_return_values"`
}

type nativeRBDJournalState struct {
	Token          string                  `json:"token"`
	RequestID      int64                   `json:"request_id"`
	NativeGlobalID uint64                  `json:"native_global_id"`
	Phase          string                  `json:"phase"`
	Phases         []nativeRBDJournalPhase `json:"phases"`
}

type nativeRBDJournalObject struct {
	Number uint64 `json:"number"`
	Object string `json:"object"`
	Exists bool   `json:"exists"`
	Size   uint64 `json:"size"`
	Offset uint64 `json:"offset"`
	SHA256 string `json:"sha256"`
}

type nativeRBDJournalCLI struct {
	Argv       []string        `json:"argv"`
	ReturnCode int             `json:"returncode"`
	Stdout     string          `json:"stdout"`
	Stderr     string          `json:"stderr"`
	ElapsedNS  int64           `json:"elapsed_ns"`
	Parsed     json.RawMessage `json:"parsed"`
}

type nativeRBDJournal struct {
	Action         string `json:"action"`
	RequestID      int64  `json:"request_id"`
	PoolID         int64  `json:"pool_id"`
	NativeGlobalID uint64 `json:"native_global_id"`
	StartedNS      int64  `json:"started_ns"`
	FinishedNS     int64  `json:"finished_ns"`
	ImageID        string `json:"image_id"`
	HeaderObject   string `json:"header_object"`
	JournalHeader  string `json:"journal_header"`
	JournalPrefix  string `json:"journal_prefix"`
	Stat           struct {
		Size       uint64 `json:"size"`
		ObjectSize uint64 `json:"obj_size"`
		Objects    uint64 `json:"num_objs"`
		Order      int    `json:"order"`
		Prefix     string `json:"block_name_prefix"`
	} `json:"stat"`
	Features       uint64                    `json:"features"`
	Flags          uint64                    `json:"flags"`
	Lockers        nativeRBDWriteLockers     `json:"lockers"`
	Watchers       []nativeRBDWatcher        `json:"watchers"`
	ClassCalls     []nativeRBDWriteClassCall `json:"class_calls"`
	JournalObjects []nativeRBDJournalObject  `json:"journal_objects"`
	Info           *nativeRBDJournalCLI      `json:"info"`
	Status         *nativeRBDJournalCLI      `json:"status"`
	Inspect        *nativeRBDJournalCLI      `json:"inspect"`
	Blobs          map[string]nativeRBDBlob  `json:"blobs"`
	OracleRegister *nativeRBDWriteClassCall  `json:"oracle_register"`
	Events         []nativeRBDJournalApplied `json:"events"`
	CloseNS        int64                     `json:"close_ns"`
	AcquireNS      int64                     `json:"acquire_ns"`
	Owner          *bool                     `json:"owner"`
	Phases         []nativeRBDJournalPhase   `json:"phases"`
	AioReturns     []int64                   `json:"aio_return_values"`
	FinalWaitNS    int64                     `json:"final_wait_ns"`
}

type nativeRBDJournalResult struct {
	Source    string               `json:"source_commit"`
	Token     string               `json:"token"`
	RequestID int64                `json:"request_id"`
	Action    string               `json:"action"`
	Plan      nativeRBDJournalPlan `json:"plan"`
	Native    *nativeRBDJournal    `json:"native"`
	Process   *struct {
		Stdout, Stderr []byte
		ExitCode       int
		TimedOut       bool
	} `json:"native_process"`
	Mutation *struct {
		Kind      string `json:"kind"`
		RequestID int64  `json:"request_id"`
		ReadOnly  bool   `json:"read_only"`
		State     string `json:"state"`
	} `json:"mutation"`
	Complete    bool   `json:"complete"`
	Error       string `json:"error"`
	ElapsedMS   int64  `json:"elapsed_ms"`
	ObservedUTC string `json:"observed_utc"`
}

// nativeRBDJournalStatus is `rbd journal status --format json`
// (do_show_journal_status with cls::journal::Client::dump).
type nativeRBDJournalStatus struct {
	MinimumSet uint64 `json:"minimum_set"`
	ActiveSet  uint64 `json:"active_set"`
	Clients    []struct {
		ID             string `json:"id"`
		Data           string `json:"data"`
		CommitPosition struct {
			Positions []struct {
				Object uint64 `json:"object_number"`
				Tag    uint64 `json:"tag_tid"`
				Entry  uint64 `json:"entry_tid"`
			} `json:"object_positions"`
		} `json:"commit_position"`
		State string `json:"state"`
	} `json:"registered_clients"`
}

// nativeRBDJournalInfo is `rbd journal info --format json`.
type nativeRBDJournalInfo struct {
	JournalID    string `json:"journal_id"`
	HeaderOID    string `json:"header_oid"`
	ObjectPrefix string `json:"object_oid_prefix"`
	Order        int    `json:"order"`
	SplayWidth   int    `json:"splay_width"`
	ObjectPool   string `json:"object_pool"`
}

// nativeRBDJournalInspect is `rbd journal inspect --verbose` as the native
// driver parses it: per replayed entry its tag and the player's commit tid
// with the EventEntry dump, and the summary.
type nativeRBDJournalInspect struct {
	Entries []struct {
		TagID     uint64 `json:"tag_id"`
		CommitTID uint64 `json:"commit_tid"`
		Event     struct {
			Type        string  `json:"event_type"`
			Offset      *uint64 `json:"offset"`
			Length      *uint64 `json:"length"`
			Granularity *uint64 `json:"discard_granularity_bytes"`
			Timestamp   string  `json:"timestamp"`
		} `json:"event"`
	} `json:"entries"`
	Summary struct {
		Total  int `json:"total"`
		Errors int `json:"errors"`
	} `json:"summary"`
}

// Test-only cls_journal and librbd journal encodings.

type rbdJournalPosition struct{ Object, Tag, Entry uint64 }

type rbdJournalClient struct {
	ID        string
	Data      []byte
	Positions []rbdJournalPosition
	State     uint8
}

type rbdJournalTag struct {
	TID, Class uint64
	Data       []byte
}

// rbdJournalTagData is librbd::journal::TagData.
type rbdJournalTagData struct {
	MirrorUUID     string
	PredecessorMU  string
	CommitValid    bool
	PredecessorTag uint64
	PredecessorEnt uint64
}

// rbdJournalEntry is one decoded journal::Entry with its place in the
// journal data object.
type rbdJournalEntry struct {
	Object       uint64
	ObjectOffset int
	Length       int
	Tag, EntryID uint64
	Payload      []byte
	CRC          uint32
}

// rbdJournalEventEntry is librbd::journal::EventEntry as decoded here.
type rbdJournalEventEntry struct {
	Version             uint8
	Type                uint32
	Offset, Length      uint64
	Data                []byte
	SkipPartialDiscard  bool
	DiscardGranularity  uint32
	Seconds, Nanosecond uint32
}

func rbdJournalBL(b []byte) []byte {
	return append(binary.LittleEndian.AppendUint32(nil, uint32(len(b))), b...)
}

func (d *rbdDecoder) blob(limit int) []byte {
	n := d.u32()
	if int64(n) > int64(limit) {
		d.err = errors.New("journal bufferlist exceeds its bound")
		return nil
	}
	return d.take(int(n))
}

func (d *rbdDecoder) journalPosition() rbdJournalPosition {
	_, s := d.header(1)
	p := rbdJournalPosition{Object: s.u64(), Tag: s.u64(), Entry: s.u64()}
	d.adopt(s)
	return p
}

// journalPositions is ObjectSetPosition: ENCODE_START(1, 1) and a list of
// ObjectPosition, most recent first.
func (d *rbdDecoder) journalPositions() []rbdJournalPosition {
	_, s := d.header(1)
	n := s.u32()
	if n > 64 {
		s.err = errors.New("journal commit position too long")
	}
	positions := []rbdJournalPosition{}
	for i := uint32(0); i < n && s.err == nil; i++ {
		positions = append(positions, s.journalPosition())
	}
	d.adopt(s)
	return positions
}

func (d *rbdDecoder) journalClient() rbdJournalClient {
	_, s := d.header(1)
	c := rbdJournalClient{ID: s.str(), Data: slices.Clone(s.blob(4096))}
	c.Positions = s.journalPositions()
	c.State = s.u8()
	d.adopt(s)
	return c
}

func decodeRBDJournalClient(data []byte) (rbdJournalClient, error) {
	d := &rbdDecoder{data: data}
	c := d.journalClient()
	return c, d.done()
}

// decodeRBDJournalClients is std::set<cls::journal::Client>.
func decodeRBDJournalClients(data []byte) ([]rbdJournalClient, error) {
	d := &rbdDecoder{data: data}
	n := d.u32()
	if n > rbdJournalMaxReturn {
		return nil, errors.New("journal client list too long")
	}
	clients := []rbdJournalClient{}
	for i := uint32(0); i < n && d.err == nil; i++ {
		clients = append(clients, d.journalClient())
	}
	return clients, d.done()
}

// decodeRBDJournalTags is std::set<cls::journal::Tag>.
func decodeRBDJournalTags(data []byte) ([]rbdJournalTag, error) {
	d := &rbdDecoder{data: data}
	n := d.u32()
	if n > rbdJournalMaxReturn {
		return nil, errors.New("journal tag list too long")
	}
	tags := []rbdJournalTag{}
	for i := uint32(0); i < n && d.err == nil; i++ {
		_, s := d.header(1)
		tags = append(tags, rbdJournalTag{TID: s.u64(), Class: s.u64(), Data: slices.Clone(s.blob(4096))})
		d.adopt(s)
	}
	return tags, d.done()
}

func decodeRBDJournalTagData(data []byte) (rbdJournalTagData, error) {
	d := &rbdDecoder{data: data}
	t := rbdJournalTagData{MirrorUUID: d.str(), PredecessorMU: d.str(), CommitValid: d.u8() != 0, PredecessorTag: d.u64(), PredecessorEnt: d.u64()}
	return t, d.done()
}

func rbdJournalTagDataEncode(t rbdJournalTagData) []byte {
	p := append(rbdString(t.MirrorUUID), rbdString(t.PredecessorMU)...)
	valid := byte(0)
	if t.CommitValid {
		valid = 1
	}
	p = append(p, valid)
	return append(append(p, rbdLE64(t.PredecessorTag)...), rbdLE64(t.PredecessorEnt)...)
}

// decodeRBDJournalImageClientMeta is ClientData{ImageClientMeta}:
// ENCODE_START(2, 1), the u32 meta type, the u64 tag class and the bool
// resync flag.
func decodeRBDJournalImageClientMeta(data []byte) (version uint8, tagClass uint64, resync bool, err error) {
	d := &rbdDecoder{data: data}
	v, s := d.header(1)
	if typ := s.u32(); s.err == nil && typ != rbdJournalImageClientMeta {
		s.err = fmt.Errorf("journal client meta type %d is not the image client", typ)
	}
	tagClass, resync = s.u64(), s.u8() != 0
	d.adopt(s)
	return v, tagClass, resync, d.done()
}

// rbdJournalMirrorPeerData is ClientData{MirrorPeerClientMeta} as
// rbd-mirror registers it: the local image id, the replay state, no sync
// object count, sync points or snapshot sequences.
func rbdJournalMirrorPeerData(imageID string, state uint32) []byte {
	p := binary.LittleEndian.AppendUint32(nil, rbdJournalMirrorPeerMeta)
	p = append(p, rbdString(imageID)...)
	p = binary.LittleEndian.AppendUint32(p, state)
	p = append(p, rbdLE64(0)...)
	p = binary.LittleEndian.AppendUint32(p, 0)
	p = binary.LittleEndian.AppendUint32(p, 0)
	return append(binary.LittleEndian.AppendUint32([]byte{2, 1}, uint32(len(p))), p...)
}

func rbdJournalPositionsEncode(positions []rbdJournalPosition) []byte {
	p := binary.LittleEndian.AppendUint32(nil, uint32(len(positions)))
	for _, o := range positions {
		p = append(p, rbdEnvelope(append(append(rbdLE64(o.Object), rbdLE64(o.Tag)...), rbdLE64(o.Entry)...))...)
	}
	return rbdEnvelope(p)
}

// rbdJournalCRC is bufferlist::crc32c(0): CRC-32C with seed 0 and no final
// inversion.
func rbdJournalCRC(data []byte) uint32 { return rbdCRC32C(data) }

// rbdJournalEntryEncode is journal::Entry::encode.
func rbdJournalEntryEncode(tag, entry uint64, payload []byte) []byte {
	b := append(rbdLE64(rbdJournalPreamble), rbdJournalEntryVersion)
	b = append(append(b, rbdLE64(entry)...), rbdLE64(tag)...)
	b = append(b, rbdJournalBL(payload)...)
	return binary.LittleEndian.AppendUint32(b, rbdJournalCRC(b))
}

// decodeRBDJournalObject walks one journal data object as ObjectPlayer
// does: entries back to back, zero bytes as append padding.
func decodeRBDJournalObject(number uint64, data []byte) ([]rbdJournalEntry, error) {
	entries := []rbdJournalEntry{}
	for at := 0; at < len(data); {
		if data[at] == 0 {
			at++
			continue
		}
		d := &rbdDecoder{data: data[at:]}
		preamble, version := d.u64(), d.u8()
		e := rbdJournalEntry{Object: number, ObjectOffset: at}
		e.EntryID, e.Tag = d.u64(), d.u64()
		e.Payload = slices.Clone(d.blob(1 << 20))
		covered := len(data) - at - len(d.data)
		e.CRC = d.u32()
		if d.err != nil {
			return entries, fmt.Errorf("journal object %d entry at %d: %w", number, at, d.err)
		}
		if preamble != rbdJournalPreamble || version != rbdJournalEntryVersion {
			return entries, fmt.Errorf("journal object %d entry at %d: preamble %#x version %d", number, at, preamble, version)
		}
		if crc := rbdJournalCRC(data[at : at+covered]); crc != e.CRC {
			return entries, fmt.Errorf("journal object %d entry at %d: CRC %#x, computed %#x", number, at, e.CRC, crc)
		}
		e.Length = covered + 4
		entries = append(entries, e)
		at += e.Length
	}
	return entries, nil
}

// rbdJournalWriteEvent is EventEntry{AioWriteEvent} with its metadata.
func rbdJournalWriteEvent(offset uint64, data []byte, at cephmsgr.OSDTime) []byte {
	p := binary.LittleEndian.AppendUint32(nil, rbdJournalEventWrite)
	p = append(append(p, rbdLE64(offset)...), rbdLE64(uint64(len(data)))...)
	p = append(p, rbdJournalBL(data)...)
	b := append(binary.LittleEndian.AppendUint32([]byte{5, 1}, uint32(len(p))), p...)
	meta := binary.LittleEndian.AppendUint32(binary.LittleEndian.AppendUint32(nil, at.Seconds), at.Nanoseconds)
	return append(b, rbdEnvelope(meta)...)
}

func decodeRBDJournalEvent(data []byte) (rbdJournalEventEntry, error) {
	d := &rbdDecoder{data: data}
	v, s := d.header(1)
	e := rbdJournalEventEntry{Version: v, Type: s.u32()}
	switch e.Type {
	case rbdJournalEventWrite:
		e.Offset, e.Length = s.u64(), s.u64()
		e.Data = slices.Clone(s.blob(1 << 20))
		if s.err == nil && uint64(len(e.Data)) != e.Length {
			s.err = errors.New("AioWrite length differs from its data")
		}
	case rbdJournalEventDiscard:
		e.Offset, e.Length = s.u64(), s.u64()
		if v >= 4 {
			e.SkipPartialDiscard = s.u8() != 0
		}
		if v >= 5 {
			e.DiscardGranularity = s.u32()
		}
	case rbdJournalEventFlush:
	default:
		s.err = fmt.Errorf("unexpected journal event type %d", e.Type)
	}
	if s.err == nil && len(s.data) != 0 {
		s.err = fmt.Errorf("%d trailing event bytes", len(s.data))
	}
	d.adopt(s)
	if v >= 4 {
		_, m := d.header(1)
		e.Seconds, e.Nanosecond = m.u32(), m.u32()
		if m.err == nil && len(m.data) != 0 {
			m.err = errors.New("trailing event metadata bytes")
		}
		d.adopt(m)
	}
	return e, d.done()
}

// rbdJournalCommitPosition restates JournalMetadata::committed for entries
// committed in order: each committed entry goes to the front, then one
// position per splay offset is kept.
func rbdJournalCommitPosition(previous []rbdJournalPosition, committed []rbdJournalEntry, splay uint64) []rbdJournalPosition {
	positions := slices.Clone(previous)
	for _, e := range committed {
		positions = append([]rbdJournalPosition{{e.Object, e.Tag, e.EntryID}}, positions...)
	}
	seen := map[uint64]bool{}
	pruned := []rbdJournalPosition{}
	for _, p := range positions {
		if !seen[p.Object%splay] {
			seen[p.Object%splay] = true
			pruned = append(pruned, p)
		}
	}
	return pruned
}

// rbdMutationOutcome classifies one mutation's result for the outcome
// ledger. A reply without error is applied (code 0) or an expected server
// refusal (a negative code the caller asked for). An OSDError is a server
// refusal: the OSD answered and applied nothing. OutcomeUnknownError means
// transmission had started without a reply, and any other error is treated
// the same way, conservatively: the mutation may or may not have been
// applied. The caller never resends a mutation of any class.
func rbdMutationOutcome(reply cephmsgr.OSDReply, err error) (class, detail string) {
	var server *cephmsgr.OSDError
	var unknown *cephmsgr.OutcomeUnknownError
	switch {
	case err == nil && reply.Code == 0:
		return "applied", ""
	case err == nil:
		return "refused", ""
	case errors.As(err, &server):
		return "refused", err.Error()
	case errors.As(err, &unknown):
		return "uncertain", err.Error()
	default:
		return "uncertain", err.Error()
	}
}

func TestRBDMutationOutcomeLedger(t *testing.T) {
	// Unit level: a lost reply cannot be induced safely on the live journal,
	// so the unknown-result classes are checked here; the live session
	// covers applied and refused.
	for _, c := range []struct {
		reply cephmsgr.OSDReply
		err   error
		want  string
	}{
		{cephmsgr.OSDReply{Transaction: 7}, nil, "applied"},
		{cephmsgr.OSDReply{Transaction: 7, Code: -17}, nil, "refused"},
		{cephmsgr.OSDReply{Transaction: 7, Code: -16}, &cephmsgr.OSDError{Code: -16}, "refused"},
		{cephmsgr.OSDReply{}, fmt.Errorf("wrapped: %w", &cephmsgr.OSDError{Code: -2}), "refused"},
		{cephmsgr.OSDReply{}, &cephmsgr.OutcomeUnknownError{Cause: context.Canceled}, "uncertain"},
		{cephmsgr.OSDReply{}, fmt.Errorf("wrapped: %w", &cephmsgr.OutcomeUnknownError{Cause: errors.New("connection reset")}), "uncertain"},
		{cephmsgr.OSDReply{}, context.DeadlineExceeded, "uncertain"},
	} {
		class, detail := rbdMutationOutcome(c.reply, c.err)
		if class != c.want || (c.err == nil) != (detail == "") {
			t.Fatalf("outcome of %+v / %v: %s %q, want %s", c.reply, c.err, class, detail, c.want)
		}
	}
}

// rbdJournalHexdump parses bufferlist::hexdump lines ("<offset>  <16 hex
// bytes>  |ascii|", then the final offset).
func rbdJournalHexdump(s string) ([]byte, error) {
	var out []byte
	for _, line := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "*") {
			return nil, errors.New("collapsed hexdump lines")
		}
		fields := strings.Fields(strings.SplitN(line, "|", 2)[0])
		if len(fields) == 0 {
			return nil, errors.New("empty hexdump line")
		}
		if off, err := strconv.ParseUint(fields[0], 16, 64); err != nil || off != uint64(len(out)) {
			return nil, fmt.Errorf("hexdump offset %q differs", fields[0])
		}
		for _, f := range fields[1:] {
			b, err := hex.DecodeString(f)
			if err != nil || len(b) != 1 {
				return nil, fmt.Errorf("hexdump byte %q", f)
			}
			out = append(out, b[0])
		}
	}
	return out, nil
}

func rbdJournalPattern(token, phase string, offset, length uint64) []byte {
	var data []byte
	for counter := 0; uint64(len(data)) < length; counter++ {
		sum := sha256.Sum256(fmt.Appendf(nil, "ceph-msgr-rbd-journal:%s:%s:%d:%d", token, phase, offset, counter))
		data = append(data, sum[:]...)
	}
	return data[:length]
}

// rbdJournalTagListInput is tag_list(start_after 0, JOURNAL_MAX_RETURN,
// client id, optional tag class).
func rbdJournalTagListInput(client string, class uint64) []byte {
	p := append(append(rbdLE64(0), rbdLE64(rbdJournalMaxReturn)...), rbdString(client)...)
	return append(append(p, 1), rbdLE64(class)...)
}

func rbdJournalClientListInput() []byte {
	return append(rbdString(""), rbdLE64(rbdJournalMaxReturn)...)
}

func TestRBDJournalTestOnlyEncodings(t *testing.T) {
	// journal::Entry: preamble, version 1, entry tid, tag tid, bufferlist,
	// CRC-32C of everything before it, ceph_crc32c(0, ...): the pinned
	// test/common/test_crc32c.cc vectors.
	if got := rbdJournalCRC([]byte("foo bar baz")); got != 4119623852 || rbdJournalCRC([]byte("whiz bang boom")) != 2360230088 {
		t.Fatalf("seed-0 CRC-32C differs: %#x", got)
	}
	entry := rbdJournalEntryEncode(3, 1, []byte("data"))
	if hex.EncodeToString(entry[:33]) != "9397585326594131"+"01"+"0100000000000000"+"0300000000000000"+"04000000"+hex.EncodeToString([]byte("data")) || len(entry) != 37 ||
		binary.LittleEndian.Uint32(entry[33:]) != rbdJournalCRC(entry[:33]) {
		t.Fatalf("Entry framing differs: %x", entry)
	}
	decoded, err := decodeRBDJournalObject(5, append(append(slices.Clone(entry), 0, 0, 0), entry...))
	if err != nil || len(decoded) != 2 || decoded[0].Tag != 3 || decoded[0].EntryID != 1 || string(decoded[1].Payload) != "data" || decoded[1].ObjectOffset != len(entry)+3 {
		t.Fatal("Entry walk differs", decoded, err)
	}
	broken := slices.Clone(entry)
	broken[30] ^= 1
	if _, err := decodeRBDJournalObject(0, broken); err == nil {
		t.Fatal("corrupt Entry payload accepted")
	}
	event := rbdJournalWriteEvent(4096, []byte{1, 2, 3}, cephmsgr.OSDTime{Seconds: 7, Nanoseconds: 9})
	if hex.EncodeToString(event) != "0501"+"1b000000"+"01000000"+"0010000000000000"+"0300000000000000"+"03000000"+"010203"+"0101"+"08000000"+"07000000"+"09000000" {
		t.Fatalf("AioWrite EventEntry differs: %x", event)
	}
	if e, err := decodeRBDJournalEvent(event); err != nil || e.Type != rbdJournalEventWrite || e.Offset != 4096 || !bytes.Equal(e.Data, []byte{1, 2, 3}) || e.Seconds != 7 || e.Nanosecond != 9 {
		t.Fatal("AioWrite EventEntry decoding differs", e, err)
	}
	discard := "0501" + "19000000" + "00000000" + "0000030000000000" + "0000010000000000" + "01" + "00000100" + "0101" + "08000000" + "07000000" + "09000000"
	if e, err := decodeRBDJournalEvent(mustHex(t, discard)); err != nil || e.Type != rbdJournalEventDiscard || e.Offset != 192<<10 || e.Length != 64<<10 || !e.SkipPartialDiscard || e.DiscardGranularity != 64<<10 {
		t.Fatal("AioDiscard EventEntry decoding differs", e, err)
	}
	positions := []rbdJournalPosition{{2, 1, 6}, {1, 1, 5}}
	if hex.EncodeToString(rbdJournalPositionsEncode(positions)) != "0101"+"40000000"+"02000000"+"010118000000"+"0200000000000000"+"0100000000000000"+"0600000000000000"+
		"010118000000"+"0100000000000000"+"0100000000000000"+"0500000000000000" {
		t.Fatalf("ObjectSetPosition encoding differs: %x", rbdJournalPositionsEncode(positions))
	}
	client := rbdEnvelope(append(append(append(rbdString("x"), rbdJournalBL([]byte{9})...), rbdJournalPositionsEncode(positions)...), 1))
	if c, err := decodeRBDJournalClient(client); err != nil || c.ID != "x" || !bytes.Equal(c.Data, []byte{9}) || !reflect.DeepEqual(c.Positions, positions) || c.State != 1 {
		t.Fatal("cls::journal::Client decoding differs", c, err)
	}
	// JournalMetadata::committed: in-order commits push to the front and
	// keep the most recent position of each splay offset.
	committed := []rbdJournalEntry{{Object: 0, Tag: 1, EntryID: 0}, {Object: 1, Tag: 1, EntryID: 1}, {Object: 2, Tag: 1, EntryID: 2}, {Object: 3, Tag: 1, EntryID: 3}, {Object: 0, Tag: 1, EntryID: 4}}
	if got := rbdJournalCommitPosition(nil, committed, 4); !reflect.DeepEqual(got, []rbdJournalPosition{{0, 1, 4}, {3, 1, 3}, {2, 1, 2}, {1, 1, 1}}) {
		t.Fatal("commit position rule differs", got)
	}
	tag := rbdJournalTagData{CommitValid: true, PredecessorTag: 1, PredecessorEnt: 4}
	if got, err := decodeRBDJournalTagData(rbdJournalTagDataEncode(tag)); err != nil || got != tag || hex.EncodeToString(rbdJournalTagDataEncode(tag)) != "00000000"+"00000000"+"01"+"0100000000000000"+"0400000000000000" {
		t.Fatal("TagData encoding differs", got, err)
	}
	if hex.EncodeToString(rbdJournalMirrorPeerData("ab", 1)) != "0201"+"1e000000"+"01000000"+"02000000"+"6162"+"01000000"+"0000000000000000"+"00000000"+"00000000" {
		t.Fatalf("MirrorPeerClientMeta differs: %x", rbdJournalMirrorPeerData("ab", 1))
	}
	if v, class, resync, err := decodeRBDJournalImageClientMeta(mustHex(t, "0201"+"0d000000"+"00000000"+"0000000000000000"+"00")); err != nil || v != 2 || class != 0 || resync {
		t.Fatal("ImageClientMeta decoding differs", v, class, resync, err)
	}
	dump := "00000000  02 01 0d 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|\n00000010  00                                                |.|\n00000011\n"
	if got, err := rbdJournalHexdump(dump); err != nil || len(got) != 17 || got[0] != 2 || got[2] != 0x0d {
		t.Fatal("hexdump parsing differs", got, err)
	}
	if hex.EncodeToString(rbdJournalTagListInput("", 0)) != "0000000000000000"+"0001000000000000"+"00000000"+"01"+"0000000000000000" ||
		hex.EncodeToString(rbdJournalClientListInput()) != "00000000"+"0001000000000000" {
		t.Fatal("cls_journal list inputs differ")
	}
}

type rbdJournalNotification struct {
	AfterMS          float64
	NotifyID         uint64
	NotifierGlobalID uint64
	Cookie           uint64
	Opcode           uint8
	PayloadHex       string
	AckError         string `json:",omitempty"`
}

type rbdJournalDecodedEntry struct {
	Object, Tag, Entry uint64
	ObjectOffset       int
	Length             int
	CRC                uint32
	EventVersion       uint8
	EventType          uint32
	Offset, DataLength uint64
	DataSHA256         string `json:",omitempty"`
	Granularity        uint32 `json:",omitempty"`
	Timestamp          string
}

type rbdJournalTestEvidence struct {
	Token, Pool       string
	PoolID            int64
	GoIdentity        string
	GoGlobalID        uint64
	Image, ImageID    string
	JournalHeader     string
	JournalPrefix     string
	Requests          []rbdWriteRequestEvidence
	ClassOutputs      map[string]string
	Entries           map[string][]rbdJournalDecodedEntry
	Positions         map[string][]rbdJournalPosition
	Tags              map[string][]map[string]any
	Notifications     []rbdJournalNotification
	Pings             int
	PingErrors        []string
	GoAppends         []map[string]any
	NativeActions     []rbdNativeActionEvidence
	Inspect           map[string]any
	Steps             []rbdLockStepEvidence
	Timings           map[string]string
	ExpectedFinalHead string
	// InspectShutdownLines counts the tolerated "failed to locate client:
	// INSPECT" lines of rbd journal inspect.
	InspectShutdownLines int
	// The concurrent session: the native driver's published state, Go's
	// notifies on the image and journal headers, the notifications Go's
	// image header watch received and answered, and the outcome of every Go
	// append and data write sent while Go owned the lock.
	ConcurrentState         *nativeRBDJournalState `json:",omitempty"`
	ConcurrentNotifies      []*rbdLockGoNotify
	ConcurrentNotifications []rbdLockNotification
	ConcurrentOutcomes      []map[string]any
}

var rbdJournalInspectShutdownLine = regexp.MustCompile(`^\S+ [0-9a-f]+ -1 JournalMetadata: 0x[0-9a-f]+ failed to locate client: INSPECT$`)

func TestCephRBDJournalIntegration(t *testing.T) {
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if os.Getenv("CEPH_MSGR_TEST_RBD") != "1" || os.Getenv("CEPH_MSGR_TEST_OSD") != "1" || dir == "" {
		t.Skip("requires the opt-in disposable RBD fixture (CEPH_MSGR_TEST_RBD=1)")
	}
	var oracle nativeRBDOracle
	if err := readMDSFreshJSON(filepath.Join(dir, "rbd-base-oracle.json"), &oracle); err != nil {
		t.Fatal(err)
	}
	token, identity := oracle.Token, os.Getenv("CEPH_MSGR_RBD_BASE_IDENTITY")
	if oracle.Source != mdsFreshSource || len(token) != 32 || oracle.FSID != os.Getenv("CEPH_MSGR_FSID") || oracle.Plan.Pool != "ceph-msgr-rbd-"+token || oracle.Pool.Name != oracle.Plan.Pool ||
		oracle.Pool.Flags&1 != 0 || identity != "client.rbd-base-"+token || oracle.ClientIdentity != identity || oracle.Caps["mon"] != "profile rbd" || oracle.Caps["osd"] != "profile rbd pool="+oracle.Plan.Pool {
		t.Fatal("native RBD oracle scope differs", oracle.Source, oracle.Pool, identity)
	}
	options := integrationOptions(t)
	encoded, err := os.ReadFile(os.Getenv("CEPH_MSGR_RBD_BASE_KEY_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	private, err := cephx.ParseKey(strings.TrimSpace(string(encoded)))
	wantType, profileErr := mdsFreshExpectedKeyType(os.Getenv("CEPH_MSGR_TEST_KEY_TYPE"))
	if err != nil || profileErr != nil || private.Type() != wantType {
		t.Fatal("RBD credential does not match the selected key profile")
	}
	if options.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(encoded))); err != nil {
		t.Fatal(err)
	}
	clear(encoded)
	options.Identity, options.EnableOSD = identity, true
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	started := time.Now()
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("authenticate the RBD-profile Go client", err)
	}
	defer c.Close()
	if c.Snapshot().OSDTicket.Expires.IsZero() {
		t.Fatal("OSD service ticket missing")
	}
	gid := c.Snapshot().GlobalID
	evidence := rbdJournalTestEvidence{Token: token, Pool: oracle.Plan.Pool, PoolID: oracle.Pool.ID, GoIdentity: identity, GoGlobalID: gid, ClassOutputs: map[string]string{},
		Entries: map[string][]rbdJournalDecodedEntry{}, Positions: map[string][]rbdJournalPosition{}, Tags: map[string][]map[string]any{}, Inspect: map[string]any{}, Timings: map[string]string{}}
	var evidenceMu sync.Mutex
	defer func() {
		evidenceMu.Lock()
		defer evidenceMu.Unlock()
		evidence.Timings["total"] = time.Since(started).String()
		data, err := json.Marshal(evidence)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "rbd-journal-results.json"), data)
		}
		if err != nil {
			t.Error("RBD journal evidence", err)
		}
	}()
	step := func(label string, value any) {
		evidence.Steps = append(evidence.Steps, rbdLockStepEvidence{Label: label, Value: value})
	}

	// Native control: one closed action at a time, each journaled by the
	// fixture and answered with its own receipt.
	var plan nativeRBDJournalPlan
	results := map[string]nativeRBDJournalResult{}
	clearReceipt := func(t *testing.T, action string) {
		t.Helper()
		if err := os.Remove(filepath.Join(dir, "rbd-journal-"+action+".json")); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	// nativeReceipt validates the receipt of one completed native action.
	nativeReceipt := func(t *testing.T, action string, begun time.Time) *nativeRBDJournal {
		t.Helper()
		receipt := filepath.Join(dir, "rbd-journal-"+action+".json")
		evidence.Timings["native_"+action] = time.Since(begun).String()
		var result nativeRBDJournalResult
		if err := readMDSFreshJSON(receipt, &result); err != nil {
			t.Fatal(err)
		}
		n := result.Native
		if result.Source != mdsFreshSource || result.Token != token || result.Action != action || !result.Complete || result.Error != "" || result.Mutation == nil ||
			result.Mutation.Kind != "native-journal-"+action || result.Mutation.State != "applied" || result.Mutation.ReadOnly != strings.HasPrefix(action, "observe-") ||
			result.Mutation.RequestID != result.RequestID || result.Process == nil || result.Process.ExitCode != 0 || result.Process.TimedOut || n == nil ||
			n.Action != action || n.RequestID != result.RequestID || n.NativeGlobalID == 0 || n.NativeGlobalID == gid || n.PoolID != oracle.Pool.ID ||
			(plan.Image != "" && !reflect.DeepEqual(result.Plan, plan)) {
			t.Fatal("native RBD journal receipt differs", action, result.Error)
		}
		// librbd and the journaler log failed steps at error level.
		if len(result.Process.Stderr) != 0 {
			t.Fatalf("native RBD journal %s logged errors: %q", action, result.Process.Stderr)
		}
		for _, cli := range []*nativeRBDJournalCLI{n.Info, n.Status, n.Inspect} {
			if cli == nil {
				continue
			}
			stderr := cli.Stderr
			if cli == n.Inspect {
				// The inspector unregisters its INSPECT client and then shuts
				// its JournalMetadata down; the refresh its own unregister
				// notification triggers no longer finds the client and logs
				// that at error level. Every other error line is refused.
				var kept []string
				for _, line := range strings.Split(strings.TrimRight(stderr, "\n"), "\n") {
					if rbdJournalInspectShutdownLine.MatchString(line) {
						evidence.InspectShutdownLines++
						continue
					}
					if line != "" {
						kept = append(kept, line)
					}
				}
				stderr = strings.Join(kept, "\n")
			}
			if cli.ReturnCode != 0 || stderr != "" {
				t.Fatalf("native rbd %v failed: %d %q", cli.Argv, cli.ReturnCode, cli.Stderr)
			}
		}
		results[action] = result
		evidence.NativeActions = append(evidence.NativeActions, rbdNativeActionEvidence{Action: action, RequestID: result.RequestID, NativeGlobalID: n.NativeGlobalID, ElapsedMS: result.ElapsedMS, StderrBytes: len(result.Process.Stderr)})
		return n
	}
	native := func(t *testing.T, action string) *nativeRBDJournal {
		t.Helper()
		clearReceipt(t, action)
		begun := time.Now()
		if err := testcluster.RequestControl(ctx, dir, "rbd-journal", "rbd-journal-completed", token, action); err != nil {
			t.Fatal("native RBD journal", action, err)
		}
		return nativeReceipt(t, action, begun)
	}
	statusOf := func(t *testing.T, n *nativeRBDJournal) nativeRBDJournalStatus {
		t.Helper()
		var s nativeRBDJournalStatus
		if err := json.Unmarshal(n.Status.Parsed, &s); err != nil {
			t.Fatal("native rbd journal status", err)
		}
		return s
	}
	inspectOf := func(t *testing.T, n *nativeRBDJournal) nativeRBDJournalInspect {
		t.Helper()
		var s nativeRBDJournalInspect
		if n.Inspect == nil || json.Unmarshal(n.Inspect.Parsed, &s) != nil {
			t.Fatal("native rbd journal inspect missing", n.Action)
		}
		return s
	}

	setup := native(t, "setup")
	plan = results["setup"].Plan
	k := uint64(1) << 10
	if plan.Pool != oracle.Plan.Pool || plan.BaseImage != oracle.Plan.Image || plan.Image != "ceph-msgr-journal-"+token || plan.Order != 16 || plan.Size != 256*k ||
		plan.Features != rbdJournalImageFeatures || plan.OracleClient != "ceph-msgr-oracle-"+token[:8] || plan.GoClient != "ceph-msgr-mirror-"+token[:8] ||
		!reflect.DeepEqual(plan.SetupEvents, []nativeRBDJournalEvent{{"write", 0, 8 * k}, {"write", 72 * k, 8 * k}, {"write", 136 * k, 4 * k}, {"write", 196 * k, 8 * k}, {"discard", 192 * k, 64 * k}}) ||
		!reflect.DeepEqual(plan.WriteEvents, []nativeRBDJournalEvent{{"write", 16 * k, 8 * k}, {"write", 80 * k, 4 * k}, {"discard", 128 * k, 64 * k}}) ||
		!reflect.DeepEqual(plan.GoWrites, []nativeRBDExtent{{40 * k, 4 * k}, {100 * k, 4 * k}}) ||
		!reflect.DeepEqual(plan.ConcurrentOwnerEvents, []nativeRBDJournalEvent{{"write", 24 * k, 4 * k}, {"write", 152 * k, 4 * k}}) ||
		!reflect.DeepEqual(plan.GoConcurrentWrites, []nativeRBDExtent{{48 * k, 8 * k}, {112 * k, 4 * k}, {176 * k, 4 * k}}) ||
		!reflect.DeepEqual(plan.ConcurrentEvents, []nativeRBDJournalEvent{{"write", 52 * k, 4 * k}, {"write", 200 * k, 4 * k}, {"write", 232 * k, 4 * k}}) {
		t.Fatal("RBD journal plan differs", plan)
	}
	imageID := setup.ImageID
	journalHeader, prefix, header := "journal."+imageID, fmt.Sprintf("journal_data.%d.%s.", oracle.Pool.ID, imageID), "rbd_header."+imageID
	if setup.JournalHeader != journalHeader || setup.JournalPrefix != prefix || setup.HeaderObject != header || setup.Features != plan.Features || setup.Flags != 0 ||
		setup.Stat.Size != plan.Size || setup.Stat.Order != plan.Order || !setup.Lockers.Empty || len(setup.Watchers) != 0 || setup.OracleRegister == nil || setup.OracleRegister.Code != 0 ||
		setup.OracleRegister.InputHex != hex.EncodeToString(append(rbdString(plan.OracleClient), rbdJournalBL(nil)...)) {
		t.Fatal("native RBD journal image setup differs", setup.ImageID, setup.Features, setup.Flags, setup.Lockers, setup.OracleRegister)
	}
	evidence.Image, evidence.ImageID, evidence.JournalHeader, evidence.JournalPrefix = plan.Image, imageID, journalHeader, prefix

	// The expected image: each native event applied in order, Go's writes
	// only once a native journal replay has applied them.
	expected := make([]byte, plan.Size)
	applyEvents := func(t *testing.T, phase string, applied []nativeRBDJournalApplied, events []nativeRBDJournalEvent) {
		t.Helper()
		if len(applied) != len(events) {
			t.Fatal("native events differ from the plan", phase)
		}
		for i, e := range events {
			a := applied[i]
			if a.Kind != e.Kind || a.Offset != e.Offset || a.Length != e.Length || a.Phase != phase {
				t.Fatal("native event differs from the plan", phase, a)
			}
			switch e.Kind {
			case "write":
				data := rbdJournalPattern(token, phase, e.Offset, e.Length)
				sum := sha256.Sum256(data)
				if a.SHA256 != hex.EncodeToString(sum[:]) {
					t.Fatal("test-only journal pattern differs from the native driver", a)
				}
				copy(expected[e.Offset:], data)
			case "discard":
				clear(expected[e.Offset : e.Offset+e.Length])
			}
		}
	}
	checkImage := func(t *testing.T, n *nativeRBDJournal) {
		t.Helper()
		if !bytes.Equal(readRBDBlob(t, dir, n.Blobs["image_head"]), expected) {
			t.Fatal("native librbd image read differs from the expected image", n.Action)
		}
	}
	applyEvents(t, "setup", setup.Events, plan.SetupEvents)
	checkImage(t, setup)

	addresses := osdDumpAddresses(t, ctx, c)
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, o := range connections {
			o.Close()
		}
	}()
	route := func(t *testing.T, object string) (rbdRoute, *cephmsgr.OSDConnection) {
		t.Helper()
		p, err := recoveryObjectPlacement(ctx, c, plan.Pool, object)
		if err != nil {
			t.Fatal(err)
		}
		r := rbdRoute{Object: object, Pool: p.PoolID, Hash: p.Hash, Epoch: p.Epoch, RawPGID: p.RawPGID, PGID: p.PGID, Primary: p.Primary, Address: addresses[p.Primary]}
		if r.Pool != oracle.Pool.ID || r.Primary < 0 || r.Address == "" || p.Acting[0] != p.Primary {
			t.Fatal("caller-side RBD object placement differs", r)
		}
		if connections[r.Primary] == nil {
			o, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: r.Primary, Address: r.Address})
			if err != nil {
				t.Fatal("explicit OSD connection", err)
			}
			connections[r.Primary] = o
		}
		return r, connections[r.Primary]
	}
	request := func(r rbdRoute, ops ...cephmsgr.OSDOperation) cephmsgr.OSDRequest {
		return cephmsgr.OSDRequest{MapEpoch: r.Epoch, Object: cephmsgr.OSDObject{Pool: r.Pool, Name: r.Object, Hash: r.Hash}, Operations: ops}
	}
	record := func(label string, r rbdRoute, req cephmsgr.OSDRequest, reply cephmsgr.OSDReply, err error) {
		e := rbdWriteRequestEvidence{Label: label, Route: r, Submission: reply.Submission, Transaction: reply.Transaction, GlobalID: reply.GlobalID,
			MapEpoch: reply.MapEpoch, PG: reply.PG, Code: reply.Code, Flags: reply.Flags, UserVersion: reply.UserVersion, LegacyVersion: reply.LegacyVersion, ReplayVersion: reply.ReplayVersion}
		if req.ModificationTime != (cephmsgr.OSDTime{}) {
			mtime := req.ModificationTime
			e.ModificationTime = &mtime
		}
		for _, op := range req.Operations {
			sum := sha256.Sum256(op.Data)
			in := rbdOperationInputEvidence{Code: uint16(op.Code), Flags: uint32(op.Flags), Offset: op.Offset, Length: op.Length, DataLength: len(op.Data), DataSHA256: hex.EncodeToString(sum[:]),
				Class: op.Class.Class, Method: op.Class.Method, Mode: uint8(op.Class.Mode)}
			if op.Code == cephmsgr.OSDSetAllocHint {
				hint := op.AllocHint
				in.AllocHint = &hint
			}
			if len(op.Data) <= 512 {
				in.DataHex = hex.EncodeToString(op.Data)
			}
			e.Inputs = append(e.Inputs, in)
		}
		for _, op := range reply.Operations {
			sum := sha256.Sum256(op.Data)
			o := rbdOperationEvidence{Code: uint16(op.Code), Result: op.Result, Length: op.Length, DataLength: len(op.Data), DataSHA256: hex.EncodeToString(sum[:]), Watchers: op.Watchers}
			if len(op.Data) <= 512 {
				o.DataHex = hex.EncodeToString(op.Data)
			}
			if op.Code == cephmsgr.OSDCall {
				o.ClassOutput = op.Class.OutputLength
			}
			e.Operations = append(e.Operations, o)
		}
		if err != nil {
			e.Error = err.Error()
		}
		evidenceMu.Lock()
		evidence.Requests = append(evidence.Requests, e)
		evidenceMu.Unlock()
	}
	checkReply := func(t *testing.T, r rbdRoute, reply cephmsgr.OSDReply, code int32, mutation bool) {
		t.Helper()
		if reply.Code != code || reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.GlobalID != gid || reply.Submission.MapEpoch != r.Epoch ||
			reply.Object != r.Object || reply.PG.Pool != uint64(r.Pool) || reply.PG.Seed != r.Hash || reply.MapEpoch < r.Epoch || reply.RetryAttempt != 0 ||
			mutation && code == 0 && (reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.UserVersion == 0) {
			t.Fatalf("OSD reply identity differs: code=%d tid=%d gid=%d pg=%+v epoch=%d flags=%#x version=%d", reply.Code, reply.Transaction, reply.GlobalID, reply.PG, reply.MapEpoch, reply.Flags, reply.UserVersion)
		}
	}
	now := func() cephmsgr.OSDTime {
		t := time.Now()
		return cephmsgr.OSDTime{Seconds: uint32(t.Unix()), Nanoseconds: uint32(t.Nanosecond())}
	}
	classOp := func(class, method string, mode cephmsgr.OSDCallMode, input []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: input, Class: cephmsgr.OSDClassInput{Class: class, Method: method, Mode: mode, MaxOutputBytes: 65536}}
	}
	// onMutation, when set, receives every mutation vector's reply and error
	// right after the raw record and before any assertion, so a classified
	// outcome is kept even when the vector then fails the test.
	var onMutation func(label string, reply cephmsgr.OSDReply, err error)
	// vector sends one request and requires the aggregate code; write
	// vectors carry a modification time and no SnapContext, as librados
	// sends the journal's metadata operations.
	vector := func(t *testing.T, label, object string, code int32, ops ...cephmsgr.OSDOperation) cephmsgr.OSDReply {
		t.Helper()
		r, o := route(t, object)
		req := request(r, ops...)
		mutation := slices.ContainsFunc(ops, func(op cephmsgr.OSDOperation) bool {
			return op.Code == cephmsgr.OSDCall && op.Class.Mode == cephmsgr.OSDCallWrite || op.Code == cephmsgr.OSDAppend || op.Code == cephmsgr.OSDWrite
		})
		if mutation {
			req.ModificationTime = now()
		}
		reply, err := o.Request(ctx, req)
		record(label, r, req, reply, err)
		if mutation && onMutation != nil {
			onMutation(label, reply, err)
		}
		var server *cephmsgr.OSDError
		if code != 0 && errors.As(err, &server) && server.Code == code {
			err = nil
		}
		if err != nil {
			t.Fatalf("Go %s: %v", label, err)
		}
		checkReply(t, r, reply, code, mutation)
		if len(reply.Operations) != len(ops) {
			t.Fatal(label, "per-op results missing")
		}
		for i, op := range reply.Operations {
			if ops[i].Code != cephmsgr.OSDCall {
				continue
			}
			in := ops[i].Class
			if op.Class.ClassLength != uint8(len(in.Class)) || op.Class.MethodLength != uint8(len(in.Method)) || op.Class.InputLength != uint32(len(ops[i].Data)) ||
				op.Class.OutputLength != uint64(len(op.Data)) || in.Mode == cephmsgr.OSDCallWrite && len(op.Data) != 0 {
				t.Fatalf("%s: CALL %s.%s union echo differs: %+v", label, in.Class, in.Method, op.Class)
			}
			evidence.ClassOutputs[fmt.Sprintf("%s#%d", label, i)] = hex.EncodeToString(op.Data)
		}
		return reply
	}
	readRaw := func(t *testing.T, label, object string, limit uint64) []byte {
		t.Helper()
		r, o := route(t, object)
		req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: limit})
		reply, err := o.Request(ctx, req)
		record(label, r, req, reply, err)
		var server *cephmsgr.OSDError
		if errors.As(err, &server) && server.Code == -2 {
			checkReply(t, r, reply, -2, false)
			return nil
		}
		if err != nil {
			t.Fatal(label, err)
		}
		checkReply(t, r, reply, 0, false)
		data := reply.Operations[1].Data
		if binary.LittleEndian.Uint64(reply.Operations[0].Data) != uint64(len(data)) {
			t.Fatal(label, "STAT size differs from the READ length")
		}
		return data
	}

	// headerReads sends librbd's journal-header vectors and compares every
	// per-op output with the native exec of the same input in the latest
	// observation (rbdJournalCalls order).
	type headerView struct {
		Order, Splay      uint8
		PoolID            int64
		MinimumSet        uint64
		ActiveSet         uint64
		Clients           []rbdJournalClient
		Image, Go, Oracle *rbdJournalClient
		NextTag           uint64
		Tags              []rbdJournalTag
		GoClientCode      int32
	}
	clientIDs := []string{"", plan.GoClient, plan.OracleClient}
	headerReads := func(t *testing.T, label string, n *nativeRBDJournal) headerView {
		t.Helper()
		var outputs [][]byte
		var codes []int32
		empty := []byte{}
		// JournalMetadata C_ImmutableMetadata and C_MutableMetadata: one
		// read vector each; the client list and single-client reads alone.
		r := vector(t, label+"_immutable", journalHeader, 0, classOp("journal", "get_order", cephmsgr.OSDCallRead, empty),
			classOp("journal", "get_splay_width", cephmsgr.OSDCallRead, empty), classOp("journal", "get_pool_id", cephmsgr.OSDCallRead, empty))
		for _, op := range r.Operations {
			outputs, codes = append(outputs, op.Data), append(codes, op.Result)
		}
		r = vector(t, label+"_mutable", journalHeader, 0, classOp("journal", "get_minimum_set", cephmsgr.OSDCallRead, empty), classOp("journal", "get_active_set", cephmsgr.OSDCallRead, empty))
		for _, op := range r.Operations {
			outputs, codes = append(outputs, op.Data), append(codes, op.Result)
		}
		r = vector(t, label+"_client_list", journalHeader, 0, classOp("journal", "client_list", cephmsgr.OSDCallRead, rbdJournalClientListInput()))
		outputs, codes = append(outputs, r.Operations[0].Data), append(codes, 0)
		for _, id := range clientIDs {
			want := int32(0)
			for _, call := range n.ClassCalls[6:9] {
				if call.InputHex == hex.EncodeToString(rbdString(id)) {
					want = call.Code
				}
			}
			r = vector(t, label+"_get_client_"+id, journalHeader, want, classOp("journal", "get_client", cephmsgr.OSDCallRead, rbdString(id)))
			outputs, codes = append(outputs, r.Operations[0].Data), append(codes, r.Operations[0].Result)
		}
		r = vector(t, label+"_next_tag", journalHeader, 0, classOp("journal", "get_next_tag_tid", cephmsgr.OSDCallRead, empty))
		outputs, codes = append(outputs, r.Operations[0].Data), append(codes, 0)
		r = vector(t, label+"_tag_list", journalHeader, 0, classOp("journal", "tag_list", cephmsgr.OSDCallRead, rbdJournalTagListInput("", 0)))
		outputs, codes = append(outputs, r.Operations[0].Data), append(codes, 0)
		inputs := [][]byte{empty, empty, empty, empty, empty, rbdJournalClientListInput(), rbdString(""), rbdString(plan.GoClient), rbdString(plan.OracleClient), empty, rbdJournalTagListInput("", 0)}
		methods := []string{"get_order", "get_splay_width", "get_pool_id", "get_minimum_set", "get_active_set", "client_list", "get_client", "get_client", "get_client", "get_next_tag_tid", "tag_list"}
		if len(n.ClassCalls) != len(methods) {
			t.Fatal("native journal exec list differs")
		}
		for i, call := range n.ClassCalls {
			if call.Object != journalHeader || call.Class != "journal" || call.Method != methods[i] || call.InputHex != hex.EncodeToString(inputs[i]) ||
				call.Code != codes[i] || call.OutputHex != hex.EncodeToString(outputs[i]) {
				t.Fatalf("%s: Go journal.%s differs from native librados exec: native=%+v go_code=%d go_output=%x", label, methods[i], call, codes[i], outputs[i])
			}
		}
		var v headerView
		d := &rbdDecoder{data: slices.Concat(outputs[0], outputs[1], outputs[2], outputs[3], outputs[4])}
		v.Order, v.Splay, v.PoolID, v.MinimumSet, v.ActiveSet = d.u8(), d.u8(), int64(d.u64()), d.u64(), d.u64()
		if err := d.done(); err != nil {
			t.Fatal(label, "journal metadata decoding", err)
		}
		var err error
		if v.Clients, err = decodeRBDJournalClients(outputs[5]); err != nil {
			t.Fatal(label, "client_list decoding", err)
		}
		for i, slot := range []**rbdJournalClient{&v.Image, &v.Go, &v.Oracle} {
			if codes[6+i] != 0 {
				continue
			}
			client, err := decodeRBDJournalClient(outputs[6+i])
			if err != nil || client.ID != clientIDs[i] {
				t.Fatal(label, "get_client decoding", clientIDs[i], err)
			}
			*slot = &client
			if !slices.ContainsFunc(v.Clients, func(l rbdJournalClient) bool { return reflect.DeepEqual(l, client) }) {
				t.Fatal(label, "get_client differs from client_list", client.ID)
			}
		}
		v.GoClientCode = codes[7]
		d = &rbdDecoder{data: outputs[9]}
		v.NextTag = d.u64()
		if err := d.done(); err != nil {
			t.Fatal(label, err)
		}
		if v.Tags, err = decodeRBDJournalTags(outputs[10]); err != nil {
			t.Fatal(label, "tag_list decoding", err)
		}
		tags := []map[string]any{}
		for _, tag := range v.Tags {
			td, err := decodeRBDJournalTagData(tag.Data)
			if err != nil {
				t.Fatal(label, "TagData decoding", tag.TID, err)
			}
			tags = append(tags, map[string]any{"tid": tag.TID, "class": tag.Class, "data": td})
		}
		evidence.Tags[label] = tags
		// The independent native views: rbd journal info and status.
		var info nativeRBDJournalInfo
		if err := json.Unmarshal(n.Info.Parsed, &info); err != nil {
			t.Fatal(err)
		}
		pool := ""
		if v.PoolID >= 0 {
			pool = plan.Pool
		}
		if info != (nativeRBDJournalInfo{JournalID: imageID, HeaderOID: journalHeader, ObjectPrefix: prefix, Order: int(v.Order), SplayWidth: int(v.Splay), ObjectPool: pool}) {
			t.Fatal(label, "rbd journal info differs from the Go decoding", info, v.Order, v.Splay, v.PoolID)
		}
		status := statusOf(t, n)
		if status.MinimumSet != v.MinimumSet || status.ActiveSet != v.ActiveSet || len(status.Clients) != len(v.Clients) {
			t.Fatal(label, "rbd journal status differs from the Go decoding", status)
		}
		for i, sc := range status.Clients {
			gc := v.Clients[i]
			data, err := rbdJournalHexdump(sc.Data)
			state := map[uint8]string{0: "connected", 1: "disconnected"}[gc.State]
			if err != nil || sc.ID != gc.ID || !bytes.Equal(data, gc.Data) || sc.State != state || len(sc.CommitPosition.Positions) != len(gc.Positions) {
				t.Fatal(label, "rbd journal status client differs from the Go decoding", sc, gc, err)
			}
			for j, p := range sc.CommitPosition.Positions {
				if (rbdJournalPosition{p.Object, p.Tag, p.Entry}) != gc.Positions[j] {
					t.Fatal(label, "rbd journal status commit position differs", sc.ID, p, gc.Positions[j])
				}
			}
		}
		return v
	}

	// readEntries reads the journal data objects of both object sets with
	// STAT+READ, compares the bytes with the native librados reads of the
	// same observation and decodes every Entry and EventEntry.
	type journalEntry struct {
		rbdJournalEntry
		Event rbdJournalEventEntry
	}
	readEntries := func(t *testing.T, label string, n *nativeRBDJournal, splay uint64) []journalEntry {
		t.Helper()
		view := readRBDBlob(t, dir, n.Blobs["journal_objects"])
		var all []journalEntry
		for i, obj := range n.JournalObjects {
			name := prefix + strconv.Itoa(i)
			data := readRaw(t, fmt.Sprintf("%s_object_%d", label, i), name, 256<<10)
			if obj.Object != name || obj.Exists != (data != nil) || obj.Size != uint64(len(data)) || !bytes.Equal(view[obj.Offset:obj.Offset+obj.Size], data) {
				t.Fatalf("%s: Go read of %s differs from native librados (native exists=%v size=%d, Go %d bytes)", label, name, obj.Exists, obj.Size, len(data))
			}
			entries, err := decodeRBDJournalObject(uint64(i), data)
			if err != nil {
				t.Fatal(label, err)
			}
			for _, e := range entries {
				ev, err := decodeRBDJournalEvent(e.Payload)
				if err != nil {
					t.Fatalf("%s: object %d entry %d/%d EventEntry: %v", label, i, e.Tag, e.EntryID, err)
				}
				if e.EntryID%splay != uint64(i)%splay {
					t.Fatalf("%s: entry %d/%d is in object %d, not at its splay offset", label, e.Tag, e.EntryID, i)
				}
				all = append(all, journalEntry{e, ev})
			}
		}
		sort.Slice(all, func(i, j int) bool {
			if all[i].Tag != all[j].Tag {
				return all[i].Tag < all[j].Tag
			}
			return all[i].EntryID < all[j].EntryID
		})
		decoded := []rbdJournalDecodedEntry{}
		for i, e := range all {
			if i > 0 && all[i-1].Tag == e.Tag && all[i-1].EntryID+1 != e.EntryID || (i == 0 || all[i-1].Tag != e.Tag) && e.EntryID != 0 {
				t.Fatalf("%s: entry tids of tag %d are not contiguous from 0", label, e.Tag)
			}
			de := rbdJournalDecodedEntry{Object: e.Object, Tag: e.Tag, Entry: e.EntryID, ObjectOffset: e.ObjectOffset, Length: e.Length, CRC: e.CRC, EventVersion: e.Event.Version,
				EventType: e.Event.Type, Offset: e.Event.Offset, DataLength: e.Event.Length, Granularity: e.Event.DiscardGranularity,
				Timestamp: time.Unix(int64(e.Event.Seconds), int64(e.Event.Nanosecond)).UTC().Format(time.RFC3339Nano)}
			if e.Event.Type == rbdJournalEventWrite {
				sum := sha256.Sum256(e.Event.Data)
				de.DataSHA256 = hex.EncodeToString(sum[:])
			}
			decoded = append(decoded, de)
		}
		evidence.Entries[label] = decoded
		return all
	}
	// checkEvents matches decoded entries of one tag with planned events and
	// their native timing.
	checkEvents := func(t *testing.T, label string, entries []journalEntry, phase string, events []nativeRBDJournalEvent, applied []nativeRBDJournalApplied) {
		t.Helper()
		if len(entries) != len(events) {
			t.Fatalf("%s: %d journal entries, want %d planned events", label, len(entries), len(events))
		}
		for i, e := range entries {
			want := events[i]
			ev := e.Event
			at := time.Unix(int64(ev.Seconds), int64(ev.Nanosecond))
			// EventEntry's timestamp is taken when librbd builds the event.
			if ev.Version != 5 || ev.Offset != want.Offset || ev.Length != want.Length || at.UnixNano() < applied[i].StartedNS-int64(time.Second) || at.UnixNano() > applied[i].FinishedNS+int64(time.Second) {
				t.Fatalf("%s: entry %d/%d event differs from native event %+v: %+v at %v", label, e.Tag, e.EntryID, want, ev, at)
			}
			switch want.Kind {
			case "write":
				if ev.Type != rbdJournalEventWrite || !bytes.Equal(ev.Data, rbdJournalPattern(token, phase, want.Offset, want.Length)) {
					t.Fatalf("%s: entry %d/%d AioWrite data differs from the native write", label, e.Tag, e.EntryID)
				}
			case "discard":
				if ev.Type != rbdJournalEventDiscard || !ev.SkipPartialDiscard || ev.DiscardGranularity == 0 {
					t.Fatalf("%s: entry %d/%d AioDiscard differs: %+v", label, e.Tag, e.EntryID, ev)
				}
			}
		}
	}
	// checkInspect compares rbd journal inspect's replayed entries with the
	// Go decoding of every entry, in replay order.
	checkInspect := func(t *testing.T, label string, n *nativeRBDJournal, entries []journalEntry) {
		t.Helper()
		in := inspectOf(t, n)
		evidence.Inspect[label] = in
		if in.Summary.Total != len(entries) || in.Summary.Errors != 0 || len(in.Entries) != len(entries) {
			t.Fatalf("%s: rbd journal inspect replayed %d entries (%d errors), Go decoded %d", label, in.Summary.Total, in.Summary.Errors, len(entries))
		}
		for i, ne := range in.Entries {
			e := entries[i]
			ts, err := time.Parse("2006-01-02T15:04:05.000000-0700", ne.Event.Timestamp)
			name := map[uint32]string{rbdJournalEventWrite: "AioWrite", rbdJournalEventDiscard: "AioDiscard", rbdJournalEventFlush: "AioFlush"}[e.Event.Type]
			if err != nil || ne.TagID != e.Tag || ne.CommitTID != uint64(i+1) || ne.Event.Type != name || ne.Event.Offset == nil || *ne.Event.Offset != e.Event.Offset ||
				ne.Event.Length == nil || *ne.Event.Length != e.Event.Length || ts.Unix() != int64(e.Event.Seconds) || ts.Nanosecond()/1000 != int(e.Event.Nanosecond)/1000 ||
				(e.Event.Type == rbdJournalEventDiscard) != (ne.Event.Granularity != nil) || ne.Event.Granularity != nil && *ne.Event.Granularity != uint64(e.Event.DiscardGranularity) {
				t.Fatalf("%s: rbd journal inspect entry %d differs from the Go decoding: native=%+v go=%d/%d %+v (%v)", label, i, ne, e.Tag, e.EntryID, e.Event, err)
			}
		}
	}
	entriesOfTag := func(entries []journalEntry, tag uint64) []journalEntry {
		var out []journalEntry
		for _, e := range entries {
			if e.Tag == tag {
				out = append(out, e)
			}
		}
		return out
	}
	plainEntries := func(entries []journalEntry) []rbdJournalEntry {
		out := []rbdJournalEntry{}
		for _, e := range entries {
			out = append(out, e.rbdJournalEntry)
		}
		return out
	}

	var setupView, replayView headerView
	var setupEntries, replayEntries []journalEntry
	var setupTag uint64
	t.Run("journal_header", func(t *testing.T) {
		v := headerReads(t, "setup", setup)
		setupView = v
		// librbd's journal::CreateRequest: order 24, splay 4, pool -1 (no
		// rbd_journal_pool: data objects in the header's pool); tag 0 for the image client's new tag class; the image
		// client "" with ImageClientMeta. Lock acquisition opened the
		// journal and allocated local tag 1 (no predecessor commit yet).
		if v.Order != rbdJournalOrder || v.Splay != rbdJournalSplay || v.PoolID != -1 || v.MinimumSet != 0 || v.ActiveSet != 0 || len(v.Clients) != 2 ||
			v.Image == nil || v.Oracle == nil || v.Go != nil || v.GoClientCode != -2 || v.Clients[0].ID != "" || v.Clients[1].ID != plan.OracleClient {
			t.Fatalf("journal metadata differs: order=%d splay=%d pool=%d sets=%d/%d clients=%+v", v.Order, v.Splay, v.PoolID, v.MinimumSet, v.ActiveSet, v.Clients)
		}
		ver, class, resync, err := decodeRBDJournalImageClientMeta(v.Image.Data)
		if err != nil || ver != 2 || class != 0 || resync || v.Image.State != 0 || len(v.Image.Positions) == 0 {
			t.Fatal("image journal client differs", ver, class, resync, err, v.Image)
		}
		if len(v.Oracle.Data) != 0 || len(v.Oracle.Positions) != 0 || v.Oracle.State != 0 {
			t.Fatal("native oracle journal client differs", v.Oracle)
		}
		if len(v.Tags) == 0 || v.NextTag != v.Tags[len(v.Tags)-1].TID+1 {
			t.Fatal("journal tags differ", v.Tags, v.NextTag)
		}
		last := v.Tags[len(v.Tags)-1]
		td, _ := decodeRBDJournalTagData(last.Data)
		if last.Class != class || td.MirrorUUID != "" || td.PredecessorMU != "" || td.CommitValid {
			t.Fatal("setup's local tag differs", last, td)
		}
		setupTag = last.TID
		evidence.Positions["image_after_setup"] = v.Image.Positions
		step("journal_header", map[string]any{"order": v.Order, "splay": v.Splay, "pool_id": v.PoolID, "next_tag": v.NextTag, "setup_tag": setupTag})
	})
	if t.Failed() {
		return
	}
	t.Run("journal_entries", func(t *testing.T) {
		all := readEntries(t, "setup", setup, uint64(setupView.Splay))
		// Object set 1 stays absent; set 0 holds every entry.
		for _, obj := range setup.JournalObjects[4:] {
			if obj.Exists {
				t.Fatal("journal object set 1 exists", obj)
			}
		}
		setupEntries = entriesOfTag(all, setupTag)
		if len(setupEntries) != len(all) {
			t.Fatal("setup entries carry another tag", all)
		}
		checkEvents(t, "setup", setupEntries, "setup", plan.SetupEvents, setup.Events)
		checkInspect(t, "setup", setup, all)
		// The image client committed every entry on close.
		if want := rbdJournalCommitPosition(nil, plainEntries(setupEntries), uint64(setupView.Splay)); !reflect.DeepEqual(setupView.Image.Positions, want) {
			t.Fatalf("image client commit position %+v, the commit rule over the decoded entries gives %+v", setupView.Image.Positions, want)
		}
	})
	if t.Failed() {
		return
	}

	// The Go journal client: MirrorPeerClientMeta data as rbd-mirror
	// registers it, then a commit after each replayed native batch.
	goData := rbdJournalMirrorPeerData("ceph-msgr-go-"+token[:8], rbdJournalMirrorReplaying)
	var goPosition []rbdJournalPosition
	registered := false
	defer func() {
		if registered {
			r, o := route(t, journalHeader)
			req := request(r, classOp("journal", "client_unregister", cephmsgr.OSDCallWrite, rbdString(plan.GoClient)))
			req.ModificationTime = now()
			reply, err := o.Request(context.Background(), req)
			record("compensate_client_unregister", r, req, reply, err)
		}
	}()
	t.Run("go_client_commit", func(t *testing.T) {
		vector(t, "client_register", journalHeader, 0, classOp("journal", "client_register", cephmsgr.OSDCallWrite, append(rbdString(plan.GoClient), rbdJournalBL(goData)...)))
		registered = true
		r := vector(t, "get_client_registered", journalHeader, 0, classOp("journal", "get_client", cephmsgr.OSDCallRead, rbdString(plan.GoClient)))
		client, err := decodeRBDJournalClient(r.Operations[0].Data)
		// client_register records the minimum commit position of the
		// connected clients: the oracle's empty one.
		if err != nil || client.ID != plan.GoClient || !bytes.Equal(client.Data, goData) || len(client.Positions) != 0 || client.State != 0 {
			t.Fatal("registered Go journal client differs", client, err)
		}
		// A consumer that replayed every setup entry commits the position
		// JournalMetadata would.
		goPosition = rbdJournalCommitPosition(nil, plainEntries(setupEntries), uint64(setupView.Splay))
		vector(t, "client_commit_setup", journalHeader, 0, classOp("journal", "client_commit", cephmsgr.OSDCallWrite, append(rbdString(plan.GoClient), rbdJournalPositionsEncode(goPosition)...)))
		n := native(t, "observe-registered")
		checkImage(t, n)
		v := headerReads(t, "observe_registered", n)
		if v.Go == nil || !reflect.DeepEqual(v.Go.Positions, goPosition) || !reflect.DeepEqual(v.Go.Positions, v.Image.Positions) || !bytes.Equal(v.Go.Data, goData) ||
			len(v.Clients) != 3 || v.NextTag != setupView.NextTag {
			t.Fatal("native view of the Go journal client differs", v.Go, v.Image)
		}
		evidence.Positions["go_after_setup"] = goPosition
	})
	if t.Failed() {
		return
	}

	t.Run("header_watch_native_write", func(t *testing.T) {
		hr, ho := route(t, journalHeader)
		watchRequest := request(hr)
		watchRequest.Operations = nil
		w, reply, err := ho.WatchObject(ctx, watchRequest, cephmsgr.OSDWatchOptions{TimeoutSeconds: 30})
		record("journal_watch_register", hr, watchRequest, reply, err)
		if err != nil {
			t.Fatal("Go journal header watch", err)
		}
		var mu sync.Mutex
		var failure error
		watchCtx, stopWatch := context.WithCancel(ctx)
		done := make(chan struct{})
		watchStarted := time.Now()
		go func() {
			defer close(done)
			lastPing := time.Now()
			for {
				if time.Since(lastPing) >= rbdJournalPingInterval {
					_, err := w.Ping(watchCtx, hr.Epoch)
					if watchCtx.Err() != nil {
						return
					}
					lastPing = time.Now()
					evidenceMu.Lock()
					evidence.Pings++
					if err != nil {
						evidence.PingErrors = append(evidence.PingErrors, err.Error())
					}
					evidenceMu.Unlock()
				}
				wait, stop := context.WithDeadline(watchCtx, lastPing.Add(rbdJournalPingInterval))
				event, err := w.Next(wait)
				stop()
				if watchCtx.Err() != nil {
					return
				}
				if errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				if err != nil {
					mu.Lock()
					failure = fmt.Errorf("journal header watch ended: %w", err)
					mu.Unlock()
					return
				}
				// JournalMetadata::handle_watch_notify acknowledges every
				// header update with an empty payload and refreshes.
				n := rbdJournalNotification{AfterMS: float64(time.Since(watchStarted).Microseconds()) / 1000, NotifyID: event.NotifyID, NotifierGlobalID: event.NotifierGlobalID,
					Cookie: event.Cookie, Opcode: event.Opcode, PayloadHex: hex.EncodeToString(event.Payload)}
				if event.Opcode != cephmsgr.OSDWatchNotification || event.Cookie != w.Cookie() {
					mu.Lock()
					failure = fmt.Errorf("unexpected journal header watch event %d", event.Opcode)
					mu.Unlock()
				} else if _, err := w.Acknowledge(watchCtx, hr.Epoch, event, nil); err != nil {
					n.AckError = err.Error()
				}
				evidenceMu.Lock()
				evidence.Notifications = append(evidence.Notifications, n)
				evidenceMu.Unlock()
			}
		}()
		unwatched := false
		defer func() {
			stopWatch()
			<-done
			if !unwatched {
				reply, err := w.Unwatch(context.Background(), hr.Epoch)
				record("journal_watch_unwatch_compensate", hr, watchRequest, reply, err)
			}
		}()
		n := native(t, "write")
		applyEvents(t, "write", n.Events, plan.WriteEvents)
		checkImage(t, n)
		stopWatch()
		<-done
		reply, err = w.Unwatch(ctx, hr.Epoch)
		record("journal_watch_unwatch", hr, watchRequest, reply, err)
		unwatched = true
		if err != nil {
			t.Fatal("Go journal header unwatch", err)
		}
		mu.Lock()
		if failure != nil {
			t.Fatal(failure)
		}
		mu.Unlock()
		evidenceMu.Lock()
		notes := slices.Clone(evidence.Notifications)
		evidenceMu.Unlock()
		// The native writer's JournalMetadata notifies the header after its
		// tag allocation and commit-position updates, with empty payloads.
		if len(notes) == 0 {
			t.Fatal("no journal header notification from the native writer")
		}
		for _, note := range notes {
			if note.NotifierGlobalID != n.NativeGlobalID || note.PayloadHex != "" || note.AckError != "" {
				t.Fatal("journal header notification differs", note)
			}
		}
		step("journal_notifications", map[string]any{"count": len(notes), "native_action_ms": results["write"].ElapsedMS})

		v := headerReads(t, "write", n)
		all := readEntries(t, "write", n, uint64(v.Splay))
		if len(v.Tags) == 0 {
			t.Fatal("journal tags missing")
		}
		last := v.Tags[len(v.Tags)-1]
		td, _ := decodeRBDJournalTagData(last.Data)
		// The new local tag's predecessor is the image client's commit
		// position front from the setup.
		front := setupView.Image.Positions[0]
		if last.TID <= setupTag || td.MirrorUUID != "" || !td.CommitValid || td.PredecessorTag != front.Tag || td.PredecessorEnt != front.Entry || v.NextTag != last.TID+1 {
			t.Fatal("write's local tag differs", last, td, front)
		}
		writeEntries := entriesOfTag(all, last.TID)
		if !reflect.DeepEqual(entriesOfTag(all, setupTag), setupEntries) || len(writeEntries)+len(setupEntries) != len(all) {
			t.Fatal("journal entries after the native write differ")
		}
		checkEvents(t, "write", writeEntries, "write", plan.WriteEvents, n.Events)
		if want := rbdJournalCommitPosition(setupView.Image.Positions, plainEntries(writeEntries), uint64(v.Splay)); !reflect.DeepEqual(v.Image.Positions, want) {
			t.Fatalf("image client commit position %+v, commit rule gives %+v", v.Image.Positions, want)
		}
		// The Go consumer was not advanced by the native writer.
		if !reflect.DeepEqual(v.Go.Positions, goPosition) {
			t.Fatal("Go journal client moved", v.Go.Positions)
		}
		goPosition = rbdJournalCommitPosition(goPosition, plainEntries(writeEntries), uint64(v.Splay))
		vector(t, "client_commit_write", journalHeader, 0, classOp("journal", "client_commit", cephmsgr.OSDCallWrite, append(rbdString(plan.GoClient), rbdJournalPositionsEncode(goPosition)...)))
		o := native(t, "observe-committed")
		checkImage(t, o)
		v = headerReads(t, "observe_committed", o)
		if v.Go == nil || !reflect.DeepEqual(v.Go.Positions, v.Image.Positions) || !reflect.DeepEqual(v.Go.Positions, goPosition) {
			t.Fatal("native view of the committed Go journal client differs", v.Go, v.Image)
		}
		evidence.Positions["go_after_write"] = goPosition
		evidence.Positions["image_after_write"] = v.Image.Positions
		vector(t, "client_unregister", journalHeader, 0, classOp("journal", "client_unregister", cephmsgr.OSDCallWrite, rbdString(plan.GoClient)))
		registered = false
		vector(t, "get_client_unregistered", journalHeader, -2, classOp("journal", "get_client", cephmsgr.OSDCallRead, rbdString(plan.GoClient)))
		setupView = v
		setupEntries = all
	})
	if t.Failed() {
		return
	}

	t.Run("go_append_under_lock", func(t *testing.T) {
		before := setupView
		// librbd's managed lock: Go's image header watch handle is the
		// lock cookie.
		hr, ho := route(t, header)
		watchRequest := request(hr)
		watchRequest.Operations = nil
		w, reply, err := ho.WatchObject(ctx, watchRequest, cephmsgr.OSDWatchOptions{TimeoutSeconds: 30})
		record("image_watch_register", hr, watchRequest, reply, err)
		if err != nil {
			t.Fatal("Go image header watch", err)
		}
		cookie := rbdLockCookie(w.Cookie())
		locked, unlocked, unwatched := false, false, false
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			if locked && !unlocked {
				r, o := route(t, header)
				req := request(r, classOp("lock", "unlock", cephmsgr.OSDCallWrite, rbdLockUnlockInput(cookie)))
				req.ModificationTime = now()
				reply, err := o.Request(cleanup, req)
				record("lock_unlock_compensate", r, req, reply, err)
			}
			if !unwatched {
				reply, err := w.Unwatch(cleanup, hr.Epoch)
				record("image_watch_unwatch_compensate", hr, watchRequest, reply, err)
			}
		}()
		lockStarted := time.Now()
		locked = true
		vector(t, "lock_lock", header, 0, classOp("lock", "lock", cephmsgr.OSDCallWrite, rbdLockLockInput(cookie)))
		r := vector(t, "lock_get_info", header, 0, classOp("lock", "get_info", cephmsgr.OSDCallRead, rbdLockGetInfoInput()))
		info, err := decodeRBDLockInfo(r.Operations[0].Data)
		if err != nil || len(info.Lockers) != 1 || info.Lockers[0].NameNumber != int64(gid) || info.Lockers[0].Cookie != cookie || info.Tag != rbdLockTag {
			t.Fatal("Go does not hold the image's exclusive lock", info, err)
		}
		// Journal open after lock acquisition (librbd/Journal.cc
		// allocate_local_tag): the image client's cached commit front is
		// the predecessor of a new tag in the image client's tag class.
		r = vector(t, "get_client_image", journalHeader, 0, classOp("journal", "get_client", cephmsgr.OSDCallRead, rbdString("")))
		image, err := decodeRBDJournalClient(r.Operations[0].Data)
		if err != nil || !reflect.DeepEqual(image, *before.Image) {
			t.Fatal("image journal client changed", image, err)
		}
		_, class, _, err := decodeRBDJournalImageClientMeta(image.Data)
		if err != nil {
			t.Fatal(err)
		}
		r = vector(t, "get_next_tag_tid", journalHeader, 0, classOp("journal", "get_next_tag_tid", cephmsgr.OSDCallRead, nil))
		tag := binary.LittleEndian.Uint64(r.Operations[0].Data)
		front := image.Positions[0]
		tagData := rbdJournalTagDataEncode(rbdJournalTagData{CommitValid: true, PredecessorTag: front.Tag, PredecessorEnt: front.Entry})
		tagInput := append(append(rbdLE64(tag), rbdLE64(class)...), rbdJournalBL(tagData)...)
		vector(t, "tag_create", journalHeader, 0, classOp("journal", "tag_create", cephmsgr.OSDCallWrite, tagInput))
		// A second tag_create with the same tid is refused (-EEXIST) and
		// changes nothing.
		vector(t, "tag_create_repeat", journalHeader, -17, classOp("journal", "tag_create", cephmsgr.OSDCallWrite, tagInput))
		softMax := rbdLE64(1 << before.Order)
		for i, e := range plan.GoWrites {
			at := now()
			entry := rbdJournalEntryEncode(tag, uint64(i), rbdJournalWriteEvent(e.Offset, rbdJournalPattern(token, "go", e.Offset, e.Length), at))
			object := uint64(before.ActiveSet)*uint64(before.Splay) + uint64(i)%uint64(before.Splay)
			name := prefix + strconv.FormatUint(object, 10)
			old := readRaw(t, fmt.Sprintf("append_%d_before", i), name, 256<<10)
			label := "journal_append"
			ops := []cephmsgr.OSDOperation{classOp("journal", "append", cephmsgr.OSDCallWrite, append(slices.Clone(softMax), rbdJournalBL(entry)...))}
			if i == 1 {
				// ObjectRecorder's compat form (require_osd_release before
				// octopus): guard_append then a plain APPEND. librbd sets
				// FADVISE_DONTNEED on the APPEND, which the client cannot
				// express; the OSD treats it as a hint.
				label = "journal_guard_append"
				ops = []cephmsgr.OSDOperation{classOp("journal", "guard_append", cephmsgr.OSDCallWrite, softMax), {Code: cephmsgr.OSDAppend, Data: entry, Length: uint64(len(entry))}}
			}
			reply := vector(t, label, name, 0, ops...)
			got := readRaw(t, fmt.Sprintf("append_%d_after", i), name, 256<<10)
			if !bytes.Equal(got, append(slices.Clone(old), entry...)) {
				t.Fatalf("%s: journal object %s is not the previous bytes followed by the Go entry (%d -> %d bytes, entry %d)", label, name, len(old), len(got), len(entry))
			}
			evidence.GoAppends = append(evidence.GoAppends, map[string]any{"label": label, "object": name, "tag": tag, "entry": i, "offset": len(old), "length": len(entry),
				"entry_sha256": fmt.Sprintf("%x", sha256.Sum256(entry)), "per_op": func() []int32 {
					var codes []int32
					for _, op := range reply.Operations {
						codes = append(codes, op.Result)
					}
					return codes
				}(), "user_version": reply.UserVersion, "timestamp": at})
		}
		vector(t, "lock_unlock", header, 0, classOp("lock", "unlock", cephmsgr.OSDCallWrite, rbdLockUnlockInput(cookie)))
		unlocked = true
		evidence.Timings["go_lock_held"] = time.Since(lockStarted).String()
		// Go closes its image as librbd does after releasing the lock: the
		// header watch goes too, so no later native lock notify waits for an
		// acknowledgement from it.
		reply, err = w.Unwatch(ctx, hr.Epoch)
		record("image_watch_unwatch", hr, watchRequest, reply, err)
		unwatched = true
		if err != nil {
			t.Fatal("Go image header unwatch", err)
		}

		// Native rbd journal inspect replays every entry after the oracle's
		// empty position: the native ones and Go's.
		n := native(t, "inspect")
		checkImage(t, n)
		v := headerReads(t, "inspect", n)
		all := readEntries(t, "inspect", n, uint64(v.Splay))
		goEntries := entriesOfTag(all, tag)
		if len(goEntries) != len(plan.GoWrites) || len(all) != len(setupEntries)+len(goEntries) || !reflect.DeepEqual(all[:len(setupEntries)], setupEntries) {
			t.Fatal("journal entries after the Go appends differ", len(all), len(goEntries))
		}
		for i, e := range goEntries {
			want := plan.GoWrites[i]
			if e.Event.Type != rbdJournalEventWrite || e.Event.Offset != want.Offset || !bytes.Equal(e.Event.Data, rbdJournalPattern(token, "go", want.Offset, want.Length)) || e.Object != uint64(i) {
				t.Fatal("Go journal entry decodes differently", e)
			}
		}
		checkInspect(t, "inspect", n, all)
		last := v.Tags[len(v.Tags)-1]
		td, _ := decodeRBDJournalTagData(last.Data)
		if last.TID != tag || last.Class != class || !td.CommitValid || td.PredecessorTag != front.Tag || td.PredecessorEnt != front.Entry ||
			!reflect.DeepEqual(v.Image.Positions, before.Image.Positions) || v.Go != nil || len(v.Clients) != 2 || !n.Lockers.Empty || len(n.Watchers) != 0 {
			t.Fatal("journal header after the Go appends differs", last, td, v.Image, v.Clients, n.Lockers)
		}

		// Native librbd acquires the lock, opens the journal and replays
		// Go's entries onto the image.
		p := native(t, "replay")
		if p.Owner == nil || !*p.Owner {
			t.Fatal("native librbd did not acquire the exclusive lock")
		}
		for _, e := range plan.GoWrites {
			copy(expected[e.Offset:], rbdJournalPattern(token, "go", e.Offset, e.Length))
		}
		checkImage(t, p)
		sum := sha256.Sum256(expected)
		evidence.ExpectedFinalHead = hex.EncodeToString(sum[:])
		v = headerReads(t, "replay", p)
		all = readEntries(t, "replay", p, uint64(v.Splay))
		checkInspect(t, "replay", p, all)
		// Replay committed Go's entries for the image client; the new local
		// tag's predecessor is Go's last entry.
		if want := rbdJournalCommitPosition(before.Image.Positions, plainEntries(goEntries), uint64(v.Splay)); !reflect.DeepEqual(v.Image.Positions, want) {
			t.Fatalf("image client commit position after replay %+v, commit rule gives %+v", v.Image.Positions, want)
		}
		last = v.Tags[len(v.Tags)-1]
		td, _ = decodeRBDJournalTagData(last.Data)
		if last.TID != tag+1 || !td.CommitValid || td.PredecessorTag != tag || td.PredecessorEnt != uint64(len(plan.GoWrites)-1) {
			t.Fatal("native tag after the replay differs", last, td)
		}
		evidence.Positions["image_after_replay"] = v.Image.Positions
		step("replay", map[string]any{"acquire_ns": p.AcquireNS, "close_ns": p.CloseNS, "tag": last.TID})
		replayView, replayEntries = v, all
	})
	if t.Failed() {
		return
	}

	// The concurrent session: native librbd keeps the image open read-write
	// throughout, and the image's exclusive lock goes native -> Go -> native
	// by librbd's own protocol (RequestLock, ReleasedLock, AcquiredLock).
	// Native aio writes are issued while Go owns the lock, so both clients
	// have journal appends outstanding at once; the lock decides who appends.
	// Go appends, applies and commits as a librbd lock owner would, and
	// releases when native librbd asks. Simultaneous appends by two lock
	// owners are not a valid librbd configuration and are not attempted.
	t.Run("concurrent_appenders", func(t *testing.T) {
		before := replayView
		lastTag := before.Tags[len(before.Tags)-1].TID
		_, class, _, err := decodeRBDJournalImageClientMeta(before.Image.Data)
		if err != nil || before.ActiveSet != 0 || before.MinimumSet != 0 {
			t.Fatal("journal state before the concurrent session differs", err, before.ActiveSet, before.MinimumSet)
		}
		splay := uint64(before.Splay)
		// The outcome ledger: every Go mutation of the session, classified
		// before any assertion; a failed mutation is never resent.
		outcome := func(label string, reply cephmsgr.OSDReply, err error) {
			class, detail := rbdMutationOutcome(reply, err)
			o := map[string]any{"label": label, "transaction": reply.Transaction, "submission": reply.Submission.Transaction, "code": reply.Code,
				"user_version": reply.UserVersion, "outcome": class}
			if detail != "" {
				o["error"] = detail
			}
			evidenceMu.Lock()
			evidence.ConcurrentOutcomes = append(evidence.ConcurrentOutcomes, o)
			evidenceMu.Unlock()
		}
		onMutation = outcome
		defer func() { onMutation = nil }()
		ledgerOf := func(label string) map[string]any {
			evidenceMu.Lock()
			defer evidenceMu.Unlock()
			for _, o := range evidence.ConcurrentOutcomes {
				if o["label"] == label {
					return o
				}
			}
			return nil
		}
		statePath := filepath.Join(dir, "rbd-journal-state.json")
		for i := 0; i <= 3; i++ {
			name := statePath
			if i > 0 {
				name = filepath.Join(dir, fmt.Sprintf("rbd-journal-proceed-%d", i))
			}
			if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
		}
		clearReceipt(t, "concurrent")
		begun := time.Now()
		var controlErr error
		controlDone := make(chan struct{})
		go func() {
			controlErr = testcluster.RequestControl(ctx, dir, "rbd-journal", "rbd-journal-completed", token, "concurrent")
			close(controlDone)
		}()
		defer func() {
			// After a failure the driver gives up at its own 30 s proceed
			// deadline; do not leave the control request behind.
			select {
			case <-controlDone:
			case <-time.After(45 * time.Second):
			}
		}()
		var state nativeRBDJournalState
		waitState := func(t *testing.T, phase string) nativeRBDJournalPhase {
			t.Helper()
			read := func() (nativeRBDJournalPhase, bool) {
				data, err := os.ReadFile(statePath)
				var s nativeRBDJournalState
				if err != nil || json.Unmarshal(data, &s) != nil {
					return nativeRBDJournalPhase{}, false
				}
				i := slices.IndexFunc(s.Phases, func(p nativeRBDJournalPhase) bool { return p.Phase == phase })
				if i < 0 {
					return nativeRBDJournalPhase{}, false
				}
				state = s
				evidence.ConcurrentState = &state
				return s.Phases[i], true
			}
			for {
				if p, ok := read(); ok {
					return p
				}
				select {
				case <-controlDone:
					if p, ok := read(); ok {
						return p
					}
					t.Fatalf("native concurrent action ended before phase %s: %v", phase, controlErr)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(20 * time.Millisecond):
				}
			}
		}
		proceed := func(t *testing.T, step int) {
			t.Helper()
			signal := fmt.Sprintf("%s %d %d", token, state.RequestID, step)
			if err := testcluster.WritePublicEvidence(filepath.Join(dir, fmt.Sprintf("rbd-journal-proceed-%d", step)), []byte(signal)); err != nil {
				t.Fatal(err)
			}
		}

		// Native owner: its synchronous writes took the lock and appended
		// under a new local tag.
		owner := waitState(t, "owner")
		nativeGID := state.NativeGlobalID
		if state.Token != token || nativeGID == 0 || nativeGID == gid || state.RequestID <= 0 || !owner.IsOwner || len(owner.Watchers) != 1 || owner.Watchers[0].ID != nativeGID {
			t.Fatal("native concurrent session scope differs", state.Token, nativeGID, owner)
		}
		nativeID := rbdClientID{GID: nativeGID, Handle: owner.Watchers[0].Cookie}
		applyEvents(t, "concurrent-owner", owner.Events, plan.ConcurrentOwnerEvents)

		// Go's image header watch and its librbd-style responder.
		hr, ho := route(t, header)
		watchRequest := request(hr)
		watchRequest.Operations = nil
		w, reply, err := ho.WatchObject(ctx, watchRequest, cephmsgr.OSDWatchOptions{TimeoutSeconds: 30})
		record("concurrent_watch_register", hr, watchRequest, reply, err)
		if err != nil {
			t.Fatal("Go image header watch", err)
		}
		goID := rbdClientID{GID: gid, Handle: w.Cookie()}
		cookie := rbdLockCookie(w.Cookie())
		responder := &rbdLockResponder{changed: make(chan struct{})}
		respondCtx, stopResponding := context.WithCancel(ctx)
		responderDone := make(chan struct{})
		responderStarted := time.Now()
		go func() {
			defer close(responderDone)
			lastPing := time.Now()
			for {
				if time.Since(lastPing) >= rbdJournalPingInterval {
					_, err := w.Ping(respondCtx, hr.Epoch)
					if respondCtx.Err() != nil {
						return
					}
					lastPing = time.Now()
					responder.mu.Lock()
					responder.pings++
					if err != nil {
						responder.pingErrors = append(responder.pingErrors, err.Error())
					}
					responder.mu.Unlock()
				}
				wait, stop := context.WithDeadline(respondCtx, lastPing.Add(rbdJournalPingInterval))
				event, err := w.Next(wait)
				stop()
				if respondCtx.Err() != nil {
					return
				}
				if errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				if err != nil {
					responder.fail(fmt.Errorf("image header watch ended: %w", err))
					return
				}
				responder.handle(respondCtx, w, hr.Epoch, goID, responderStarted, event)
			}
		}()
		watching, lockSent, unlocked := true, false, false
		stopResponder := func() {
			stopResponding()
			<-responderDone
		}
		defer func() {
			// Compensation after a failure: unlock what Go may hold (never a
			// repeated lock or append), tell librbd, then remove the watch.
			cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			if lockSent && !unlocked {
				req := request(hr, classOp("lock", "unlock", cephmsgr.OSDCallWrite, rbdLockUnlockInput(cookie)))
				req.ModificationTime = now()
				reply, err := ho.Request(cleanup, req)
				record("concurrent_unlock_compensate", hr, req, reply, err)
				outcome("concurrent_unlock_compensate", reply, err)
				notify := request(hr)
				notify.Operations = nil
				if n, reply, err := ho.NotifyObject(cleanup, notify, rbdLockNotify(rbdNotifyReleasedLock, goID), cephmsgr.OSDWatchOptions{TimeoutSeconds: 5}); err == nil {
					record("concurrent_released_lock_compensate", hr, notify, reply, err)
					_, _ = n.Next(cleanup)
					n.Close()
				}
			}
			stopResponder()
			if watching {
				reply, err := w.Unwatch(cleanup, hr.Epoch)
				record("concurrent_watch_unwatch_compensate", hr, watchRequest, reply, err)
			}
			events, _ := responder.snapshot()
			evidenceMu.Lock()
			evidence.ConcurrentNotifications = events
			evidenceMu.Unlock()
		}()
		waitNotification := func(t *testing.T, from int, label string, match func(rbdLockNotification) bool) rbdLockNotification {
			t.Helper()
			deadline := time.After(20 * time.Second)
			for {
				responder.mu.Lock()
				for _, n := range responder.events[min(from, len(responder.events)):] {
					if match(n) {
						responder.mu.Unlock()
						return n
					}
				}
				changed, failure := responder.changed, responder.failure
				responder.mu.Unlock()
				if failure != nil {
					t.Fatal(label, "image header watch responder:", failure)
				}
				select {
				case <-changed:
				case <-deadline:
					t.Fatal(label, "notification not received")
				case <-ctx.Done():
					t.Fatal(label, ctx.Err())
				}
			}
		}
		notifications := func() int {
			responder.mu.Lock()
			defer responder.mu.Unlock()
			return len(responder.events)
		}
		// goNotify sends one notify with librbd's 5 s timeout and returns
		// its completion; the caller judges the acknowledgements.
		goNotify := func(t *testing.T, label, object string, payload []byte) *rbdLockGoNotify {
			t.Helper()
			r, o := route(t, object)
			req := request(r)
			req.Operations = nil
			notifyStarted := time.Now()
			n, reply, err := o.NotifyObject(ctx, req, payload, cephmsgr.OSDWatchOptions{TimeoutSeconds: 5})
			record(label, r, req, reply, err)
			if err != nil {
				t.Fatal("Go notify", label, err)
			}
			defer n.Close()
			checkReply(t, r, reply, 0, false)
			notifyID := binary.LittleEndian.Uint64(reply.Operations[0].Data)
			wait, stop := context.WithTimeout(ctx, 20*time.Second)
			defer stop()
			completion, err := n.Next(wait)
			if err != nil {
				t.Fatal("Go notify completion", label, err)
			}
			e := &rbdLockGoNotify{Label: label, PayloadHex: hex.EncodeToString(payload), Cookie: n.Cookie(), NotifyID: notifyID, ReplyDataHex: hex.EncodeToString(reply.Operations[0].Data),
				Opcode: completion.Opcode, CompletionID: completion.NotifyID, ReturnCode: completion.ReturnCode, Missed: completion.Missed,
				RawFrontHex: hex.EncodeToString(completion.RawFront), RawDataHex: hex.EncodeToString(completion.RawData), CompletedAfterMS: float64(time.Since(notifyStarted).Microseconds()) / 1000}
			for _, reply := range completion.Replies {
				e.Replies = append(e.Replies, rbdNotifyReplyEvidence{GlobalID: reply.GlobalID, Cookie: reply.Cookie, DataHex: hex.EncodeToString(reply.Data)})
			}
			evidenceMu.Lock()
			evidence.ConcurrentNotifies = append(evidence.ConcurrentNotifies, e)
			evidenceMu.Unlock()
			if completion.Opcode != cephmsgr.OSDWatchCompletion || completion.NotifyID != notifyID || completion.Cookie != n.Cookie() || completion.ReturnCode != 0 || len(completion.Missed) != 0 ||
				time.Since(notifyStarted) >= rbdNotifyTimeout {
				t.Fatalf("%s completion differs: %+v", label, e)
			}
			if _, err := n.Next(ctx); !errors.Is(err, cephmsgr.ErrOSDNotifyComplete) {
				t.Fatal("notify stream did not end after its completion", label, err)
			}
			return e
		}
		// imageNotify is a lock notify on the image header: native librbd and
		// Go's own watch must both acknowledge, both empty unless native
		// librbd answers as the lock owner.
		imageNotify := func(t *testing.T, label string, payload []byte) *rbdLockGoNotify {
			t.Helper()
			e := goNotify(t, label, header, payload)
			if len(e.Replies) != 2 || !slices.ContainsFunc(e.Replies, func(r rbdNotifyReplyEvidence) bool {
				return r.GlobalID == gid && r.Cookie == goID.Handle && r.DataHex == ""
			}) ||
				!slices.ContainsFunc(e.Replies, func(r rbdNotifyReplyEvidence) bool { return r.GlobalID == nativeID.GID && r.Cookie == nativeID.Handle }) {
				t.Fatalf("%s acknowledgements differ: %+v", label, e.Replies)
			}
			return e
		}
		// journalNotify is JournalMetadata's header update: an empty payload;
		// every watcher present acknowledges empty.
		journalNotify := func(t *testing.T, label string) {
			t.Helper()
			e := goNotify(t, label, journalHeader, nil)
			for _, r := range e.Replies {
				if r.DataHex != "" {
					t.Fatalf("%s acknowledgement differs: %+v", label, r)
				}
			}
		}

		// Go asks for the lock; native librbd (automatic policy) answers
		// ResponseMessage(0) and releases it.
		mark := notifications()
		requested := imageNotify(t, "concurrent_notify_request_lock", rbdRequestLockNotify(goID, false))
		answer := slices.IndexFunc(requested.Replies, func(r rbdNotifyReplyEvidence) bool { return r.GlobalID == nativeID.GID })
		data, _ := hex.DecodeString(requested.Replies[answer].DataHex)
		if result, err := decodeRBDResponseMessage(data); err != nil || result != 0 {
			t.Fatal("native librbd did not answer RequestLock with a release", result, err)
		}
		released := waitNotification(t, mark, "native ReleasedLock", func(n rbdLockNotification) bool {
			return n.Op == rbdNotifyReleasedLock && n.NotifierGlobalID == nativeGID
		})
		if released.Client != nativeID || released.AckHex != "" || released.AckError != "" {
			t.Fatal("native ReleasedLock differs", released)
		}

		// Go takes the lock as librbd's AcquireRequest does and announces it.
		lockSent = true
		vector(t, "concurrent_lock_lock", header, 0, classOp("lock", "lock", cephmsgr.OSDCallWrite, rbdLockLockInput(cookie)))
		responder.setOwning(true)
		r := vector(t, "concurrent_lock_get_info", header, 0, classOp("lock", "get_info", cephmsgr.OSDCallRead, rbdLockGetInfoInput()))
		info, err := decodeRBDLockInfo(r.Operations[0].Data)
		if err != nil || len(info.Lockers) != 1 || info.Lockers[0].NameNumber != int64(gid) || info.Lockers[0].Cookie != cookie || info.Tag != rbdLockTag {
			t.Fatal("Go does not hold the image's exclusive lock", info, err)
		}
		imageNotify(t, "concurrent_notify_acquired_lock", rbdLockNotify(rbdNotifyAcquiredLock, goID))
		lockedAt := time.Now()

		// Journal open as the new owner: the metadata vectors, the image
		// client, the tags and every entry of object set 0. The native owner
		// must have committed every entry when it released the lock; Go does
		// not replay a journal, so an uncommitted entry stops the test.
		r = vector(t, "concurrent_immutable", journalHeader, 0, classOp("journal", "get_order", cephmsgr.OSDCallRead, nil),
			classOp("journal", "get_splay_width", cephmsgr.OSDCallRead, nil), classOp("journal", "get_pool_id", cephmsgr.OSDCallRead, nil))
		if !bytes.Equal(r.Operations[0].Data, []byte{before.Order}) || !bytes.Equal(r.Operations[1].Data, []byte{before.Splay}) {
			t.Fatal("journal immutable metadata changed")
		}
		r = vector(t, "concurrent_mutable", journalHeader, 0, classOp("journal", "get_minimum_set", cephmsgr.OSDCallRead, nil), classOp("journal", "get_active_set", cephmsgr.OSDCallRead, nil))
		if binary.LittleEndian.Uint64(r.Operations[0].Data) != before.MinimumSet || binary.LittleEndian.Uint64(r.Operations[1].Data) != before.ActiveSet {
			t.Fatal("journal object sets changed")
		}
		r = vector(t, "concurrent_get_client_image", journalHeader, 0, classOp("journal", "get_client", cephmsgr.OSDCallRead, rbdString("")))
		imageAtLock, err := decodeRBDJournalClient(r.Operations[0].Data)
		if err != nil {
			t.Fatal(err)
		}
		r = vector(t, "concurrent_tag_list", journalHeader, 0, classOp("journal", "tag_list", cephmsgr.OSDCallRead, rbdJournalTagListInput("", 0)))
		tagsAtLock, err := decodeRBDJournalTags(r.Operations[0].Data)
		if err != nil || len(tagsAtLock) != len(before.Tags)+1 || !reflect.DeepEqual(tagsAtLock[:len(before.Tags)], before.Tags) {
			t.Fatal("native owner did not allocate exactly one new tag", tagsAtLock, err)
		}
		ownerTag := tagsAtLock[len(tagsAtLock)-1]
		td, err := decodeRBDJournalTagData(ownerTag.Data)
		replayFront := before.Image.Positions[0]
		if err != nil || ownerTag.TID <= lastTag || ownerTag.Class != class || td.MirrorUUID != "" || !td.CommitValid || td.PredecessorTag != replayFront.Tag || td.PredecessorEnt != replayFront.Entry {
			t.Fatal("native owner's local tag differs", ownerTag, td, replayFront, err)
		}
		olds := map[uint64][]byte{}
		var atLock []journalEntry
		for i := uint64(0); i < 2*splay; i++ {
			object := before.ActiveSet*splay + i
			data := readRaw(t, fmt.Sprintf("concurrent_lock_object_%d", object), prefix+strconv.FormatUint(object, 10), 256<<10)
			if i >= splay {
				if data != nil {
					t.Fatal("journal object set 1 exists", object)
				}
				continue
			}
			olds[object] = data
			entries, err := decodeRBDJournalObject(object, data)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range entries {
				ev, err := decodeRBDJournalEvent(e.Payload)
				if err != nil {
					t.Fatal(err)
				}
				atLock = append(atLock, journalEntry{e, ev})
			}
		}
		sort.Slice(atLock, func(i, j int) bool {
			return atLock[i].Tag < atLock[j].Tag || atLock[i].Tag == atLock[j].Tag && atLock[i].EntryID < atLock[j].EntryID
		})
		ownerEntries := entriesOfTag(atLock, ownerTag.TID)
		if len(atLock) != len(replayEntries)+len(ownerEntries) || !reflect.DeepEqual(atLock[:len(replayEntries)], replayEntries) {
			t.Fatal("journal entries at Go's lock acquisition differ", len(atLock), len(replayEntries), len(ownerEntries))
		}
		checkEvents(t, "concurrent_owner", ownerEntries, "concurrent-owner", plan.ConcurrentOwnerEvents, owner.Events)
		committed := rbdJournalCommitPosition(before.Image.Positions, plainEntries(ownerEntries), splay)
		if !reflect.DeepEqual(imageAtLock.Positions, committed) {
			t.Fatalf("image client commit position %+v does not cover the native owner's entries (%+v); Go does not replay", imageAtLock.Positions, committed)
		}
		evidence.Positions["image_at_go_lock"] = imageAtLock.Positions

		// Tag allocation (Journal::allocate_local_tag): the predecessor is
		// the image client's commit front, the native owner's last entry.
		r = vector(t, "concurrent_get_next_tag_tid", journalHeader, 0, classOp("journal", "get_next_tag_tid", cephmsgr.OSDCallRead, nil))
		goTag := binary.LittleEndian.Uint64(r.Operations[0].Data)
		front := imageAtLock.Positions[0]
		if goTag != ownerTag.TID+1 || front.Tag != ownerTag.TID || front.Entry != uint64(len(ownerEntries)-1) {
			t.Fatal("next tag or commit front differs", goTag, front)
		}
		tagData := rbdJournalTagDataEncode(rbdJournalTagData{CommitValid: true, PredecessorTag: front.Tag, PredecessorEnt: front.Entry})
		tagInput := append(append(rbdLE64(goTag), rbdLE64(class)...), rbdJournalBL(tagData)...)
		vector(t, "concurrent_tag_create", journalHeader, 0, classOp("journal", "tag_create", cephmsgr.OSDCallWrite, tagInput))
		// A live server refusal through the ledger: the same tag_create again
		// is refused with -EEXIST, changes nothing and is not resent.
		vector(t, "concurrent_tag_create_repeat", journalHeader, -17, classOp("journal", "tag_create", cephmsgr.OSDCallWrite, tagInput))
		if o := ledgerOf("concurrent_tag_create_repeat"); o == nil || o["outcome"] != "refused" || o["code"] != int32(-17) {
			t.Fatal("refused tag_create is not in the outcome ledger as refused", o)
		}
		journalNotify(t, "concurrent_journal_notify_tag")

		softMax := rbdLE64(1 << before.Order)
		entries := make([][]byte, len(plan.GoConcurrentWrites))
		stamps := make([]cephmsgr.OSDTime, len(entries))
		names := make([]string, len(entries))
		for i, e := range plan.GoConcurrentWrites {
			stamps[i] = now()
			entries[i] = rbdJournalEntryEncode(goTag, uint64(i), rbdJournalWriteEvent(e.Offset, rbdJournalPattern(token, "go-concurrent", e.Offset, e.Length), stamps[i]))
			names[i] = prefix + strconv.FormatUint(before.ActiveSet*splay+uint64(i)%splay, 10)
		}
		// Entry 0 in librbd's form while the native writer is idle.
		reply = vector(t, "concurrent_append_0", names[0], 0, classOp("journal", "append", cephmsgr.OSDCallWrite, append(slices.Clone(softMax), rbdJournalBL(entries[0])...)))

		// The native writer issues its aio writes now; librbd queues them and
		// asks Go, the lock owner, for the lock.
		mark = notifications()
		proceed(t, 1)
		issued := waitState(t, "issued")
		asked := waitNotification(t, mark, "native RequestLock", func(n rbdLockNotification) bool {
			return n.Op == rbdNotifyRequestLock && n.NotifierGlobalID == nativeGID
		})
		if asked.Client != nativeID || asked.Force || !asked.AnsweredAsOwner || asked.AckHex != hex.EncodeToString(rbdResponseMessage(0)) || issued.IsOwner {
			t.Fatal("native RequestLock while Go owns the lock differs", asked, issued.IsOwner)
		}

		// Entries 1 and 2 are submitted from two goroutines started together,
		// in librbd's form, while the native writes wait for the lock. The
		// API exposes no submission event, so whether both were on the wire
		// at the same time is not observed and not claimed; only distinct
		// transactions and complete reply identity are checked. cls_journal's append checks no lock: the
		// image's exclusive lock lives on rbd_header.<id>, and a class method
		// acts only on its request's object, so a journal data object append
		// cannot carry an assertion of that lock. Ownership is the client
		// protocol's alone.
		type sent struct {
			route rbdRoute
			ops   []cephmsgr.OSDOperation
			reply cephmsgr.OSDReply
			err   error
		}
		concurrent := make([]sent, len(entries)-1)
		conns := make([]*cephmsgr.OSDConnection, len(concurrent))
		for j := range concurrent {
			i := j + 1
			concurrent[j].route, conns[j] = route(t, names[i])
			concurrent[j].ops = []cephmsgr.OSDOperation{classOp("journal", "append", cephmsgr.OSDCallWrite, append(slices.Clone(softMax), rbdJournalBL(entries[i])...))}
		}
		var wg sync.WaitGroup
		for j := range concurrent {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s := &concurrent[j]
				req := request(s.route, s.ops...)
				req.ModificationTime = now()
				s.reply, s.err = conns[j].Request(ctx, req)
				label := fmt.Sprintf("concurrent_append_%d", j+1)
				record(label, s.route, req, s.reply, s.err)
				outcome(label, s.reply, s.err)
			}()
		}
		wg.Wait()
		for j, s := range concurrent {
			if s.err != nil {
				t.Fatalf("Go concurrent append %d: %v (recorded, not resent)", j+1, s.err)
			}
			checkReply(t, s.route, s.reply, 0, true)
			if len(s.reply.Operations) != 1 || s.reply.Operations[0].Result != 0 || s.reply.Operations[0].Class.OutputLength != 0 {
				t.Fatal("concurrent append per-op results differ", s.reply.Operations)
			}
		}
		if concurrent[0].reply.Transaction == concurrent[1].reply.Transaction {
			t.Fatal("concurrent appends share a transaction")
		}
		for i := range entries {
			object := before.ActiveSet*splay + uint64(i)%splay
			if got := readRaw(t, fmt.Sprintf("concurrent_append_%d_after", i), names[i], 256<<10); !bytes.Equal(got, append(slices.Clone(olds[object]), entries[i]...)) {
				t.Fatalf("journal object %s is not its previous bytes followed by Go entry %d", names[i], i)
			}
		}
		for i, e := range entries {
			evidence.GoAppends = append(evidence.GoAppends, map[string]any{"label": fmt.Sprintf("concurrent_append_%d", i), "object": names[i], "tag": goTag, "entry": i,
				"offset": len(olds[before.ActiveSet*splay+uint64(i)%splay]), "length": len(e), "entry_sha256": fmt.Sprintf("%x", sha256.Sum256(e)), "timestamp": stamps[i]})
		}

		// After the journal is safe, librbd's ObjectWriteRequest applies each
		// event: [SETALLOCHINT FAILOK, WRITE] without SnapContext.
		objectSize := uint64(1) << plan.Order
		for i, e := range plan.GoConcurrentWrites {
			number, offset := e.Offset/objectSize, e.Offset%objectSize
			data := rbdJournalPattern(token, "go-concurrent", e.Offset, e.Length)
			label := fmt.Sprintf("concurrent_data_write_%d", i)
			reply = vector(t, label, fmt.Sprintf("%s.%016x", setup.Stat.Prefix, number),
				0, cephmsgr.OSDOperation{Code: cephmsgr.OSDSetAllocHint, Flags: cephmsgr.OSDFailOK, AllocHint: cephmsgr.OSDAllocHint{ExpectedObjectSize: objectSize, ExpectedWriteSize: objectSize}},
				cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: offset, Length: e.Length, Data: data})
			copy(expected[e.Offset:], data)
		}
		// The commit position of the applied entries
		// (JournalMetadata::committed), then the header update.
		goEntries := make([]rbdJournalEntry, len(entries))
		for i := range entries {
			object := before.ActiveSet*splay + uint64(i)%splay
			goEntries[i] = rbdJournalEntry{Object: object, Tag: goTag, EntryID: uint64(i)}
		}
		goCommitted := rbdJournalCommitPosition(imageAtLock.Positions, goEntries, splay)
		vector(t, "concurrent_client_commit", journalHeader, 0, classOp("journal", "client_commit", cephmsgr.OSDCallWrite, append(rbdString(""), rbdJournalPositionsEncode(goCommitted)...)))
		r = vector(t, "concurrent_get_client_committed", journalHeader, 0, classOp("journal", "get_client", cephmsgr.OSDCallRead, rbdString("")))
		if c, err := decodeRBDJournalClient(r.Operations[0].Data); err != nil || !reflect.DeepEqual(c.Positions, goCommitted) {
			t.Fatal("image client commit after Go's entries differs", c, err)
		}
		journalNotify(t, "concurrent_journal_notify_commit")
		evidence.Positions["image_after_go"] = goCommitted

		// Release as librbd's ReleaseRequest does; native librbd then
		// reacquires the lock for its queued writes.
		mark = notifications()
		responder.setOwning(false)
		vector(t, "concurrent_lock_unlock", header, 0, classOp("lock", "unlock", cephmsgr.OSDCallWrite, rbdLockUnlockInput(cookie)))
		unlocked = true
		evidence.Timings["concurrent_go_lock_held"] = time.Since(lockedAt).String()
		imageNotify(t, "concurrent_notify_released_lock", rbdLockNotify(rbdNotifyReleasedLock, goID))
		acquired := waitNotification(t, mark, "native AcquiredLock", func(n rbdLockNotification) bool {
			return n.Op == rbdNotifyAcquiredLock && n.NotifierGlobalID == nativeGID
		})
		if acquired.Client != nativeID || acquired.AckHex != "" || acquired.AckError != "" {
			t.Fatal("native AcquiredLock differs", acquired)
		}
		completed := waitState(t, "completed")
		if !completed.IsOwner || len(completed.AioReturnValues) != len(plan.ConcurrentEvents) {
			t.Fatal("native librbd does not own the lock after its queued writes", completed)
		}
		for i, rv := range completed.AioReturnValues {
			if rv != 0 {
				t.Fatalf("native aio write %d returned %d", i, rv)
			}
		}
		applyEvents(t, "concurrent", completed.Events, plan.ConcurrentEvents)

		// Native librbd closes the image (ReleasedLock); Go unwatches.
		mark = notifications()
		proceed(t, 2)
		closing := waitNotification(t, mark, "native ReleasedLock on close", func(n rbdLockNotification) bool {
			return n.Op == rbdNotifyReleasedLock && n.NotifierGlobalID == nativeGID
		})
		if closing.Client != nativeID || closing.AckHex != "" {
			t.Fatal("native ReleasedLock on close differs", closing)
		}
		waitState(t, "closed")
		stopResponder()
		events, failure := responder.snapshot()
		if failure != nil {
			t.Fatal("image header watch responder", failure)
		}
		reply, err = w.Unwatch(ctx, hr.Epoch)
		record("concurrent_watch_unwatch", hr, watchRequest, reply, err)
		watching = false
		if err != nil {
			t.Fatal("Go image header unwatch", err)
		}
		// Every notification came from native librbd or Go's own watch.
		var nativeOps, ownOps []uint32
		for _, n := range events {
			switch {
			case n.Own && n.NotifierGlobalID == gid:
				ownOps = append(ownOps, n.Op)
			case n.NotifierGlobalID == nativeGID && n.Client == nativeID:
				nativeOps = append(nativeOps, n.Op)
			default:
				t.Fatal("image header notification from another client", n)
			}
		}
		if !slices.Equal(ownOps, []uint32{rbdNotifyRequestLock, rbdNotifyAcquiredLock, rbdNotifyReleasedLock}) || len(nativeOps) < 4 || nativeOps[0] != rbdNotifyReleasedLock ||
			nativeOps[len(nativeOps)-2] != rbdNotifyAcquiredLock || nativeOps[len(nativeOps)-1] != rbdNotifyReleasedLock ||
			slices.ContainsFunc(nativeOps[1:len(nativeOps)-2], func(op uint32) bool { return op != rbdNotifyRequestLock }) {
			t.Fatal("image header notification sequence differs", ownOps, nativeOps)
		}
		step("concurrent_lock_handoff", map[string]any{"native_request_locks": len(nativeOps) - 3, "native_client": nativeID, "go_client": goID, "pings": responder.pings})
		proceed(t, 3)
		select {
		case <-controlDone:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if controlErr != nil {
			t.Fatal("native RBD journal concurrent", controlErr)
		}
		n := nativeReceipt(t, "concurrent", begun)
		if !reflect.DeepEqual(n.Phases, state.Phases) || !slices.Equal(n.AioReturns, completed.AioReturnValues) {
			t.Fatal("native concurrent output differs from its published state")
		}

		// Independent native views after the session.
		checkImage(t, n)
		sum := sha256.Sum256(expected)
		evidence.ExpectedFinalHead = hex.EncodeToString(sum[:])
		v := headerReads(t, "concurrent", n)
		all := readEntries(t, "concurrent", n, splay)
		if len(v.Tags) != len(before.Tags)+3 || !reflect.DeepEqual(v.Tags[:len(tagsAtLock)], tagsAtLock) || v.Tags[len(tagsAtLock)].TID != goTag || v.NextTag != goTag+2 {
			t.Fatal("tags after the concurrent session differ", v.Tags, v.NextTag)
		}
		goTagView, nativeTag := v.Tags[len(tagsAtLock)], v.Tags[len(tagsAtLock)+1]
		gtd, _ := decodeRBDJournalTagData(goTagView.Data)
		ntd, _ := decodeRBDJournalTagData(nativeTag.Data)
		if goTagView.Class != class || !bytes.Equal(goTagView.Data, tagData) || nativeTag.TID != goTag+1 || nativeTag.Class != class || ntd.MirrorUUID != "" || !ntd.CommitValid ||
			ntd.PredecessorTag != goTag || ntd.PredecessorEnt != uint64(len(entries)-1) {
			t.Fatal("Go or native reacquisition tag differs", goTagView, gtd, nativeTag, ntd)
		}
		if !reflect.DeepEqual(all[:len(atLock)], atLock) {
			t.Fatal("entries before Go's appends changed")
		}
		decodedGo := entriesOfTag(all, goTag)
		if len(decodedGo) != len(entries) {
			t.Fatal("Go entries after the session differ", len(decodedGo))
		}
		for i, e := range decodedGo {
			want := plan.GoConcurrentWrites[i]
			if e.Object != before.ActiveSet*splay+uint64(i)%splay || e.ObjectOffset != len(olds[e.Object]) || e.Length != len(entries[i]) || e.Event.Type != rbdJournalEventWrite ||
				e.Event.Offset != want.Offset || !bytes.Equal(e.Event.Data, rbdJournalPattern(token, "go-concurrent", want.Offset, want.Length)) {
				t.Fatal("Go journal entry decodes differently", e.rbdJournalEntry, e.Event.Offset)
			}
		}
		// The queued native writes: one entry each under the reacquisition
		// tag, in librbd's dispatch order, each matched by its offset.
		nativeEntries := entriesOfTag(all, nativeTag.TID)
		if len(nativeEntries)+len(decodedGo)+len(atLock) != len(all) || len(nativeEntries) != len(plan.ConcurrentEvents) {
			t.Fatal("native entries after the session differ", len(nativeEntries), len(all))
		}
		seen := map[uint64]bool{}
		for _, e := range nativeEntries {
			i := slices.IndexFunc(plan.ConcurrentEvents, func(p nativeRBDJournalEvent) bool { return p.Offset == e.Event.Offset })
			if i < 0 || seen[e.Event.Offset] {
				t.Fatal("native entry has no planned write", e.Event.Offset)
			}
			seen[e.Event.Offset] = true
			want, applied := plan.ConcurrentEvents[i], completed.Events[i]
			at := time.Unix(int64(e.Event.Seconds), int64(e.Event.Nanosecond)).UnixNano()
			if e.Event.Version != 5 || e.Event.Type != rbdJournalEventWrite || e.Event.Length != want.Length || !bytes.Equal(e.Event.Data, rbdJournalPattern(token, "concurrent", want.Offset, want.Length)) ||
				at < applied.StartedNS-int64(time.Second) || at > applied.FinishedNS+int64(time.Second) {
				t.Fatal("native concurrent entry differs from its aio write", e.rbdJournalEntry, applied)
			}
		}
		finalPositions := rbdJournalCommitPosition(goCommitted, plainEntries(nativeEntries), splay)
		if !reflect.DeepEqual(v.Image.Positions, finalPositions) {
			t.Fatalf("image client commit position %+v, commit rule over the native entries gives %+v", v.Image.Positions, finalPositions)
		}
		if v.Go != nil || len(v.Clients) != 2 || v.Oracle == nil || len(v.Oracle.Positions) != 0 || !n.Lockers.Empty || len(n.Watchers) != 0 {
			t.Fatal("journal clients or image lock after the session differ", v.Clients, n.Lockers, n.Watchers)
		}
		checkInspect(t, "concurrent", n, all)
		evidence.Positions["image_after_concurrent"] = v.Image.Positions
		step("concurrent", map[string]any{"owner_tag": ownerTag.TID, "go_tag": goTag, "native_tag": nativeTag.TID, "entries": len(all), "close_ns": n.CloseNS})
		// A compact record of the observed identities for the validation
		// record; the full evidence is in rbd-journal-results.json.
		where := func(entries []journalEntry) []string {
			var out []string
			for _, e := range entries {
				out = append(out, fmt.Sprintf("%d/%d@obj%d+%d", e.Tag, e.EntryID, e.Object, e.ObjectOffset))
			}
			return out
		}
		t.Logf("tags: native owner %d pred (%d,%d), Go %d pred (%d,%d), native reacquire %d pred (%d,%d); next %d", ownerTag.TID, td.PredecessorTag, td.PredecessorEnt,
			goTag, gtd.PredecessorTag, gtd.PredecessorEnt, nativeTag.TID, ntd.PredecessorTag, ntd.PredecessorEnt, v.NextTag)
		t.Logf("entries: owner %v, Go %v, native %v; inspect %d entries, %d errors", where(ownerEntries), where(decodedGo), where(nativeEntries), len(all), inspectOf(t, n).Summary.Errors)
		t.Logf("image client commit: at Go lock %v, after Go %v, final %v", imageAtLock.Positions, goCommitted, v.Image.Positions)
		t.Logf("Go appends: tid %d (uv %d), concurrent tids %d and %d (uv %d, %d); notifications own %v native %v; lock held %s; aio returns %v; native close %d us",
			ledgerOf("concurrent_append_0")["transaction"], ledgerOf("concurrent_append_0")["user_version"], concurrent[0].reply.Transaction, concurrent[1].reply.Transaction,
			concurrent[0].reply.UserVersion, concurrent[1].reply.UserVersion, ownOps, nativeOps, evidence.Timings["concurrent_go_lock_held"], n.AioReturns, n.CloseNS/1000)
	})
}
