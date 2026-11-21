package msgr

import (
	"errors"
	"fmt"
	"slices"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// MGR-side daemon messages (protocol stage 3, SPEC.md "Protocol stages"):
// what a MGR daemon role receives from the daemons and clients connected to
// it, and what it sends back. Wire authority, pinned Ceph v20.2.4
// 7f793731f1b39eb4f465e960113d2363c311b964:
// ceph/ceph/blob/v20.2.4/src/messages/MMgrOpen.h, MMgrUpdate.h, MMgrReport.h
// (PerfCounterType), MMgrClose.h, MMgrConfigure.h, MMgrCommand.h,
// MMgrCommandReply.h, MCommand.h, MCommandReply.h and MPGStats.h (pgstats.go);
// ceph/ceph/blob/v20.2.4/src/mgr/MetricTypes.h, OSDPerfMetricTypes.h and
// DaemonHealthMetric.h for the nested metric types;
// ceph/ceph/blob/v20.2.4/src/mgr/MgrClient.cc (_send_report: how packed
// counters are written) and ceph/ceph/blob/v20.2.4/src/mgr/DaemonState.cc
// (DaemonPerfCounters::update: how they are read).
//
// The decoders read exactly the fields of each header version the pinned
// decode_payload reads, check every count against the remaining bytes and
// require the front to be consumed. A nested versioned structure of the
// version this file knows must be consumed exactly; one of a newer version
// whose compat this file meets has its appended tail skipped, as
// DECODE_FINISH does. Nothing is interpreted: which daemon a message
// belongs to, what a counter or a PG statistic means, and every reply are
// the consumer's. The OSD performance-metric queries, limits and reports
// and the metric payload variants are kept opaque, each as its complete
// length-delimited encoding, whose boundary the decoder proves.

// ErrMgrCounterSchema reports packed counters that do not match the
// declarations of their connection.
var ErrMgrCounterSchema = errors.New("ceph messenger: MGR report counters do not match their declarations")

// ErrMgrCounterSchemaLimit refuses a report whose new declarations would
// take one connection's retained counter types past MaxMgrCounterSchemaTypes
// or MaxMgrCounterSchemaPathBytes. It wraps wire.ErrLimit.
var ErrMgrCounterSchemaLimit = errors.New("ceph messenger: MGR counter declarations exceed the connection's bound")

// The retained counter-type bounds of one connection (MgrCounterSchema).
const (
	MaxMgrCounterSchemaTypes     = 1 << 16
	MaxMgrCounterSchemaPathBytes = 4 << 20
)

// MgrDaemonOpen is a received MMgrOpen (header versions 1 to 3).
// Metadata and Status are present only for a service daemon; nil means
// absent. Config and ConfigDefaults are the raw config_bl and
// config_defaults_bl buffers.
type MgrDaemonOpen struct {
	Version                 uint16
	DaemonName, ServiceName string
	ServiceDaemon           bool
	Metadata, Status        map[string]string
	Config, ConfigDefaults  []byte
}

// MgrDaemonUpdate is a received MMgrUpdate (header versions 1 and 2).
// Metadata and Status are present only with NeedMetadataUpdate.
type MgrDaemonUpdate struct {
	Version                 uint16
	DaemonName, ServiceName string
	NeedMetadataUpdate      bool
	Metadata, Status        map[string]string
}

// MgrDaemonClose is a received MMgrClose. Its front is kept so that the
// acknowledgement repeats it exactly (MgrCloseEcho).
type MgrDaemonClose struct {
	DaemonName, ServiceName string
	front                   []byte
}

// MgrPerfCounterType is one PerfCounterType declaration (structure 3/1):
// Type is the perfcounter_type_d bit set, Unit the unit_t byte. A version 1
// declaration carries no priority, which then reads as native's default
// PRIO_USEFUL (5); a version below 3 carries no unit (zero).
type MgrPerfCounterType struct {
	Version                 uint8
	Path, Description, Nick string
	Type, Priority, Unit    uint8
}

// MgrPerfCounterLongRunAvg is PERFCOUNTER_LONGRUNAVG: such a counter's
// packed value is followed by its two average counts.
const MgrPerfCounterLongRunAvg = 0x4

// MgrHealthMetric is one DaemonHealthMetric (structure 1/1): the
// daemon_metric type byte and its 64-bit value (two packed 32-bit halves
// for some types, which are not split here).
type MgrHealthMetric struct {
	Type  uint8
	Value uint64
}

// MgrOpaqueEntry is one entry of an OSD performance-metric map, each side
// its complete encoding: in a report, an OSDPerfMetricQuery and an
// OSDPerfMetricReport (each a 1/1 structure); in a configure, an
// OSDPerfMetricQuery and its OSDPerfMetricLimits (a u32 count of 1/1
// OSDPerfMetricLimit structures).
type MgrOpaqueEntry struct {
	Key, Value []byte
}

// MgrMetricPayload is a MetricReportMessage or MetricConfigMessage: the
// variant type (0 OSD, 1 MDS) and its complete 1/1 payload structure.
type MgrMetricPayload struct {
	Type    uint32
	Payload []byte
}

// Metric payload variant types (MetricReportType, MetricConfigType).
const (
	MgrMetricOSD uint32 = 0
	MgrMetricMDS uint32 = 1
)

// MgrDaemonReport is a received MMgrReport (header versions 1 to 9).
// Packed is the packed counter buffer as sent, its 1/1 envelope included;
// it is read against the connection's declarations by MgrCounterSchema.
// Status and TaskStatus are optional: nil means absent, a non-nil empty
// map present and empty. MetricReport is nil when absent.
type MgrDaemonReport struct {
	Version              uint16
	DaemonName           string
	ServiceName          string
	Declare              []MgrPerfCounterType
	Undeclare            []string
	Packed               []byte
	Status               map[string]string
	HealthMetrics        []MgrHealthMetric
	Config               []byte
	OSDPerfMetricReports []MgrOpaqueEntry
	TaskStatus           map[string]string
	MetricReport         *MgrMetricPayload
}

// MgrCommandRequest is a received MMgrCommand or MCommand: the FSID field
// (zero from a client's MMgrCommand, the cluster FSID in a tell MCommand)
// and the command strings. The bulk input is the message's data segment.
type MgrCommandRequest struct {
	FSID    [16]byte
	Command []string
}

const (
	maxMgrMapEntries   = 1 << 16
	maxMgrCounterTypes = 1 << 20
	maxMgrCommandWords = 1024
)

func frontDecoder(m MessageData, typ uint16, minVersion, maxVersion uint16, limit uint32) (*wire.Decoder, error) {
	if m.Type != typ || len(m.Middle) != 0 {
		return nil, ErrFrame
	}
	if m.Version < minVersion || m.Version > maxVersion || m.CompatVersion > 1 || m.CompatVersion > m.Version {
		return nil, wire.ErrVersion
	}
	if uint64(len(m.Front)) > uint64(limit) {
		return nil, wire.ErrLimit
	}
	return wire.NewDecoderLimit(m.Front, limit), nil
}

func decodeSortedStringMap(d *wire.Decoder) map[string]string {
	n := d.Count(8, maxMgrMapEntries)
	out := make(map[string]string, n)
	previous := ""
	for i := 0; i < n && d.Err() == nil; i++ {
		k, v := d.String(), d.String()
		if i > 0 && k <= previous {
			// A std::map encodes strictly ascending keys.
			d.Fail(fmt.Errorf("%w: map keys not strictly ascending", ErrFrame))
		}
		previous = k
		out[k] = v
	}
	return out
}

func decodeOptionalStringMap(d *wire.Decoder) map[string]string {
	if !d.Bool() {
		return nil
	}
	return decodeSortedStringMap(d)
}

func decodeStrings(d *wire.Decoder, max uint32) []string {
	n := d.Count(4, max)
	out := make([]string, 0, n)
	for i := 0; i < n && d.Err() == nil; i++ {
		out = append(out, d.String())
	}
	return out
}

// structure reads a versioned structure the caller knows up to version
// known: it refuses a compat above known and returns the version and a
// decoder of its payload. end checks that payload's tail.
func structure(d *wire.Decoder, known uint8) (uint8, *wire.Decoder) {
	return d.Struct(known)
}

// end requires a structure of a known version to be consumed exactly and
// lets a newer one's appended tail go, as DECODE_FINISH skips it.
func end(d *wire.Decoder, version, known uint8, payload *wire.Decoder) {
	if err := payload.Err(); err != nil {
		d.Fail(err)
		return
	}
	if version <= known {
		if err := payload.Done(); err != nil {
			d.Fail(err)
		}
	}
}

// rawStructure returns one complete versioned structure (version, compat,
// u32 length and payload) whose compat this decoder meets.
func rawStructure(d *wire.Decoder, known uint8) []byte {
	header := d.Raw(2)
	if header != nil && (header[0] == 0 || header[1] > header[0] || header[1] > known) {
		d.Fail(wire.ErrVersion)
	}
	payload := d.Bytes()
	if d.Err() != nil {
		return nil
	}
	out := make([]byte, 0, 6+len(payload))
	out = append(out, header...)
	out = append(out, byte(len(payload)), byte(len(payload)>>8), byte(len(payload)>>16), byte(len(payload)>>24))
	return append(out, payload...)
}

// DecodeMgrDaemonOpen reads MMgrOpen versions 1 to 3 as DaemonServer
// receives it. The data segment carries nothing and must be empty.
func DecodeMgrDaemonOpen(m MessageData, limit uint32) (o MgrDaemonOpen, err error) {
	defer wrapDecode("MGR open", &err)
	d, err := frontDecoder(m, MgrOpenMessage, 1, 3, limit)
	if err != nil {
		return o, err
	}
	if len(m.Data) != 0 {
		return o, ErrFrame
	}
	o.Version = m.Version
	o.DaemonName = d.String()
	if m.Version >= 2 {
		o.ServiceName = d.String()
		o.ServiceDaemon = d.Bool()
		if o.ServiceDaemon {
			o.Metadata = decodeSortedStringMap(d)
			o.Status = decodeSortedStringMap(d)
		}
	}
	if m.Version >= 3 {
		o.Config = slices.Clone(d.Bytes())
		o.ConfigDefaults = slices.Clone(d.Bytes())
	}
	if err := d.Done(); err != nil {
		return MgrDaemonOpen{}, err
	}
	return o, nil
}

// DecodeMgrDaemonUpdate reads MMgrUpdate versions 1 and 2.
func DecodeMgrDaemonUpdate(m MessageData, limit uint32) (u MgrDaemonUpdate, err error) {
	defer wrapDecode("MGR update", &err)
	d, err := frontDecoder(m, MgrUpdateMessage, 1, 2, limit)
	if err != nil {
		return u, err
	}
	if len(m.Data) != 0 {
		return u, ErrFrame
	}
	u.Version = m.Version
	u.DaemonName = d.String()
	if m.Version >= 2 {
		u.ServiceName = d.String()
		u.NeedMetadataUpdate = d.Bool()
		if u.NeedMetadataUpdate {
			u.Metadata = decodeSortedStringMap(d)
			u.Status = decodeSortedStringMap(d)
		}
	}
	if err := d.Done(); err != nil {
		return MgrDaemonUpdate{}, err
	}
	return u, nil
}

// DecodeMgrDaemonClose reads an MMgrClose a daemon or client sent (header
// 1/1, empty middle and data).
func DecodeMgrDaemonClose(m MessageData, limit uint32) (MgrDaemonClose, error) {
	service, daemon, err := DecodeMgrClose(m, limit)
	if err != nil {
		return MgrDaemonClose{}, err
	}
	return MgrDaemonClose{DaemonName: daemon, ServiceName: service, front: slices.Clone(m.Front)}, nil
}

// MgrCloseEcho is native handle_close's acknowledgement: the same MMgrClose
// sent back, header 1/1, its front byte for byte.
func MgrCloseEcho(c MgrDaemonClose) (MessageData, error) {
	if c.front == nil {
		return MessageData{}, fmt.Errorf("%w: no received MGR close to acknowledge", ErrFrame)
	}
	return MessageData{Type: MgrCloseMessage, Version: 1, CompatVersion: 1, Priority: 127, Front: slices.Clone(c.front)}, nil
}

func decodePerfCounterType(d *wire.Decoder) MgrPerfCounterType {
	v, p := structure(d, 3)
	t := MgrPerfCounterType{Version: v, Priority: 5}
	t.Path, t.Description, t.Nick = p.String(), p.String(), p.String()
	t.Type = p.U8()
	if v >= 2 {
		t.Priority = p.U8()
	}
	if v >= 3 {
		t.Unit = p.U8()
	}
	end(d, v, 3, p)
	return t
}

func decodeHealthMetric(d *wire.Decoder) MgrHealthMetric {
	v, p := structure(d, 1)
	h := MgrHealthMetric{Type: p.U8(), Value: p.U64()}
	end(d, v, 1, p)
	return h
}

// decodeMetricPayload reads boost::optional<Metric*Message>: a presence
// byte, the u32 variant type and its 1/1 payload structure. Native decode
// aborts on an unknown variant; it is refused here.
func decodeMetricPayload(d *wire.Decoder) *MgrMetricPayload {
	if !d.Bool() {
		return nil
	}
	typ := d.U32()
	if d.Err() == nil && typ != MgrMetricOSD && typ != MgrMetricMDS {
		d.Fail(fmt.Errorf("%w: unknown metric payload type %d", ErrFrame, typ))
	}
	payload := rawStructure(d, 1)
	if d.Err() != nil {
		return nil
	}
	return &MgrMetricPayload{Type: typ, Payload: payload}
}

// DecodeMgrDaemonReport reads MMgrReport versions 1 to 9. Declarations,
// withdrawals and the packed buffer are returned together and in order;
// MgrCounterSchema reads the packed values.
func DecodeMgrDaemonReport(m MessageData, limit uint32) (r MgrDaemonReport, err error) {
	defer wrapDecode("MGR report", &err)
	d, err := frontDecoder(m, MgrReportMessage, 1, 9, limit)
	if err != nil {
		return r, err
	}
	if len(m.Data) != 0 {
		return r, ErrFrame
	}
	r.Version = m.Version
	r.DaemonName = d.String()
	n := d.Count(6+12, maxMgrCounterTypes)
	r.Declare = make([]MgrPerfCounterType, 0, n)
	for i := 0; i < n && d.Err() == nil; i++ {
		r.Declare = append(r.Declare, decodePerfCounterType(d))
	}
	r.Packed = slices.Clone(d.Bytes())
	if m.Version >= 2 {
		r.Undeclare = decodeStrings(d, maxMgrCounterTypes)
	}
	if m.Version >= 3 {
		r.ServiceName = d.String()
		r.Status = decodeOptionalStringMap(d)
	}
	if m.Version >= 5 {
		n := d.Count(6+9, maxMgrMapEntries)
		r.HealthMetrics = make([]MgrHealthMetric, 0, n)
		for i := 0; i < n && d.Err() == nil; i++ {
			r.HealthMetrics = append(r.HealthMetrics, decodeHealthMetric(d))
		}
	}
	if m.Version >= 6 {
		r.Config = slices.Clone(d.Bytes())
	}
	if m.Version >= 7 {
		n := d.Count(12, maxMgrMapEntries)
		r.OSDPerfMetricReports = make([]MgrOpaqueEntry, 0, n)
		for i := 0; i < n && d.Err() == nil; i++ {
			r.OSDPerfMetricReports = append(r.OSDPerfMetricReports, MgrOpaqueEntry{Key: rawStructure(d, 1), Value: rawStructure(d, 1)})
		}
	}
	if m.Version >= 8 {
		r.TaskStatus = decodeOptionalStringMap(d)
	}
	if m.Version >= 9 {
		r.MetricReport = decodeMetricPayload(d)
	}
	if err := d.Done(); err != nil {
		return MgrDaemonReport{}, err
	}
	return r, nil
}

// MgrCounterValue is one packed counter of a report: its declared path and
// type, its value, and for a LONGRUNAVG counter its two average counts
// (native MgrClient writes avgcount and avgcount2; the native receiver keeps
// only the first).
type MgrCounterValue struct {
	Path                       string
	Type                       uint8
	Value, AvgCount, AvgCount2 uint64
}

// MgrCounterSchema is the counter declaration state of one connection: a
// sender declares a counter once per connection and later reports carry
// only its packed value, so a report can be read only after every earlier
// report of the same connection. A new connection starts a new schema (a
// stock MgrClient declares again on every new session).
//
// Apply follows native DaemonPerfCounters::update exactly: the report's
// declarations are added first (a path already known keeps its first type,
// as std::map::insert does), its withdrawals then leave the connection's
// set, and the packed buffer holds, for every surviving path in ascending
// bytewise order, a u64 value and, for LONGRUNAVG, two more u64 counts. A
// withdrawn path keeps its type, as native's per-daemon type table does. The
// packed 1/1 envelope must be consumed exactly (native skips a tail; a tail
// here means the declarations disagree). A report that fails leaves the
// schema unchanged; the connection cannot be read further.
//
// Retained state is bounded for the connection's life, stricter than
// native: only each path and its type byte are kept (not the description,
// nick, priority or unit), and the retained paths, withdrawn ones
// included, may not exceed MaxMgrCounterSchemaTypes entries or
// MaxMgrCounterSchemaPathBytes path bytes. A report whose new paths would
// pass either bound is refused with ErrMgrCounterSchemaLimit before any of
// its declarations are committed, so the previous schema stays intact; as
// for any failed report, the connection then ends. Within the bound a
// known path keeps its first type. Native keeps every type ever declared
// in a table shared by all daemons for the MGR's life with no bound of its
// own (ceph/ceph/blob/v20.2.4/src/mgr/DaemonState.cc
// DaemonPerfCounters::update inserts into types and never erases;
// ceph/ceph/blob/v20.2.4/src/mgr/DaemonState.h DaemonStateIndex::types),
// relying on stock senders, whose counter sets are fixed by their code and
// lie far below these bounds; a single report is bounded only by the
// messenger's frame limits there, as by MaxFrameSize here.
type MgrCounterSchema struct {
	types     map[string]uint8
	pathBytes int
	declared  map[string]struct{}
}

// Declared returns the connection's current counter paths in packed order.
func (s *MgrCounterSchema) Declared() []string {
	out := make([]string, 0, len(s.declared))
	for p := range s.declared {
		out = append(out, p)
	}
	slices.Sort(out)
	return out
}

// Apply reads one report's packed counters and, on success, commits its
// declarations and withdrawals.
func (s *MgrCounterSchema) Apply(r MgrDaemonReport) ([]MgrCounterValue, error) {
	types := s.types
	var added map[string]uint8
	addedBytes := 0
	for _, t := range r.Declare {
		if _, ok := types[t.Path]; !ok {
			if _, ok := added[t.Path]; !ok {
				if added == nil {
					added = make(map[string]uint8)
				}
				added[t.Path] = t.Type
				addedBytes += len(t.Path)
			}
		}
	}
	if len(types)+len(added) > MaxMgrCounterSchemaTypes || s.pathBytes+addedBytes > MaxMgrCounterSchemaPathBytes {
		return nil, fmt.Errorf("%w: %d retained and %d new counter types, %d and %d path bytes: %w",
			ErrMgrCounterSchemaLimit, len(types), len(added), s.pathBytes, addedBytes, wire.ErrLimit)
	}
	declared := make(map[string]struct{}, len(s.declared)+len(r.Declare))
	for p := range s.declared {
		declared[p] = struct{}{}
	}
	for _, t := range r.Declare {
		declared[t.Path] = struct{}{}
	}
	typeOf := func(path string) uint8 {
		if t, ok := types[path]; ok {
			return t
		}
		return added[path]
	}
	for _, p := range r.Undeclare {
		delete(declared, p)
	}
	paths := make([]string, 0, len(declared))
	for p := range declared {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	d := wire.NewDecoder(r.Packed)
	version, packed := d.Struct(1)
	values := make([]MgrCounterValue, 0, len(paths))
	for _, p := range paths {
		t := typeOf(p)
		v := MgrCounterValue{Path: p, Type: t, Value: packed.U64()}
		if t&MgrPerfCounterLongRunAvg != 0 {
			v.AvgCount, v.AvgCount2 = packed.U64(), packed.U64()
		}
		values = append(values, v)
	}
	if err := d.Done(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMgrCounterSchema, err)
	}
	if err := packed.Err(); err != nil {
		return nil, fmt.Errorf("%w: %d paths: %w", ErrMgrCounterSchema, len(paths), err)
	}
	if version <= 1 {
		if err := packed.Done(); err != nil {
			return nil, fmt.Errorf("%w: %d paths: %w", ErrMgrCounterSchema, len(paths), err)
		}
	}
	if len(added) != 0 {
		if s.types == nil {
			s.types = make(map[string]uint8, len(added))
		}
		for p, t := range added {
			s.types[p] = t
		}
		s.pathBytes += addedBytes
	}
	s.declared = declared
	return values, nil
}

// DecodeMgrCommandRequest reads a received MMgrCommand or MCommand (header
// version at least 1, compat at most 1, empty middle). Its data segment is
// the command's bulk input; Transaction is the TID a reply must carry.
func DecodeMgrCommandRequest(m MessageData, limit uint32) (c MgrCommandRequest, err error) {
	defer wrapDecode("command", &err)
	if m.Type != MgrCommandMessage && m.Type != TellCommandMessage {
		return c, ErrFrame
	}
	d, err := frontDecoder(m, m.Type, 1, 0xffff, limit)
	if err != nil {
		return c, err
	}
	copy(c.FSID[:], d.Raw(16))
	c.Command = decodeStrings(d, maxMgrCommandWords)
	if err := d.Done(); err != nil {
		return MgrCommandRequest{}, err
	}
	return c, nil
}

// CommandReplyFor builds the reply of the received command family: an
// MMgrCommandReply for MMgrCommand, an MCommandReply for MCommand, header
// 1/1, carrying the request's TID (zero included: a native MgrClient's
// CommandTable numbers its first command 0, ceph/ceph/blob/v20.2.4/src/common/CommandTable.h), the signed result (errorcode32_t: Linux
// errno values on the wire), the status string and the bulk output as the
// data segment.
func CommandReplyFor(request uint16, transaction uint64, result int32, status string, output []byte, limit uint32) (MessageData, error) {
	typ := MgrCommandReplyMessage
	switch request {
	case MgrCommandMessage:
	case TellCommandMessage:
		typ = TellCommandReplyMessage
	default:
		return MessageData{}, fmt.Errorf("%w: type %d is not a command", ErrFrame, request)
	}
	if uint64(len(status))+uint64(len(output))+8 > uint64(limit) {
		return MessageData{}, wire.ErrLimit
	}
	e := wire.Encoder{}
	e.U32(uint32(result))
	e.String(status)
	return MessageData{Transaction: transaction, Type: typ, Version: 1, CompatVersion: 1, Priority: 127, Front: e.Data, Data: slices.Clone(output)}, nil
}

// MgrConfigureData is the content of an MMgrConfigure the MGR sends:
// statistics period (seconds; 0 stops reporting) and threshold (the lowest
// counter priority reported), OSD perf-metric queries with their limits,
// and an optional metric configuration variant, all the consumer's.
type MgrConfigureData struct {
	StatsPeriod, StatsThreshold uint32
	OSDPerfMetricQueries        []MgrOpaqueEntry
	MetricConfig                *MgrMetricPayload
}

// serverPacificMask is CEPH_FEATUREMASK_SERVER_PACIFIC (bit 20 with the
// incarnation-3 bits 28 and 57).
const serverPacificMask = 1<<20 | 1<<28 | 1<<57

func checkRawStructure(b []byte, known uint8) bool {
	d := wire.NewDecoder(b)
	raw := rawStructure(d, known)
	return raw != nil && d.Done() == nil
}

func checkLimits(b []byte) bool {
	d := wire.NewDecoder(b)
	n := d.Count(6, maxMgrMapEntries)
	for i := 0; i < n && d.Err() == nil; i++ {
		rawStructure(d, 1)
	}
	return d.Done() == nil
}

// EncodeMgrConfigure builds MMgrConfigure header 4/1. Each query must be a
// complete OSDPerfMetricQuery structure with an OSDPerfMetricLimits set; the
// consumer orders them as the native map orders its keys. As native
// should_encode does, an MDS metric configuration is sent as absent to a
// peer whose negotiated features lack SERVER_PACIFIC.
func EncodeMgrConfigure(c MgrConfigureData, peerFeatures uint64, limit uint32) (MessageData, error) {
	// The caller's queries and configuration are bounded before any of
	// their bytes is copied: the front holds at least these.
	if len(c.OSDPerfMetricQueries) > maxMgrMapEntries {
		return MessageData{}, wire.ErrLimit
	}
	size := uint64(4 + 4 + 4 + 1)
	for _, q := range c.OSDPerfMetricQueries {
		size += uint64(len(q.Key)) + uint64(len(q.Value))
	}
	if m := c.MetricConfig; m != nil && (m.Type != MgrMetricMDS || peerFeatures&serverPacificMask == serverPacificMask) {
		size += 4 + uint64(len(m.Payload))
	}
	if size > uint64(limit) {
		return MessageData{}, wire.ErrLimit
	}
	e := wire.Encoder{}
	e.U32(c.StatsPeriod)
	e.U32(c.StatsThreshold)
	e.U32(uint32(len(c.OSDPerfMetricQueries)))
	for _, q := range c.OSDPerfMetricQueries {
		if !checkRawStructure(q.Key, 1) || !checkLimits(q.Value) {
			return MessageData{}, fmt.Errorf("%w: OSD perf-metric query is not a complete encoding", ErrFrame)
		}
		e.Raw(q.Key)
		e.Raw(q.Value)
	}
	switch m := c.MetricConfig; {
	case m == nil, m.Type == MgrMetricMDS && peerFeatures&serverPacificMask != serverPacificMask:
		e.U8(0)
	case m.Type != MgrMetricOSD && m.Type != MgrMetricMDS, !checkRawStructure(m.Payload, 1):
		return MessageData{}, fmt.Errorf("%w: metric configuration is not a complete encoding", ErrFrame)
	default:
		e.U8(1)
		e.U32(m.Type)
		e.Raw(m.Payload)
	}
	if uint64(len(e.Data)) > uint64(limit) {
		return MessageData{}, wire.ErrLimit
	}
	return MessageData{Type: MgrConfigureMessage, Version: 4, CompatVersion: 1, Priority: 127, Front: e.Data}, nil
}

func wrapDecode(what string, err *error) {
	if *err != nil {
		*err = fmt.Errorf("%w: invalid %s: %w", ErrFrame, what, *err)
	}
}
