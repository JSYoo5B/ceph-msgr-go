package msgr

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Synthetic MGR-side message tests. The byte layouts are written out from
// the pinned MMgrReport.h, MMgrOpen.h, MMgrConfigure.h and the command
// headers independently of the decoder under test; the native vectors are
// in mgr_daemon_messages_native_test.go.

func le64(v uint64) []byte { return binary.LittleEndian.AppendUint64(nil, v) }

func envelope(version, compat byte, payload ...[]byte) []byte {
	body := cat(payload...)
	return cat([]byte{version, compat}, le32(uint32(len(body))), body)
}

func counterType(path string, typ, priority, unit byte) []byte {
	return envelope(3, 1, str(path), str("d"), str("n"), []byte{typ, priority, unit})
}

func packed(values ...uint64) []byte {
	var body [][]byte
	for _, v := range values {
		body = append(body, le64(v))
	}
	return envelope(1, 1, body...)
}

// report9 is an MMgrReport v9 front with the given declarations, packed
// buffer and withdrawals and every later field empty or absent.
func report9(declare [][]byte, pack []byte, undeclare ...string) MessageData {
	parts := [][]byte{str("0"), le32(uint32(len(declare)))}
	parts = append(parts, declare...)
	parts = append(parts, str(string(pack)), le32(uint32(len(undeclare))))
	for _, u := range undeclare {
		parts = append(parts, str(u))
	}
	parts = append(parts, str(""), []byte{0}, le32(0), le32(0), le32(0), []byte{0}, []byte{0})
	return MessageData{Type: MgrReportMessage, Version: 9, CompatVersion: 1, Front: cat(parts...)}
}

func decodeReport(t *testing.T, m MessageData) MgrDaemonReport {
	t.Helper()
	r, err := DecodeMgrDaemonReport(m, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestMgrCounterSchemaFollowsConnectionDeclarations is the two-report case
// derived from native MgrClient::_send_report and DaemonPerfCounters::update:
// declare a scalar b and an average c; then withdraw b, keep c without
// redeclaring it and declare z. The second packed body is c.value,
// c.avgcount, c.avgcount2, z.value in that order. The same second report on
// a fresh schema (a new connection) has no c and must fail: an old path set
// reused across connections would read it silently.
func TestMgrCounterSchemaFollowsConnectionDeclarations(t *testing.T) {
	first := report9([][]byte{counterType("b", 0x0a, 8, 0), counterType("c", 0x05, 5, 1)}, packed(11, 21, 22, 23))
	second := report9([][]byte{counterType("z", 0x02, 2, 0)}, packed(31, 32, 33, 41), "b")
	var schema MgrCounterSchema
	values, err := schema.Apply(decodeReport(t, first))
	want := []MgrCounterValue{{Path: "b", Type: 0x0a, Value: 11}, {Path: "c", Type: 0x05, Value: 21, AvgCount: 22, AvgCount2: 23}}
	if err != nil || !reflect.DeepEqual(values, want) {
		t.Fatal("first report", values, err)
	}
	values, err = schema.Apply(decodeReport(t, second))
	want = []MgrCounterValue{{Path: "c", Type: 0x05, Value: 31, AvgCount: 32, AvgCount2: 33}, {Path: "z", Type: 0x02, Value: 41}}
	if err != nil || !reflect.DeepEqual(values, want) || !reflect.DeepEqual(schema.Declared(), []string{"c", "z"}) {
		t.Fatal("second report", values, err, schema.Declared())
	}
	var fresh MgrCounterSchema
	if _, err := fresh.Apply(decodeReport(t, second)); !errors.Is(err, ErrMgrCounterSchema) || len(fresh.Declared()) != 0 {
		t.Fatal("a fresh connection must not read a report that relies on earlier declarations", err)
	}
	// A failing report leaves the schema unchanged.
	short := report9(nil, packed(1))
	if _, err := schema.Apply(decodeReport(t, short)); !errors.Is(err, ErrMgrCounterSchema) || !reflect.DeepEqual(schema.Declared(), []string{"c", "z"}) {
		t.Fatal("short packed body", err, schema.Declared())
	}
	long := report9(nil, packed(1, 2, 3, 4, 5))
	if _, err := schema.Apply(decodeReport(t, long)); !errors.Is(err, ErrMgrCounterSchema) {
		t.Fatal("a version-1 packed body with a tail must be refused", err)
	}
	// A redeclared path keeps its first type, as std::map::insert does.
	redeclare := report9([][]byte{counterType("z", 0x05, 2, 0)}, packed(1, 2, 3, 4))
	values, err = schema.Apply(decodeReport(t, redeclare))
	if err != nil || len(values) != 2 || values[1].Type != 0x02 || values[1].Value != 4 {
		t.Fatal("redeclared path", values, err)
	}
	if _, err := schema.Apply(MgrDaemonReport{Packed: []byte{1}}); !errors.Is(err, ErrMgrCounterSchema) {
		t.Fatal("a packed buffer without its envelope must be refused", err)
	}
}

func TestMgrDaemonReportVersionsAndOptionals(t *testing.T) {
	// Version 1: name, declarations and packed only.
	v1 := MessageData{Type: MgrReportMessage, Version: 1, CompatVersion: 1, Front: cat(str("x"), le32(0), str(string(packed())))}
	r, err := DecodeMgrDaemonReport(v1, 1<<20)
	if err != nil || r.DaemonName != "x" || r.Status != nil || r.TaskStatus != nil || r.MetricReport != nil {
		t.Fatal("v1 report", r, err)
	}
	// Present-empty and populated optionals, health metrics, config,
	// an opaque OSD perf-metric entry and an OSD metric payload.
	query, report := envelope(1, 1, []byte{9}), envelope(1, 1, []byte{8, 7})
	payload := envelope(1, 1, le32(1), query, report)
	front := cat(str("d1"), le32(0), str(string(packed())), le32(1), str("gone"), str("svc"),
		[]byte{1}, strMap("s", "ok"), le32(2), envelope(1, 1, []byte{0}, le64(7)), envelope(1, 1, []byte{1}, le64(2<<32|1)),
		str("cfg"), le32(1), query, report, []byte{1}, le32(0), []byte{1}, le32(MgrMetricOSD), payload)
	m := MessageData{Type: MgrReportMessage, Version: 9, CompatVersion: 1, Front: front}
	r, err = DecodeMgrDaemonReport(m, 1<<20)
	if err != nil || r.ServiceName != "svc" || r.Status["s"] != "ok" || r.TaskStatus == nil || len(r.TaskStatus) != 0 ||
		!reflect.DeepEqual(r.HealthMetrics, []MgrHealthMetric{{0, 7}, {1, 2<<32 | 1}}) || string(r.Config) != "cfg" ||
		!reflect.DeepEqual(r.Undeclare, []string{"gone"}) || len(r.OSDPerfMetricReports) != 1 ||
		!bytes.Equal(r.OSDPerfMetricReports[0].Key, query) || !bytes.Equal(r.OSDPerfMetricReports[0].Value, report) ||
		r.MetricReport == nil || r.MetricReport.Type != MgrMetricOSD || !bytes.Equal(r.MetricReport.Payload, payload) {
		t.Fatalf("populated report %+v %v", r, err)
	}
	for name, mutate := range map[string]func(MessageData) MessageData{
		"version 10": func(m MessageData) MessageData { m.Version = 10; return m },
		"compat 2":   func(m MessageData) MessageData { m.CompatVersion = 2; return m },
		"middle":     func(m MessageData) MessageData { m.Middle = []byte{1}; return m },
		"data":       func(m MessageData) MessageData { m.Data = []byte{1}; return m },
		"trailing":   func(m MessageData) MessageData { m.Front = append(bytes.Clone(m.Front), 0); return m },
		"truncated":  func(m MessageData) MessageData { m.Front = m.Front[:len(m.Front)-1]; return m },
		"unknown variant": func(m MessageData) MessageData {
			f := bytes.Clone(m.Front)
			f[len(f)-len(payload)-4] = 2
			m.Front = f
			return m
		},
		"bad presence": func(m MessageData) MessageData {
			f := bytes.Clone(m.Front)
			f[len(f)-len(payload)-5] = 2
			m.Front = f
			return m
		},
		"newer payload": func(m MessageData) MessageData {
			f := bytes.Clone(m.Front)
			f[len(f)-len(payload)+1] = 2
			m.Front = f
			return m
		},
		"another type":     func(m MessageData) MessageData { m.Type = MgrOpenMessage; return m },
		"limit below size": func(m MessageData) MessageData { return m },
	} {
		limit := uint32(1 << 20)
		if name == "limit below size" {
			limit = uint32(len(m.Front) - 1)
		}
		if _, err := DecodeMgrDaemonReport(mutate(m), limit); !errors.Is(err, ErrFrame) {
			t.Error(name, "accepted", err)
		}
	}
	// A declaration of a newer version keeps its known fields and skips
	// the tail; one older than the decoder's compat cannot exist.
	newer := envelope(4, 3, str("p"), str(""), str(""), []byte{2, 9, 1}, []byte{0xee})
	r, err = DecodeMgrDaemonReport(MessageData{Type: MgrReportMessage, Version: 2, CompatVersion: 1, Front: cat(str("x"), le32(1), newer, str(string(packed(5))), le32(0))}, 1<<20)
	if err != nil || len(r.Declare) != 1 || r.Declare[0].Path != "p" || r.Declare[0].Unit != 1 || r.Declare[0].Priority != 9 {
		t.Fatal("newer declaration", r, err)
	}
	old := envelope(1, 1, str("p"), str(""), str(""), []byte{2})
	r, err = DecodeMgrDaemonReport(MessageData{Type: MgrReportMessage, Version: 2, CompatVersion: 1, Front: cat(str("x"), le32(1), old, str(string(packed(5))), le32(0))}, 1<<20)
	if err != nil || r.Declare[0].Priority != 5 || r.Declare[0].Unit != 0 {
		t.Fatal("version-1 declaration reads native's default priority", r, err)
	}
	exact := envelope(3, 1, str("p"), str(""), str(""), []byte{2, 9, 1}, []byte{0xee})
	if _, err := DecodeMgrDaemonReport(MessageData{Type: MgrReportMessage, Version: 2, CompatVersion: 1, Front: cat(str("x"), le32(1), exact, str(string(packed(5))), le32(0))}, 1<<20); !errors.Is(err, ErrFrame) {
		t.Fatal("a known-version declaration with a tail must be refused", err)
	}
}

func TestMgrDaemonOpenUpdateAndCloseEcho(t *testing.T) {
	osd := MessageData{Type: MgrOpenMessage, Version: 3, CompatVersion: 1, Front: cat(str("0"), str(""), []byte{0}, str("c"), str("d"))}
	o, err := DecodeMgrDaemonOpen(osd, 1<<20)
	if err != nil || o.DaemonName != "0" || o.ServiceDaemon || o.Metadata != nil || string(o.Config) != "c" || string(o.ConfigDefaults) != "d" {
		t.Fatal("OSD open", o, err)
	}
	service := MessageData{Type: MgrOpenMessage, Version: 2, CompatVersion: 1, Front: cat(str("d1"), str("svc"), []byte{1}, strMap("a", "1"), strMap())}
	o, err = DecodeMgrDaemonOpen(service, 1<<20)
	if err != nil || !o.ServiceDaemon || o.Metadata["a"] != "1" || o.Status == nil || len(o.Status) != 0 || o.Config != nil {
		t.Fatal("v2 service open", o, err)
	}
	unsorted := MessageData{Type: MgrOpenMessage, Version: 2, CompatVersion: 1, Front: cat(str("d1"), str("svc"), []byte{1}, strMap("b", "1", "a", "2"), strMap())}
	if _, err := DecodeMgrDaemonOpen(unsorted, 1<<20); !errors.Is(err, ErrFrame) {
		t.Fatal("a std::map with unsorted keys must be refused", err)
	}
	update := MessageData{Type: MgrUpdateMessage, Version: 2, CompatVersion: 1, Front: cat(str("a"), str("mon"), []byte{0})}
	u, err := DecodeMgrDaemonUpdate(update, 1<<20)
	if err != nil || u.NeedMetadataUpdate || u.Metadata != nil || u.ServiceName != "mon" {
		t.Fatal("update", u, err)
	}
	close := MessageData{Type: MgrCloseMessage, Version: 1, CompatVersion: 1, Front: cat(str("d1"), str("svc"))}
	c, err := DecodeMgrDaemonClose(close, 1<<20)
	if err != nil || c.DaemonName != "d1" || c.ServiceName != "svc" {
		t.Fatal("close", c, err)
	}
	echo, err := MgrCloseEcho(c)
	if err != nil || echo.Type != MgrCloseMessage || echo.Version != 1 || echo.CompatVersion != 1 || !bytes.Equal(echo.Front, close.Front) {
		t.Fatal("echo", echo, err)
	}
	if _, err := MgrCloseEcho(MgrDaemonClose{DaemonName: "d1"}); err == nil {
		t.Fatal("an echo needs a received close")
	}
}

func TestMgrCommandRequestAndReply(t *testing.T) {
	var fsid [16]byte
	fsid[0] = 7
	for _, typ := range []uint16{MgrCommandMessage, TellCommandMessage} {
		m := MessageData{Transaction: 5, Type: typ, Version: 1, CompatVersion: 1, Front: cat(fsid[:], le32(1), str("{}")), Data: []byte{0, 255}}
		c, err := DecodeMgrCommandRequest(m, 1<<20)
		if err != nil || c.FSID != fsid || !reflect.DeepEqual(c.Command, []string{"{}"}) {
			t.Fatal("command", c, err)
		}
		reply, err := CommandReplyFor(typ, 5, -22, "bad", []byte{1, 2}, 1<<20)
		want := map[uint16]uint16{MgrCommandMessage: MgrCommandReplyMessage, TellCommandMessage: TellCommandReplyMessage}[typ]
		if err != nil || reply.Type != want || reply.Transaction != 5 || reply.Version != 1 || reply.CompatVersion != 1 ||
			!bytes.Equal(reply.Front, cat(le32(uint32(0xffffffea)), str("bad"))) || !bytes.Equal(reply.Data, []byte{1, 2}) {
			t.Fatal("reply", reply, err)
		}
		if code, status, err := CommandReply(reply, 1<<20); err != nil || code != -22 || status != "bad" {
			t.Fatal("the client's reply decoder reads the reply", code, status, err)
		}
	}
	if _, err := CommandReplyFor(MgrOpenMessage, 5, 0, "", nil, 1<<20); err == nil {
		t.Fatal("reply to a non-command")
	}
	if r, err := CommandReplyFor(MgrCommandMessage, 0, 0, "", nil, 1<<20); err != nil || r.Transaction != 0 {
		t.Fatal("a native client's first command has TID 0", r, err)
	}
	if _, err := CommandReplyFor(MgrCommandMessage, 1, 0, "", make([]byte, 64), 32); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("oversized reply", err)
	}
	if _, err := DecodeMgrCommandRequest(MessageData{Type: MgrCommandMessage, Version: 1, CompatVersion: 2, Front: cat(fsid[:], le32(0))}, 1<<20); !errors.Is(err, ErrFrame) {
		t.Fatal("compat 2 command", err)
	}
}

func TestMgrConfigureEncodingValidatesOpaqueEntries(t *testing.T) {
	m, err := EncodeMgrConfigure(MgrConfigureData{StatsPeriod: 5, StatsThreshold: 2}, 0, 1<<20)
	if err != nil || m.Type != MgrConfigureMessage || m.Version != 4 || m.CompatVersion != 1 || !bytes.Equal(m.Front, cat(le32(5), le32(2), le32(0), []byte{0})) {
		t.Fatal("plain configure", m, err)
	}
	if c, err := DecodeMgrConfigure(m, 1<<20); err != nil || c.StatsPeriod != 5 || c.StatsThreshold != 2 {
		t.Fatal("the service daemon's configure decoder reads it", c, err)
	}
	query, limits := envelope(1, 1, []byte{3}), cat(le32(1), envelope(1, 1, []byte{3}, le64(10)))
	m, err = EncodeMgrConfigure(MgrConfigureData{StatsPeriod: 1, OSDPerfMetricQueries: []MgrOpaqueEntry{{Key: query, Value: limits}}}, 0, 1<<20)
	if err != nil || !bytes.Equal(m.Front, cat(le32(1), le32(0), le32(1), query, limits, []byte{0})) {
		t.Fatal("query configure", m, err)
	}
	mds := &MgrMetricPayload{Type: MgrMetricMDS, Payload: envelope(1, 1, le32(0))}
	m, err = EncodeMgrConfigure(MgrConfigureData{MetricConfig: mds}, serverPacificMask, 1<<20)
	if err != nil || !bytes.Equal(m.Front, cat(le32(0), le32(0), le32(0), []byte{1}, le32(MgrMetricMDS), mds.Payload)) {
		t.Fatal("MDS configure", m, err)
	}
	m, err = EncodeMgrConfigure(MgrConfigureData{MetricConfig: mds}, serverPacificMask&^(1<<20), 1<<20)
	if err != nil || !bytes.Equal(m.Front, cat(le32(0), le32(0), le32(0), []byte{0})) {
		t.Fatal("an MDS configuration without SERVER_PACIFIC goes as absent", m, err)
	}
	for name, c := range map[string]MgrConfigureData{
		"query tail":     {OSDPerfMetricQueries: []MgrOpaqueEntry{{Key: append(bytes.Clone(query), 0), Value: limits}}},
		"limits short":   {OSDPerfMetricQueries: []MgrOpaqueEntry{{Key: query, Value: limits[:len(limits)-1]}}},
		"query compat":   {OSDPerfMetricQueries: []MgrOpaqueEntry{{Key: envelope(2, 2, []byte{3}), Value: limits}}},
		"variant":        {MetricConfig: &MgrMetricPayload{Type: 2, Payload: mds.Payload}},
		"payload length": {MetricConfig: &MgrMetricPayload{Type: MgrMetricOSD, Payload: mds.Payload[:5]}},
	} {
		if _, err := EncodeMgrConfigure(c, serverPacificMask, 1<<20); !errors.Is(err, ErrFrame) {
			t.Error(name, "accepted", err)
		}
	}
	if _, err := EncodeMgrConfigure(MgrConfigureData{}, 0, 8); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("oversized configure", err)
	}
}

// TestMgrCounterSchemaBoundsRetainedTypes fills one connection's schema to
// MaxMgrCounterSchemaTypes and MaxMgrCounterSchemaPathBytes: a report whose
// new paths would pass a bound is refused with ErrMgrCounterSchemaLimit
// (and wire.ErrLimit) and leaves the schema as it was; redeclaring a known
// path adds nothing and keeps its first type; a withdrawn path still
// counts, because its type stays retained.
func TestMgrCounterSchemaBoundsRetainedTypes(t *testing.T) {
	declare := func(paths ...string) []MgrPerfCounterType {
		out := make([]MgrPerfCounterType, 0, len(paths))
		for _, p := range paths {
			out = append(out, MgrPerfCounterType{Path: p, Type: 0x02})
		}
		return out
	}
	zeros := func(n int) []byte { return packed(make([]uint64, n)...) }
	refused := func(err error) bool {
		return errors.Is(err, ErrMgrCounterSchemaLimit) && errors.Is(err, wire.ErrLimit) && !errors.Is(err, ErrMgrCounterSchema)
	}
	var schema MgrCounterSchema
	paths := make([]string, MaxMgrCounterSchemaTypes-1)
	for i := range paths {
		paths[i] = fmt.Sprintf("p%05d", i)
	}
	if _, err := schema.Apply(MgrDaemonReport{Declare: declare(paths...), Packed: zeros(len(paths))}); err != nil {
		t.Fatal("filling below the bound", err)
	}
	if _, err := schema.Apply(MgrDaemonReport{Declare: declare("q0", "q1"), Packed: zeros(len(paths) + 2)}); !refused(err) {
		t.Fatal("two new paths past the type bound", err)
	}
	if got := schema.Declared(); len(got) != len(paths) || len(schema.types) != len(paths) || slices.Contains(got, "q0") {
		t.Fatal("a refused report changed the schema", len(got), len(schema.types))
	}
	// Exactly at the bound; p00000 redeclared as LONGRUNAVG keeps its
	// first (scalar) type, so it still packs one u64.
	redeclared := append(declare("q0"), MgrPerfCounterType{Path: "p00000", Type: MgrPerfCounterLongRunAvg})
	values, err := schema.Apply(MgrDaemonReport{Declare: redeclared, Packed: zeros(len(paths) + 1)})
	if err != nil || len(values) != MaxMgrCounterSchemaTypes || values[0].Type != 0x02 {
		t.Fatal("a report exactly at the bound", len(values), err)
	}
	if _, err := schema.Apply(MgrDaemonReport{Packed: zeros(len(paths)), Undeclare: []string{"q0"}}); err != nil {
		t.Fatal("withdrawal", err)
	}
	if _, err := schema.Apply(MgrDaemonReport{Declare: declare("q1"), Packed: zeros(len(paths) + 1)}); !refused(err) {
		t.Fatal("a withdrawn path must still count toward the bound", err)
	}
	if _, err := schema.Apply(MgrDaemonReport{Packed: zeros(len(paths))}); err != nil || slices.Contains(schema.Declared(), "q1") {
		t.Fatal("the schema after the refusals", err)
	}

	var bytesSchema MgrCounterSchema
	half := MaxMgrCounterSchemaPathBytes / 2
	a, b := strings.Repeat("a", half), strings.Repeat("b", half)
	if _, err := bytesSchema.Apply(MgrDaemonReport{Declare: declare(a, b), Packed: zeros(2)}); err != nil {
		t.Fatal("path bytes exactly at the bound", err)
	}
	if _, err := bytesSchema.Apply(MgrDaemonReport{Declare: declare("c"), Packed: zeros(3)}); !refused(err) {
		t.Fatal("one path byte past the bound", err)
	}
	if got := bytesSchema.Declared(); len(got) != 2 || bytesSchema.pathBytes != MaxMgrCounterSchemaPathBytes {
		t.Fatal("a refused report changed the byte count", len(got), bytesSchema.pathBytes)
	}
}
