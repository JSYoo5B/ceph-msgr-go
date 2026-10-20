package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

type v9RuntimeExpected struct {
	Name        string            `json:"name"`
	Reply       cephmsgr.OSDReply `json:"go_reply"`
	Route       osdFixtureOracle  `json:"native_route"`
	Incarnation uint32            `json:"client_incarnation"`
	PGOp        bool              `json:"pg_op"`
	Error       string            `json:"error,omitempty"`
	Unknown     bool              `json:"outcome_unknown"`
}

// checkActualV9Prefix interprets the pinned native prefix as consumer evidence.
// It never calls the product request encoder or admission decoder to manufacture
// a captured frame. PGOP keeps its empty object and explicit locator hash.
func checkActualV9Prefix(t *testing.T, m msgr.MessageData, want v9RuntimeExpected) {
	t.Helper()
	f := m.Front
	r := want.Route
	if m.Type != 42 || m.Version != 9 || m.CompatVersion != 3 || m.Transaction != want.Reply.Transaction || len(f) < 106 {
		t.Fatal("actual CRC request silently changed encoding", m.Type, m.Version, m.CompatVersion, m.Transaction, len(f))
	}
	hash := r.Hash
	object := r.Name
	locatorHash := int64(-1)
	if want.PGOp {
		hash = r.PG
		object = ""
		locatorHash = int64(r.PG)
	}
	if !bytes.Equal(f[:2], []byte{1, 1}) || binary.LittleEndian.Uint32(f[2:6]) != 18 || f[6] != 1 || binary.LittleEndian.Uint64(f[7:15]) != uint64(r.Pool) || binary.LittleEndian.Uint32(f[15:19]) != r.PG || binary.LittleEndian.Uint32(f[19:23]) != ^uint32(0) || f[23] != 255 || binary.LittleEndian.Uint32(f[24:28]) != hash || binary.LittleEndian.Uint32(f[28:32]) != want.Reply.Submission.MapEpoch {
		t.Fatal("actual v9 actual-PG/raw-hash/NO_SHARD/epoch prefix differs", want.Name)
	}
	flags := binary.LittleEndian.Uint32(f[32:36])
	if (flags&0x400 != 0) != want.PGOp {
		t.Fatal("actual v9 PGOP flag differs", flags, want.PGOp)
	}
	if !bytes.Equal(f[36:42], []byte{2, 2, 21, 0, 0, 0}) || !bytes.Equal(f[42:59], make([]byte, 17)) || binary.LittleEndian.Uint32(f[59:63]) != want.Incarnation || !bytes.Equal(f[63:87], make([]byte, 24)) || !bytes.Equal(f[87:94], []byte{1, 1, 1, 0, 0, 0, 0}) {
		t.Fatal("actual v9 reqid, legacy trace or empty seven-byte OTEL differs", want.Name)
	}
	d := wire.NewDecoderLimit(f[94:], 1<<20)
	if d.U32() != want.Incarnation {
		t.Fatal("actual v9 body incarnation differs")
	}
	d.U32()
	d.U32()
	version, locator := d.Struct(6)
	if version != 6 || locator.U64() != uint64(r.Pool) || locator.U32() != ^uint32(0) || locator.String() != "" || locator.String() != r.Namespace || int64(locator.U64()) != locatorHash || locator.Done() != nil || d.String() != object || d.Err() != nil {
		t.Fatal("actual v9 locator or empty PGOP object differs", want.Name)
	}
	const required = uint64(1)<<49 | uint64(1)<<57 | uint64(1)<<58 | uint64(1)<<56 | uint64(1)<<21
	if len(f) < 8 || binary.LittleEndian.Uint64(f[len(f)-8:])&required != required {
		t.Fatal("actual v9 frame lost its full fixed feature masks", want.Name)
	}
}
func TestCephOSDRequestV9Integration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "replicated-pg-request-v9-reply-v8" {
		t.Skip("requires explicit request-v9/reply-v8 fixture")
	}
	_, _, base, _ := replicatedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	original := replicatedPGNative(t, ctx, base.Name).Actual
	var expected []v9RuntimeExpected
	var captured []testcluster.CRCObservedMessage
	var captureError string
	var tapsMu sync.Mutex
	var taps []*testcluster.CRCMessageTapConn
	var before, after mutationSnapshot
	var cleanup omapWriteReceipt
	prepared, cleaned, capturedCRC := false, false, false
	reason := "secure frames are encrypted; native interoperability is recorded separately from CRC header observation"
	t.Cleanup(func() {
		defer func() {
			tapsMu.Lock()
			local := append([]*testcluster.CRCMessageTapConn(nil), taps...)
			tapsMu.Unlock()
			// Preserve transferred frames even when an operation or assertion failed.
			for _, tap := range local {
				records, captureErr := tap.Snapshot()
				captured = append(captured, records...)
				if captureErr != nil {
					captureError = captureErr.Error()
				}
			}
			evidence := struct {
				Profile          string                           `json:"profile"`
				Scope            string                           `json:"scope"`
				CapturedCRC      bool                             `json:"captured_crc"`
				ObservationLimit string                           `json:"observation_limit"`
				Expected         []v9RuntimeExpected              `json:"requests"`
				Captured         []testcluster.CRCObservedMessage `json:"actual_crc_frames"`
				CaptureError     string                           `json:"capture_error,omitempty"`
				Before, After    mutationSnapshot
				Cleanup          omapWriteReceipt `json:"cleanup"`
				Cleaned          bool             `json:"cleaned"`
			}{os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE"), "request42/v9/compat3 untraced fixed profile; reply43/v8/compat2; no fallback or automatic replay", capturedCRC, reason, expected, captured, captureError, before, after, cleanup, cleaned}
			data, err := json.Marshal(evidence)
			if err == nil && len(data) <= 1<<20 {
				err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-request-v9-results.json"), data)
			} else if err == nil {
				err = fmt.Errorf("v9 runtime evidence exceeds bound")
			}
			if err != nil {
				t.Error(err)
			}
		}()
		if prepared {
			call, stop := context.WithTimeout(context.Background(), 40*time.Second)
			defer stop()
			cleanup = replicatedPGOwned(t, call, base, "cleanup", omapWriteInput{})
			cleaned = cleanup.Cleanup.Removed
			if !cleaned {
				t.Error("native v9 owned cleanup incomplete")
			}
			if fresh := replicatedPGNative(t, call, base.Name).Actual; !reflect.DeepEqual(fresh, original) {
				t.Error("v9 acceptance changed original fixture bytes/version/mtime")
			}
		}
	})
	seed := []byte{0, 255, 128, 'v', '9', '-', 0, 'n', 'a', 't', 'i', 'v', 'e'}
	owned := replicatedPGOwned(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: seed}})
	prepared = true
	if len(owned.Objects) != 1 {
		t.Fatal("v9 runtime requires one owned UUID head")
	}
	native := replicatedPGNative(t, ctx, owned.Objects[0])
	route := native.Placement
	before = native.Actual
	if before.Code != 0 || !bytes.Equal(before.Data, seed) || route.Hash == route.PG {
		t.Fatal("native v9 baseline lacks distinct raw hash and actual PG")
	}
	options := integrationOptions(t)
	options.EnableOSD = true
	options.OSDProfile = cephmsgr.OSDProfileReplicatedPGRequestV9ReplyV8
	if options.ConnectionMode == cephmsgr.CRCMode {
		reason = "CRC observation pending validation; transferred object frames are retained on failure"
		dial := options.DialContext
		options.DialContext = func(call context.Context, network, endpoint string) (net.Conn, error) {
			conn, err := dial(call, network, endpoint)
			if err != nil {
				return nil, err
			}
			_, daemon, _ := testcluster.SplitPeer(endpoint)
			if daemon != fmt.Sprintf("osd.%d", route.ID) {
				return conn, nil
			}
			tap := &testcluster.CRCMessageTapConn{Conn: conn}
			tapsMu.Lock()
			taps = append(taps, tap)
			tapsMu.Unlock()
			return tap, nil
		}
	}
	subject, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("request-v9 MON admission", err)
	}
	t.Cleanup(func() { _ = subject.Close() })
	o := replicatedPGConnection(t, subject, ctx, route)
	t.Cleanup(func() { _ = o.Close() })
	if options.ConnectionMode == cephmsgr.CRCMode {
		tapsMu.Lock()
		if len(taps) != 1 {
			tapsMu.Unlock()
			t.Fatal("v9 CRC target unexpectedly redialed")
		}
		taps[0].Arm()
		tapsMu.Unlock()
	}
	readRequest := replicatedPGRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(seed) + 32)})
	readRequest.ClientIncarnation = 161
	first, err := o.Request(ctx, readRequest)
	if err != nil {
		t.Fatal("native v9 stat/read", err)
	}
	checkReplicatedPGReply(t, first, route)
	if len(first.Operations) != 2 || len(first.Operations[0].Data) != 16 || binary.LittleEndian.Uint64(first.Operations[0].Data) != uint64(len(seed)) || !bytes.Equal(first.Operations[1].Data, seed) || first.UserVersion != before.Version {
		t.Fatal("native v9 read bytes/stat/version differ", replyV8Summary(first))
	}
	expected = append(expected, v9RuntimeExpected{Name: "stat-read", Reply: first, Route: route, Incarnation: 161})
	tail := []byte{0, 255, 128, 'v', '9', '-', 'o', 'n', 'c', 'e', 0}
	write := replicatedPGRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail})
	write.ClientIncarnation = 163
	write.ModificationTime = cephmsgr.OSDTime{Seconds: 1700001400, Nanoseconds: 123456789}
	appended, err := o.Request(ctx, write)
	// A sent mutation error is never resubmitted by this acceptance helper.
	writeEvidence := v9RuntimeExpected{Name: "append-once", Reply: appended, Route: route, Incarnation: 163}
	if err != nil {
		writeEvidence.Error = err.Error()
		var unknown *cephmsgr.OutcomeUnknownError
		writeEvidence.Unknown = errors.As(err, &unknown)
	}
	expected = append(expected, writeEvidence)
	if err != nil {
		t.Fatal("v9 single APPEND outcome retained without retry", appended.Submission, err)
	}
	checkReplicatedPGReply(t, appended, route)
	native = replicatedPGNative(t, ctx, route.Name)
	after = native.Actual
	want := append(bytes.Clone(seed), tail...)
	if after.Code != 0 || !bytes.Equal(after.Data, want) || after.Version <= before.Version || after.Version != appended.UserVersion || after.Seconds != write.ModificationTime.Seconds || after.Nanos != write.ModificationTime.Nanoseconds || appended.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("native v9 APPEND postcondition differs", replyV8Summary(appended), after.Version)
	}
	route = native.Placement
	freshRequest := replicatedPGRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(want) + 32)})
	freshRequest.ClientIncarnation = 165
	fresh, err := o.Request(ctx, freshRequest)
	if err != nil {
		t.Fatal("v9 caller-selected fresh read", err)
	}
	checkReplicatedPGReply(t, fresh, route)
	if len(fresh.Operations) != 1 || !bytes.Equal(fresh.Operations[0].Data, want) || fresh.UserVersion != after.Version {
		t.Fatal("native v9 fresh readback differs", replyV8Summary(fresh))
	}
	expected = append(expected, v9RuntimeExpected{Name: "fresh-read", Reply: fresh, Route: route, Incarnation: 165})
	pageRequest := cephmsgr.OSDPGListRequest{ClientIncarnation: 167, MapEpoch: route.Epoch, PG: cephmsgr.OSDPGListTarget{Pool: uint64(route.Pool), Seed: route.PG, Primary: route.ID}, Namespace: route.Namespace, Count: 1, MaxOutputBytes: 4096}
	page, err := o.ListPG(ctx, pageRequest)
	if err != nil || len(page.Entries) > 1 || page.Submission.Transaction == 0 || page.Submission.ClientIncarnation != 167 || page.Submission.MapEpoch != route.Epoch || page.PG != (cephmsgr.OSDPG{Pool: uint64(route.Pool), Seed: route.PG}) {
		t.Fatal("v9 PGOP request/reply metadata differs", page.Submission, err)
	}
	expected = append(expected, v9RuntimeExpected{Name: "PGNLS-page", Reply: page.OSDReply, Route: route, Incarnation: 167, PGOp: true})
	if observed := replicatedPGNative(t, ctx, route.Name).Actual; !reflect.DeepEqual(observed, after) {
		t.Fatal("v9 read/PG enumeration altered the native head")
	}
	if options.ConnectionMode == cephmsgr.CRCMode {
		tapsMu.Lock()
		local := append([]*testcluster.CRCMessageTapConn(nil), taps...)
		tapsMu.Unlock()
		if len(local) != 1 {
			t.Fatal("v9 runtime reopened an OSD connection")
		}
		observed, observeErr := local[0].Snapshot()
		err = observeErr
		if err != nil {
			t.Fatal("actual CRC object-frame observation", err)
		}
		outgoing, incoming := map[uint64]int{}, map[uint64]int{}
		for _, record := range observed {
			var wanted *v9RuntimeExpected
			for i := range expected {
				if expected[i].Reply.Transaction == record.Message.Transaction {
					wanted = &expected[i]
					break
				}
			}
			if wanted == nil {
				t.Fatal("unexpected object request/reply in v9 CRC stream", record.Direction, record.Message.Transaction)
			}
			if record.Direction == "outgoing" {
				checkActualV9Prefix(t, record.Message, *wanted)
				outgoing[record.Message.Transaction]++
			} else {
				m := record.Message
				if m.Type != 43 || m.Version != 8 || m.CompatVersion != 2 || !bytes.Equal(m.Front, wanted.Reply.RawFront) || !bytes.Equal(m.Data, wanted.Reply.RawData) {
					t.Fatal("actual CRC response is not retained strict v8/compat2", m.Type, m.Version, m.CompatVersion)
				}
				incoming[m.Transaction]++
			}
		}
		for _, wanted := range expected {
			if outgoing[wanted.Reply.Transaction] != 1 || incoming[wanted.Reply.Transaction] != 1 {
				t.Fatal("actual v9 request or v8 reply missing/duplicated", wanted.Name, outgoing[wanted.Reply.Transaction], incoming[wanted.Reply.Transaction])
			}
		}
		capturedCRC = true
		reason = "actual CRC plaintext frames observed after authentication; secure headers are not decrypted by this test"
	}
}
