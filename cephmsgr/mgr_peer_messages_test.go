package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// Synthetic checks of the MGR daemon messages on a MgrPeer: typed delivery,
// per-connection counter declarations, Configure and the reply handles. The
// peers are this repository's test peers; the native vectors these tests
// replay come from internal/msgr/testdata (native codec evidence, not live
// interoperability).

func nativeFront(t *testing.T, name string) []byte {
	t.Helper()
	var proof struct {
		Fixtures map[string]struct {
			Front string `json:"front_hex"`
		} `json:"fixtures"`
	}
	raw, err := os.ReadFile(filepath.Join("..", "internal", "msgr", "testdata", "mgr-messages-native-provenance.json"))
	if err != nil || json.Unmarshal(raw, &proof) != nil {
		t.Fatal("native vectors", err)
	}
	front, err := hex.DecodeString(proof.Fixtures[name].Front)
	if err != nil || len(front) == 0 {
		t.Fatal("native vector", name, err)
	}
	return front
}

func (o *testOSD) message(t *testing.T, m msgr.MessageData) {
	t.Helper()
	if err := o.w.Write(m.Frame()); err != nil {
		t.Fatal(err)
	}
}

// next returns the next message frame the MGR sent, skipping ACKs and
// keepalives.
func (o *testOSD) next(t *testing.T) msgr.MessageData {
	t.Helper()
	o.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		f, err := o.r.Read()
		if err != nil {
			t.Fatal("MGR frame", err)
		}
		if f.Tag != msgr.Message {
			continue
		}
		m, err := msgr.DecodeMessage(f)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
}

func acceptPeer(t *testing.T, f *listenerFixture, c testOSDConfig) (*testOSD, *MgrPeer) {
	t.Helper()
	o, err := f.osd(t, c)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, err := f.listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close(); o.conn.Close() })
	return o, p
}

func nextMessage(t *testing.T, p *MgrPeer) MgrInboundMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m, err := p.Next(ctx)
	if err != nil {
		t.Fatal("peer message", err, p.Err())
	}
	return m
}

func TestMgrPeerDeliversDecodedOSDMessagesAndConfigures(t *testing.T) {
	f := newListenerFixture(t, MgrListenerOptions{Authorize: allowAll})
	o, p := acceptPeer(t, f, testOSDConfig{osd: 2})
	o.message(t, msgr.MessageData{Sequence: 1, Type: msgr.MgrOpenMessage, Version: 3, CompatVersion: 1, Front: nativeFront(t, "open-osd")})
	m := nextMessage(t, p)
	if m.Open == nil || m.Open.DaemonName != "0" || m.Open.ServiceDaemon || m.Report != nil || len(m.Front) == 0 {
		t.Fatalf("open %+v", m)
	}
	if err := p.Configure(context.Background(), MgrConfigure{StatsPeriod: 5, StatsThreshold: 2}); err != nil {
		t.Fatal(err)
	}
	if c := o.next(t); c.Type != msgr.MgrConfigureMessage || c.Version != 4 || !bytes.Equal(c.Front, nativeFront(t, "configure-plain")) {
		t.Fatal("configure on the wire", c)
	}
	for seq, name := range []string{"report-declare", "report-second"} {
		o.message(t, msgr.MessageData{Sequence: uint64(seq + 2), Type: msgr.MgrReportMessage, Version: 9, CompatVersion: 1, Front: nativeFront(t, name)})
	}
	first, second := nextMessage(t, p), nextMessage(t, p)
	if first.Report == nil || len(first.Report.Counters) != 2 || first.Report.Counters[1].AvgCount2 != 23 ||
		second.Report == nil || len(second.Report.Counters) != 2 || second.Report.Counters[0].Path != "c" || second.Report.Counters[0].Value != 31 ||
		second.Report.Counters[1].Path != "z" || second.Report.Counters[1].Value != 41 {
		t.Fatalf("reports %+v %+v", first.Report, second.Report)
	}
	o.message(t, msgr.MessageData{Sequence: 4, Type: msgr.PGStatsMessage, Version: 2, CompatVersion: 1, Front: nativeFront(t, "pgstats-acceptor")})
	if m := nextMessage(t, p); m.PGStats == nil || len(m.PGStats.PGs) != 2 || m.PGStats.Epoch != 77 || m.PGStats.OSD.UpFrom != 12 {
		t.Fatalf("PG statistics %+v", m.PGStats)
	}
}

// TestMgrPeerReportWithoutItsDeclarationsEndsTheSession sends, on a fresh
// connection, a report that relies on declarations of an earlier one: the
// connection ends with the counter-schema cause after the messages before it.
func TestMgrPeerReportWithoutItsDeclarationsEndsTheSession(t *testing.T) {
	f := newListenerFixture(t, MgrListenerOptions{Authorize: allowAll})
	o, p := acceptPeer(t, f, testOSDConfig{osd: 2})
	o.send(t, 1, rawOnlyType, []byte("before"))
	o.message(t, msgr.MessageData{Sequence: 2, Type: msgr.MgrReportMessage, Version: 9, CompatVersion: 1, Front: nativeFront(t, "report-second")})
	if m := nextMessage(t, p); string(m.Front) != "before" {
		t.Fatal("accepted message before the failure", m)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := p.Next(ctx); !errors.Is(err, msgr.ErrMgrCounterSchema) {
		t.Fatal("cause", err)
	}
	if err := p.Configure(context.Background(), MgrConfigure{StatsPeriod: 1}); !errors.Is(err, ErrNotSent) {
		t.Fatal("Configure after the terminal cause", err)
	}
}

// TestMgrPeerCounterDeclarationsPastTheBoundEndTheSession sends reports
// that each declare and withdraw one long counter path: withdrawn types stay
// retained, so the ninth report would take the connection's path bytes past
// msgr.MaxMgrCounterSchemaPathBytes. The eight before it are delivered; the
// ninth ends the session with ErrLimitExceeded, and nothing is sent after.
func TestMgrPeerCounterDeclarationsPastTheBoundEndTheSession(t *testing.T) {
	f := newListenerFixture(t, MgrListenerOptions{Authorize: allowAll})
	o, p := acceptPeer(t, f, testOSDConfig{osd: 2})
	str := func(v string) []byte { return append(binary.LittleEndian.AppendUint32(nil, uint32(len(v))), v...) }
	one := binary.LittleEndian.AppendUint32(nil, 1)
	const length = 512<<10 - 4<<10
	report := func(seq uint64, letter string) msgr.MessageData {
		path := strings.Repeat(letter, length)
		body := slices.Concat(str(path), str("d"), str("n"), []byte{2, 5, 0})
		declaration := append(binary.LittleEndian.AppendUint32([]byte{3, 1}, uint32(len(body))), body...)
		front := slices.Concat(str("0"), one, declaration, str(string([]byte{1, 1, 0, 0, 0, 0})), one, str(path),
			str(""), []byte{0}, make([]byte, 12), []byte{0, 0})
		return msgr.MessageData{Sequence: seq, Type: msgr.MgrReportMessage, Version: 9, CompatVersion: 1, Front: front}
	}
	if 8*length > msgr.MaxMgrCounterSchemaPathBytes || 9*length <= msgr.MaxMgrCounterSchemaPathBytes {
		t.Fatal("the path length does not straddle the bound")
	}
	for i, letter := range strings.Split("abcdefgh", "") {
		o.message(t, report(uint64(i+1), letter))
		if m := nextMessage(t, p); m.Report == nil || len(m.Report.Counters) != 0 {
			t.Fatalf("report %d within the bound %+v", i, m.Report)
		}
	}
	o.message(t, report(9, "i"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := p.Next(ctx); !errors.Is(err, ErrLimitExceeded) || !errors.Is(err, msgr.ErrMgrCounterSchemaLimit) {
		t.Fatal("cause", err)
	}
	if err := p.Configure(context.Background(), MgrConfigure{StatsPeriod: 1}); !errors.Is(err, ErrNotSent) {
		t.Fatal("Configure after the terminal cause", err)
	}
}

func commandMessage(seq, tid uint64, family uint16, input []byte) msgr.MessageData {
	front := make([]byte, 16)
	front = binary.LittleEndian.AppendUint32(front, 1)
	front = binary.LittleEndian.AppendUint32(front, 2)
	front = append(front, "{}"...)
	return msgr.MessageData{Sequence: seq, Transaction: tid, Type: family, Version: 1, CompatVersion: 1, Front: front, Data: input}
}

func TestMgrPeerCommandRepliesAreBoundToFamilyTIDAndConnection(t *testing.T) {
	f := newListenerFixture(t, MgrListenerOptions{Authorize: allowAll})
	o, p := acceptPeer(t, f, testOSDConfig{client: true, globalID: 9001})
	if id := p.Identity(); id.Entity != "client.test" || id.PeerID != 9001 || id.GlobalID != 9001 {
		t.Fatalf("client identity %+v", id)
	}
	input := []byte{0, 1, 0xfe, 0xff}
	o.message(t, commandMessage(1, 5, msgr.MgrCommandMessage, input))
	o.message(t, commandMessage(2, 6, msgr.TellCommandMessage, nil))
	mgr, tell := nextMessage(t, p).Command, nextMessage(t, p).Command
	if mgr == nil || mgr.Family != MgrCommandFamily || mgr.Transaction != 5 || !bytes.Equal(mgr.Input, input) || mgr.Generation != p.Identity().Generation ||
		tell == nil || tell.Family != TellCommandFamily || tell.Transaction != 6 {
		t.Fatalf("commands %+v %+v", mgr, tell)
	}
	// A canceled reply sends nothing and keeps the handle.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mgr.Reply(canceled, -22, "x", nil); !errors.Is(err, ErrNotSent) {
		t.Fatal("canceled reply", err)
	}
	if err := mgr.Reply(context.Background(), -22, "bad input", []byte{9, 8}); err != nil {
		t.Fatal(err)
	}
	if r := o.next(t); r.Type != msgr.MgrCommandReplyMessage || r.Transaction != 5 || !bytes.Equal(r.Data, []byte{9, 8}) {
		t.Fatal("MMgrCommandReply on the wire", r)
	} else if code, status, err := msgr.CommandReply(r, 1<<20); err != nil || code != -22 || status != "bad input" {
		t.Fatal("reply content", code, status, err)
	}
	if err := mgr.Reply(context.Background(), 0, "", nil); !errors.Is(err, ErrMgrReplyDuplicate) {
		t.Fatal("second reply", err)
	}
	// Two racing answers through one handle: exactly one is sent.
	results := make(chan error, 2)
	var start sync.WaitGroup
	start.Add(1)
	for i := 0; i < 2; i++ {
		go func() { start.Wait(); results <- tell.Reply(context.Background(), -61, "gone", nil) }()
	}
	start.Done()
	a, b := <-results, <-results
	if (a == nil) == (b == nil) || !errors.Is(errors.Join(a, b), ErrMgrReplyDuplicate) {
		t.Fatal("racing replies", a, b)
	}
	if r := o.next(t); r.Type != msgr.TellCommandReplyMessage || r.Transaction != 6 {
		t.Fatal("MCommandReply on the wire", r)
	}
	// A command whose connection ended: its handle is expired, also after
	// the same client reconnects and resends the same TID.
	o.message(t, commandMessage(3, 7, msgr.MgrCommandMessage, nil))
	held := nextMessage(t, p).Command
	p.Close()
	o2, p2 := acceptPeer(t, f, testOSDConfig{client: true, globalID: 9001})
	o2.message(t, commandMessage(1, 7, msgr.MgrCommandMessage, nil))
	resent := nextMessage(t, p2).Command
	if err := held.Reply(context.Background(), 0, "late", nil); !errors.Is(err, ErrMgrReplyExpired) || !errors.Is(err, ErrNotSent) {
		t.Fatal("reply on an ended connection", err)
	}
	if resent.Transaction != 7 || resent.Generation == held.Generation {
		t.Fatal("resent command", resent, held)
	}
	if err := resent.Reply(context.Background(), 0, "ok", nil); err != nil {
		t.Fatal(err)
	}
	if r := o2.next(t); r.Transaction != 7 || r.Type != msgr.MgrCommandReplyMessage {
		t.Fatal("reply on the replacement connection", r)
	}
}

// TestMgrPeerCommandReplyIgnoresEditedMetadata edits the exported family
// and TID of a received command and of a copy sharing its handle: the
// reply still goes out in the received family with the received TID, once.
func TestMgrPeerCommandReplyIgnoresEditedMetadata(t *testing.T) {
	f := newListenerFixture(t, MgrListenerOptions{Authorize: allowAll})
	o, p := acceptPeer(t, f, testOSDConfig{client: true, globalID: 9003})
	o.message(t, commandMessage(1, 3, msgr.MgrCommandMessage, nil))
	received := nextMessage(t, p).Command
	copied := *received
	copied.Family, copied.Transaction = TellCommandFamily, 4
	received.Family, received.Transaction = TellCommandFamily, 5
	if err := copied.Reply(context.Background(), 0, "ok", nil); err != nil {
		t.Fatal(err)
	}
	if r := o.next(t); r.Type != msgr.MgrCommandReplyMessage || r.Transaction != 3 {
		t.Fatal("reply correlation followed edited fields", r.Type, r.Transaction)
	}
	if err := received.Reply(context.Background(), 0, "again", nil); !errors.Is(err, ErrMgrReplyDuplicate) {
		t.Fatal("the original shares the copy's spent handle", err)
	}
}

func TestMgrPeerCloseAcknowledgementEchoesOnce(t *testing.T) {
	f := newListenerFixture(t, MgrListenerOptions{Authorize: allowAll})
	o, p := acceptPeer(t, f, testOSDConfig{client: true, globalID: 9002})
	front := nativeFront(t, "close")
	o.message(t, msgr.MessageData{Sequence: 1, Type: msgr.MgrCloseMessage, Version: 1, CompatVersion: 1, Front: front})
	c := nextMessage(t, p).Close
	if c == nil || c.DaemonName != "d1" || c.ServiceName != "ceph-msgr" {
		t.Fatalf("close %+v", c)
	}
	if err := c.Acknowledge(context.Background()); err != nil {
		t.Fatal(err)
	}
	if e := o.next(t); e.Type != msgr.MgrCloseMessage || e.Version != 1 || e.CompatVersion != 1 || !bytes.Equal(e.Front, front) {
		t.Fatal("echo on the wire", e)
	}
	if err := c.Acknowledge(context.Background()); !errors.Is(err, ErrMgrReplyDuplicate) {
		t.Fatal("second acknowledgement", err)
	}
}

func TestMgrListenerBindsClientIdentToItsGlobalID(t *testing.T) {
	f := newListenerFixture(t, MgrListenerOptions{Authorize: allowAll})
	if _, err := f.osd(t, testOSDConfig{client: true, globalID: 9003, identWrongID: true}); err == nil {
		t.Fatal("a client naming another global ID was admitted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := f.listener.WaitRefusal(ctx, 0)
	if err != nil || !r.Refused || r.Entity != "client.test" || r.Stage != "session identification" {
		t.Fatal("refusal", r, err)
	}
}

func TestMgrPeerMalformedDaemonMessageEndsTheSession(t *testing.T) {
	f := newListenerFixture(t, MgrListenerOptions{Authorize: allowAll})
	o, p := acceptPeer(t, f, testOSDConfig{osd: 2})
	front := nativeFront(t, "open-osd")
	o.message(t, msgr.MessageData{Sequence: 1, Type: msgr.MgrOpenMessage, Version: 3, CompatVersion: 1, Front: front[:len(front)-1]})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := p.Next(ctx); !errors.Is(err, msgr.ErrFrame) {
		t.Fatal("cause", err)
	}
}
