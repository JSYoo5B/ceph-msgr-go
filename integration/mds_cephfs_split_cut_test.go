package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// A test-only cut of the split reconnect's connection between two fragments
// (issue #10). The client's MDS dials go straight to the daemon, without the
// recovery relay, so that every Messenger frame reaches the connection as one
// write (msgr.Writer writes each frame with one WriteFull). The cut is armed
// for the next dial of the replacement's endpoint. On that connection the
// first reconnect fragment passes only when the daemon connection accepted
// every byte of it, and the next write is refused unsent and closes the
// connection. The product then resumes its Messenger session on a new
// connection. Nothing here changes frame contents.
//
// What identifies the two writes depends on the connection mode:
//
//   - crc: frames are plaintext. Each write is decoded as exactly one
//     Messenger frame (msgr.Reader, CRCs checked, no byte left over) and, for
//     a MESSAGE frame, as MClientReconnect (mds.DecodeReconnect). The passed
//     write must be the first MClientReconnect, marked "more"; the refused
//     write must be the MClientReconnect of the next message sequence. The
//     inodes of both are kept, so the cell can compare them with the
//     replacement's per-capability log.
//   - secure: frames are encrypted and only write boundaries and lengths are
//     observable. The passed write is the first one of at least
//     splitCutFrameBytes and the refused write the next one, of at least
//     splitCutMessageBytes. That it is the second fragment rests on the
//     writer's one write per frame and on nothing else being queued then (no
//     resubmission or cap flush in this recipe, keepalives an hour apart);
//     the secure row does not observe it.

// splitCutFrameBytes is the least size of a write the secure cut treats as
// the first reconnect fragment: half the 8 KiB fragment bound of the recipe.
const splitCutFrameBytes = 4 << 10

// splitCutMessageBytes is the least size of the refused write on the secure
// row: larger than any control frame (ACK, keepalive), which carry at most a
// few dozen payload bytes.
const splitCutMessageBytes = 512

// splitCutFrame is the decoded witness of one write on the crc row.
type splitCutFrame struct {
	Tag      uint8
	Type     uint16 `json:",omitempty"`
	Sequence uint64 `json:",omitempty"`
	More     bool   `json:",omitempty"`
	Inodes   []uint64
	Error    string `json:",omitempty"`
}

// splitCutWrite is one write on the cut connection: its length, the bytes
// the daemon connection accepted (absent for the refused write) and, on the
// crc row, its frame.
type splitCutWrite struct {
	Bytes     int
	Forwarded *int           `json:",omitempty"`
	Frame     *splitCutFrame `json:",omitempty"`
	Refused   bool           `json:",omitempty"`
}

// splitCutEvidence is the retained record of the cut.
type splitCutEvidence struct {
	Endpoint string
	// Mode is the connection mode, Witness "frame" (crc) or "length".
	Mode, Witness string
	// Writes are the writes on the cut connection in order; the last one is
	// the refused write.
	Writes []splitCutWrite
	// PassedFragmentBytes is the size of the fragment that passed in full,
	// and RefusedBytes the size of the write refused at the cut.
	PassedFragmentBytes, RefusedBytes int
	// Passed and Refused are their frames on the crc row.
	Passed, Refused *splitCutFrame `json:",omitempty"`
	// ShortWrite records a write the daemon connection did not accept in
	// full, or that failed; the cut is then not evidence.
	ShortWrite string `json:",omitempty"`
	CutAt      time.Time
	// DialsAfterCut counts dials of the endpoint after the cut, and Resumes
	// is the connection's Messenger resume count once the session opened.
	DialsAfterCut int
	Resumes       uint64
}

// splitCut arms and records the cut.
type splitCut struct {
	mu        sync.Mutex
	record    *splitCutEvidence
	crc       bool
	armed     string
	used, cut bool
}

// newSplitCut records into record; crc selects the frame witness.
func newSplitCut(record *splitCutEvidence, crc bool) *splitCut {
	record.Mode, record.Witness = "secure", "length"
	if crc {
		record.Mode, record.Witness = "crc", "frame"
	}
	return &splitCut{record: record, crc: crc}
}

// arm makes the next dial of endpoint the cut connection.
func (c *splitCut) arm(endpoint string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.armed, c.record.Endpoint = endpoint, endpoint
}

// wrap returns the connection of one dial of address.
func (c *splitCut) wrap(address string, conn net.Conn) net.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	if address != c.armed || c.armed == "" {
		return conn
	}
	if c.used {
		if c.cut {
			c.record.DialsAfterCut++
		}
		return conn
	}
	c.used = true
	return &splitCutConn{Conn: conn, cut: c}
}

// decodeSplitCutFrame decodes one write as exactly one crc-mode frame and a
// MESSAGE frame as MClientReconnect. Any other frame keeps its tag only.
func decodeSplitCutFrame(p []byte) *splitCutFrame {
	input := bytes.NewReader(p)
	f, err := msgr.NewReader(input, 1<<20).Read()
	if err == nil && input.Len() != 0 {
		err = fmt.Errorf("%d bytes after the frame", input.Len())
	}
	if err != nil {
		return &splitCutFrame{Error: "not one crc-mode frame: " + err.Error()}
	}
	out := &splitCutFrame{Tag: uint8(f.Tag)}
	if f.Tag != msgr.Message {
		return out
	}
	m, err := msgr.DecodeMessage(f)
	if err != nil {
		out.Error = err.Error()
		return out
	}
	out.Type, out.Sequence = m.Type, m.Sequence
	if m.Type != mds.ClientReconnectMessage {
		return out
	}
	caps, _, more, err := mds.DecodeReconnect(m, 1<<20)
	if err != nil {
		out.Error = err.Error()
		return out
	}
	out.More = more
	for _, c := range caps {
		out.Inodes = append(out.Inodes, c.Ino)
	}
	return out
}

// reconnect reports whether f is a decoded MClientReconnect.
func (f *splitCutFrame) reconnect() bool {
	return f != nil && f.Error == "" && f.Tag == uint8(msgr.Message) && f.Type == mds.ClientReconnectMessage
}

// check requires a completed cut between the first fragment, accepted in
// full, and the next write, identified as the next fragment by the mode's
// witness, and a resumed connection.
func (c *splitCut) check(resumes uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.record
	r.Resumes = resumes
	switch {
	case !c.used || !c.cut:
		return errors.New("the reconnect connection was not cut between fragments")
	case r.ShortWrite != "":
		return fmt.Errorf("a write on the cut connection was not accepted in full: %s", r.ShortWrite)
	case c.crc && (!r.Passed.reconnect() || !r.Passed.More || len(r.Passed.Inodes) == 0):
		return fmt.Errorf("the write that passed is not a nonfinal MClientReconnect: %+v", r.Passed)
	case c.crc && (!r.Refused.reconnect() || r.Refused.Sequence != r.Passed.Sequence+1 || len(r.Refused.Inodes) == 0):
		return fmt.Errorf("the refused write is not the next MClientReconnect after sequence %d: %+v", r.Passed.Sequence, r.Refused)
	case !c.crc && (r.PassedFragmentBytes < splitCutFrameBytes || r.RefusedBytes < splitCutMessageBytes):
		return fmt.Errorf("the cut did not fall between two fragment-sized writes: passed %d, refused %d bytes", r.PassedFragmentBytes, r.RefusedBytes)
	case r.DialsAfterCut == 0 || resumes == 0:
		return fmt.Errorf("the cut connection was not resumed: %d dials after the cut, %d resumes", r.DialsAfterCut, resumes)
	}
	return nil
}

// checkFragments compares, on the crc row, the inodes of the passed and the
// refused fragment with the first two fragments of the replacement's
// per-capability log: the fragment the cut withheld is the one the MDS
// handled second, and each exactly once.
func (c *splitCut) checkFragments(fragments []splitReconnectFragment) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.crc {
		return nil
	}
	r := c.record
	if len(fragments) < 2 || r.Passed == nil || r.Refused == nil || !slices.Equal(r.Passed.Inodes, fragments[0].Inodes) || !slices.Equal(r.Refused.Inodes, fragments[1].Inodes) ||
		fragments[1].More != r.Refused.More {
		return fmt.Errorf("the cut fragments differ from the fragments the replacement handled: passed %+v refused %+v log %+v", r.Passed, r.Refused, fragments)
	}
	return nil
}

// splitCutConn is the armed connection.
type splitCutConn struct {
	net.Conn
	cut    *splitCut
	passed bool
}

func (c *splitCutConn) Write(p []byte) (int, error) {
	c.cut.mu.Lock()
	defer c.cut.mu.Unlock()
	r := c.cut.record
	if c.cut.cut {
		return 0, net.ErrClosed
	}
	w := splitCutWrite{Bytes: len(p)}
	if c.cut.crc {
		w.Frame = decodeSplitCutFrame(p)
	}
	if c.passed {
		// The write after the first fragment, whatever it is, is refused
		// unsent; check decides whether it was the second fragment.
		w.Refused = true
		r.Writes = append(r.Writes, w)
		c.cut.cut, r.RefusedBytes, r.Refused, r.CutAt = true, len(p), w.Frame, time.Now()
		c.Conn.Close()
		return 0, errors.New("test cut of the split reconnect connection between fragments")
	}
	n, err := c.Conn.Write(p)
	w.Forwarded = &n
	r.Writes = append(r.Writes, w)
	if n != len(p) || err != nil {
		// A partly forwarded frame is never counted as the first fragment.
		if r.ShortWrite == "" {
			r.ShortWrite = fmt.Sprintf("write %d forwarded %d of %d bytes: %v", len(r.Writes)-1, n, len(p), err)
		}
		return n, err
	}
	fragment := len(p) >= splitCutFrameBytes
	if c.cut.crc {
		fragment = w.Frame.reconnect()
	}
	if fragment {
		c.passed, r.PassedFragmentBytes, r.Passed = true, len(p), w.Frame
	}
	return n, err
}

// splitCutClient dials one client of the completion identity whose MDS dials
// go through the cut and are recorded in dials, as recoveryClient's are. It
// admits no OSD dial.
func splitCutClient(t *testing.T, ctx context.Context, identity, keyPath string, dials *recoveryDials, cut *splitCut) *cephmsgr.Client {
	t.Helper()
	encoded, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatal("read split credential", err)
	}
	private, err := cephx.ParseKey(strings.TrimSpace(string(encoded)))
	want, profileErr := mdsFreshExpectedKeyType(os.Getenv("CEPH_MSGR_TEST_KEY_TYPE"))
	if err != nil || profileErr != nil || private.Type() != want {
		t.Fatal("split credential does not match selected key profile")
	}
	options := integrationOptions(t)
	if options.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(encoded))); err != nil {
		t.Fatal(err)
	}
	options.Identity, options.MaxMDSConnections, options.EnableMDSMaps = identity, 1, false
	options.KeepaliveInterval, options.KeepaliveTimeout = time.Hour, 2*time.Hour
	options.ConnectTimeout, options.MaxFrameSize = 5*time.Second, 1<<20
	options.EnableMDS, options.EnableCephFS = true, true
	dial := options.DialContext
	options.DialContext = func(call context.Context, network, address string) (net.Conn, error) {
		_, role, _ := testcluster.SplitPeer(address)
		if strings.HasPrefix(role, "osd.") {
			dials.OSD.Add(1)
			return nil, errors.New("the split client must not dial an OSD")
		}
		dials.mu.Lock()
		isMDS := dials.mds[address]
		dials.mu.Unlock()
		if !isMDS {
			return dial(call, network, address)
		}
		dials.MDS.Add(1)
		record := recoveryDial{At: time.Now(), Address: address}
		conn, err := dial(call, network, address)
		if err != nil {
			record.Err = err.Error()
		}
		dials.mu.Lock()
		dials.log = append(dials.log, record)
		dials.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return cut.wrap(address, conn), nil
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("authenticate split client", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// splitCutTestFrames are real crc-mode frames of a two-fragment reconnect and
// a control frame.
func splitCutTestFrames(t *testing.T) (first, second, other, control []byte) {
	t.Helper()
	frame := func(m msgr.MessageData) []byte {
		var b bytes.Buffer
		if err := msgr.NewWriter(&b, 1<<20).Write(m.Frame()); err != nil {
			t.Fatal(err)
		}
		return b.Bytes()
	}
	var caps []mds.CapReconnect
	for i := range 30 {
		caps = append(caps, mds.CapReconnect{Ino: 0x10000000001 + uint64(i), Path: strings.Repeat("s", 225), PathBase: 0x10000000000})
	}
	reconnect := func(seq uint64, chunk []mds.CapReconnect, more bool) []byte {
		m, err := mds.EncodeReconnect(chunk, nil, more)
		if err != nil {
			t.Fatal(err)
		}
		m.Sequence = seq
		return frame(m)
	}
	first, second, other = reconnect(7, caps[:25], true), reconnect(8, caps[25:], false), reconnect(9, caps[25:], false)
	var b bytes.Buffer
	if err := msgr.NewWriter(&b, 1<<20).Write(msgr.Frame{Tag: msgr.Ack, Segments: [][]byte{make([]byte, 8)}}); err != nil {
		t.Fatal(err)
	}
	return first, second, other, b.Bytes()
}

// splitCutTestConn accepts at most limit bytes of each write.
type splitCutTestConn struct {
	net.Conn
	limit   int
	written int
}

func (c *splitCutTestConn) Write(p []byte) (int, error) {
	n := min(len(p), c.limit)
	c.written += n
	if n < len(p) {
		return n, errors.New("short write")
	}
	return n, nil
}

func (c *splitCutTestConn) Close() error { return nil }

// TestCephFSSplitCutRefusesTheSecondFragment is a cluster-free check of the
// cut: on the crc row the passed write must be the first MClientReconnect in
// full and the refused write the next one; on the secure row the lengths
// bound both. A short first write, a control write after the first fragment,
// a refused write that is not the next fragment, an unused or unresumed cut
// and inodes that differ from the replacement's log are refused.
func TestCephFSSplitCutRefusesTheSecondFragment(t *testing.T) {
	first, second, other, control := splitCutTestFrames(t)
	if len(first) < splitCutFrameBytes || len(second) < splitCutMessageBytes || len(control) >= splitCutMessageBytes {
		t.Fatal("test frames do not span the secure bounds", len(first), len(second), len(control))
	}
	handshake := [][]byte{make([]byte, 26), make([]byte, 300)}
	run := func(crc bool, limit int, writes ...[]byte) (*splitCut, *splitCutTestConn, []error) {
		cut := newSplitCut(&splitCutEvidence{}, crc)
		under := &splitCutTestConn{limit: limit}
		if cut.wrap("e", under) != under {
			t.Fatal("an unarmed dial was wrapped")
		}
		cut.arm("e")
		conn := cut.wrap("e", under)
		if conn == under || cut.wrap("other", under) != under {
			t.Fatal("the armed dial was not wrapped, or another endpoint was")
		}
		var errs []error
		for _, w := range writes {
			_, err := conn.Write(w)
			errs = append(errs, err)
		}
		if cut.wrap("e", under) != under {
			t.Fatal("a dial after the cut was wrapped")
		}
		return cut, under, errs
	}
	all := func(w ...[]byte) [][]byte { return append(slices.Clone(handshake), w...) }
	for _, crc := range []bool{true, false} {
		cut, under, errs := run(crc, 1<<30, all(first, second)...)
		if err := cut.check(1); err != nil {
			t.Fatal("a cut between the two fragments was refused", crc, err)
		}
		if under.written != 26+300+len(first) || errs[len(errs)-1] == nil || errs[len(errs)-2] != nil || cut.record.RefusedBytes != len(second) || cut.record.PassedFragmentBytes != len(first) {
			t.Fatal("the refused write reached the daemon or the record differs", crc, under.written, *cut.record)
		}
		if cut.check(0) == nil {
			t.Fatal("a cut without a resume was accepted", crc)
		}
	}
	crcCut, _, _ := run(true, 1<<30, all(first, second)...)
	if crcCut.record.Passed.Sequence != 7 || !crcCut.record.Passed.More || len(crcCut.record.Passed.Inodes) != 25 || crcCut.record.Refused.Sequence != 8 ||
		crcCut.record.Refused.More || len(crcCut.record.Refused.Inodes) != 5 || crcCut.record.Witness != "frame" {
		t.Fatal("the crc witness differs", *crcCut.record.Passed, *crcCut.record.Refused)
	}
	log := []splitReconnectFragment{{More: true, Inodes: crcCut.record.Passed.Inodes}, {Inodes: crcCut.record.Refused.Inodes}}
	if err := crcCut.checkFragments(log); err != nil {
		t.Fatal(err)
	}
	if crcCut.checkFragments([]splitReconnectFragment{{More: true, Inodes: log[0].Inodes[1:]}, log[1]}) == nil || crcCut.checkFragments(log[:1]) == nil {
		t.Fatal("inodes that differ from the replacement's log were accepted")
	}
	for name, tc := range map[string]struct {
		crc    bool
		limit  int
		writes [][]byte
	}{
		"crc: short first fragment":                 {true, 2000, all(first, second)},
		"secure: short first fragment":              {false, 2000, all(first, second)},
		"crc: control write after the fragment":     {true, 1 << 30, all(first, control, second)},
		"secure: control write after the fragment":  {false, 1 << 30, all(first, control, second)},
		"crc: refused write is not the next one":    {true, 1 << 30, all(first, other)},
		"crc: refused write is the first fragment":  {true, 1 << 30, all(first, first)},
		"crc: final fragment passed first":          {true, 1 << 30, all(second, first)},
		"crc: no fragment at all":                   {true, 1 << 30, all(control, control)},
		"secure: refused write below message bound": {false, 1 << 30, all(first, make([]byte, 100))},
	} {
		cut, _, _ := run(tc.crc, tc.limit, tc.writes...)
		if err := cut.check(1); err == nil {
			t.Fatal("wrong cut accepted:", name, *cut.record)
		}
	}
	// A large write that is no frame never passes on the crc row.
	junk, _, _ := run(true, 1<<30, all(make([]byte, 8000), control)...)
	if junk.record.Passed != nil || junk.cut {
		t.Fatal("a large write that is no MClientReconnect passed on the crc row", *junk.record)
	}
	if f := decodeSplitCutFrame(append(slices.Clone(first), 0)); f.Error == "" {
		t.Fatal("a write with a byte after the frame decoded as one frame")
	}
}
