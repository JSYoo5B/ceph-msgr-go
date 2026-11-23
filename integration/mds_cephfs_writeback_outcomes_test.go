package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Test-only consumer bookkeeping of the further interrupted-writeback cells of
// issue #10 checkpoint 2: a reply withheld after the OSD applied the write, a
// consumer write applied in part, two writers on one file, an uncertain
// metadata flush of the confirmed prefix across an MDS restart, eviction with
// a later confirmed extent in place and an OSD restart. Which bytes count as
// file data and what a metadata flush reports stay the consumer's decisions.

// cephfsWritebackOutcomes is the file plan of these cells: four stripe units
// of one period, two per object. A and C share object 0, B and D object 1.
// P is one consumer write of C and D together: two object extents.
type cephfsWritebackOutcomes struct {
	A, B, C, D [2]uint64
}

// P is the file range of the two-extent write of C and D.
func (m cephfsWritebackOutcomes) P() [2]uint64 { return [2]uint64{m.C[0], m.C[1] + m.D[1]} }

// end is the end of D, the end of the planned file range.
func (m cephfsWritebackOutcomes) end() uint64 { return m.D[0] + m.D[1] }

// cephfsWritebackOutcomesPlan lays A, B, C and D out as consecutive stripe
// units and checks the object placement the cells rely on: every range is one
// extent, A and C are disjoint ranges of one object, B and D of another, and
// P is exactly C's extent followed by D's.
func cephfsWritebackOutcomesPlan(l cephfsStriping) (cephfsWritebackOutcomes, error) {
	su := l.StripeUnit
	m := cephfsWritebackOutcomes{A: [2]uint64{0, su}, B: [2]uint64{su, su}, C: [2]uint64{2 * su, su}, D: [2]uint64{3 * su, su}}
	var ext [4]cephfsObjectExtent
	for i, r := range [][2]uint64{m.A, m.B, m.C, m.D} {
		e := l.fileToExtents(r[0], r[1])
		if len(e) != 1 {
			return m, fmt.Errorf("file range %v is %d object extents, not one", r, len(e))
		}
		ext[i] = e[0]
	}
	a, b, c, d := ext[0], ext[1], ext[2], ext[3]
	switch {
	case a.ObjectNo != c.ObjectNo || a.Offset+a.Length > c.Offset:
		return m, errors.New("A and C must be disjoint ranges of one object")
	case b.ObjectNo == a.ObjectNo || b.ObjectNo != d.ObjectNo || b.Offset+b.Length > d.Offset:
		return m, errors.New("B and D must be disjoint ranges of another object")
	}
	p := l.fileToExtents(m.P()[0], m.P()[1])
	if len(p) != 2 || p[0].ObjectNo != c.ObjectNo || p[0].Offset != c.Offset || p[0].Length != c.Length ||
		p[1].ObjectNo != d.ObjectNo || p[1].Offset != d.Offset || p[1].Length != d.Length {
		return m, errors.New("P must be C's extent followed by D's")
	}
	return m, nil
}

// cephfsOutcomesState is what a cell's first leaf hands to its next one:
// the file, its plan and ledger, and the native state every later observer
// must see (object images, object versions, size and time).
type cephfsOutcomesState struct {
	File      *cephfsDataFileEvidence
	Plan      cephfsWritebackOutcomes
	Trunc     cephfsTrunc
	Context   cephmsgr.OSDSnapContext
	Writer1   *cephfsWritebackLedger
	Images    map[uint64][]byte
	Versions  [2]uint64
	Committed uint64
	MTime     cephmsgr.MDSTime
}

// cephfsWritersCommittedEnd is the size the writers' metadata flushes may
// report together: each writer reports only the gap-free completed prefix of
// its own ledger, because it does not know another writer's unknown outcome,
// and the MDS keeps the largest reported size (Locker::_do_cap_update only
// grows it).
func cephfsWritersCommittedEnd(ledgers ...*cephfsWritebackLedger) uint64 {
	var end uint64
	for _, l := range ledgers {
		end = max(end, l.committedEnd(0))
	}
	return end
}

// cephfsWritebackFault wraps every connection of one Go client by the
// endpoint it dialed (testcluster.LostReplyConn). Arming an endpoint
// withholds the next bytes the client reads from it (the reply of a write the
// OSD already applied) or drops the next bytes it writes there (a request
// lost before OSD receipt); other endpoints are unaffected.
type cephfsWritebackFault struct {
	mu      sync.Mutex
	flags   map[string]*cephfsFaultFlags
	dials   map[string]int
	blocked chan string
	dropped chan string
}

type cephfsFaultFlags struct {
	withhold, drop atomic.Bool
}

func newCephFSWritebackFault() *cephfsWritebackFault {
	return &cephfsWritebackFault{flags: map[string]*cephfsFaultFlags{}, dials: map[string]int{}, blocked: make(chan string, 8), dropped: make(chan string, 8)}
}

func cephfsFaultEndpoint(endpoint string) string {
	if parsed, err := netip.ParseAddrPort(endpoint); err == nil {
		return parsed.String()
	}
	return endpoint
}

func (f *cephfsWritebackFault) endpoint(endpoint string) *cephfsFaultFlags {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.flags[endpoint] == nil {
		f.flags[endpoint] = &cephfsFaultFlags{}
	}
	return f.flags[endpoint]
}

// wrap installs the fault in front of the client's dialer.
func (f *cephfsWritebackFault) wrap(o *cephmsgr.Options) {
	dial := o.DialContext
	o.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		endpoint := cephfsFaultEndpoint(address)
		flags := f.endpoint(endpoint)
		f.mu.Lock()
		f.dials[endpoint]++
		f.mu.Unlock()
		signal := func(ch chan string) func() {
			return func() {
				select {
				case ch <- endpoint:
				default:
				}
			}
		}
		return &testcluster.LostReplyConn{Conn: conn, Armed: &flags.withhold, DropWrites: &flags.drop, Closed: make(chan struct{}),
			SignalBlocked: signal(f.blocked), SignalWriteDropped: signal(f.dropped)}, nil
	}
}

// arm withholds the next reply from, or drops the next request to, one
// endpoint.
func (f *cephfsWritebackFault) arm(endpoint string, withhold, drop bool) {
	flags := f.endpoint(endpoint)
	flags.withhold.Store(withhold)
	flags.drop.Store(drop)
}

func (f *cephfsWritebackFault) disarm(endpoint string) { f.arm(endpoint, false, false) }

func (f *cephfsWritebackFault) dialsTo(endpoint string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dials[endpoint]
}

// await returns the endpoint a fault fired on, or "" after limit.
func cephfsAwaitFault(ch chan string, limit time.Duration) string {
	select {
	case endpoint := <-ch:
		return endpoint
	case <-time.After(limit):
		return ""
	}
}

func TestCephFSWritebackOutcomesPlanAndWriterBound(t *testing.T) {
	layout := cephfsStriping{65536, 2, 131072}
	m, err := cephfsWritebackOutcomesPlan(layout)
	if err != nil || m.A != [2]uint64{0, 65536} || m.B != [2]uint64{65536, 65536} || m.C != [2]uint64{131072, 65536} || m.D != [2]uint64{196608, 65536} ||
		m.P() != [2]uint64{131072, 131072} || m.end() != 262144 {
		t.Fatal("writeback outcomes plan differs", m, err)
	}
	// One stripe per object puts B beside A; three objects per period
	// separate A from C and B from D: both refused.
	for _, l := range []cephfsStriping{{65536, 1, 4 << 20}, {65536, 3, 131072}, {65536, 4, 65536}} {
		if _, err := cephfsWritebackOutcomesPlan(l); err == nil {
			t.Fatal("a layout that breaks the placement was accepted", l)
		}
	}

	// Writer 1: A and C completed, D (the second extent of P) unknown, X
	// refused by the OSD. Writer 2: B unknown although natively applied.
	w1 := &cephfsWritebackLedger{Writes: []cephfsWritebackWrite{
		{Phase: "a", Spans: [][2]uint64{m.A}, Outcome: cephfsWriteCompleted},
		{Phase: "p", Spans: [][2]uint64{m.C}, Outcome: cephfsWriteCompleted},
		{Phase: "p", Spans: [][2]uint64{m.D}, Outcome: cephfsWriteUncertain},
		{Phase: "x", Spans: [][2]uint64{m.D}, Outcome: cephfsWriteRefused},
	}}
	w2 := &cephfsWritebackLedger{Writes: []cephfsWritebackWrite{{Phase: "b", Spans: [][2]uint64{m.B}, Outcome: cephfsWriteUncertain}}}
	if w1.committedEnd(0) != m.A[1] || w2.committedEnd(0) != 0 || cephfsWritersCommittedEnd(w1, w2) != m.A[1] || w1.completedEnd() != m.C[0]+m.C[1] {
		t.Fatal("writer bounds differ", w1.committedEnd(0), w2.committedEnd(0), w1.completedEnd())
	}
	for i, want := range []bool{false, false, false, true} {
		if (w1.mayResend(i) == nil) != want {
			t.Fatal("writer 1 resend rule differs", i)
		}
	}
	if w2.mayResend(0) == nil {
		t.Fatal("writer 2 may resend its unknown write")
	}
	// A completed B of writer 2 alone does not let writer 1 report past its
	// own gap, and a writer with B and C completed reports C's end.
	w2.Writes[0].Outcome = cephfsWriteCompleted
	if w1.committedEnd(0) != m.A[1] || cephfsWritersCommittedEnd(w1, w2) != m.A[1] {
		t.Fatal("one writer's completion moved another writer's bound")
	}
	both := &cephfsWritebackLedger{Writes: append(slices.Clone(w1.Writes[:2]), w2.Writes[0])}
	if both.committedEnd(0) != m.C[0]+m.C[1] {
		t.Fatal("a writer with A, B and C completed does not cover C", both.committedEnd(0))
	}
	if cephfsWritersCommittedEnd() != 0 {
		t.Fatal("no writer has a bound")
	}

	// The expected object images: B applied alone on object 1, A then C on
	// object 0; D and X never land.
	a, b, c := cephfsDataStream("a", m.A[1]), cephfsDataStream("b", m.B[1]), cephfsDataStream("c", m.C[1])
	images := cephfsObjectImages(layout, []cephfsFileWrite{{m.A[0], a}, {m.B[0], b}, {m.C[0], c}})
	if len(images) != 2 || !slices.Equal(images[0], append(slices.Clone(a), c...)) || !slices.Equal(images[1], b) {
		t.Fatal("expected object images differ")
	}
}

// The fault wrapper arms one endpoint only: a withheld read blocks until the
// connection is closed, a dropped write reports success without reaching the
// peer, and a connection to another endpoint is unaffected.
func TestCephFSWritebackFaultArmsOneEndpoint(t *testing.T) {
	f := newCephFSWritebackFault()
	peers := map[string]net.Conn{}
	o := cephmsgr.Options{DialContext: func(_ context.Context, _, address string) (net.Conn, error) {
		local, remote := net.Pipe()
		peers[address] = remote
		return local, nil
	}}
	f.wrap(&o)
	armed, other := "192.0.2.1:6800", "192.0.2.2:6800"
	a, err := o.DialContext(context.Background(), "tcp", armed)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := o.DialContext(context.Background(), "tcp", other)
	if f.dialsTo(armed) != 1 || f.dialsTo(other) != 1 {
		t.Fatal("dials were not counted by endpoint")
	}
	f.arm(armed, false, true)
	if n, err := a.Write([]byte("request")); n != 7 || err != nil {
		t.Fatal("a dropped write did not report local success", n, err)
	}
	if cephfsAwaitFault(f.dropped, time.Second) != armed {
		t.Fatal("the dropped write was not signalled")
	}
	go peers[other].Write([]byte("x"))
	buf := make([]byte, 1)
	if n, err := b.Read(buf); n != 1 || err != nil {
		t.Fatal("the other endpoint was affected", n, err)
	}
	f.arm(armed, true, false)
	go peers[armed].Write([]byte("reply"))
	done := make(chan error, 1)
	go func() {
		_, err := a.Read(make([]byte, 8))
		done <- err
	}()
	if cephfsAwaitFault(f.blocked, time.Second) != armed {
		t.Fatal("the withheld reply was not signalled")
	}
	select {
	case err := <-done:
		t.Fatal("a withheld read returned before close", err)
	case <-time.After(50 * time.Millisecond):
	}
	a.Close()
	if err := <-done; !errors.Is(err, net.ErrClosed) {
		t.Fatal("the withheld read did not end with the close", err)
	}
	f.disarm(armed)
	c, _ := o.DialContext(context.Background(), "tcp", armed)
	go peers[armed].Write([]byte("y"))
	if _, err := io.ReadFull(c, buf); err != nil || f.dialsTo(armed) != 2 {
		t.Fatal("a disarmed endpoint still withholds", err)
	}
	b.Close()
	c.Close()
}
