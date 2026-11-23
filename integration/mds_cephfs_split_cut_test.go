package integration_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// A test-only cut of the split reconnect's connection between two fragments
// (issue #10). The client's MDS dials go straight to the daemon, without the
// recovery relay, so that every Messenger frame reaches the connection as one
// write (msgr.Writer writes each frame with one WriteFull). The cut is armed
// for the next dial of the replacement's endpoint: on that connection the
// first write of at least splitCutFrameBytes (the first reconnect fragment)
// passes, and the next write closes the connection without sending a byte.
// Handshake frames and acknowledgements are far smaller. The product then
// resumes its Messenger session on a new connection. Nothing here interprets
// or changes frame contents.

// splitCutFrameBytes is the least size of a write the cut treats as a
// reconnect fragment: half the 8 KiB fragment bound of the split recipe.
const splitCutFrameBytes = 4 << 10

// splitCutEvidence is the retained record of the cut.
type splitCutEvidence struct {
	Endpoint string
	// Writes are the sizes of the writes on the cut connection in order;
	// the last one is the refused write.
	Writes []int
	// PassedFragmentBytes is the size of the fragment that passed, and
	// RefusedBytes the size of the write refused at the cut.
	PassedFragmentBytes, RefusedBytes int
	CutAt                             time.Time
	// DialsAfterCut counts dials of the endpoint after the cut, and Resumes
	// is the connection's Messenger resume count once the session opened.
	DialsAfterCut int
	Resumes       uint64
}

// splitCut arms and records the cut.
type splitCut struct {
	mu        sync.Mutex
	record    *splitCutEvidence
	armed     string
	used, cut bool
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

// check requires a completed cut between the first and a later fragment and
// a resumed connection.
func (c *splitCut) check(resumes uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.record
	r.Resumes = resumes
	switch {
	case !c.used || !c.cut:
		return errors.New("the reconnect connection was not cut between fragments")
	case r.PassedFragmentBytes < splitCutFrameBytes || r.RefusedBytes == 0:
		return fmt.Errorf("the cut did not fall between two frames after a full fragment: %+v", *r)
	case r.DialsAfterCut == 0 || resumes == 0:
		return fmt.Errorf("the cut connection was not resumed: %d dials after the cut, %d resumes", r.DialsAfterCut, resumes)
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
	r := c.cut.record
	if c.cut.cut {
		c.cut.mu.Unlock()
		return 0, net.ErrClosed
	}
	r.Writes = append(r.Writes, len(p))
	if !c.passed {
		if len(p) >= splitCutFrameBytes {
			c.passed, r.PassedFragmentBytes = true, len(p)
		}
		c.cut.mu.Unlock()
		return c.Conn.Write(p)
	}
	c.cut.cut, r.RefusedBytes, r.CutAt = true, len(p), time.Now()
	c.cut.mu.Unlock()
	c.Conn.Close()
	return 0, errors.New("test cut of the split reconnect connection between fragments")
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

// TestCephFSSplitCutRefusesTheSecondFragment is a cluster-free check of the
// cut: small writes and the first fragment pass, the next write is refused
// unsent and closes the connection, later dials are counted and not cut, and
// check refuses every incomplete cut.
func TestCephFSSplitCutRefusesTheSecondFragment(t *testing.T) {
	record := &splitCutEvidence{}
	cut := &splitCut{record: record}
	local, remote := net.Pipe()
	defer remote.Close()
	total := make(chan int, 1)
	go func() {
		buffer, sum := make([]byte, 64<<10), 0
		for {
			n, err := remote.Read(buffer)
			sum += n
			if err != nil {
				total <- sum
				return
			}
		}
	}()
	if got := cut.wrap("10.0.0.1:6800", local); got != local {
		t.Fatal("an unarmed dial was wrapped")
	}
	cut.arm("10.0.0.1:6800")
	if cut.check(1) == nil {
		t.Fatal("an unused cut was accepted")
	}
	conn := cut.wrap("10.0.0.1:6800", local)
	if conn == local || cut.wrap("10.0.0.2:6800", local) != local {
		t.Fatal("the armed dial was not wrapped, or another endpoint was")
	}
	write := func(n int) error {
		_, err := conn.Write(make([]byte, n))
		return err
	}
	for _, n := range []int{26, 300, 8000} {
		if err := write(n); err != nil {
			t.Fatal("a write before the cut was refused", n, err)
		}
	}
	if cut.check(1) == nil {
		t.Fatal("a cut that never happened was accepted")
	}
	if err := write(3000); err == nil {
		t.Fatal("the write after the first fragment was not refused")
	}
	// The peer saw the bytes before the cut and then the closed connection.
	if sum := <-total; sum != 26+300+8000 {
		t.Fatal("the refused write reached the peer", sum)
	}
	if err := write(10); err == nil {
		t.Fatal("a write after the cut was accepted")
	}
	if record.PassedFragmentBytes != 8000 || record.RefusedBytes != 3000 || len(record.Writes) != 4 {
		t.Fatal("the cut record differs", *record)
	}
	if cut.check(1) == nil {
		t.Fatal("a cut without a later dial was accepted")
	}
	other, _ := net.Pipe()
	if cut.wrap("10.0.0.1:6800", other) != other || record.DialsAfterCut != 1 {
		t.Fatal("a dial after the cut was wrapped or not counted")
	}
	if cut.check(0) == nil {
		t.Fatal("a cut without a resume was accepted")
	}
	if err := cut.check(1); err != nil {
		t.Fatal(err)
	}
	// A connection whose first large write never came is not cut by small
	// writes.
	small := &splitCut{record: &splitCutEvidence{}}
	small.arm("e")
	a, b := net.Pipe()
	defer b.Close()
	go func() {
		buffer := make([]byte, 1024)
		for {
			if _, err := b.Read(buffer); err != nil {
				return
			}
		}
	}()
	c := small.wrap("e", a)
	for range 5 {
		if _, err := c.Write(make([]byte, 100)); err != nil {
			t.Fatal("small writes were cut", err)
		}
	}
	if small.cut {
		t.Fatal("small writes triggered the cut")
	}
}
