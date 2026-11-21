package cephmsgr

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/netip"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/maps"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// nativeTestDigest returns one natively encoded PGMapDigest of
// internal/msgr/testdata/mgr-report-native-provenance.json; the synthetic
// MON here only carries it.
func nativeTestDigest(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile("../internal/msgr/testdata/mgr-report-native-provenance.json")
	var proof struct {
		Fixtures map[string]struct {
			Bytes string `json:"bytes_hex"`
		} `json:"fixtures"`
	}
	if err != nil || json.Unmarshal(raw, &proof) != nil {
		t.Fatal("native report provenance", err)
	}
	b, err := hex.DecodeString(proof.Fixtures[name].Bytes)
	if err != nil || len(b) == 0 {
		t.Fatal("native digest", name, err)
	}
	return b
}

func (f *fakeMgrMon) receivedReports() []msgr.MessageData {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]msgr.MessageData(nil), f.reports...)
}

func waitReports(t *testing.T, f *fakeMgrMon, n int) []msgr.MessageData {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if r := f.receivedReports(); len(r) >= n {
			return r
		}
		if time.Now().After(deadline) {
			t.Fatalf("the MON received %d of %d reports", len(f.receivedReports()), n)
		}
	}
}

func TestMgrMonitorReportCarriesConsumerContentAndOwnGlobalID(t *testing.T) {
	f := newFakeMgrMon(t)
	d, err := DialMgrDaemon(context.Background(), f.options())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	f.next(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h, err := d.Monitor(ctx)
	if err != nil || h.Generation() != 1 || h.Endpoint() == "" {
		t.Fatal("handle", h, err)
	}
	digest := nativeTestDigest(t, "digest-a")
	report := MgrMonReport{
		HealthChecks:   []MgrHealthCheck{{Code: "GOTEST", Severity: MgrHealthWarn, Summary: "s", Detail: []string{"d"}, Count: 2}},
		ProgressEvents: []MgrProgressEvent{{ID: "p", Message: "m", Progress: 0.5, AddToStatus: true}},
		Digest:         digest,
	}
	if err := h.SendReport(ctx, report); err != nil {
		t.Fatal(err)
	}
	got := waitReports(t, f, 1)[0]
	want, err := msgr.EncodeMonMgrReport(msgr.MonMgrReport{
		HealthChecks: []msgr.HealthCheck{{Code: "GOTEST", Severity: msgr.HealthWarn, Summary: "s", Detail: []string{"d"}, Count: 2}},
		Progress:     []msgr.ProgressEvent{{ID: "p", Message: "m", Progress: 0.5, AddToStatus: true}},
		GlobalID:     4242, Digest: digest,
	}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != msgr.MonMgrReportMessage || got.Version != 3 || got.CompatVersion != 1 || !bytes.Equal(got.Front, want.Front) || !bytes.Equal(got.Data, digest) {
		t.Fatalf("received report %+v", got)
	}
	// Content the MON could not decode never leaves the daemon.
	if err := h.SendReport(ctx, MgrMonReport{}); !errors.Is(err, ErrMgrReportContent) {
		t.Fatal("report without a digest", err)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if err := h.SendReport(canceled, report); !errors.Is(err, ErrNotSent) || errors.Is(err, ErrMgrMonitorExpired) || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled report", err)
	}
	time.Sleep(100 * time.Millisecond)
	if n := len(f.receivedReports()); n != 1 {
		t.Fatal("refused reports reached the MON", n)
	}
}

// TestMgrMonitorHandleExpiresWithItsSession ends the MON session a handle is
// bound to. The daemon reconnects; the old handle's Done closes and its
// report is refused before anything is queued, and never appears on the
// replacement session, which a new handle reaches.
func TestMgrMonitorHandleExpiresWithItsSession(t *testing.T) {
	f := newFakeMgrMon(t)
	d, err := DialMgrDaemon(context.Background(), f.options())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	first := f.next(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	old, err := d.Monitor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first.conn.Close()
	f.next(t)
	select {
	case <-old.Done():
	case <-ctx.Done():
		t.Fatal("handle Done stayed open")
	}
	waitDaemon(t, d, func(s MgrDaemonState) bool { return s.Monitor.Ready && s.Monitor.Generation == 2 })
	report := MgrMonReport{Digest: nativeTestDigest(t, "digest-empty")}
	if err := old.SendReport(ctx, report); !errors.Is(err, ErrMgrMonitorExpired) || !errors.Is(err, ErrNotSent) {
		t.Fatal("report through the replaced session's handle", err)
	}
	current, err := d.Monitor(ctx)
	if err != nil || current.Generation() != 2 {
		t.Fatal("new handle", current, err)
	}
	if err := current.SendReport(ctx, report); err != nil {
		t.Fatal(err)
	}
	waitReports(t, f, 1)
	time.Sleep(100 * time.Millisecond)
	if n := len(f.receivedReports()); n != 1 {
		t.Fatal("the expired handle's report was sent", n)
	}
	d.Close()
	if err := current.SendReport(ctx, report); !errors.Is(err, ErrMgrMonitorExpired) || !errors.Is(err, ErrClosed) {
		t.Fatal("report after Close", err)
	}
	if _, err := d.Monitor(ctx); !errors.Is(err, ErrClosed) {
		t.Fatal("Monitor after Close", err)
	}
}

// stallConn stops the MON side's reads while stalled, so the daemon's
// writer blocks inside a frame and later sends stay queued.
type stallConn struct {
	net.Conn
	gate *stallGate
}

type stallGate struct {
	mu      sync.Mutex
	stalled bool
	release chan struct{}
}

func (g *stallGate) stall() { g.mu.Lock(); g.stalled = true; g.mu.Unlock() }

func (c stallConn) Read(p []byte) (int, error) {
	c.gate.mu.Lock()
	stalled, release := c.gate.stalled, c.gate.release
	c.gate.mu.Unlock()
	if stalled {
		<-release
	}
	return c.Conn.Read(p)
}

// TestMgrMonitorQueuedReportIsWithdrawnOnClose stalls the MON's reads, so
// the writer blocks inside one frame (a report or a keepalive) and at least
// one of two large reports waits unstarted. Close records the end under the
// daemon lock and withdraws it there: while Close is held after that record
// and before any session fails, the unstarted report has already returned
// ErrMgrMonitorExpired wrapping ErrNotSent and ErrClosed, and a new report
// is refused. A started one ends as an OutcomeUnknownError, never nil, and
// the MON receives neither.
func TestMgrMonitorQueuedReportIsWithdrawnOnClose(t *testing.T) {
	f := newFakeMgrMon(t)
	gate := &stallGate{release: make(chan struct{})}
	defer close(gate.release)
	f.wrap = func(c net.Conn) net.Conn { return stallConn{Conn: c, gate: gate} }
	results := make(chan error, 2)
	var h *MgrMonitorSession
	var early []error
	var late error
	hooks := &mgrDaemonHooks{closeRecorded: func() {
		for range 2 {
			select {
			case err := <-results:
				early = append(early, err)
			case <-time.After(2 * time.Second):
				early = append(early, errors.New("a report did not end before the sessions failed"))
			}
		}
		late = h.SendReport(context.Background(), MgrMonReport{Digest: nativeTestDigest(t, "digest-empty")})
	}}
	d, err := DialMgrDaemon(context.WithValue(context.Background(), mgrDaemonHooksKey{}, hooks), f.options())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	f.next(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if h, err = d.Monitor(ctx); err != nil {
		t.Fatal(err)
	}
	big := MgrMonReport{HealthChecks: []MgrHealthCheck{{Code: "BIG", Summary: strings.Repeat("x", 512<<10)}}, Digest: nativeTestDigest(t, "digest-empty")}
	gate.stall()
	for range 2 {
		go func() { results <- h.SendReport(ctx, big) }()
	}
	waitFor := func(ok func() bool) {
		for deadline := time.Now().Add(5 * time.Second); !ok(); time.Sleep(5 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal("reports did not reach the writer")
			}
		}
	}
	waitFor(func() bool { d.mu.Lock(); defer d.mu.Unlock(); return len(d.sends) == 2 })
	time.Sleep(200 * time.Millisecond) // let the writer take one into its blocked write
	d.Close()
	if len(early) != 2 || !errors.Is(late, ErrMgrMonitorExpired) || !errors.Is(late, ErrNotSent) || !errors.Is(late, ErrClosed) {
		t.Fatal("Close hook", early, late)
	}
	withdrawn := 0
	for _, err := range early {
		var unknown *OutcomeUnknownError
		switch {
		case errors.Is(err, ErrMgrMonitorExpired) && errors.Is(err, ErrNotSent) && errors.Is(err, ErrClosed):
			withdrawn++
		case errors.As(err, &unknown) && !errors.Is(err, ErrNotSent) && errors.Is(err, ErrClosed):
		default:
			t.Fatal("report outcome across Close", err)
		}
	}
	if withdrawn == 0 {
		t.Fatal("no queued report was withdrawn")
	}
	d.mu.Lock()
	left := len(d.sends)
	d.mu.Unlock()
	if left != 0 {
		t.Fatal("sends left registered", left)
	}
	if n := len(f.receivedReports()); n != 0 {
		t.Fatal("the MON received a report across Close", n)
	}
}

// TestMgrMonitorWaitsForAdmissionAndHonorsContext: Monitor returns a ctx
// error without consuming anything when no session is admitted in time.
func TestMgrMonitorWaitsForAdmissionAndHonorsContext(t *testing.T) {
	f := newFakeMgrMon(t)
	d, err := DialMgrDaemon(context.Background(), f.options())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	f.next(t)
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := d.Monitor(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled Monitor", err)
	}
}

// allocatedPerCall is the heap bytes f allocates per call, averaged over
// ten calls; the daemon's own workers add only small noise.
func allocatedPerCall(f func()) uint64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range 10 {
		f()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / 10
}

// TestOversizedReportAndBeaconAreRefusedBeforeCopying: an 8 MiB health
// summary, detail line or progress message, and an 8 MiB beacon metadata
// value, module option or command, are refused as ErrLimitExceeded under a
// 64 KiB frame limit without converting or copying them, and nothing
// reaches the MON.
func TestOversizedReportAndBeaconAreRefusedBeforeCopying(t *testing.T) {
	f := newFakeMgrMon(t)
	options := f.options()
	options.MaxFrameSize = 64 << 10
	d, err := DialMgrDaemon(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	f.next(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h, err := d.Monitor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	digest := nativeTestDigest(t, "digest-empty")
	big := strings.Repeat("x", 8<<20)
	for name, r := range map[string]MgrMonReport{
		"summary":  {Digest: digest, HealthChecks: []MgrHealthCheck{{Code: "A", Summary: big}}},
		"detail":   {Digest: digest, HealthChecks: []MgrHealthCheck{{Code: "A", Detail: []string{big}}}},
		"progress": {Digest: digest, ProgressEvents: []MgrProgressEvent{{ID: "p", Message: big}}},
	} {
		var err error
		bytes := allocatedPerCall(func() { err = h.SendReport(ctx, r) })
		if !errors.Is(err, ErrLimitExceeded) || errors.Is(err, ErrNotSent) {
			t.Fatal(name, err)
		}
		if bytes > 64<<10 {
			t.Errorf("report %s: refusing allocated %d bytes per call", name, bytes)
		}
	}
	for name, b := range map[string]MgrBeacon{
		"metadata": {Metadata: map[string]string{"k": big}},
		"option":   {Modules: []ManagerModule{{Name: "m", Options: map[string]ManagerModuleOption{"o": {Tags: []string{big}}}}}},
		"command":  {Commands: []MgrCommandDescriptor{{Help: big}}},
	} {
		var err error
		bytes := allocatedPerCall(func() { err = d.SendBeacon(ctx, b) })
		if !errors.Is(err, ErrLimitExceeded) {
			t.Fatal(name, err)
		}
		if bytes > 64<<10 {
			t.Errorf("beacon %s: refusing allocated %d bytes per call", name, bytes)
		}
	}
	time.Sleep(100 * time.Millisecond)
	f.mu.Lock()
	beacons, reports := len(f.beacons), len(f.reports)
	f.mu.Unlock()
	if beacons != 0 || reports != 0 {
		t.Fatal("refused content reached the MON", beacons, reports)
	}
}

// TestMgrBeaconLowerBoundNeverExceedsTheFrame: the early beacon bound
// never refuses a beacon whose encoding fits, duplicated set entries
// included.
func TestMgrBeaconLowerBoundNeverExceedsTheFrame(t *testing.T) {
	address := ManagerAddress{Type: 2, Nonce: 7, Endpoint: netip.MustParseAddrPort("192.0.2.1:6800")}
	option := ManagerModuleOption{Name: "o", DefaultValue: "d", EnumAllowed: []string{"a", "a", "bb"}, Tags: []string{"t", "t"}, SeeAlso: []string{"s"}}
	for name, b := range map[string]MgrBeacon{
		"empty":     {},
		"populated": {Available: true, Addresses: []ManagerAddress{address, address}, Metadata: map[string]string{"a": "b"}, Services: map[string]string{"x": "y"}, Features: 9},
		"modules": {Modules: []ManagerModule{{Name: "m", ErrorString: "e", Options: map[string]ManagerModuleOption{"o": option, "p": {}}}, {Name: "m"}},
			Commands: []MgrCommandDescriptor{{Command: "c", Help: "h", Module: "m", Permissions: "r", Flags: 1}},
			Clients:  []MgrBeaconClient{{Name: "c", Addresses: []ManagerAddress{address}}, {Name: "b"}}},
	} {
		content := maps.MgrBeacon{Available: b.Available, ServerAddresses: wireAddresses(b.Addresses), Features: b.Features, Metadata: b.Metadata, Services: b.Services, Name: "x"}
		for _, m := range b.Modules {
			content.Modules = append(content.Modules, wireModule(m))
		}
		for _, c := range b.Commands {
			content.Commands = append(content.Commands, maps.MgrCommand{Command: c.Command, Help: c.Help, Module: c.Module, Permissions: c.Permissions, Flags: c.Flags})
		}
		for _, c := range b.Clients {
			content.Clients = append(content.Clients, maps.MgrClient{Name: c.Name, Addresses: wireAddresses(c.Addresses)})
		}
		if bound, front := mgrBeaconLowerBound(b), uint64(len(maps.EncodeMgrBeacon(content).Front)); bound > front {
			t.Errorf("%s: bound %d exceeds the %d-byte front", name, bound, front)
		}
	}
}
