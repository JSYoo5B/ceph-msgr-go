package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Destination primary changes around COPY_FROM and COPY_FROM2. The pinned
// source (Ceph v20.2.4) gives the two cells below their meaning:
//
//   - src/osd/PrimaryLogPG.cc:13147 (on_change) cancels every copy op of a PG
//     whose interval changed with requeue = is_primary(), and
//     PrimaryLogPG.cc:344-368 (CopyFromCallback::finish) drops a copy
//     cancelled with -ECANCELED without a reply ("client resends"). A former
//     primary therefore answers nothing; this client never resends, so only
//     the caller's explicit resubmission can follow.
//   - src/osdc/Objecter.cc:2820-2830 pauses a READ op while the OSDMap carries
//     pauserd, and src/osd/OSD.cc:8215-8217 shares each map with the OSD's
//     own Objecter on receipt, before its PGs consume it. The destination
//     primary reads a copy source through that Objecter (_copy_some), so
//     pauserd holds the copy before its source read without stopping a daemon.
//     The MON sets pauserd only together with pausewr, as `osd set pause`
//     (src/mon/OSDMonitor.cc:11995-11996); this client and the oracle's observations do
//     not consult either flag, and the oracle reads no object while it is set.
//   - Explicit resubmission follows internal/osd/RESUBMIT_CONTRACT.md: the
//     original transaction and incarnation, a raised RetryAttempt, a fresh
//     connection and the caller's new placement. The OSD answers a request
//     whose identity its PG log holds from that log and executes it otherwise.
//
// The primary is moved with `osd out`, as in the RBD journal primary-change
// control: the OSD keeps running, CRUSH takes it out of the up set and the
// new primary keeps it in the acting set as its last member. Primary
// affinity is not used: a non-default affinity requires a client feature the
// legacy-v6 client does not advertise (integration/fixture/rbd_journal.go).

// objecterRequestsOp is one op of the observed source in an OSD's internal
// Objecter (tools/osd_objecter_requests_oracle.py source_ops).
type objecterRequestsOp struct {
	Index               int      `json:"index"`
	TID                 uint64   `json:"tid"`
	PG                  string   `json:"pg"`
	OSD                 int32    `json:"osd"`
	Paused              bool     `json:"paused"`
	Attempts            int      `json:"attempts"`
	TargetObjectID      string   `json:"target_object_id"`
	TargetObjectLocator string   `json:"target_object_locator"`
	OSDOps              []string `json:"osd_ops"`
	CopyGetMax          []uint64 `json:"copy_get_max"`
}

// objecterRequestsSnapshot is the fail-closed receipt of one bracketed
// snapshot of an OSD's internal Objecter. Complete is false on a refused,
// oversized or unparsable reply and whenever the placement brackets differ.
type objecterRequestsSnapshot struct {
	Complete           bool                 `json:"complete"`
	Failures           []string             `json:"failures"`
	Code               int                  `json:"code"`
	Bytes              int                  `json:"bytes"`
	LimitBytes         int                  `json:"limit_bytes"`
	SHA256             string               `json:"sha256"`
	Raw                string               `json:"raw"`
	Before             osdFixtureOracle     `json:"before"`
	After              osdFixtureOracle     `json:"after"`
	OSD                int32                `json:"osd"`
	PGID               string               `json:"pgid"`
	NumOps             int                  `json:"num_ops"`
	Sections           map[string]int       `json:"sections"`
	AbsenceEstablished bool                 `json:"absence_established"`
	Ops                []objecterRequestsOp `json:"ops"`
}

// copySnapshotInput names the observed object and which OSD a bracketed
// snapshot asks: nil is the object's primary, anything else that OSD.
func copySnapshotInput(slot int, object string, osd *int32) map[string]any {
	in := map[string]any{"slot": slot, "object": object}
	if osd != nil {
		in["osd"] = *osd
	}
	return in
}

// copyObjecterRequests takes one bracketed snapshot of the internal Objecter
// of the destination's primary (or of osd) and rechecks a complete receipt's
// raw bytes. An incomplete receipt is returned for the caller to judge.
func copyObjecterRequests(t *testing.T, ctx context.Context, slot int, object string, osd *int32, sourcePool int64, source string) objecterRequestsSnapshot {
	t.Helper()
	in := copySnapshotInput(slot, object, osd)
	in["source"] = map[string]any{"pool": sourcePool, "name": source}
	s := nativePool(t, ctx, "objecterrequests", in).ObjecterRequests
	if s.Complete {
		raw, err := base64.StdEncoding.DecodeString(s.Raw)
		digest := sha256.Sum256(raw)
		if err != nil || len(raw) != s.Bytes || s.Bytes > s.LimitBytes || hex.EncodeToString(digest[:]) != s.SHA256 {
			t.Fatal("objecter snapshot raw bytes differ from their receipt", err, len(raw), s.Bytes, s.LimitBytes)
		}
		if n := bytes.Count(raw, []byte(`"object_id":"`+source+`"`)); n != len(s.Ops) {
			t.Fatalf("parser found %d ops of %s; the raw reply names it %d times", len(s.Ops), source, n)
		}
	}
	return s
}

// copyOpsInFlight is the same for the op tracker of that OSD and one client
// entity and incarnation.
func copyOpsInFlight(t *testing.T, ctx context.Context, slot int, object string, osd *int32, gid uint64, incarnation int32) opsInFlightSnapshot {
	t.Helper()
	in := copySnapshotInput(slot, object, osd)
	in["entity"] = map[string]any{"type": "client", "num": gid, "inc": incarnation}
	s := nativePool(t, ctx, "opsinflight", in).OpsInFlight
	if s.Complete {
		raw, err := base64.StdEncoding.DecodeString(s.Raw)
		digest := sha256.Sum256(raw)
		if err != nil || len(raw) != s.Bytes || s.Bytes > s.LimitBytes || hex.EncodeToString(digest[:]) != s.SHA256 {
			t.Fatal("in-flight snapshot raw bytes differ from their receipt", err, len(raw), s.Bytes, s.LimitBytes)
		}
		prefix := fmt.Sprintf(`"description":"osd_op(client.%d.%d:`, gid, incarnation)
		if n := bytes.Count(raw, []byte(prefix)); n != len(s.Ops) {
			t.Fatalf("parser found %d ops of client.%d.%d; the raw reply holds %d", len(s.Ops), gid, incarnation, n)
		}
	}
	return s
}

// boundedRaw decodes a receipt's raw native reply for a failure message.
func boundedRaw(encoded string) string {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "undecodable raw reply"
	}
	if len(raw) > 16<<10 {
		return fmt.Sprintf("%s... (%d bytes)", raw[:16<<10], len(raw))
	}
	return string(raw)
}

// checkCopyClientOp is the part every copy barrier shares: both snapshots
// are complete and were answered by the route's primary for the route's PG
// between identical placement records of the route, and the op tracker holds
// exactly one op of the copying client entity and incarnation, for the
// route's PG at the route's epoch. It returns that op.
func checkCopyClientOp(ops opsInFlightSnapshot, objecter objecterRequestsSnapshot, route osdFixtureOracle, gid uint64, incarnation int32) (opsInFlightOp, error) {
	for name, s := range map[string]struct {
		complete bool
		failures []string
		code     int
		before   osdFixtureOracle
		after    osdFixtureOracle
		osd      int32
		pgid     string
	}{
		"op tracker": {ops.Complete, ops.Failures, ops.Code, ops.Before, ops.After, ops.OSD, ops.PGID},
		"objecter":   {objecter.Complete, objecter.Failures, objecter.Code, objecter.Before, objecter.After, objecter.OSD, objecter.PGID},
	} {
		if !s.complete || len(s.failures) != 0 || s.code != 0 {
			return opsInFlightOp{}, fmt.Errorf("incomplete %s snapshot: code %d, %q", name, s.code, s.failures)
		}
		if !reflect.DeepEqual(s.before, s.after) || !samePlacement(s.before, route) || s.before.Epoch < route.Epoch {
			return opsInFlightOp{}, fmt.Errorf("the %s snapshot is not bracketed by the route's placement: %+v vs %+v", name, s.before, route)
		}
		if s.osd != route.PrimaryID || s.osd != route.ID || s.pgid != route.PGID {
			return opsInFlightOp{}, fmt.Errorf("the %s snapshot of osd.%d for %s is not the route's primary osd.%d for %s", name, s.osd, s.pgid, route.ID, route.PGID)
		}
	}
	if want := fmt.Sprintf("client.%d.%d", gid, incarnation); ops.Client != want || len(ops.Ops) != 1 {
		return opsInFlightOp{}, fmt.Errorf("%d in-flight ops of %s (want 1 of %s)", len(ops.Ops), ops.Client, want)
	}
	op := ops.Ops[0]
	// A legacy-v6 request names the raw PG of its object hash, a replicated
	// PG request the actual PG (tools/osd_ops_in_flight_oracle.py).
	if op.TID == 0 || op.Reqid != fmt.Sprintf("client.%d.%d:%d", gid, incarnation, op.TID) || (op.PGID != route.PGID && op.PGID != route.RawPGID) || op.Epoch != route.Epoch {
		return op, fmt.Errorf("in-flight op %s for %s at epoch %d; want the route's %s or %s at epoch %d", op.Reqid, op.PGID, op.Epoch, route.PGID, route.RawPGID, route.Epoch)
	}
	return op, nil
}

// checkCopyHeld judges the phase barrier of the in-flight cell from two
// snapshots of the destination primary taken while pauserd is set: its op
// tracker holds exactly one op of the copying client entity and incarnation,
// for the route's PG at the route's epoch, and its internal Objecter holds
// exactly one op of the owned source, paused and never sent, that is a
// single copy-get. The Objecter counts every prepared send in attempts
// (src/osdc/Objecter.cc:3385-3400 clears paused and increments it) and
// pauses an op already sent when a later map calls for it
// (Objecter.cc:3081), so a copy-get sent once and waiting to be sent again
// is paused too; only attempts 0 shows that the source read never left the
// OSD. Both snapshots answered for the route's primary between identical
// placement records of the route. It returns both ops.
func checkCopyHeld(ops opsInFlightSnapshot, objecter objecterRequestsSnapshot, route osdFixtureOracle, gid uint64, incarnation int32) (opsInFlightOp, objecterRequestsOp, error) {
	op, err := checkCopyClientOp(ops, objecter, route, gid, incarnation)
	if err != nil {
		return op, objecterRequestsOp{}, err
	}
	if objecter.AbsenceEstablished || ops.AbsenceEstablished {
		return op, objecterRequestsOp{}, errors.New("a snapshot claims absence")
	}
	if len(objecter.Ops) != 1 {
		return op, objecterRequestsOp{}, fmt.Errorf("the objecter holds %d ops of the source; want one held copy-get: %+v", len(objecter.Ops), objecter.Ops)
	}
	read := objecter.Ops[0]
	if !read.Paused || read.Attempts != 0 || len(read.CopyGetMax) != 1 || len(read.OSDOps) != 1 {
		return op, read, fmt.Errorf("the source op is not one paused copy-get that was never sent: %+v", read)
	}
	return op, read, nil
}

// checkCopyDropped judges two snapshots of the former primary after the
// primary change, while pauserd is still set: neither its op tracker nor its
// Objecter holds anything of the copy any more. The barrier saw the copy-get
// paused with no send attempt, and pauserd has stayed set since, so it was
// never sent and its disappearance is the cancellation of the copy, not its
// completion. The brackets name the new primary, so they must not name the
// former one.
func checkCopyDropped(ops opsInFlightSnapshot, objecter objecterRequestsSnapshot, former int32) error {
	if !ops.Complete || len(ops.Failures) != 0 || !objecter.Complete || len(objecter.Failures) != 0 {
		return fmt.Errorf("incomplete snapshots of the former primary: %q, %q", ops.Failures, objecter.Failures)
	}
	if ops.OSD != former || objecter.OSD != former || ops.Before.ID == former || objecter.Before.ID == former {
		return fmt.Errorf("the snapshots asked osd.%d and osd.%d under primaries osd.%d and osd.%d; want the former primary osd.%d under another",
			ops.OSD, objecter.OSD, ops.Before.ID, objecter.Before.ID, former)
	}
	if len(ops.Ops) != 0 || len(objecter.Ops) != 0 {
		return fmt.Errorf("the former primary still holds %d client ops and %d source ops", len(ops.Ops), len(objecter.Ops))
	}
	return nil
}

// checkPrimaryMoved says whether now is the PG of before under another
// primary after the former one was marked out: a newer epoch, the same PG, a
// primary that is the acting primary, and an up set without the former one.
func checkPrimaryMoved(before, now osdFixtureOracle) error {
	if now.Epoch <= before.Epoch || now.PGID != before.PGID || now.Pool != before.Pool || now.Name != before.Name {
		return fmt.Errorf("placement %+v is not a newer epoch of %+v", now, before)
	}
	if now.ID == before.ID || now.PrimaryID != now.ID || now.ID < 0 || slices.Contains(now.Up, before.ID) || !slices.Contains(now.Acting, now.ID) {
		return fmt.Errorf("osd.%d (up %v, acting %v) is not a new primary without osd.%d in the up set", now.ID, now.Up, now.Acting, before.ID)
	}
	return nil
}

// checkPrimaryReturned says whether now is the original placement of the
// destination again after recovery: a newer epoch, and otherwise the same
// PG, up and acting sets, primary, address and no pg_temp. A clean PG whose
// primary is still the replacement is refused, so the checks that follow run
// on the original primary and not on the OSD that answered before recovery.
func checkPrimaryReturned(original, now osdFixtureOracle) error {
	if now.Epoch <= original.Epoch || !samePlacement(now, original) {
		return fmt.Errorf("placement %+v is not the original placement %+v at a newer epoch", now, original)
	}
	return nil
}

// checkPrimaryRestarted is checkPrimaryReturned for an original primary that
// was killed and started again: everything but the address must be the
// original placement, because the restarted daemon binds a new address.
func checkPrimaryRestarted(original, now osdFixtureOracle) error {
	if now.Address == "" {
		return fmt.Errorf("placement %+v has no address", now)
	}
	original.Address = now.Address
	return checkPrimaryReturned(original, now)
}

// copyPrimaryRecovery undoes the cluster changes of the primary-change and
// destination-loss tests: every paused OSD is resumed and restarted, every
// killed OSD is started again, every OSD marked out is marked in again and
// pauserd is unset. restore attempts every command before one native flag
// check and one clean check, so a failed command does not keep the others
// from running; after a restart it republishes the fixture's routing. It is
// idempotent and runs from t.Cleanup of each subtest and of the test.
type copyPrimaryRecovery struct {
	c       *cephmsgr.Client
	base    osdFixtureOracle
	dir     string
	out     map[int32]bool
	pauserd bool
	// frozen OSDs are paused (docker pause); killed ones are SIGKILLed and
	// not started yet; restarted says routing must be republished.
	frozen    map[int32]bool
	killed    map[int32]bool
	restarted bool
}

func copyMonCommand(ctx context.Context, c *cephmsgr.Client, command string) error {
	_, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(command)})
	return err
}

func (r *copyPrimaryRecovery) markOut(t *testing.T, ctx context.Context, osd int32) {
	t.Helper()
	// Recorded before the command: an unknown outcome may still have applied.
	r.out[osd] = true
	if err := copyMonCommand(ctx, r.c, fmt.Sprintf(`{"prefix":"osd out","ids":["%d"]}`, osd)); err != nil {
		t.Fatal("mark the destination primary out", err)
	}
}

// freeze pauses one OSD through the fixture's daemon control. It is recorded
// first, so an uncertain pause is still resumed.
func (r *copyPrimaryRecovery) freeze(t *testing.T, ctx context.Context, osd int32) {
	t.Helper()
	r.frozen[osd] = true
	if err := testcluster.ControlDaemon(ctx, r.dir, "pause", "osd", strconv.Itoa(int(osd))); err != nil {
		t.Fatal("pause", osd, err)
	}
}

// thaw resumes a paused OSD in place, which the tests do while it is well
// inside its heartbeat grace.
func (r *copyPrimaryRecovery) thaw(t *testing.T, ctx context.Context, osd int32) {
	t.Helper()
	if err := testcluster.ControlDaemon(ctx, r.dir, "resume", "osd", strconv.Itoa(int(osd))); err != nil {
		t.Fatal("resume", osd, err)
	}
	delete(r.frozen, osd)
}

// kill SIGKILLs one OSD and returns the fixture's receipt of its exit code,
// which must be 137 and not an orderly shutdown or OOM.
func (r *copyPrimaryRecovery) kill(t *testing.T, ctx context.Context, osd int32) int {
	t.Helper()
	r.killed[osd] = true
	if err := testcluster.ControlDaemon(ctx, r.dir, "kill", "osd", strconv.Itoa(int(osd))); err != nil {
		t.Fatal("kill", osd, err)
	}
	var crash struct {
		Daemon    string `json:"daemon"`
		ExitCode  *int   `json:"exit_code"`
		OOMKilled *bool  `json:"oom_killed"`
	}
	data, err := os.ReadFile(filepath.Join(r.dir, "osd-crash-state.json"))
	if err == nil {
		err = json.Unmarshal(data, &crash)
	}
	if err != nil || crash.Daemon != fmt.Sprintf("osd.%d", osd) || crash.ExitCode == nil || *crash.ExitCode != 137 || crash.OOMKilled == nil || *crash.OOMKilled {
		t.Fatalf("expected SIGKILL evidence for osd.%d: %+v %v", osd, crash, err)
	}
	return *crash.ExitCode
}

// markDown marks a killed OSD down instead of waiting for failure reports.
// Starting the OSD again brings it back up; nothing else undoes it.
func (r *copyPrimaryRecovery) markDown(t *testing.T, ctx context.Context, osd int32) {
	t.Helper()
	if err := copyMonCommand(ctx, r.c, fmt.Sprintf(`{"prefix":"osd down","ids":["%d"]}`, osd)); err != nil {
		t.Fatal("mark the killed OSD down", err)
	}
}

func (r *copyPrimaryRecovery) setPauseReads(t *testing.T, ctx context.Context) uint32 {
	t.Helper()
	r.pauserd = true
	if err := copyMonCommand(ctx, r.c, `{"prefix":"osd set","key":"pause"}`); err != nil {
		t.Fatal("osd set pause", err)
	}
	flags := nativePool(t, ctx, "osdflags", map[string]any{"action": "show"})
	if !slices.Contains(flags.OSDMapFlags, "pauserd") || !slices.Contains(flags.OSDMapFlags, "pausewr") || flags.Epoch == 0 {
		t.Fatalf("native OSDMap does not carry pauserd and pausewr: epoch %d flags %v", flags.Epoch, flags.OSDMapFlags)
	}
	return flags.Epoch
}

func (r *copyPrimaryRecovery) clearPauseReads(t *testing.T, ctx context.Context) {
	t.Helper()
	if err := copyMonCommand(ctx, r.c, `{"prefix":"osd unset","key":"pause"}`); err != nil {
		t.Fatal("osd unset pause", err)
	}
	r.pauserd = false
	if flags := nativePool(t, ctx, "osdflags", map[string]any{"action": "show"}); slices.Contains(flags.OSDMapFlags, "pauserd") || slices.Contains(flags.OSDMapFlags, "pausewr") {
		t.Fatalf("native OSDMap still carries a pause flag at epoch %d: %v", flags.Epoch, flags.OSDMapFlags)
	}
}

func sortedOSDs(set map[int32]bool) []int32 {
	return slices.Sorted(func(yield func(int32) bool) {
		for osd := range set {
			if !yield(osd) {
				return
			}
		}
	})
}

func (r *copyPrimaryRecovery) restore(t *testing.T) {
	t.Helper()
	if !r.pauserd && len(r.out) == 0 && len(r.frozen) == 0 && len(r.killed) == 0 && !r.restarted {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	var failed []error
	// An OSD still paused here may have been frozen past its heartbeat grace
	// (a test failed before thawing it), so it is restarted rather than
	// thawed in place, as in the source failover test.
	for _, osd := range sortedOSDs(r.frozen) {
		name := strconv.Itoa(int(osd))
		if err := testcluster.ControlDaemon(ctx, r.dir, "resume", "osd", name); err != nil {
			failed = append(failed, fmt.Errorf("resume osd.%d: %w", osd, err))
			continue
		}
		delete(r.frozen, osd)
		r.killed[osd] = true
		if err := testcluster.ControlDaemon(ctx, r.dir, "kill", "osd", name); err != nil {
			failed = append(failed, fmt.Errorf("kill thawed osd.%d: %w", osd, err))
		}
	}
	for _, osd := range sortedOSDs(r.killed) {
		if err := testcluster.ControlDaemon(ctx, r.dir, "start", "osd", strconv.Itoa(int(osd))); err != nil {
			failed = append(failed, fmt.Errorf("start osd.%d: %w", osd, err))
		} else {
			delete(r.killed, osd)
			r.restarted = true
		}
	}
	if r.pauserd {
		if err := copyMonCommand(ctx, r.c, `{"prefix":"osd unset","key":"pause"}`); err != nil {
			failed = append(failed, fmt.Errorf("osd unset pause: %w", err))
		} else {
			r.pauserd = false
		}
	}
	for _, osd := range sortedOSDs(r.out) {
		if err := copyMonCommand(ctx, r.c, fmt.Sprintf(`{"prefix":"osd in","ids":["%d"]}`, osd)); err != nil {
			failed = append(failed, fmt.Errorf("osd in %d: %w", osd, err))
		} else {
			delete(r.out, osd)
		}
	}
	if len(failed) != 0 {
		t.Errorf("restoration commands failed; the clean check was not attempted: %v", errors.Join(failed...))
		return
	}
	if flags := nativePool(t, ctx, "osdflags", map[string]any{"action": "show"}); slices.Contains(flags.OSDMapFlags, "pauserd") || slices.Contains(flags.OSDMapFlags, "pausewr") {
		t.Errorf("native OSDMap still carries a pause flag at epoch %d: %v", flags.Epoch, flags.OSDMapFlags)
	}
	waitOSDFixtureClean(t, ctx, r.base)
	if r.restarted {
		// A restarted OSD has a new address; later tests route by the
		// fixture's published routing, as after the crash tests.
		fresh := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true}).Placement
		publishReplicaOSDRouting(t, r.dir, adoptMutationPlacement(t, r.base, fresh))
		r.restarted = false
	}
	t.Log("restored: every OSD running and in, pause unset, osd-ready clean")
}

// copyPrimaryChangeCase is one opcode of a primary-change cell.
type copyPrimaryChangeCase struct {
	name string
	code cephmsgr.OSDOpcode
}

var copyPrimaryChangeCases = []copyPrimaryChangeCase{{"COPY_FROM", cephmsgr.OSDCopyFrom}, {"COPY_FROM2", cephmsgr.OSDCopyFrom2}}

// copyPrimaryChangeRun is the shared state of one subtest: one client for
// every request (resubmission is bound to its GlobalID), the owned pools,
// and a fault that withholds replies on every connection to one armed OSD.
// Every connection is wrapped when it is dialed and keyed by the endpoint it
// dialed, so arming does not depend on whether the client reuses a
// connection it already holds. The key is the endpoint of the OSD's address
// in the native map, not the fixture's peer names: those are recorded when
// the fixture starts, and an OSD that an earlier test of a broad row killed
// and restarted may listen on another port since.
type copyPrimaryChangeRun struct {
	t        *testing.T
	ctx      context.Context
	client   *cephmsgr.Client
	gid      uint64
	pools    ownedPools
	recovery *copyPrimaryRecovery
	mu       sync.Mutex
	armed    map[string]*atomic.Bool
	blocked  chan struct{}
	source   string
	dest     string
	srcRoute osdFixtureOracle
	dstRoute osdFixtureOracle
	// restartedPrimary says the original destination primary was killed and
	// started again, so it returns under a new address.
	restartedPrimary bool
}

const (
	copyPrimaryWriteIncarnation = 31
	copyPrimaryCopyIncarnation  = 33
)

func newCopyPrimaryChangeRun(t *testing.T, ctx context.Context, pools ownedPools, recovery *copyPrimaryRecovery, label string) *copyPrimaryChangeRun {
	t.Helper()
	r := &copyPrimaryChangeRun{t: t, ctx: ctx, pools: pools, recovery: recovery, blocked: make(chan struct{}, 1)}
	r.armed = map[string]*atomic.Bool{}
	options := integrationOptions(t)
	options.EnableOSD = true
	if isReplicatedPGFixtureProfile() {
		options.OSDProfile = replicatedPGFixtureOSDProfile(t)
	}
	dial := options.DialContext
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		normalized := endpoint
		if parsed, e := netip.ParseAddrPort(endpoint); e == nil {
			normalized = parsed.String()
		}
		return &testcluster.LostReplyConn{Conn: conn, Armed: r.fault(normalized), Closed: make(chan struct{}), SignalBlocked: func() {
			select {
			case r.blocked <- struct{}{}:
			default:
			}
		}}, nil
	}
	client, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	r.client, r.gid = client, client.Snapshot().GlobalID
	r.source, r.dest = "copy-primary-"+label+"-source", "copy-primary-"+label+"-destination"
	if poolObserve(t, ctx, 1, r.source, "", "").Code != -2 || poolObserve(t, ctx, 2, r.dest, "", "").Code != -2 {
		t.Fatal("source or destination head already exists")
	}
	r.srcRoute, r.dstRoute = poolPlace(t, ctx, 1, r.source, ""), poolPlace(t, ctx, 2, r.dest, "")
	if r.dstRoute.PGTemp || len(r.dstRoute.Acting) != 3 || !slices.Equal(r.dstRoute.Up, r.dstRoute.Acting) || r.dstRoute.PrimaryID != r.dstRoute.ID {
		t.Fatalf("the destination PG is not settled on three OSDs: %+v", r.dstRoute)
	}
	if r.srcRoute.PGID == r.dstRoute.PGID || r.srcRoute.Pool != pools.ids[1] || r.dstRoute.Pool != pools.ids[2] {
		t.Fatal("source and destination are not in distinct owned PGs", r.srcRoute.PGID, r.dstRoute.PGID)
	}
	return r
}

// fault is the reply fault of every connection to one dialed endpoint.
func (r *copyPrimaryChangeRun) fault(endpoint string) *atomic.Bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.armed[endpoint] == nil {
		r.armed[endpoint] = &atomic.Bool{}
	}
	return r.armed[endpoint]
}

// send issues one Go request once on a new connection to route and requires
// a committed success with a reply that names its own submission.
func (r *copyPrimaryChangeRun) send(label string, route osdFixtureOracle, request cephmsgr.OSDRequest) cephmsgr.OSDReply {
	r.t.Helper()
	o := mutationConnection(r.t, r.client, r.ctx, route)
	defer o.Close()
	reply, err := o.Request(r.ctx, request)
	if err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		r.t.Fatalf("%s: err=%v code=%d flags=%#x; not resent", label, err, reply.Code, reply.Flags)
	}
	if reply.Transaction == 0 || reply.GlobalID != r.gid || reply.Submission.GlobalID != r.gid || reply.Submission.ClientIncarnation != request.ClientIncarnation ||
		reply.Object != route.Name || reply.PG.Pool != uint64(route.Pool) || len(reply.Operations) != len(request.Operations) {
		r.t.Fatalf("%s: reply identity differs from its submission: %+v", label, reply.Submission)
	}
	if request.Resubmit == nil && (reply.Submission.Transaction != reply.Transaction || reply.RetryAttempt != 0) {
		r.t.Fatalf("%s: reply transaction %d attempt %d differ from submission %d", label, reply.Transaction, reply.RetryAttempt, reply.Submission.Transaction)
	}
	return reply
}

// writeSource replaces the source head with one Go WRITEFULL and SETXATTR,
// routed by the source's current placement.
func (r *copyPrimaryChangeRun) writeSource(label string, seconds uint32, data, attr []byte) (cephmsgr.OSDReply, nativeXattrSnapshot) {
	r.t.Helper()
	r.srcRoute = poolPlace(r.t, r.ctx, 1, r.source, "")
	request := r.pools.request(r.srcRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "copy-primary-attr"}, Data: attr})
	request.ClientIncarnation, request.ModificationTime = copyPrimaryWriteIncarnation, cephmsgr.OSDTime{Seconds: seconds}
	reply := r.send(label, r.srcRoute, request)
	native := poolObserve(r.t, r.ctx, 1, r.source, "", "")
	if native.Code != 0 || !bytes.Equal(native.Data, data) || attrNames(native)["copy-primary-attr"] != string(attr) || native.Version != reply.UserVersion {
		r.t.Fatalf("%s: native source differs from the Go write: code %d version %d reply %d", label, native.Code, native.Version, reply.UserVersion)
	}
	return reply, native
}

// copyRequest is the copy of the source onto the destination under the copy
// incarnation, routed to route; a resubmission keeps its operations and
// modification time.
func (r *copyPrimaryChangeRun) copyRequest(code cephmsgr.OSDOpcode, route osdFixtureOracle) cephmsgr.OSDRequest {
	request := r.pools.request(route, cephmsgr.OSDOperation{Code: code, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: r.pools.ids[1], SourceName: r.source}})
	request.ClientIncarnation, request.ModificationTime = copyPrimaryCopyIncarnation, cephmsgr.OSDTime{Seconds: 1700009100}
	return request
}

// waitMoved polls the destination placement after the primary was marked
// out until two observations a second apart agree on a moved primary.
func (r *copyPrimaryChangeRun) waitMoved() osdFixtureOracle {
	r.t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	var last osdFixtureOracle
	for {
		now := poolPlace(r.t, r.ctx, 2, r.dest, "")
		err := checkPrimaryMoved(r.dstRoute, now)
		if err == nil && reflect.DeepEqual(now, last) {
			return now
		}
		last = now
		if time.Now().After(deadline) {
			r.t.Fatalf("the destination primary did not settle on another OSD: %v (last %+v)", err, now)
		}
		time.Sleep(time.Second)
	}
}

// history takes one bracketed snapshot of the destination's primary PG log
// for the copy's identity, the source writes and near misses of the copy.
func (r *copyPrimaryChangeRun) history(route osdFixtureOracle, copyKey pgHistoryTarget, sourceKeys ...pgHistoryTarget) pgHistorySnapshot {
	r.t.Helper()
	misses := []pgHistoryTarget{
		{Type: "client", Num: copyKey.Num, Inc: copyKey.Inc + 1, TID: copyKey.TID},
		{Type: "client", Num: copyKey.Num, Inc: copyKey.Inc, TID: copyKey.TID + 1<<20},
		{Type: "client", Num: copyKey.Num + 1, Inc: copyKey.Inc, TID: copyKey.TID},
	}
	targets := append(append([]pgHistoryTarget{copyKey}, sourceKeys...), misses...)
	s := primaryPGHistory(r.t, r.ctx, 2, r.dest, targets)
	requirePGHistoryOnRoute(r.t, s, route)
	for i := range misses {
		if got := s.Targets[1+len(sourceKeys)+i]; len(got.Locations) != 0 {
			r.t.Fatalf("near-miss identity %s matched %+v", got.Reqid, got.Locations)
		}
	}
	return s
}

// sameNative compares two native observations in code, bytes, attributes,
// user version and mtime.
func sameNative(a, b nativeXattrSnapshot) bool {
	return a.Code == b.Code && bytes.Equal(a.Data, b.Data) && reflect.DeepEqual(attrNames(a), attrNames(b)) && a.Version == b.Version && a.Seconds == b.Seconds && a.Nanos == b.Nanos
}

// waitReturned polls the destination placement after recovery until two
// observations a second apart agree on the original placement.
func (r *copyPrimaryChangeRun) waitReturned() osdFixtureOracle {
	r.t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	var last osdFixtureOracle
	for {
		now := poolPlace(r.t, r.ctx, 2, r.dest, "")
		check := checkPrimaryReturned
		if r.restartedPrimary {
			check = checkPrimaryRestarted
		}
		err := check(r.dstRoute, now)
		if err == nil && reflect.DeepEqual(now, last) {
			return now
		}
		last = now
		if time.Now().After(deadline) {
			r.t.Fatalf("the original destination primary osd.%d did not return: %v", r.dstRoute.ID, err)
		}
		time.Sleep(time.Second)
	}
}

// finish restores the cluster and waits until the original destination
// primary leads the PG again, then reads the destination once on a fresh
// connection to it and observes it natively after a delay: it must still be
// exactly want, so neither the former nor the current primary replayed the
// copy. The PG log of the returned primary must still hold the copy once.
func (r *copyPrimaryChangeRun) finish(want nativeXattrSnapshot, last uint64, copyKey, sourceKey pgHistoryTarget, binding copyFromHistoryWant) {
	r.t.Helper()
	r.recovery.restore(r.t)
	if r.t.Failed() {
		r.t.FailNow()
	}
	route := r.waitReturned()
	read := r.pools.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(want.Data) + 64)})
	o := mutationConnection(r.t, r.client, r.ctx, route)
	reply, err := o.Request(r.ctx, read)
	o.Close()
	if err != nil || reply.Code != 0 || reply.Transaction <= last || !bytes.Equal(reply.Operations[0].Data, want.Data) {
		r.t.Fatalf("fresh-session read: err=%v code=%d tid=%d (last %d) %d bytes", err, reply.Code, reply.Transaction, last, len(reply.Operations[0].Data))
	}
	time.Sleep(2 * time.Second)
	if after := poolObserve(r.t, r.ctx, 2, r.dest, "", ""); !sameNative(after, want) {
		r.t.Fatalf("destination changed after recovery: version %d -> %d, %d bytes", want.Version, after.Version, len(after.Data))
	}
	s := r.history(route, copyKey, sourceKey)
	if err := copyFromDestinationBinding(s.Targets[0].Locations, s.Targets[1].Locations, binding); err != nil {
		r.t.Fatalf("returned primary osd.%d: %v", route.ID, err)
	}
	r.t.Logf("after recovery: primary osd.%d epoch %d, fresh read tid %d, destination version %d unchanged, copy %s once in the PG log", route.ID, route.Epoch, reply.Transaction, want.Version, copyKey)
}

// copyPrimaryChangeFixture starts the three-BlueStore fixture, two owned
// pools and the run-level recovery.
func copyPrimaryChangeFixture(t *testing.T, bound time.Duration) (context.Context, ownedPools, *copyPrimaryRecovery) {
	t.Helper()
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("a destination primary change needs three BlueStore replicas")
	}
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), bound)
	t.Cleanup(cancel)
	waitOSDFixtureClean(t, ctx, base)
	recovery := &copyPrimaryRecovery{c: c, base: base, dir: os.Getenv("CEPH_MSGR_CONTROL_DIR"), out: map[int32]bool{}, frozen: map[int32]bool{}, killed: map[int32]bool{}}
	// Registered after the pools, so it runs before their removal.
	pools := createOwnedPools(t, ctx, base, 1, 2)
	t.Cleanup(func() { recovery.restore(t) })
	return ctx, pools, recovery
}

// A COPY_FROM or COPY_FROM2 executes on the destination primary while its
// reply is withheld, and the caller's wait ends as an unknown outcome. The
// source is then replaced by a second Go write, so a second execution of the
// copy would be visible, and the destination primary is marked out. One
// explicit resubmission to the new primary, under the original identity and
// the caller's new placement, must be answered from the replicated PG log:
// the original transaction, code 0, the user version native observation
// gave the executed copy, ONDISK, and a destination that still holds the
// first source bytes, attributes, version and mtime. The new primary's PG log
// holds the copy's identity once, bound to the first source write. After
// recovery the original primary must lead the PG again; a fresh session to it
// reads the same bytes, nothing changes, and its PG log holds the copy once.
//
//fixture:isolated marks OSDs out and in, waits osd-ready clean without a pg_temp proof
func TestCephOSDCopyFromLostReplyPrimaryChangeIntegration(t *testing.T) {
	ctx, pools, recovery := copyPrimaryChangeFixture(t, 900*time.Second)
	for _, tc := range copyPrimaryChangeCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(func() { recovery.restore(t) })
			r := newCopyPrimaryChangeRun(t, ctx, pools, recovery, "lost-"+strconv.Itoa(int(tc.code)))
			first, firstSource := r.writeSource("first source write", 1700009000, []byte("copy-primary\x00\xff first source bytes"), []byte{1, 0xff})

			o := mutationConnection(t, r.client, ctx, r.dstRoute)
			endpoint := osdEndpoint(t, r.dstRoute.Address)
			_, peer, _ := testcluster.SplitPeer(endpoint)
			t.Logf("reply fault armed for osd.%d at %s (fixture peer name %q)", r.dstRoute.ID, endpoint, peer)
			r.fault(endpoint).Store(true)
			call, cancel := context.WithCancel(ctx)
			defer cancel()
			type outcome struct {
				reply cephmsgr.OSDReply
				err   error
			}
			done := make(chan outcome, 1)
			request := r.copyRequest(tc.code, r.dstRoute)
			go func() {
				reply, err := o.Request(call, request)
				done <- outcome{reply, err}
			}()
			select {
			case <-r.blocked:
			case <-time.After(60 * time.Second):
				t.Fatalf("receive fault not activated within 60 s on %s", endpoint)
			}
			var executed nativeXattrSnapshot
			for deadline := time.Now().Add(60 * time.Second); ; {
				executed = poolObserve(t, ctx, 2, r.dest, "", "")
				if executed.Code == 0 && bytes.Equal(executed.Data, firstSource.Data) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("the copy is not visible natively", executed.Code)
				}
				time.Sleep(100 * time.Millisecond)
			}
			if !reflect.DeepEqual(attrNames(executed), attrNames(firstSource)) || executed.Version == 0 {
				t.Fatalf("native destination differs from the source: attrs %v version %d", attrNames(executed), executed.Version)
			}
			cancel()
			lost := <-done
			var unknown *cephmsgr.OutcomeUnknownError
			if !errors.As(lost.err, &unknown) || !errors.Is(lost.err, context.Canceled) || lost.reply.Transaction != 0 || lost.reply.Submission.Transaction == 0 ||
				lost.reply.Submission.GlobalID != r.gid || lost.reply.Submission.ClientIncarnation != copyPrimaryCopyIncarnation || lost.reply.Submission.MapEpoch != r.dstRoute.Epoch {
				t.Fatalf("lost reply metadata: %+v %v", lost.reply, lost.err)
			}
			o.Close()
			r.fault(endpoint).Store(false)

			second, _ := r.writeSource("second source write", 1700009200, []byte("copy-primary second source bytes, longer"), []byte{2, 0xfe})
			r.recovery.markOut(t, ctx, r.dstRoute.ID)
			moved := r.waitMoved()
			if after := poolObserve(t, ctx, 2, r.dest, "", ""); !sameNative(after, executed) {
				t.Fatalf("the destination changed across the primary change: version %d -> %d", executed.Version, after.Version)
			}

			again := r.copyRequest(tc.code, moved)
			again.Resubmit, again.RetryAttempt = &lost.reply.Submission, 1
			answered := r.send("resubmission to the new primary", moved, again)
			t.Logf("%s resubmitted to osd.%d (epoch %d, former primary osd.%d): code=%d tid=%d attempt=%d flags=%#x user=%d replay=%s ops=%+v",
				tc.name, moved.ID, moved.Epoch, r.dstRoute.ID, answered.Code, answered.Transaction, answered.RetryAttempt, answered.Flags, answered.UserVersion,
				replayVersion(answered), answered.Operations)
			if answered.Transaction != lost.reply.Submission.Transaction || answered.RetryAttempt != 1 || answered.UserVersion != executed.Version || answered.Operations[0].Result != 0 {
				t.Fatalf("the resubmission was not answered for the executed copy: tid %d attempt %d user %d (native %d) op %d",
					answered.Transaction, answered.RetryAttempt, answered.UserVersion, executed.Version, answered.Operations[0].Result)
			}
			time.Sleep(500 * time.Millisecond)
			if after := poolObserve(t, ctx, 2, r.dest, "", ""); !sameNative(after, executed) {
				t.Fatalf("the resubmission copied again: version %d -> %d, %d bytes", executed.Version, after.Version, len(after.Data))
			}
			copyKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryCopyIncarnation, TID: lost.reply.Submission.Transaction}
			sourceKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: first.Transaction}
			secondKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: second.Transaction}
			binding := copyFromHistoryWant{object: hobjectHead(moved), version: replayVersion(answered), sourceUserVersion: first.UserVersion}
			s := r.history(moved, copyKey, sourceKey, secondKey)
			if err := copyFromDestinationBinding(s.Targets[0].Locations, s.Targets[1].Locations, binding); err != nil {
				t.Fatalf("new primary osd.%d: %v", moved.ID, err)
			}
			if got := s.Targets[2].Locations; len(got) != 0 {
				t.Fatalf("the second source write is bound at the destination: %+v", got)
			}
			r.finish(executed, answered.Transaction, copyKey, sourceKey, binding)
		})
	}
}

// A COPY_FROM or COPY_FROM2 is held on the destination primary before its
// source read: pauserd keeps the primary's internal Objecter from sending
// the copy-get. Two bracketed native snapshots of that primary are the phase
// barrier: its op tracker holds the client's op and its Objecter one paused
// copy-get of the owned source with no send attempt. The primary is then marked out. Snapshots of
// the former primary, still under pauserd, must show both gone, and the
// caller's wait ends as an unknown outcome with the barrier's transaction.
// After pauserd is unset the native destination must still be its seeded
// state and the new primary's PG log must not name the copy. Only then does
// the caller resubmit to the new primary under the original identity, which
// executes the copy once; after a second source write a second resubmission
// must be answered from the PG log without copying again. After recovery the
// original primary must lead the PG again; a fresh session to it reads the
// copy, nothing changes, and its PG log holds the copy once.
//
//fixture:isolated sets pause and marks OSDs out, restored checked, waits osd-ready clean without pg_temp proof
func TestCephOSDCopyFromInFlightPrimaryChangeIntegration(t *testing.T) {
	ctx, pools, recovery := copyPrimaryChangeFixture(t, 900*time.Second)
	for _, tc := range copyPrimaryChangeCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(func() { recovery.restore(t) })
			r := newCopyPrimaryChangeRun(t, ctx, pools, recovery, "held-"+strconv.Itoa(int(tc.code)))
			first, firstSource := r.writeSource("first source write", 1700009000, []byte("copy-primary\x00\xff held source bytes"), []byte{3, 0xff})
			seeded := poolSeed(t, ctx, 2, r.dest, "", []byte("stale destination bytes"), []nativeXattrEntry{{Name: []byte("stale-attr"), Value: []byte{9}}})
			seeded = poolObserve(t, ctx, 2, r.dest, "", "")
			if seeded.Code != 0 || seeded.Version == 0 {
				t.Fatal("native destination seed", seeded.Code)
			}

			flagEpoch := r.recovery.setPauseReads(t, ctx)
			route := poolPlace(t, ctx, 2, r.dest, "")
			if route.Epoch < flagEpoch || !samePlacement(route, r.dstRoute) {
				t.Fatalf("destination placement at the pauserd epoch %d differs: %+v vs %+v", flagEpoch, route, r.dstRoute)
			}
			o := mutationConnection(t, r.client, ctx, route)
			call, cancel := context.WithCancel(ctx)
			defer cancel()
			type outcome struct {
				reply cephmsgr.OSDReply
				err   error
			}
			done := make(chan outcome, 1)
			go func() {
				reply, err := o.Request(call, r.copyRequest(tc.code, route))
				done <- outcome{reply, err}
			}()
			pending := func(stage string) {
				t.Helper()
				select {
				case got := <-done:
					t.Fatalf("the held copy was answered %s: code %d err %v", stage, got.reply.Code, got.err)
				default:
				}
			}
			var held opsInFlightOp
			var read objecterRequestsOp
			for deadline := time.Now().Add(30 * time.Second); ; {
				pending("before the barrier")
				ops := copyOpsInFlight(t, ctx, 2, r.dest, nil, r.gid, copyPrimaryCopyIncarnation)
				objecter := copyObjecterRequests(t, ctx, 2, r.dest, nil, pools.ids[1], r.source)
				var err error
				if held, read, err = checkCopyHeld(ops, objecter, route, r.gid, copyPrimaryCopyIncarnation); err == nil {
					t.Logf("barrier on osd.%d epoch %d: op %s flag point %q events %q; objecter tid %d pg %s osd %d paused %t attempts %d ops %q",
						ops.OSD, ops.Before.Epoch, held.Reqid, held.FlagPoint, held.Events, read.TID, read.PG, read.OSD, read.Paused, read.Attempts, read.OSDOps)
					break
				} else if time.Now().After(deadline) {
					t.Fatalf("no phase barrier: %v\nop tracker: %s\nobjecter: %s", err, boundedRaw(ops.Raw), boundedRaw(objecter.Raw))
				}
				time.Sleep(250 * time.Millisecond)
			}

			former := route.ID
			r.recovery.markOut(t, ctx, former)
			moved := r.waitMoved()
			for deadline := time.Now().Add(60 * time.Second); ; {
				pending("after the primary change")
				ops := copyOpsInFlight(t, ctx, 2, r.dest, &former, r.gid, copyPrimaryCopyIncarnation)
				objecter := copyObjecterRequests(t, ctx, 2, r.dest, &former, pools.ids[1], r.source)
				err := checkCopyDropped(ops, objecter, former)
				if err == nil {
					t.Logf("former primary osd.%d at epoch %d holds neither the op nor the copy-get under pauserd; new primary osd.%d", former, ops.Before.Epoch, moved.ID)
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("the former primary still holds the copy: %v\nop tracker: %s\nobjecter: %s", err, boundedRaw(ops.Raw), boundedRaw(objecter.Raw))
				}
				time.Sleep(500 * time.Millisecond)
			}
			// Nothing will answer; only the caller ends the wait.
			select {
			case got := <-done:
				t.Fatalf("the dropped copy was answered: code %d err %v", got.reply.Code, got.err)
			case <-time.After(2 * time.Second):
			}
			cancel()
			lost := <-done
			var unknown *cephmsgr.OutcomeUnknownError
			if !errors.As(lost.err, &unknown) || !errors.Is(lost.err, context.Canceled) || lost.reply.Transaction != 0 || lost.reply.Submission.Transaction != held.TID ||
				lost.reply.Submission.GlobalID != r.gid || lost.reply.Submission.ClientIncarnation != copyPrimaryCopyIncarnation || lost.reply.Submission.MapEpoch != route.Epoch {
				t.Fatalf("held copy metadata (barrier tid %d): %+v %v", held.TID, lost.reply, lost.err)
			}
			o.Close()
			r.recovery.clearPauseReads(t, ctx)

			// Native evidence that the copy did not run, before any resubmission.
			time.Sleep(2 * time.Second)
			if after := poolObserve(t, ctx, 2, r.dest, "", ""); !sameNative(after, seeded) {
				t.Fatalf("the dropped copy changed the destination: version %d -> %d, %d bytes", seeded.Version, after.Version, len(after.Data))
			}
			if after := poolObserve(t, ctx, 1, r.source, "", ""); !sameNative(after, firstSource) {
				t.Fatal("the source changed", after.Version, firstSource.Version)
			}
			moved = poolPlace(t, ctx, 2, r.dest, "")
			if err := checkPrimaryMoved(r.dstRoute, moved); err != nil {
				t.Fatal("placement after pauserd:", err)
			}
			copyKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryCopyIncarnation, TID: held.TID}
			sourceKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: first.Transaction}
			if s := r.history(moved, copyKey, sourceKey); len(s.Targets[0].Locations) != 0 {
				t.Fatalf("the new primary's PG log names the dropped copy: %+v", s.Targets[0].Locations)
			}

			again := r.copyRequest(tc.code, moved)
			again.Resubmit, again.RetryAttempt = &lost.reply.Submission, 1
			executed := r.send("first resubmission to the new primary", moved, again)
			t.Logf("%s resubmitted to osd.%d (epoch %d, former primary osd.%d): code=%d tid=%d attempt=%d flags=%#x user=%d replay=%s",
				tc.name, moved.ID, moved.Epoch, former, executed.Code, executed.Transaction, executed.RetryAttempt, executed.Flags, executed.UserVersion, replayVersion(executed))
			// A copy gives the destination the source's user version
			// (src/osd/PrimaryLogPG.cc:355-356, 10116), not a newer one.
			if executed.Transaction != held.TID || executed.RetryAttempt != 1 || executed.UserVersion != first.UserVersion || executed.Operations[0].Result != 0 {
				t.Fatalf("first resubmission: tid %d attempt %d user %d (source %d, seeded destination %d) op %d",
					executed.Transaction, executed.RetryAttempt, executed.UserVersion, first.UserVersion, seeded.Version, executed.Operations[0].Result)
			}
			copied := poolObserve(t, ctx, 2, r.dest, "", "")
			if copied.Code != 0 || !bytes.Equal(copied.Data, firstSource.Data) || !reflect.DeepEqual(attrNames(copied), attrNames(firstSource)) || copied.Version != executed.UserVersion {
				t.Fatalf("native destination after the first resubmission: code %d version %d (reply %d) attrs %v", copied.Code, copied.Version, executed.UserVersion, attrNames(copied))
			}
			binding := copyFromHistoryWant{object: hobjectHead(moved), version: replayVersion(executed), sourceUserVersion: first.UserVersion}
			s := r.history(moved, copyKey, sourceKey)
			if err := copyFromDestinationBinding(s.Targets[0].Locations, s.Targets[1].Locations, binding); err != nil {
				t.Fatalf("new primary osd.%d after the first resubmission: %v", moved.ID, err)
			}

			second, _ := r.writeSource("second source write", 1700009200, []byte("copy-primary second held source bytes"), []byte{4, 0xfe})
			retry := r.copyRequest(tc.code, moved)
			retry.Resubmit, retry.RetryAttempt = &lost.reply.Submission, 2
			answered := r.send("second resubmission to the new primary", moved, retry)
			t.Logf("%s second resubmission: code=%d tid=%d attempt=%d flags=%#x user=%d replay=%s",
				tc.name, answered.Code, answered.Transaction, answered.RetryAttempt, answered.Flags, answered.UserVersion, replayVersion(answered))
			if answered.Transaction != held.TID || answered.RetryAttempt != 2 || answered.UserVersion != executed.UserVersion || replayVersion(answered) != replayVersion(executed) || answered.Operations[0].Result != 0 {
				t.Fatalf("second resubmission was not answered from the PG log: tid %d attempt %d user %d (want %d) replay %s (want %s)",
					answered.Transaction, answered.RetryAttempt, answered.UserVersion, executed.UserVersion, replayVersion(answered), replayVersion(executed))
			}
			time.Sleep(500 * time.Millisecond)
			if after := poolObserve(t, ctx, 2, r.dest, "", ""); !sameNative(after, copied) {
				t.Fatalf("the second resubmission copied again: version %d -> %d, %d bytes", copied.Version, after.Version, len(after.Data))
			}
			secondKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: second.Transaction}
			s = r.history(moved, copyKey, sourceKey, secondKey)
			if err := copyFromDestinationBinding(s.Targets[0].Locations, s.Targets[1].Locations, binding); err != nil {
				t.Fatalf("new primary osd.%d after the second resubmission: %v", moved.ID, err)
			}
			if got := s.Targets[2].Locations; len(got) != 0 {
				t.Fatalf("the second source write is bound at the destination: %+v", got)
			}
			r.finish(copied, answered.Transaction, copyKey, sourceKey, binding)
		})
	}
}

func TestCopyFromPrimaryChangeJudgements(t *testing.T) {
	const gid, inc = 4641, 33
	route := osdFixtureOracle{Up: []int32{1, 0, 2}, Acting: []int32{1, 0, 2}, Replicas: 3, Pool: 4, Name: "dst", ID: 1, PrimaryID: 1, Shard: -1, Epoch: 30, PGID: "4.0", RawPGID: "4.d6c2a1e7", Address: "v2:192.0.2.11:6800/1"}
	at := route
	at.Epoch = 31
	held := opsInFlightSnapshot{Complete: true, Before: at, After: at, OSD: 1, PGID: "4.0", Client: "client.4641.33",
		Ops: []opsInFlightOp{{Reqid: "client.4641.33:5", TID: 5, PGID: "4.0", Epoch: 30, FlagPoint: "started", Events: []string{"reached_pg", "started"}}}}
	read := objecterRequestsSnapshot{Complete: true, Before: at, After: at, OSD: 1, PGID: "4.0",
		Ops: []objecterRequestsOp{{TID: 9, PG: "3.0", OSD: 2, Paused: true, OSDOps: []string{"copy-get max 8388608 in=35b"}, CopyGetMax: []uint64{8388608}}}}
	if op, got, err := checkCopyHeld(held, read, route, gid, inc); err != nil || op.TID != 5 || got.TID != 9 {
		t.Fatal("held copy refused:", err)
	}
	legacy := held
	legacy.Ops = []opsInFlightOp{held.Ops[0]}
	legacy.Ops[0].PGID = route.RawPGID
	if _, _, err := checkCopyHeld(legacy, read, route, gid, inc); err != nil {
		t.Fatal("held copy under its raw PG refused:", err)
	}
	for name, edit := range map[string]func(*opsInFlightSnapshot, *objecterRequestsSnapshot){
		"op tracker incomplete": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Complete = false },
		"objecter failure":      func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) { r.Failures = []string{"x"} },
		"brackets differ":       func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.After.Epoch = 32 },
		"other placement":       func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) { r.Before.ID, r.After.ID = 2, 2 },
		"behind the route":      func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Before.Epoch, o.After.Epoch = 29, 29 },
		"another OSD answered":  func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) { r.OSD = 2 },
		"another PG":            func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.PGID = "4.1" },
		"no client op":          func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Ops = nil },
		"two client ops":        func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Ops = append(o.Ops, o.Ops[0]) },
		"another incarnation":   func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Client = "client.4641.34" },
		"op of another PG":      func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Ops[0].PGID = "4.1" },
		"op at another epoch":   func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Ops[0].Epoch = 31 },
		"op identity differs":   func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Ops[0].Reqid = "client.4641.33:6" },
		"no source read":        func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) { r.Ops = nil },
		"two source reads":      func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) { r.Ops = append(r.Ops, r.Ops[0]) },
		"source read sent": func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) {
			r.Ops[0].Paused, r.Ops[0].Attempts = false, 1
		},
		"source read sent once and paused again": func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) {
			r.Ops[0].Paused, r.Ops[0].Attempts = true, 1
		},
		"source read not copy-get": func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) {
			r.Ops[0].OSDOps, r.Ops[0].CopyGetMax = []string{"read 0~10"}, nil
		},
		"source read with a prefix": func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) {
			r.Ops[0].OSDOps = []string{"assert-version v3", "copy-get max 1"}
		},
		"absence claimed": func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) { r.AbsenceEstablished = true },
	} {
		o, r := held, read
		o.Ops, r.Ops = slices.Clone(held.Ops), slices.Clone(read.Ops)
		edit(&o, &r)
		if _, _, err := checkCopyHeld(o, r, route, gid, inc); err == nil {
			t.Error("barrier accepted:", name)
		}
	}

	moved := route
	moved.Epoch, moved.ID, moved.PrimaryID, moved.Up, moved.Acting, moved.PGTemp = 33, 0, 0, []int32{0, 2}, []int32{0, 2, 1}, true
	if err := checkPrimaryMoved(route, moved); err != nil {
		t.Fatal("moved primary refused:", err)
	}
	for name, edit := range map[string]func(*osdFixtureOracle){
		"same epoch":         func(p *osdFixtureOracle) { p.Epoch = 30 },
		"same primary":       func(p *osdFixtureOracle) { p.ID, p.PrimaryID = 1, 1 },
		"former still up":    func(p *osdFixtureOracle) { p.Up = []int32{0, 1} },
		"another PG":         func(p *osdFixtureOracle) { p.PGID = "4.1" },
		"not acting primary": func(p *osdFixtureOracle) { p.PrimaryID = 2 },
		"no primary":         func(p *osdFixtureOracle) { p.ID, p.PrimaryID = -1, -1 },
	} {
		p := moved
		p.Up = slices.Clone(moved.Up)
		edit(&p)
		if checkPrimaryMoved(route, p) == nil {
			t.Error("moved primary accepted:", name)
		}
	}

	returned := route
	returned.Epoch = 36
	if err := checkPrimaryReturned(route, returned); err != nil {
		t.Fatal("returned primary refused:", err)
	}
	for name, edit := range map[string]func(*osdFixtureOracle){
		"replacement still primary": func(p *osdFixtureOracle) { *p = moved; p.Epoch = 36 },
		"replacement leads clean": func(p *osdFixtureOracle) {
			p.ID, p.PrimaryID, p.Up, p.Acting = 0, 0, []int32{0, 1, 2}, []int32{0, 1, 2}
		},
		"pg_temp left":    func(p *osdFixtureOracle) { p.PGTemp = true },
		"acting differs":  func(p *osdFixtureOracle) { p.Acting = []int32{1, 2, 0} },
		"not newer":       func(p *osdFixtureOracle) { p.Epoch = route.Epoch },
		"another PG":      func(p *osdFixtureOracle) { p.PGID = "4.1" },
		"another address": func(p *osdFixtureOracle) { p.Address = "v2:192.0.2.12:6800/1" },
	} {
		p := returned
		p.Up, p.Acting = slices.Clone(returned.Up), slices.Clone(returned.Acting)
		edit(&p)
		if checkPrimaryReturned(route, p) == nil {
			t.Error("returned primary accepted:", name)
		}
	}

	const former = 1
	dropped := opsInFlightSnapshot{Complete: true, Before: moved, After: moved, OSD: former, PGID: "4.0", Client: "client.4641.33"}
	gone := objecterRequestsSnapshot{Complete: true, Before: moved, After: moved, OSD: former, PGID: "4.0"}
	if err := checkCopyDropped(dropped, gone, former); err != nil {
		t.Fatal("dropped copy refused:", err)
	}
	for name, edit := range map[string]func(*opsInFlightSnapshot, *objecterRequestsSnapshot){
		"incomplete":           func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Complete = false },
		"objecter failure":     func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) { r.Failures = []string{"x"} },
		"asked the new":        func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.OSD = 0 },
		"former still primary": func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) { r.Before.ID = former },
		"client op left":       func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Ops = held.Ops },
		"source read left":     func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) { r.Ops = read.Ops },
	} {
		o, r := dropped, gone
		edit(&o, &r)
		if checkCopyDropped(o, r, former) == nil {
			t.Error("dropped copy accepted:", name)
		}
	}
}
