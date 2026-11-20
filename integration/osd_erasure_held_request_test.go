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
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// opsInFlightOp is one in-flight client op of the observed entity
// (tools/osd_ops_in_flight_oracle.py client_ops).
type opsInFlightOp struct {
	Index       int      `json:"index"`
	Reqid       string   `json:"reqid"`
	TID         uint64   `json:"tid"`
	PGID        string   `json:"pgid"`
	Epoch       uint32   `json:"epoch"`
	FlagPoint   string   `json:"flag_point"`
	Events      []string `json:"events"`
	Description string   `json:"description"`
}

// opsInFlightSnapshot is the fail-closed receipt of one bracketed snapshot of
// the primary's op tracker. Complete is false on a refused, oversized or
// unparsable reply and whenever the placement brackets differ.
type opsInFlightSnapshot struct {
	Complete           bool             `json:"complete"`
	Failures           []string         `json:"failures"`
	Code               int              `json:"code"`
	Bytes              int              `json:"bytes"`
	LimitBytes         int              `json:"limit_bytes"`
	SHA256             string           `json:"sha256"`
	Raw                string           `json:"raw"`
	Before             osdFixtureOracle `json:"before"`
	After              osdFixtureOracle `json:"after"`
	OSD                int32            `json:"osd"`
	PGID               string           `json:"pgid"`
	NumOps             int              `json:"num_ops"`
	Client             string           `json:"client"`
	AbsenceEstablished bool             `json:"absence_established"`
	Ops                []opsInFlightOp  `json:"ops"`
}

// dialSnapshot copies the client's dial counts per endpoint.
func (f *erasureFaultClient) dialSnapshot() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return maps.Clone(f.dials)
}

// dialChange is one endpoint whose dial count changed since a snapshot.
type dialChange struct {
	endpoint    string
	before, now int
}

func (c dialChange) String() string {
	return fmt.Sprintf("%s (%d -> %d)", c.endpoint, c.before, c.now)
}

// newDials reports every endpoint the client dialed since snapshot, except
// the endpoints it had dialed before (its MON sessions may reconnect), and
// any further dial of the OSD endpoint, with the counts before and now.
func newDials(snapshot, now map[string]int, osd string) []dialChange {
	var found []dialChange
	for endpoint, n := range now {
		if before, ok := snapshot[endpoint]; !ok || endpoint == osd && n != before {
			found = append(found, dialChange{endpoint, before, n})
		}
	}
	slices.SortFunc(found, func(a, b dialChange) int { return strings.Compare(a.endpoint, b.endpoint) })
	return found
}

// checkOneDial requires the dials since snapshot to be exactly one attempt at
// endpoint at, and no attempt at any other OSD endpoint: the OSD endpoint osd
// or one the client had not dialed before. A MON redial is allowed.
func checkOneDial(snapshot, now map[string]int, osd, at string) error {
	found := newDials(snapshot, now, osd)
	if len(found) != 1 || found[0].endpoint != at || found[0].now-found[0].before != 1 {
		return fmt.Errorf("dials since the snapshot %v; want exactly one new attempt at %s (%d before) and none at another OSD", found, at, snapshot[at])
	}
	return nil
}

// endWithin waits up to wait for the call's result. If none arrives it
// cancels the call and waits at most grace for a late result, which it
// reports only as a diagnostic, so a worker that stays blocked after the
// cancellation cannot hold the caller. ok is false whenever no result arrived
// within wait.
func endWithin[R any](finished <-chan R, wait, grace time.Duration, cancel func()) (result R, ok bool, late string) {
	select {
	case result = <-finished:
		return result, true, ""
	case <-time.After(wait):
	}
	cancel()
	select {
	case r := <-finished:
		return result, false, fmt.Sprintf("after the cancellation it ended with %+v", r)
	case <-time.After(grace):
		return result, false, fmt.Sprintf("it did not end within %v of the cancellation either", grace)
	}
}

// heldEvent is the event the pinned PrimaryLogPG::do_request marks when an op
// names a map epoch newer than the PG's (src/osd/PrimaryLogPG.cc:1855-1861).
const heldEvent = "op must wait for map"

// heldRequestEpochLead is how far beyond the placement's epoch the held
// request's map epoch lies: far more epochs than the test publishes.
const heldRequestEpochLead = 1000

// primaryOpsInFlight takes one bracketed snapshot of the primary's in-flight
// ops of one client entity and incarnation and rechecks its raw bytes.
func primaryOpsInFlight(t *testing.T, ctx context.Context, slot int, object string, gid uint64, incarnation int32) opsInFlightSnapshot {
	t.Helper()
	s := nativePool(t, ctx, "opsinflight", map[string]any{"slot": slot, "object": object,
		"entity": map[string]any{"type": "client", "num": gid, "inc": incarnation}}).OpsInFlight
	raw, err := base64.StdEncoding.DecodeString(s.Raw)
	digest := sha256.Sum256(raw)
	if s.Complete && (err != nil || len(raw) != s.Bytes || s.Bytes > s.LimitBytes || hex.EncodeToString(digest[:]) != s.SHA256) {
		t.Fatal("in-flight snapshot raw bytes differ from their receipt", err, len(raw), s.Bytes, s.LimitBytes)
	}
	if s.Complete {
		prefix := fmt.Sprintf(`"description":"osd_op(client.%d.%d:`, gid, incarnation)
		if n := bytes.Count(raw, []byte(prefix)); n != len(s.Ops) {
			t.Fatalf("parser found %d ops of client.%d.%d; the raw reply holds %d", len(s.Ops), gid, incarnation, n)
		}
	}
	return s
}

// checkHeldRequest judges one snapshot of the primary's op tracker,
// independently of the oracle's parse: a complete receipt between identical
// placement records that equal the route but for an epoch not older than
// the route's and older than the held one, answered by the route's primary, holding exactly one op of the client entity and
// incarnation, for the route's PG and shard at the held epoch, with its
// operations not yet decoded, delayed at the hold event, which follows the
// op's arrival at the PG without any event of execution in between or after.
// It returns that op.
func checkHeldRequest(s opsInFlightSnapshot, route osdFixtureOracle, gid uint64, incarnation int32, epoch uint32) (opsInFlightOp, error) {
	if !s.Complete || len(s.Failures) != 0 || s.Code != 0 || s.AbsenceEstablished {
		return opsInFlightOp{}, fmt.Errorf("incomplete in-flight snapshot: code %d, %q", s.Code, s.Failures)
	}
	if !samePlacement(s.Before, s.After) || s.Before.Epoch != s.After.Epoch || s.Before.Address != s.After.Address {
		return opsInFlightOp{}, fmt.Errorf("the placement records around the dump differ: %+v vs %+v", s.Before, s.After)
	}
	if !samePlacement(s.Before, route) || s.OSD != route.PrimaryID || s.PGID != route.PGID {
		return opsInFlightOp{}, fmt.Errorf("the dump of osd.%d for %s at %+v is not the route's primary: %+v", s.OSD, s.PGID, s.Before, route)
	}
	if s.Before.Epoch < route.Epoch || s.Before.Epoch >= epoch {
		return opsInFlightOp{}, fmt.Errorf("the dump at epoch %d is not between the route's epoch %d and the held epoch %d", s.Before.Epoch, route.Epoch, epoch)
	}
	if want := fmt.Sprintf("client.%d.%d", gid, incarnation); s.Client != want || len(s.Ops) != 1 {
		return opsInFlightOp{}, fmt.Errorf("%d in-flight ops of %s (want 1 of %s)", len(s.Ops), s.Client, want)
	}
	op := s.Ops[0]
	if op.Reqid != fmt.Sprintf("client.%d.%d:%d", gid, incarnation, op.TID) || op.TID == 0 {
		return op, fmt.Errorf("op identity %s, tid %d", op.Reqid, op.TID)
	}
	if op.PGID != route.PGID+"s"+strconv.Itoa(int(route.Shard)) || op.Epoch != epoch {
		return op, fmt.Errorf("op for %s at epoch %d; want %ss%d at %d", op.PGID, op.Epoch, route.PGID, route.Shard, epoch)
	}
	// do_op finishes the decode of the operations and resets the
	// description (src/osd/PrimaryLogPG.cc:2001-2011); until then the op
	// prints its raw PG and "(undecoded)" in their place
	// (src/messages/MOSDOp.h:622-641).
	if !strings.Contains(op.Description, " (undecoded) ") {
		return op, fmt.Errorf("op description %q shows decoded operations", op.Description)
	}
	// A delayed op reports its delay as its flag point
	// (src/osd/OpRequest.h:116-126, :161-163).
	reached := slices.Index(op.Events, "reached_pg")
	if op.FlagPoint != heldEvent || reached < 0 || !slices.Equal(op.Events[reached+1:], []string{heldEvent}) {
		return op, fmt.Errorf("op is at %q with events %q; want %q, right after reached_pg", op.FlagPoint, op.Events, heldEvent)
	}
	return op, nil
}

// The Go client's APPEND+SETXATTR on the optimized isa k=2,m=2 pool reaches
// the acting primary and is held there unapplied when that primary's OSD is
// killed. The request names a map epoch the cluster has not published, so the
// pinned PrimaryLogPG::do_request queues it in waiting_for_map before any
// execution stage (src/osd/PrimaryLogPG.cc:1845-1862), and only a map of that
// epoch would requeue it (src/osd/PG.cc:1225-1250). A bracketed native
// snapshot of the primary's op tracker must show exactly that op, delayed on
// the hold event. The primary's OSD is then killed through the fixture's
// forced kill; the call must end with an OutcomeUnknownError that keeps its
// original identity, and the client must open no further OSD connection.
// After the OSD's restart and clean PGs the native object must be unchanged
// and the returned primary's PG log must hold the seed write's identity but
// not the held one. The caller then resubmits the same operations once under
// the original transaction, which must apply once and appear once in that log.
//
// A PG below min_size cannot hold this client's request: the replicated-PG
// profiles advertise RADOS_BACKOFF, so a peered, inactive PG installs a
// PG-wide backoff and drops the op (PrimaryLogPG.cc:1868-1893) instead of
// queueing it in waiting_for_active. Not verified: the other PG wait queues,
// an op held inside the EC backend after do_op, a genuinely lagging primary,
// an orderly stop or partition, other shards' logs, and a primary-first
// renumbered held request (the held op is on shard 0).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2requestheldatdyingprimary key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated kills the primary OSD; restart waits osd-ready clean, which does not prove no pg_temp
func TestCephOSDErasureOptimizedK2M2RequestHeldAtDyingPrimaryIntegration(t *testing.T) {
	width := erasureK2M2
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != width.topology || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skipf("%s with overwrites needs the BlueStore OSD topology %q", width, width.topology)
	}
	fixture, request, hash := osdFixture, osdObjectRequest, false
	if isReplicatedPGFixtureProfile() {
		fixture, request, hash = replicatedPGFixture, replicatedPGRequest, true
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 720*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		r := nativePool(t, cleanup, "cleanup", nil)
		if !r.PoolsRemoved {
			t.Error("owned erasure-coded pool was not removed")
		}
		if slices.Contains(r.ECProfilesRemaining, width.profile) {
			t.Error("owned erasure-code profile was not removed", width.profile)
		}
	})
	pg := 1
	if hash {
		pg = 8
	}
	created := nativePool(t, ctx, "create", map[string]any{"slot": 1, "pg_num": pg, "hashpspool": hash, "erasure": true, "width": width.name, "optimized": true, "plugin": "isa"})
	if created.PoolType != 3 || !created.ECOptimized || created.Width != width.name || created.ErasureCodeProfile != width.profile {
		t.Fatal("pool is not the requested erasure-coded shape", created.PoolType, created.ECOptimized, created.Width, created.ErasureCodeProfile)
	}
	facts := erasurePoolMapFacts(t, ctx, c, created, width, true)
	t.Logf("EC pool %d %s optimized size=%d min_size=%d nonprimary shards %v owned OSDs %v", created.PoolID, width, facts.size, facts.minSize, facts.nonprimary, facts.owned)

	const (
		object          = "ec-held-request"
		attr            = "ec-held"
		seedIncarnation = 20
		heldIncarnation = 21
		seeded          = 1700006000
		mutated         = 1700006001
	)
	seed := bytes.Repeat([]byte{0x5a, 0, 0xff, 'H'}, 1500)
	route := poolPlace(t, ctx, 1, object, "")
	facts.checkPlacement(t, route)
	if route.PGTemp || route.ID != route.PrimaryID || int(route.Shard) >= len(route.Acting) || route.Acting[route.Shard] != route.ID {
		t.Fatalf("the placement %+v is not led by its own primary", route)
	}
	so := mutationConnection(t, c, ctx, route)
	seedRequest := request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(seed)), Data: seed})
	seedRequest.ClientIncarnation = seedIncarnation
	seedRequest.ModificationTime = cephmsgr.OSDTime{Seconds: seeded}
	seedReply, err := so.Request(ctx, seedRequest)
	so.Close()
	if err != nil || seedReply.Code != 0 || seedReply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("seed", err, seedReply.Code)
	}
	seedKey := pgHistoryTarget{Type: "client", Num: c.Snapshot().GlobalID, Inc: seedIncarnation, TID: seedReply.Transaction}
	pre := poolObserve(t, ctx, 1, object, "", "")
	if pre.Code != 0 || !bytes.Equal(pre.Data, seed) || pre.Seconds != seeded || pre.Version != seedReply.UserVersion || xattrInventory(pre)[attr] != "" {
		t.Fatal("native pre-state differs", pre.Code, pre.Version, seedReply.UserVersion, pre.Seconds)
	}
	t.Logf("pre-state: version %d, seed %s, placement epoch %d pg %s acting %v primary osd.%d shard %d", pre.Version, seedKey, route.Epoch, route.PGID, route.Acting, route.ID, route.Shard)

	endpoint := osdEndpoint(t, route.Address)
	fc := dialErasureFaultClient(t, ctx, hash, endpoint)
	defer fc.Close()
	gid := fc.Snapshot().GlobalID
	o := mutationConnection(t, fc.Client, ctx, route)
	dialed := fc.dialSnapshot()
	if dialed[endpoint] != 1 {
		t.Fatal("the held request's connection was not dialed exactly once", dialed)
	}
	tail := []byte("held\x00\xff" + object)
	value := []byte{0, 0xff, 'h'}
	ops := []cephmsgr.OSDOperation{
		{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail},
		{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: attr}, Data: value},
	}
	held := request(route, ops...)
	held.ClientIncarnation = heldIncarnation
	held.ModificationTime = cephmsgr.OSDTime{Seconds: mutated}
	held.MapEpoch = route.Epoch + heldRequestEpochLead
	// This fingerprints semantic input, excluding the explicitly changed
	// routing and retry envelope. It is not a hash of captured wire bytes.
	fingerprint := func(r cephmsgr.OSDRequest) [32]byte {
		r.MapEpoch, r.Placement, r.Resubmit, r.RetryAttempt = 0, nil, nil, 0
		data, err := json.Marshal(r)
		if err != nil {
			t.Fatal("request fingerprint", err)
		}
		return sha256.Sum256(data)
	}
	originalFingerprint := fingerprint(held)

	type result struct {
		reply cephmsgr.OSDReply
		err   error
	}
	finished := make(chan result, 1)
	callCtx, cancelCall := context.WithCancel(ctx)
	defer cancelCall()
	go func() {
		reply, err := o.Request(callCtx, held)
		finished <- result{reply, err}
	}()

	// The primary's op tracker must hold the request, delayed for the map,
	// within a bounded number of snapshots.
	var hold opsInFlightOp
	err = errors.New("no snapshot taken")
	for attempt := 1; attempt <= 20; attempt++ {
		s := primaryOpsInFlight(t, ctx, 1, object, gid, heldIncarnation)
		if hold, err = checkHeldRequest(s, route, gid, heldIncarnation, held.MapEpoch); err == nil {
			t.Logf("held at osd.%d (snapshot %d, %d ops in flight): %s, flag point %s, events %q", s.OSD, attempt, s.NumOps, hold.Description, hold.FlagPoint, hold.Events)
			break
		}
		t.Logf("snapshot %d: %v", attempt, err)
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		t.Fatal("the primary did not hold the request:", err)
	}
	select {
	case early := <-finished:
		t.Fatal("the held request ended before the primary died", early.err)
	default:
	}
	// A second snapshot: the same op is still held and the object unchanged.
	again, err := checkHeldRequest(primaryOpsInFlight(t, ctx, 1, object, gid, heldIncarnation), route, gid, heldIncarnation, held.MapEpoch)
	if err != nil || again.TID != hold.TID {
		t.Fatal("the hold did not persist:", err, again.TID, hold.TID)
	}
	if n := poolObserve(t, ctx, 1, object, "", ""); n.Version != pre.Version || !bytes.Equal(n.Data, seed) || xattrInventory(n)[attr] != "" {
		t.Fatal("native state changed while the request was held", n.Version, len(n.Data))
	}

	restore := erasureKillOSD(t, ctx, base, route.ID)
	defer restore()
	// The killed daemon's socket closes, so the session ends the call; the
	// caller's context stays live.
	lost, ended, late := endWithin(finished, 30*time.Second, 5*time.Second, cancelCall)
	if !ended {
		t.Fatal("the session did not end the held request within 30 s after the primary's OSD was killed;", late)
	}
	var unknown *cephmsgr.OutcomeUnknownError
	var server *cephmsgr.OSDError
	sub := lost.reply.Submission
	t.Logf("held request ended by the session: %v; submission tid %d global id %d incarnation %d epoch %d attempt %d placement %+v",
		lost.err, sub.Transaction, sub.GlobalID, sub.ClientIncarnation, sub.MapEpoch, sub.Attempt, sub.Placement)
	if callCtx.Err() != nil {
		t.Fatal("the caller's context ended the held request", callCtx.Err())
	}
	if !errors.As(lost.err, &unknown) || errors.As(lost.err, &server) {
		t.Fatal("expected an unknown outcome without a server result", lost.err)
	}
	if sub.Transaction != hold.TID || sub.GlobalID != gid || sub.ClientIncarnation != heldIncarnation || sub.MapEpoch != held.MapEpoch ||
		sub.Attempt != 0 || sub.Placement == nil || sub.Placement.Primary != route.ID || int8(sub.Placement.Shard) != route.Shard {
		t.Fatal("the unknown outcome lost the identity the primary held", sub, hold.TID)
	}
	o.Close()
	fc.disarm()

	fresh := erasureSettledPrimary(t, ctx, object, route, facts)
	freshEndpoint := osdEndpoint(t, fresh.Address)
	t.Logf("epoch %d -> %d: primary osd.%d shard %d -> osd.%d shard %d, acting %v -> %v, pg_temp %v",
		route.Epoch, fresh.Epoch, route.ID, route.Shard, fresh.ID, fresh.Shard, route.Acting, fresh.Acting, fresh.PGTemp)
	if fresh.Epoch <= route.Epoch || fresh.Epoch >= held.MapEpoch || slicesContains(fresh.Acting, route.ID) {
		t.Fatal("new placement does not reflect the killed primary or reached the held epoch", fresh)
	}
	away := poolObserve(t, ctx, 1, object, "", "")
	t.Logf("native under the replacement: version %d (pre %d) mtime %d xattr %q", away.Version, pre.Version, away.Seconds, xattrInventory(away)[attr])
	if away.Code != 0 || away.Version != pre.Version || !bytes.Equal(away.Data, seed) || away.Seconds != seeded || xattrInventory(away)[attr] != "" {
		t.Fatal("native state under the replacement differs from the pre-state", away.Version, len(away.Data), away.Seconds)
	}
	if extra := newDials(dialed, fc.dialSnapshot(), endpoint); len(extra) != 0 || fc.dialsTo(freshEndpoint) != 0 {
		t.Fatal("the client reconnected or resent after the unknown outcome:", extra)
	}

	// Restart the killed OSD and wait for every PG to be clean.
	restore()
	if err := testcluster.ControlDaemon(ctx, os.Getenv("CEPH_MSGR_CONTROL_DIR"), "verify", "osd-ready", "clean"); err != nil {
		t.Fatal("the fixture did not return to every OSD up and every PG clean:", err)
	}
	seen := poolPlace(t, ctx, 1, object, "")
	back, ok := restarted(route, seen)
	if !ok || !samePlacement(seen, back) || seen.Epoch <= fresh.Epoch || seen.Epoch >= held.MapEpoch {
		t.Fatalf("the PG is not back on its original placement under the restarted primary below the held epoch: %+v vs %+v", seen, route)
	}
	back.Epoch = seen.Epoch
	t.Logf("restored: epoch %d primary osd.%d shard %d acting %v at %s (was %s)", back.Epoch, back.ID, back.Shard, back.Acting, back.Address, route.Address)
	n := poolObserve(t, ctx, 1, object, "", "")
	if n.Code != 0 || n.Version != pre.Version || !bytes.Equal(n.Data, seed) || n.Seconds != seeded || n.Nanos != pre.Nanos || !maps.Equal(xattrInventory(n), xattrInventory(pre)) {
		t.Fatal("native state after the return differs from the pre-state", n.Version, pre.Version, len(n.Data), n.Seconds)
	}
	if extra := newDials(dialed, fc.dialSnapshot(), endpoint); len(extra) != 0 {
		t.Fatal("the client connected again before the caller's decision:", extra)
	}
	heldKey := pgHistoryTarget{Type: "client", Num: gid, Inc: heldIncarnation, TID: sub.Transaction}
	nearMiss := pgHistoryTarget{Type: "client", Num: gid, Inc: seedIncarnation, TID: sub.Transaction}
	history := primaryPGHistory(t, ctx, 1, object, []pgHistoryTarget{seedKey, heldKey, nearMiss})
	requirePGHistoryOnRoute(t, history, back)
	if got := history.Targets[0].Locations; len(got) != 1 || got[0].Tier != "log" || got[0].Object != hobjectHead(back) || got[0].Version != replayVersion(seedReply) {
		t.Fatalf("the returned primary's log does not hold the seed write once at %s: %+v", replayVersion(seedReply), got)
	}
	if len(history.Targets[1].Locations) != 0 || len(history.Targets[2].Locations) != 0 {
		t.Fatalf("the returned primary's log holds the held identity: %+v %+v", history.Targets[1].Locations, history.Targets[2].Locations)
	}

	// The caller's explicit decision: resubmit the same operations once
	// under the original identity to the returned primary.
	ro := mutationConnection(t, fc.Client, ctx, back)
	defer ro.Close()
	retry := request(back, ops...)
	retry.ClientIncarnation = heldIncarnation
	retry.ModificationTime = held.ModificationTime
	retry.Resubmit, retry.RetryAttempt = &sub, 1
	if fingerprint(retry) != originalFingerprint {
		t.Fatal("explicit retry changed the semantic request fingerprint")
	}
	retryCtx, stop := context.WithTimeout(ctx, 30*time.Second)
	reply, err := ro.Request(retryCtx, retry)
	stop()
	if err != nil {
		t.Fatal("explicit resubmission to the returned primary:", err)
	}
	t.Logf("resubmission: tid %d code %d flags %#x user version %d replay %s attempt %d epoch %d", reply.Transaction, reply.Code, reply.Flags, reply.UserVersion, replayVersion(reply), reply.RetryAttempt, reply.MapEpoch)
	if reply.Transaction != sub.Transaction || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.RetryAttempt != 1 || reply.UserVersion <= pre.Version ||
		reply.Submission.Transaction != sub.Transaction || reply.Submission.MapEpoch != back.Epoch || reply.Submission.GlobalID != gid || reply.Submission.ClientIncarnation != heldIncarnation {
		t.Fatal("resubmission receipt differs", reply.Transaction, reply.Code, reply.Flags, reply.RetryAttempt, reply.UserVersion)
	}
	final := poolObserve(t, ctx, 1, object, "", "")
	if final.Code != 0 || !bytes.Equal(final.Data, append(bytes.Clone(seed), tail...)) || bytes.Count(final.Data, tail) != 1 || final.Version != reply.UserVersion ||
		final.Seconds != mutated || xattrInventory(final)[attr] != string(value) {
		t.Fatal("native state after the resubmission differs", len(final.Data), final.Version, reply.UserVersion, final.Seconds)
	}
	after := primaryPGHistory(t, ctx, 1, object, []pgHistoryTarget{seedKey, heldKey, nearMiss})
	requirePGHistoryOnRoute(t, after, back)
	if got := after.Targets[1].Locations; len(got) != 1 || got[0].Tier != "log" || got[0].Version != replayVersion(reply) || got[0].Object != hobjectHead(back) ||
		len(after.Targets[0].Locations) != 1 || len(after.Targets[2].Locations) != 0 {
		t.Fatalf("the returned primary's log after the resubmission: seed %+v held %+v near miss %+v", after.Targets[0].Locations, got, after.Targets[2].Locations)
	}
	if !errors.As(lost.err, &unknown) {
		t.Fatal("the resubmission changed the original outcome")
	}
	// Exactly one attempt at the returned primary, which may listen on the
	// endpoint it had before or on another one, and no other OSD dial.
	if err := checkOneDial(dialed, fc.dialSnapshot(), endpoint, osdEndpoint(t, back.Address)); err != nil {
		t.Fatal("the resubmission did not open exactly its one connection:", err)
	}
	t.Logf("applied once by the resubmission at %s, version %d -> %d, tail count %d; the original outcome stays unknown", replayVersion(reply), pre.Version, final.Version, bytes.Count(final.Data, tail))
}

// heldSnapshot builds one synthetic receipt of PG 3.7's primary osd.3 holding
// client.4171.21:5 for epoch 1025 while the placement is at epoch 25. No
// daemon produced it.
func heldSnapshot() (opsInFlightSnapshot, osdFixtureOracle) {
	_, route := pgIntervalChange()
	before, after := route, route
	before.Up, before.Acting, after.Up, after.Acting = slices.Clone(route.Up), slices.Clone(route.Acting), slices.Clone(route.Up), slices.Clone(route.Acting)
	events := []string{"initiated", "throttled", "header_read", "all_read", "dispatched", "queued_for_pg", "reached_pg", heldEvent}
	return opsInFlightSnapshot{Complete: true, Before: before, After: after, OSD: 3, PGID: "3.7", NumOps: 2, Client: "client.4171.21",
		Ops: []opsInFlightOp{{Index: 1, Reqid: "client.4171.21:5", TID: 5, PGID: "3.7s0", Epoch: 1025, FlagPoint: heldEvent, Events: events,
			Description: "osd_op(client.4171.21:5 3.7s0 3.d6c2a1e7 (undecoded) ondisk+write+returnvec e1025)"}}}, route
}

func TestCheckHeldRequest(t *testing.T) {
	s, route := heldSnapshot()
	if op, err := checkHeldRequest(s, route, 4171, 21, 1025); err != nil || op.TID != 5 {
		t.Fatal("held request refused:", op, err)
	}
	newer, _ := heldSnapshot()
	newer.Before.Epoch, newer.After.Epoch = 1024, 1024
	if _, err := checkHeldRequest(newer, route, 4171, 21, 1025); err != nil {
		t.Fatal("held request at a newer epoch of the same placement refused:", err)
	}
	for name, edit := range map[string]func(*opsInFlightSnapshot){
		"incomplete":         func(s *opsInFlightSnapshot) { s.Complete = false },
		"a recorded failure": func(s *opsInFlightSnapshot) { s.Failures = []string{"placement or epoch changed"} },
		"refused":            func(s *opsInFlightSnapshot) { s.Code = -22 },
		"claims absence":     func(s *opsInFlightSnapshot) { s.AbsenceEstablished = true },
		"placement moved":    func(s *opsInFlightSnapshot) { s.After.Epoch = 26 },
		"acting moved":       func(s *opsInFlightSnapshot) { s.After.Acting = []int32{3, 2, 1, crushItemNone} },
		"an older epoch":     func(s *opsInFlightSnapshot) { s.Before.Epoch, s.After.Epoch = 24, 24 },
		"at the held epoch":  func(s *opsInFlightSnapshot) { s.Before.Epoch, s.After.Epoch = 1025, 1025 },
		"another primary":    func(s *opsInFlightSnapshot) { s.OSD = 2 },
		"another PG":         func(s *opsInFlightSnapshot) { s.PGID = "3.6" },
		"another address": func(s *opsInFlightSnapshot) {
			s.Before.Address, s.After.Address = "v2:192.0.2.7:6800/2", "v2:192.0.2.7:6800/2"
		},
		"another client":          func(s *opsInFlightSnapshot) { s.Client = "client.4171.20" },
		"no op":                   func(s *opsInFlightSnapshot) { s.Ops = nil },
		"two ops":                 func(s *opsInFlightSnapshot) { s.Ops = append(s.Ops, s.Ops[0]) },
		"identity of another tid": func(s *opsInFlightSnapshot) { s.Ops[0].Reqid = "client.4171.21:6" },
		"zero tid":                func(s *opsInFlightSnapshot) { s.Ops[0].Reqid, s.Ops[0].TID = "client.4171.21:0", 0 },
		"another shard":           func(s *opsInFlightSnapshot) { s.Ops[0].PGID = "3.7s2" },
		"no shard":                func(s *opsInFlightSnapshot) { s.Ops[0].PGID = "3.7" },
		"the placement's epoch":   func(s *opsInFlightSnapshot) { s.Ops[0].Epoch = 25 },
		"not delayed":             func(s *opsInFlightSnapshot) { s.Ops[0].FlagPoint = "started" },
		"decoded": func(s *opsInFlightSnapshot) {
			s.Ops[0].Description = "osd_op(client.4171.21:5 3.7s0 3:e7854b6b:::obj:head [append 0~13] snapc 0=[] ondisk+write+returnvec e1025)"
		},
		"delayed for another wait": func(s *opsInFlightSnapshot) { s.Ops[0].FlagPoint = "waiting for peered" },
		"never reached the PG":     func(s *opsInFlightSnapshot) { s.Ops[0].Events = []string{"initiated", "queued_for_pg", heldEvent} },
		"waiting for peered":       func(s *opsInFlightSnapshot) { s.Ops[0].Events[len(s.Ops[0].Events)-1] = "waiting for peered" },
		"executed after the hold":  func(s *opsInFlightSnapshot) { s.Ops[0].Events = append(s.Ops[0].Events, "started") },
		"executed before the hold": func(s *opsInFlightSnapshot) { s.Ops[0].Events = slices.Insert(s.Ops[0].Events, 7, "started") },
	} {
		changed, _ := heldSnapshot()
		edit(&changed)
		if _, err := checkHeldRequest(changed, route, 4171, 21, 1025); err == nil {
			t.Error("snapshot accepted:", name)
		}
	}
	for name, c := range map[string]struct {
		gid   uint64
		inc   int32
		epoch uint32
	}{
		"another entity":          {4172, 21, 1025},
		"another incarnation":     {4171, 20, 1025},
		"another held epoch":      {4171, 21, 1026},
		"held at the route epoch": {4171, 21, 25},
		"held before the route":   {4171, 21, 24},
	} {
		if _, err := checkHeldRequest(s, route, c.gid, c.inc, c.epoch); err == nil {
			t.Error("request accepted:", name)
		}
	}
}

func TestNewDials(t *testing.T) {
	before := map[string]int{"192.0.2.1:3300": 1, "192.0.2.7:6800": 1}
	if got := newDials(before, maps.Clone(before), "192.0.2.7:6800"); len(got) != 0 {
		t.Fatal("unchanged dials reported:", got)
	}
	// A MON session may reconnect; that is not an OSD connection.
	if got := newDials(before, map[string]int{"192.0.2.1:3300": 2, "192.0.2.7:6800": 1}, "192.0.2.7:6800"); len(got) != 0 {
		t.Fatal("a MON redial reported:", got)
	}
	for name, now := range map[string]map[string]int{
		"the OSD again":    {"192.0.2.1:3300": 1, "192.0.2.7:6800": 2},
		"another endpoint": {"192.0.2.1:3300": 1, "192.0.2.7:6800": 1, "192.0.2.8:6800": 1},
	} {
		if got := newDials(before, now, "192.0.2.7:6800"); len(got) != 1 {
			t.Error("dial not reported:", name, got)
		}
	}
	if got := newDials(before, map[string]int{"192.0.2.1:3300": 1, "192.0.2.7:6800": 3}, "192.0.2.7:6800"); len(got) != 1 || got[0] != (dialChange{"192.0.2.7:6800", 1, 3}) {
		t.Error("two attempts at the OSD not reported with their counts:", got)
	}

	// The resubmission's one connection to the returned primary, at its old
	// endpoint or at a new one.
	const mon, old, moved, other = "192.0.2.1:3300", "192.0.2.7:6800", "192.0.2.7:6802", "192.0.2.8:6800"
	for name, c := range map[string]struct {
		now map[string]int
		at  string
	}{
		"one attempt at the old endpoint": {map[string]int{mon: 1, old: 2}, old},
		"one attempt at a new endpoint":   {map[string]int{mon: 1, old: 1, moved: 1}, moved},
		"one attempt and a MON redial":    {map[string]int{mon: 3, old: 2}, old},
	} {
		if err := checkOneDial(before, c.now, old, c.at); err != nil {
			t.Errorf("%s refused: %v", name, err)
		}
	}
	for name, c := range map[string]struct {
		now map[string]int
		at  string
	}{
		"no attempt":                                  {maps.Clone(before), old},
		"two attempts at the old endpoint":            {map[string]int{mon: 1, old: 3}, old},
		"two attempts at a new endpoint":              {map[string]int{mon: 1, old: 1, moved: 2}, moved},
		"the attempt at another endpoint":             {map[string]int{mon: 1, old: 2}, moved},
		"the attempt at the new endpoint and the old": {map[string]int{mon: 1, old: 2, moved: 1}, moved},
		"one attempt and another OSD":                 {map[string]int{mon: 1, old: 2, other: 1}, old},
	} {
		if err := checkOneDial(before, c.now, old, c.at); err == nil {
			t.Error("dials accepted:", name)
		}
	}
}

// TestEndWithin covers the bounded wait for the held request's end: a
// result within the wait, a worker that ends only on the cancellation and one
// that stays blocked, which must not hold the caller past wait and grace.
func TestEndWithin(t *testing.T) {
	const wait, grace = 20 * time.Millisecond, 20 * time.Millisecond
	done := make(chan int, 1)
	done <- 7
	if got, ok, late := endWithin(done, wait, grace, func() { t.Error("cancelled a call that ended") }); !ok || got != 7 || late != "" {
		t.Fatal("result within the wait refused:", got, ok, late)
	}
	onCancel := make(chan int, 1)
	cancelled := false
	if _, ok, late := endWithin(onCancel, wait, grace, func() { cancelled = true; onCancel <- 9 }); ok || !cancelled || !strings.Contains(late, "9") {
		t.Fatal("a call that ended only on the cancellation:", ok, cancelled, late)
	}
	blocked := make(chan int, 1)
	start := time.Now()
	_, ok, late := endWithin(blocked, wait, grace, func() {})
	if ok || !strings.Contains(late, "did not end") {
		t.Fatal("a blocked call:", ok, late)
	}
	if elapsed := time.Since(start); elapsed > wait+grace+time.Second {
		t.Fatal("a blocked call held the caller for", elapsed)
	}
}
