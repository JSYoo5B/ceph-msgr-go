package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// An EC mutation is in flight on the optimized isa k=2,m=2 pool when its PG's
// primary is killed and the acting set and primary change. In one phase the
// request reached the primary and was applied on every shard, but its reply
// is withheld; in the other the request bytes never left the client. Both
// end with an OutcomeUnknownError that keeps the original transaction, and
// the client neither resends nor reconnects. The native observer then reads
// the object bytes, xattr, user version and mtime and the new epoch,
// primary and shard. Only where it shows that the first attempt was not
// applied does the test, as the caller, resubmit the same operations under
// the original identity to the replacement primary.
func TestCephOSDErasureOptimizedK2M2InFlightMutationIntegration(t *testing.T) {
	erasureInFlightScenario(t, erasureK2M2, true, "isa")
}

// erasureFaultClient wraps the connections of one Go client to a single OSD
// endpoint and counts every OSD dial, so a test can show that nothing
// reconnected or resent after an unknown outcome.
type erasureFaultClient struct {
	*cephmsgr.Client
	target     atomic.Pointer[string]
	blockReads atomic.Bool
	dropWrites atomic.Bool
	blocked    chan struct{}
	dropped    chan struct{}
	mu         sync.Mutex
	dials      map[string]int
}

// osdEndpoint is the host:port a client dials for a native v2:host:port/nonce
// OSD address.
func osdEndpoint(t *testing.T, address string) string {
	t.Helper()
	trimmed := strings.TrimPrefix(address, "v2:")
	if i := strings.LastIndex(trimmed, "/"); i >= 0 {
		trimmed = trimmed[:i]
	}
	endpoint, err := netip.ParseAddrPort(trimmed)
	if err != nil {
		t.Fatal("native OSD address", address, err)
	}
	return endpoint.String()
}

func dialErasureFaultClient(t *testing.T, ctx context.Context, hash bool, target string) *erasureFaultClient {
	t.Helper()
	f := &erasureFaultClient{blocked: make(chan struct{}, 1), dropped: make(chan struct{}, 1), dials: map[string]int{}}
	f.target.Store(&target)
	options := integrationOptions(t)
	options.EnableOSD = true
	if hash {
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
		f.mu.Lock()
		f.dials[normalized]++
		f.mu.Unlock()
		if normalized != *f.target.Load() {
			return conn, nil
		}
		return &testcluster.LostReplyConn{Conn: conn, Armed: &f.blockReads, DropWrites: &f.dropWrites, Closed: make(chan struct{}),
			SignalBlocked: func() {
				select {
				case f.blocked <- struct{}{}:
				default:
				}
			},
			SignalWriteDropped: func() {
				select {
				case f.dropped <- struct{}{}:
				default:
				}
			}}, nil
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("fault client admission", err)
	}
	f.Client = c
	return f
}

func (f *erasureFaultClient) dialsTo(endpoint string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dials[endpoint]
}

// disarm stops every fault and wraps no further connection.
func (f *erasureFaultClient) disarm() {
	none := ""
	f.blockReads.Store(false)
	f.dropWrites.Store(false)
	f.target.Store(&none)
}

// erasureKillOSD SIGKILLs one OSD and returns its idempotent restoration:
// restart, wait for clean PGs and republish the fixture object's routing
// without the legacy-only object restoration (the fixture object was not
// changed).
func erasureKillOSD(t *testing.T, ctx context.Context, base osdFixtureOracle, id int32) func() {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	name := strconv.FormatInt(int64(id), 10)
	if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", name); err != nil {
		t.Fatal("kill", id, err)
	}
	var state struct {
		ExitCode *int `json:"exit_code"`
	}
	if data, err := os.ReadFile(filepath.Join(dir, "osd-crash-state.json")); err != nil || json.Unmarshal(data, &state) != nil || state.ExitCode == nil || *state.ExitCode != 137 {
		t.Fatal("expected SIGKILL evidence", err)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			recover, stop := context.WithTimeout(context.Background(), 120*time.Second)
			defer stop()
			if err := testcluster.ControlDaemon(recover, dir, "start", "osd", name); err != nil {
				t.Error("restart killed OSD", err)
				return
			}
			waitOSDFixtureClean(t, recover, base)
			fresh := nativeMutation(t, recover, map[string]any{"compare": false, "placement": true}).Placement
			publishReplicaOSDRouting(t, dir, adoptMutationPlacement(t, base, fresh))
		})
	}
}

// erasureSettledPrimary waits until the native map names a primary other
// than old's on a primary-eligible shard. An optimized pool's first map
// after the loss may still name the nonprimary shard's OSD (pure CRUSH
// order); that OSD then has the MON set a primary-first pg_temp and a later
// map names a parity shard.
func erasureSettledPrimary(t *testing.T, ctx context.Context, object string, old osdFixtureOracle, facts erasureFacts) osdFixtureOracle {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for {
		fresh := poolPlace(t, ctx, 1, object, "")
		nonprimary := slices.Contains(facts.nonprimary, int(fresh.Shard))
		if fresh.ID != old.ID && fresh.ID >= 0 && !nonprimary {
			return fresh
		}
		if fresh.ID >= 0 && nonprimary {
			t.Logf("transient: epoch %d names osd.%d on nonprimary shard %d (acting %v)", fresh.Epoch, fresh.ID, fresh.Shard, fresh.Acting)
		}
		if time.Now().After(deadline) {
			t.Fatal("no new primary was chosen")
		}
		time.Sleep(2 * time.Second)
	}
}

func erasureInFlightScenario(t *testing.T, width erasureWidth, optimized bool, plugin string) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != width.topology || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skipf("%s with overwrites needs the BlueStore OSD topology %q", width, width.topology)
	}
	fixture, request, hash := osdFixture, osdObjectRequest, false
	if isReplicatedPGFixtureProfile() {
		fixture, request, hash = replicatedPGFixture, replicatedPGRequest, true
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
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
	created := nativePool(t, ctx, "create", map[string]any{"slot": 1, "pg_num": pg, "hashpspool": hash, "erasure": true, "width": width.name, "optimized": optimized, "plugin": plugin})
	if created.PoolType != 3 || created.ECOptimized != optimized || created.Width != width.name || created.ErasureCodeProfile != width.profile {
		t.Fatal("pool is not the requested erasure-coded shape", width, created.PoolType, created.ECOptimized, created.Width, created.ErasureCodeProfile)
	}
	facts := erasurePoolMapFacts(t, ctx, c, created, width, optimized)
	t.Logf("EC pool %d %s optimized=%v size=%d min_size=%d nonprimary shards %v owned OSDs %v",
		created.PoolID, width, optimized, facts.size, facts.minSize, facts.nonprimary, facts.owned)

	const (
		incarnation = 7
		attr        = "ec-inflight"
		seeded      = 1700005000
		mutated     = 1700005001
	)
	phase := func(t *testing.T, object string, dropRequest bool) {
		seed := bytes.Repeat([]byte{0x3c, 0, 0xff, byte(len(object))}, 1500)
		route := poolPlace(t, ctx, 1, object, "")
		facts.checkPlacement(t, route)
		so := mutationConnection(t, c, ctx, route)
		r := request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(seed)), Data: seed})
		r.ModificationTime = cephmsgr.OSDTime{Seconds: seeded}
		if _, err := so.Request(ctx, r); err != nil {
			t.Fatal("seed", err)
		}
		so.Close()
		pre := poolObserve(t, ctx, 1, object, "", "")
		if pre.Code != 0 || !bytes.Equal(pre.Data, seed) || pre.Seconds != seeded || xattrInventory(pre)[attr] != "" {
			t.Fatal("native pre-state differs", pre.Code, pre.Seconds)
		}
		t.Logf("pre-state: version %d, placement epoch %d pg %s acting %v primary osd.%d shard %d pg_temp %v primary-first %+v",
			pre.Version, route.Epoch, route.PGID, route.Acting, route.ID, route.Shard, route.PGTemp, routePrimaryFirst(route))

		endpoint := osdEndpoint(t, route.Address)
		fc := dialErasureFaultClient(t, ctx, hash, endpoint)
		defer fc.Close()
		o := mutationConnection(t, fc.Client, ctx, route)
		tail := []byte("in-flight\x00\xff" + object)
		value := []byte{0, 0xff, 'I'}
		ops := []cephmsgr.OSDOperation{
			{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail},
			{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: attr}, Data: value},
		}
		first := request(route, ops...)
		first.ClientIncarnation = incarnation
		first.ModificationTime = cephmsgr.OSDTime{Seconds: mutated}
		want := append(bytes.Clone(seed), tail...)

		type result struct {
			reply cephmsgr.OSDReply
			err   error
		}
		finished := make(chan result, 1)
		callCtx, cancelCall := context.WithCancel(ctx)
		defer cancelCall()
		signal := fc.blocked
		if dropRequest {
			fc.dropWrites.Store(true)
			signal = fc.dropped
		} else {
			fc.blockReads.Store(true)
		}
		go func() {
			reply, err := o.Request(callCtx, first)
			finished <- result{reply, err}
		}()
		select {
		case <-signal:
		case <-ctx.Done():
			t.Fatal("fault not activated", ctx.Err())
		}
		if dropRequest {
			// The request bytes are discarded locally; the primary cannot
			// apply them. Give the writer time to hand the whole frame over.
			time.Sleep(time.Second)
			select {
			case early := <-finished:
				t.Fatal("request ended before the epoch change", early.err)
			default:
			}
		} else {
			// The withheld reply belongs to a mutation the primary applied on
			// the shards; wait for the native observer to see it.
			deadline := time.Now().Add(60 * time.Second)
			for {
				n := poolObserve(t, ctx, 1, object, "", "")
				if bytes.Equal(n.Data, want) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("the mutation with the withheld reply never became visible natively", len(n.Data), n.Version)
				}
				time.Sleep(200 * time.Millisecond)
			}
		}

		restore := erasureKillOSD(t, ctx, base, route.ID)
		defer restore()
		// With its reply withheld the session cannot observe the kill, so the
		// caller ends the local wait at once. A dropped request's session
		// sees the closed connection; the local wait ends only if it does not.
		var lost result
		ended := "session failure"
		wait := 20 * time.Second
		if !dropRequest {
			wait = 0
		}
		select {
		case lost = <-finished:
		case <-time.After(wait):
			ended = "local cancellation"
			cancelCall()
			lost = <-finished
		}
		var unknown *cephmsgr.OutcomeUnknownError
		var server *cephmsgr.OSDError
		sub := lost.reply.Submission
		t.Logf("first attempt ended by %s: %v; submission tid %d incarnation %d epoch %d attempt %d placement %+v",
			ended, lost.err, sub.Transaction, sub.ClientIncarnation, sub.MapEpoch, sub.Attempt, sub.Placement)
		if !errors.As(lost.err, &unknown) || errors.As(lost.err, &server) {
			t.Fatal("expected an unknown outcome without a server result", lost.err)
		}
		if sub.Transaction == 0 || sub.GlobalID != fc.Snapshot().GlobalID || sub.ClientIncarnation != incarnation || sub.MapEpoch != route.Epoch ||
			sub.Attempt != 0 || sub.Placement == nil || sub.Placement.Primary != route.ID || int8(sub.Placement.Shard) != route.Shard {
			t.Fatal("the unknown outcome lost its original identity", sub)
		}
		o.Close()
		fc.disarm()

		fresh := erasureSettledPrimary(t, ctx, object, route, facts)
		freshEndpoint := osdEndpoint(t, fresh.Address)
		t.Logf("epoch %d -> %d: primary osd.%d shard %d -> osd.%d shard %d, acting %v -> %v, pg_temp %v primary-first %+v",
			route.Epoch, fresh.Epoch, route.ID, route.Shard, fresh.ID, fresh.Shard, route.Acting, fresh.Acting, fresh.PGTemp, routePrimaryFirst(fresh))
		if fresh.Epoch <= route.Epoch || slicesContains(fresh.Acting, route.ID) || fresh.Replicas != facts.size || fresh.Nonprimary != len(facts.nonprimary) {
			t.Fatal("new placement does not reflect the killed primary or changed the pool facts", fresh)
		}
		n := poolObserve(t, ctx, 1, object, "", "")
		applied := bytes.Equal(n.Data, want)
		t.Logf("native after the epoch change: applied=%v version %d (pre %d) mtime %d xattr %q", applied, n.Version, pre.Version, n.Seconds, xattrInventory(n)[attr])
		if fc.dialsTo(endpoint) != 1 || fc.dialsTo(freshEndpoint) != 0 {
			t.Fatal("the client reconnected or resent after the unknown outcome", fc.dialsTo(endpoint), fc.dialsTo(freshEndpoint))
		}
		if applied != !dropRequest {
			t.Fatal("native outcome differs from the injected fault", applied, dropRequest)
		}
		final := n
		if applied {
			// The native observer shows exactly one execution; the caller
			// keeps the unknown outcome and does not resubmit.
			if n.Version <= pre.Version || n.Seconds != mutated || xattrInventory(n)[attr] != string(value) {
				t.Fatal("applied mutation lacks its version, mtime or xattr", n.Version, n.Seconds)
			}
		} else {
			if n.Version != pre.Version || !bytes.Equal(n.Data, seed) || n.Seconds != seeded || xattrInventory(n)[attr] != "" {
				t.Fatal("native state changed although the request never reached the primary", n.Version, n.Seconds)
			}
			// The caller's explicit decision: resubmit the same operations
			// under the original identity, routed by the new map.
			ro := mutationConnection(t, fc.Client, ctx, fresh)
			defer ro.Close()
			retry := request(fresh, ops...)
			retry.ClientIncarnation = incarnation
			retry.ModificationTime = cephmsgr.OSDTime{Seconds: mutated}
			retry.Resubmit, retry.RetryAttempt = &sub, 1
			retryCtx, stop := context.WithTimeout(ctx, 30*time.Second)
			reply, err := ro.Request(retryCtx, retry)
			stop()
			if err != nil {
				n := poolObserve(t, ctx, 1, object, "", "")
				t.Fatalf("explicit resubmission through the new primary: %v (native version %d applied=%v)", err, n.Version, bytes.Equal(n.Data, want))
			}
			t.Logf("resubmission: tid %d code %d flags %#x user version %d attempt %d epoch %d", reply.Transaction, reply.Code, reply.Flags, reply.UserVersion, reply.RetryAttempt, reply.MapEpoch)
			if reply.Transaction != sub.Transaction || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.Submission.Attempt != 1 {
				t.Fatal("resubmission receipt differs", reply.Transaction, reply.Code, reply.Flags)
			}
			time.Sleep(300 * time.Millisecond)
			final = poolObserve(t, ctx, 1, object, "", "")
			if !bytes.Equal(final.Data, want) || final.Version != reply.UserVersion || final.Seconds != mutated || xattrInventory(final)[attr] != string(value) {
				t.Fatal("native state after the resubmission differs", len(final.Data), final.Version, reply.UserVersion, final.Seconds)
			}
		}

		// Restart the killed OSD and wait for clean PGs: the returned shard
		// must not change what the native observer reads.
		restore()
		recovered := poolObserve(t, ctx, 1, object, "", "")
		if !bytes.Equal(recovered.Data, final.Data) || recovered.Version != final.Version || recovered.Seconds != final.Seconds || xattrInventory(recovered)[attr] != xattrInventory(final)[attr] {
			t.Fatal("native state after recovery differs", recovered.Version, final.Version)
		}
		back := poolPlace(t, ctx, 1, object, "")
		t.Logf("after recovery: epoch %d primary osd.%d shard %d acting %v version %d", back.Epoch, back.ID, back.Shard, back.Acting, recovered.Version)
	}

	t.Run("reply withheld after the mutation applied", func(t *testing.T) { phase(t, "ec-inflight-applied", false) })
	t.Run("request lost before the primary", func(t *testing.T) { phase(t, "ec-inflight-lost", true) })
}
