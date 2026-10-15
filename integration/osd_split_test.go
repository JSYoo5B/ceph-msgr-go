package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

type nativeSplitPG struct {
	State string `json:"state"`
	Epoch uint32 `json:"epoch"`
	Info  struct {
		PG      string `json:"pgid"`
		History struct {
			LastSplit uint32 `json:"last_epoch_split"`
		} `json:"history"`
	} `json:"info"`
	Acting []int32 `json:"acting"`
}
type nativeSplitState struct {
	Epoch   uint32 `json:"epoch"`
	RawHash uint32 `json:"raw_hash"`
	Clean   bool   `json:"all_active_clean"`
	Pool    struct {
		ID        int64  `json:"pool"`
		Name      string `json:"pool_name"`
		PGNum     int    `json:"pg_num"`
		PGTarget  int    `json:"pg_num_target"`
		PGPending int    `json:"pg_num_pending"`
		PGPNum    int    `json:"pg_placement_num"`
		PGPTarget int    `json:"pg_placement_num_target"`
	} `json:"pool"`
	Placement struct {
		PoolID  int64   `json:"pool_id"`
		Object  string  `json:"objname"`
		PG      string  `json:"pgid"`
		RawPG   string  `json:"raw_pgid"`
		Primary int32   `json:"acting_primary"`
		Acting  []int32 `json:"acting"`
	} `json:"placement"`
	PG           nativeSplitPG       `json:"pg"`
	Parent       nativeSplitPG       `json:"parent_pg"`
	ObjectState  nativeXattrSnapshot `json:"object_state"`
	FixtureState nativeXattrSnapshot `json:"fixture_state"`
}
type nativeSplitIdentity struct {
	GlobalID          uint64 `json:"global_id"`
	Transaction       uint64 `json:"transaction"`
	ClientIncarnation uint32 `json:"client_incarnation"`
	MapEpoch          uint32 `json:"map_epoch"`
	Pool              uint64 `json:"pool"`
	Seed              uint32 `json:"pg_seed"`
	RawHash           uint32 `json:"raw_hash"`
}
type nativeSplitReceipt struct {
	nativeSplitIdentity
	Token        string `json:"token"`
	Source       string `json:"source_commit"`
	SourceType   uint8  `json:"source_type"`
	SourceNumber uint64 `json:"source_number"`
	OSD          int32  `json:"osd_id"`
	ReachedPG    bool   `json:"reached_pg"`
	BodyDecoded  bool   `json:"body_decoded"`
	Evidence     string `json:"evidence"`
}
type nativeSplitScope struct {
	Schema                int    `json:"schema_version"`
	Token                 string `json:"token"`
	Source                string `json:"source_commit"`
	Object                string `json:"object"`
	Ready, Done, Restored bool
	Before                nativeSplitState    `json:"before"`
	Split                 nativeSplitState    `json:"split"`
	After                 nativeSplitState    `json:"after"`
	PreRestore            nativeXattrSnapshot `json:"pre_restore_object"`
	Stale                 nativeSplitReceipt  `json:"stale_receipt"`
	Error                 string              `json:"error"`
}

func splitControl(ctx context.Context, action, name string) (nativeSplitScope, error) {
	var result nativeSplitScope
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	data, err := json.Marshal(map[string]string{"object": name})
	if err != nil {
		return result, err
	}
	if err = testcluster.WritePublicEvidence(filepath.Join(dir, "osd-split-input.json"), data); err != nil {
		return result, err
	}
	if err = os.Remove(filepath.Join(dir, "osd-split-result.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	if err = testcluster.ControlDaemon(ctx, dir, "verify", "osd-split", action); err != nil {
		return result, err
	}
	file, err := os.Open(filepath.Join(dir, "osd-split-result.json"))
	if err != nil {
		return result, err
	}
	defer file.Close()
	data, err = io.ReadAll(io.LimitReader(file, (512<<10)+1))
	if err != nil {
		return result, err
	}
	if len(data) > 512<<10 {
		return result, errors.New("native split scope exceeds evidence bound")
	}
	if err = json.Unmarshal(data, &result); err != nil {
		return result, err
	}
	if result.Schema != 1 || result.Source != "7f793731f1b39eb4f465e960113d2363c311b964" || result.Object != name || len(result.Token) != 32 || result.Error != "" {
		return result, errors.New("native split scope identity or state differs")
	}
	return result, nil
}
func checkSplitState(t *testing.T, s nativeSplitState, route osdFixtureOracle, count int) {
	t.Helper()
	p := s.Pool
	if p.ID != route.Pool || p.Name != "ceph-msgr-osd-fixture" || p.PGNum != count || p.PGTarget != count || p.PGPending != count || p.PGPNum != count || p.PGPTarget != count || !s.Clean || s.RawHash != route.Hash || s.Placement.Object != route.Name || s.Placement.PoolID != route.Pool || s.Placement.PG != route.PGID || s.Placement.RawPG != route.RawPGID || s.Placement.Primary != route.ID || !reflect.DeepEqual(s.Placement.Acting, []int32{0}) || s.PG.Info.PG != route.PGID || s.PG.Epoch != s.Epoch || s.Epoch == 0 {
		t.Fatal("native actual/target/pending PG state differs", s, route)
	}
}
func checkSplitUnknown(t *testing.T, reply cephmsgr.OSDReply, err error, request cephmsgr.OSDRequest, cause error) {
	t.Helper()
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(err, &unknown) || !errors.Is(err, cause) || reply.Submission.Transaction == 0 || reply.Submission.GlobalID == 0 || reply.Submission.ClientIncarnation != request.ClientIncarnation || reply.Submission.MapEpoch != request.MapEpoch || reply.Submission.Attempt != 0 || !reflect.DeepEqual(reply.Submission.Placement, request.Placement) || reply.Transaction != 0 || len(reply.RawFront) != 0 || len(reply.RawData) != 0 {
		t.Fatal("sent split request lost original unknown identity", reply, err)
	}
}

// Two separate uncertain requests exercise distinct boundaries. The first has
// independently observed execution before a reply is withheld across the split.
// The second is received at the old parent after its split epoch. Its native
// envelope receipt and unchanged snapshot do not establish a mutation outcome.
func TestCephOSDSplitIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "replicated-pg-v8" || os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "1" {
		t.Skip("requires owned single-OSD replicated-pg-v8 PG split fixture")
	}
	c, _, base, _ := replicatedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	t.Cleanup(cancel)
	var start, observed, finish nativeSplitScope
	var oldUnknown, staleUnknown, initialReply, freshRead, freshAppend cephmsgr.OSDReply
	var baseline, executed, staleObservation, final mutationSnapshot
	var oldRoute, freshRoute osdFixtureOracle
	var faultDials, staleDials atomic.Int32
	prepared, started, restored, cleaned := false, false, false, false
	var name, cleanupError string
	var pendingCancel context.CancelFunc
	var faultConn, staleConn *cephmsgr.OSDConnection
	var faultClient, staleClient *cephmsgr.Client
	t.Cleanup(func() {
		defer func() {
			evidence := struct {
				Scope                                                          string `json:"scope"`
				Start, Observed, Finish                                        nativeSplitScope
				OldPlacement, FreshPlacement                                   osdFixtureOracle
				Initial, OriginalUnknown, StaleUnknown, FreshRead, FreshAppend cephmsgr.OSDReply
				Baseline, ExecutedBeforeSplit, StaleTimeObservation, Final     mutationSnapshot
				FaultOSDDials, StaleOSDDials                                   int32
				Restored, Cleaned                                              bool
				CleanupError                                                   string
			}{"owned single-OSD split/merge; unchanged snapshot is observation only; uncertain submissions remain unknown", start, observed, finish, oldRoute, freshRoute, initialReply, oldUnknown, staleUnknown, freshRead, freshAppend, baseline, executed, staleObservation, final, faultDials.Load(), staleDials.Load(), restored, cleaned, cleanupError}
			data, err := json.Marshal(evidence)
			if err == nil {
				err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-split-results.json"), data)
			}
			if err != nil {
				t.Error(err)
			}
		}()
		if pendingCancel != nil {
			pendingCancel()
		}
		if faultConn != nil {
			_ = faultConn.Close()
		}
		if faultClient != nil {
			_ = faultClient.Close()
		}
		// Configuration restoration precedes owned object deletion. An independent
		// cleanup context remains usable when a request or the body context expired.
		if started && !restored {
			cleanup, stop := context.WithTimeout(context.Background(), 165*time.Second)
			var err error
			finish, err = splitControl(cleanup, "finish", name)
			stop()
			if err != nil {
				cleanupError = err.Error()
				t.Error("native PG split restoration unconfirmed", err)
			} else {
				restored = finish.Done && finish.Restored
			}
		}
		if staleConn != nil {
			_ = staleConn.Close()
		}
		if staleClient != nil {
			_ = staleClient.Close()
		}
		if prepared && (!started || restored) {
			cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
			defer stop()
			cleaned = replicatedPGOwned(t, cleanup, base, "cleanup", omapWriteInput{}).Cleanup.Removed
			if !cleaned {
				t.Error("native split owned cleanup incomplete")
			}
		}
		if prepared && started && !restored {
			t.Error("owned heads retained because native PG policy restoration was not confirmed")
		}
	})
	// UUID candidates are consumer setup. Select a raw hash whose bit 3 moves it
	// into a split child, but obtain every actual PG from the independent client.
	for batch := 0; batch < 4 && name == ""; batch++ {
		owned := replicatedPGOwned(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("split candidate")}, Reserve: 8})
		prepared = true
		for _, candidate := range owned.Objects {
			native := replicatedPGNative(t, ctx, candidate)
			if native.Placement.Hash&8 != 0 {
				name = candidate
				oldRoute = native.Placement
				break
			}
		}
		if name == "" {
			if !replicatedPGOwned(t, ctx, base, "cleanup", omapWriteInput{}).Cleanup.Removed {
				t.Fatal("unselected candidate cleanup incomplete")
			}
			prepared = false
		}
	}
	if name == "" {
		t.Fatal("bounded UUID candidates did not yield a native split child")
	}
	original := []byte{0, 255, 128, 's', 'p', 'l', 'i', 't', 0}
	setup := replicatedPGConnection(t, c, ctx, oldRoute)
	initial := replicatedPGRequest(oldRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(original)), Data: original})
	initial.ClientIncarnation = 89
	var err error
	initialReply, err = setup.Request(ctx, initial)
	_ = setup.Close()
	if err != nil {
		t.Fatal("create selected split head", err)
	}
	native := replicatedPGNative(t, ctx, name)
	baseline = native.Actual
	oldRoute = native.Placement
	if baseline.Code != 0 || !bytes.Equal(baseline.Data, original) || baseline.Version != initialReply.UserVersion {
		t.Fatal("native split baseline differs", native)
	}
	// First mutation executes once; only its reply path is deliberately withheld.
	var armed atomic.Bool
	blocked := make(chan struct{}, 1)
	options := integrationOptions(t)
	options.EnableOSD = true
	options.OSDProfile = cephmsgr.OSDProfileReplicatedPG
	dial := options.DialContext
	options.DialContext = func(call context.Context, network, endpoint string) (net.Conn, error) {
		conn, e := dial(call, network, endpoint)
		if e != nil {
			return nil, e
		}
		_, daemon, _ := testcluster.SplitPeer(endpoint)
		if daemon != "osd.0" {
			return conn, nil
		}
		faultDials.Add(1)
		return &testcluster.LostReplyConn{Conn: conn, Armed: &armed, Closed: make(chan struct{}), SignalBlocked: func() {
			select {
			case blocked <- struct{}{}:
			default:
			}
		}}, nil
	}
	faultClient, err = cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	faultConn = replicatedPGConnection(t, faultClient, ctx, oldRoute)
	oldTail := []byte("\x00before-split-once\xff")
	oldRequest := replicatedPGRequest(oldRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(oldTail)), Data: oldTail})
	oldRequest.ClientIncarnation = 91
	oldRequest.ModificationTime = cephmsgr.OSDTime{Seconds: 1700001100, Nanoseconds: 123456789}
	call, stopCall := context.WithCancel(ctx)
	pendingCancel = stopCall
	type completion struct {
		reply cephmsgr.OSDReply
		err   error
	}
	done := make(chan completion, 1)
	armed.Store(true)
	go func() { r, e := faultConn.Request(call, oldRequest); done <- completion{r, e} }()
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("split receive withholding was not activated", ctx.Err())
	}
	wantOld := append(bytes.Clone(original), oldTail...)
	for {
		executed = replicatedPGNative(t, ctx, name).Actual
		if executed.Code == 0 && bytes.Equal(executed.Data, wantOld) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("pre-split append was not independently observed", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	if executed.Version <= baseline.Version || executed.Seconds != oldRequest.ModificationTime.Seconds || executed.Nanos != oldRequest.ModificationTime.Nanoseconds {
		t.Fatal("native pre-split exact-once postcondition differs", executed)
	}
	staleInput := filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-split-stale-input.json")
	started = true
	start, err = splitControl(ctx, "start", name)
	if err != nil {
		t.Fatal("native actual PG split", err)
	}
	if !start.Ready || start.Done || start.Restored {
		t.Fatal("native split was not held ready", start)
	}
	checkSplitState(t, start.Before, oldRoute, 8)
	native = replicatedPGNative(t, ctx, name)
	freshRoute = native.Placement
	checkSplitState(t, start.Split, freshRoute, 16)
	if freshRoute.PG != oldRoute.PG+8 || freshRoute.Hash != oldRoute.Hash || start.Split.Parent.Info.PG != oldRoute.PGID || start.Split.Parent.Info.History.LastSplit <= oldRequest.MapEpoch || !reflect.DeepEqual(start.Before.ObjectState, start.Split.ObjectState) || !reflect.DeepEqual(executed, native.Actual) {
		t.Fatal("native actual child or preserved pre-split state differs", start, native)
	}
	stopCall()
	var completed completion
	select {
	case completed = <-done:
	case <-ctx.Done():
		t.Fatal("old split request cancellation did not complete", ctx.Err())
	}
	oldUnknown = completed.reply
	checkSplitUnknown(t, oldUnknown, completed.err, oldRequest, context.Canceled)
	if faultDials.Load() != 1 {
		t.Fatal("uncertain pre-split request opened another OSD session", faultDials.Load())
	}
	_ = faultConn.Close()
	_ = faultClient.Close()
	// A separate connection sends the old caller tuple *after* actual split.
	// This is independent of the reply withholding fault above.
	options = integrationOptions(t)
	options.EnableOSD = true
	options.OSDProfile = cephmsgr.OSDProfileReplicatedPG
	dial = options.DialContext
	options.DialContext = func(call context.Context, network, endpoint string) (net.Conn, error) {
		conn, e := dial(call, network, endpoint)
		if e == nil {
			_, daemon, _ := testcluster.SplitPeer(endpoint)
			if daemon == "osd.0" {
				staleDials.Add(1)
			}
		}
		return conn, e
	}
	staleClient, err = cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	staleConn = replicatedPGConnection(t, staleClient, ctx, freshRoute)
	sentinel := []byte("\x00stale-parent-must-remain-unknown\xff")
	staleRequest := replicatedPGRequest(oldRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(sentinel)), Data: sentinel})
	staleRequest.ClientIncarnation = 95
	short, stopShort := context.WithTimeout(ctx, 2*time.Second)
	staleUnknown, err = staleConn.Request(short, staleRequest)
	stopShort()
	checkSplitUnknown(t, staleUnknown, err, staleRequest, context.DeadlineExceeded)
	if staleDials.Load() != 1 {
		t.Fatal("stale-parent request automatically redialed", staleDials.Load())
	}
	identity := nativeSplitIdentity{staleUnknown.Submission.GlobalID, staleUnknown.Submission.Transaction, staleUnknown.Submission.ClientIncarnation, staleUnknown.Submission.MapEpoch, uint64(oldRoute.Pool), oldRoute.PG, oldRoute.Hash}
	input, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	if err = testcluster.WritePublicEvidence(staleInput, input); err != nil {
		t.Fatal(err)
	}
	observed, err = splitControl(ctx, "observe", name)
	if err != nil {
		t.Fatal("independent native stale-parent envelope receipt", err)
	}
	receipt := observed.Stale
	if observed.Token != start.Token || receipt.Token != start.Token || receipt.nativeSplitIdentity != identity || receipt.Source != start.Source || receipt.SourceType != 8 || receipt.SourceNumber != identity.GlobalID || receipt.OSD != 0 || !receipt.ReachedPG || receipt.BodyDecoded || receipt.Evidence != "native completed history; stale envelope reception only; execution remains unknown" {
		t.Fatal("native stale envelope identity or evidence boundary differs", observed)
	}
	native = replicatedPGNative(t, ctx, name)
	staleObservation = native.Actual
	freshRoute = native.Placement
	if !reflect.DeepEqual(staleObservation, executed) {
		t.Fatal("native stale-time observation differs from last positively observed state", native)
	}
	// The unchanged snapshot above is a time-bounded observation. The original
	// OutcomeUnknown replies remain unknown; no stale mutation is replayed.
	freshRead, err = staleConn.Request(ctx, replicatedPGRequest(freshRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(wantOld) + 128)}))
	if err != nil {
		t.Fatal("caller supplied current child read", err)
	}
	checkReplicatedPGReply(t, freshRead, freshRoute)
	if len(freshRead.Operations) != 1 || !bytes.Equal(freshRead.Operations[0].Data, wantOld) || freshRead.UserVersion != executed.Version {
		t.Fatal("fresh child read/native bytes differ", freshRead)
	}
	freshTail := []byte("\x00fresh-child-once\xfe")
	newRequest := replicatedPGRequest(freshRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(freshTail)), Data: freshTail})
	newRequest.ClientIncarnation = 97
	newRequest.ModificationTime = cephmsgr.OSDTime{Seconds: 1700001110, Nanoseconds: 987654321}
	freshAppend, err = staleConn.Request(ctx, newRequest)
	if err != nil {
		t.Fatal("new explicit child mutation", err)
	}
	checkReplicatedPGReply(t, freshAppend, freshRoute)
	final = replicatedPGNative(t, ctx, name).Actual
	wantFinal := append(bytes.Clone(wantOld), freshTail...)
	if freshAppend.Flags&cephmsgr.OSDReplyOnDisk == 0 || final.Code != 0 || !bytes.Equal(final.Data, wantFinal) || bytes.Contains(final.Data, sentinel) || final.Version != freshAppend.UserVersion || final.Version <= executed.Version || final.Seconds != newRequest.ModificationTime.Seconds || final.Nanos != newRequest.ModificationTime.Nanoseconds || staleDials.Load() != 1 || faultDials.Load() != 1 {
		t.Fatal("fresh child native exact-once postcondition differs", final, freshAppend)
	}
	if !reflect.DeepEqual(oldUnknown.Submission.Placement, oldRequest.Placement) || !reflect.DeepEqual(staleUnknown.Submission.Placement, staleRequest.Placement) || oldUnknown.Submission.MapEpoch != oldRequest.MapEpoch || staleUnknown.Submission.MapEpoch != staleRequest.MapEpoch {
		t.Fatal("fresh request changed retained uncertain identity")
	}
	finish, err = splitControl(ctx, "finish", name)
	if err != nil {
		t.Fatal("native actual/target PG merge restoration", err)
	}
	restored = finish.Done && finish.Restored
	if !restored || finish.Token != start.Token || !reflect.DeepEqual(finish.PreRestore, finish.After.ObjectState) || !reflect.DeepEqual(finish.Before.FixtureState, finish.After.FixtureState) {
		t.Fatal("native split merge restoration or preserved state differs", finish)
	}
	restoredNative := replicatedPGNative(t, ctx, name)
	checkSplitState(t, finish.After, restoredNative.Placement, 8)
	if restoredNative.Placement.PG != oldRoute.PG || !reflect.DeepEqual(final, restoredNative.Actual) {
		t.Fatal("native merge changed last positively observed head", restoredNative)
	}
	t.Log(fmt.Sprintf("native child %s restored to %s; both uncertain submissions retain their original tuples", freshRoute.PGID, restoredNative.Placement.PGID))
}
