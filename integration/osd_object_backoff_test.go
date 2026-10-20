package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Native receipts of tools/osd_object_backoff_oracle.py. They are consumer-only
// observations; the product receives no placement, recovery or retry policy.
type objectBackoffPlacement struct {
	PGID    string  `json:"pgid"`
	RawPGID string  `json:"raw_pgid"`
	Acting  []int32 `json:"acting"`
	Up      []int32 `json:"up"`
	Primary int32   `json:"primary"`
	Epoch   uint32  `json:"epoch"`
}
type objectBackoffPG struct {
	PGID     string  `json:"pgid"`
	State    string  `json:"state"`
	Epoch    uint32  `json:"epoch"`
	Acting   []int32 `json:"acting"`
	Degraded *int64  `json:"degraded"`
	Missing  *int64  `json:"missing"`
	Unfound  *int64  `json:"unfound"`
}
type objectBackoffWait struct {
	Placement objectBackoffPlacement `json:"placement"`
	PG        objectBackoffPG        `json:"pg"`
	Topology  struct {
		Epoch uint32   `json:"epoch"`
		Flags []string `json:"flags"`
	} `json:"topology"`
}
type objectBackoffNativeLine struct {
	Time           string `json:"time"`
	PGID           string `json:"pgid"`
	Op             string `json:"op"`
	ID             uint64 `json:"id"`
	Epoch          uint32 `json:"epoch"`
	BeginHasObject bool   `json:"begin_has_object"`
	EndHasObject   bool   `json:"end_has_object"`
	SingleObject   bool   `json:"single_object"`
	TID            uint64 `json:"tid"`
	OSD            int32  `json:"osd"`
}
type objectBackoffNative struct {
	Label     string                    `json:"label"`
	Object    string                    `json:"object"`
	Started   float64                   `json:"started"`
	Finished  bool                      `json:"finished"`
	ExitCode  *int                      `json:"exit_code"`
	Completed *float64                  `json:"completed"`
	Backoffs  []objectBackoffNativeLine `json:"backoffs"`
	Sends     []objectBackoffNativeLine `json:"sends"`
	Held      []objectBackoffNativeLine `json:"held"`
	// Bounded native log/stderr tails for diagnosing an unparsed client log.
	Diagnostic json.RawMessage `json:"diagnostic"`
}
type objectBackoffOp struct {
	Description string   `json:"description"`
	FlagPoint   string   `json:"flag_point"`
	Client      string   `json:"client"`
	TID         uint64   `json:"tid"`
	Events      []string `json:"events"`
}
type objectBackoffAck struct {
	PGID         string `json:"pgid"`
	ID           uint64 `json:"id"`
	MapEpoch     uint32 `json:"map_epoch"`
	Op           uint8  `json:"op"`
	SingleObject bool   `json:"single_object"`
	Client       string `json:"client"`
	ReachedPG    bool   `json:"reached_pg"`
	Evidence     string `json:"evidence"`
}

// objectBackoffEvidence is the public receipt retained by the diagnostics
// collector. It holds no credentials.
type objectBackoffEvidence struct {
	Profile                string                            `json:"profile"`
	Objects                map[string]string                 `json:"objects"`
	Placements             map[string]objectBackoffPlacement `json:"placements"`
	SameSessionOtherSamePG bool                              `json:"same_session_other_same_pg"`
	SameSessionRoutes      map[string]osdFixtureOracle       `json:"same_session_routes"`
	Degraded               []objectBackoffWait               `json:"degraded"`
	Block, Unblock         cephmsgr.OSDBackoffEvent
	GoAck                  *objectBackoffAck              `json:"go_ack_receipt"`
	NativeAcks             []objectBackoffAck             `json:"native_ack_history"`
	Replies                map[string]cephmsgr.OSDReply   `json:"replies"`
	Errors                 map[string]string              `json:"errors"`
	Native                 map[string]objectBackoffNative `json:"native"`
	NativeStat             json.RawMessage                `json:"native_stat"`
	HeldOps                []objectBackoffOp              `json:"held_ops"`
	Snapshots              map[string]mutationSnapshot    `json:"snapshots"`
	FailoverRoute          objectBackoffWait              `json:"failover"`
	TimingsMS              map[string]float64             `json:"timings_ms"`
	Restored               bool                           `json:"restored"`
	Cleaned                bool                           `json:"cleaned"`
	CleanupFailures        []string                       `json:"cleanup_failures"`
}

func objectBackoffControl(ctx context.Context, action string, input, result any) error {
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	data, err := json.Marshal(input)
	if err != nil {
		return err
	}
	if err := testcluster.WritePublicEvidence(filepath.Join(dir, "osd-object-backoff-input.json"), data); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(dir, "osd-object-backoff-result.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-object-backoff", action); err != nil {
		return fmt.Errorf("native object backoff %s: %w", action, err)
	}
	file, err := os.Open(filepath.Join(dir, "osd-object-backoff-result.json"))
	if err != nil {
		return err
	}
	defer file.Close()
	data, err = io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return errors.Join(err, errors.New("native object backoff result exceeds bound"))
	}
	var envelope struct {
		Schema int             `json:"schema_version"`
		Source string          `json:"source_commit"`
		Action string          `json:"action"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	if envelope.Schema != 1 || envelope.Source != "7f793731f1b39eb4f465e960113d2363c311b964" || envelope.Action != action {
		return errors.New("native object backoff receipt provenance differs")
	}
	if result == nil {
		return nil
	}
	return json.Unmarshal(envelope.Result, result)
}

func objectBackoffMust(t *testing.T, ctx context.Context, action string, input, result any) {
	t.Helper()
	if err := objectBackoffControl(ctx, action, input, result); err != nil {
		t.Fatal(err)
	}
}

func objectBackoffDaemon(t *testing.T, ctx context.Context, action string, id int32) {
	t.Helper()
	if err := testcluster.ControlDaemon(ctx, os.Getenv("CEPH_MSGR_CONTROL_DIR"), action, "osd", strconv.Itoa(int(id))); err != nil {
		t.Fatalf("%s osd.%d: %v", action, id, err)
	}
}

// PrimaryLogPG::wait_for_degraded_object marks the op delayed with this
// reason (OpRequest::mark_delayed); the live dump reports it both as the
// current flag point and as the latest event.
func objectBackoffHeldDegraded(op objectBackoffOp, client string) bool {
	return op.Client == client && op.FlagPoint == "waiting for degraded object" && slices.Contains(op.Events, "waiting for degraded object")
}

// TestCephOSDObjectBackoffFailoverIntegration drives two native recipes on a
// disposable three-BlueStore replicated pool. First, a write to an object that
// is degraded on a returned replica (whose recovery push is held by freezing
// that replica) makes the primary send an object-scope BLOCK. Second, with the
// default no-backoff configuration, a write held server-side on a degraded
// object is in flight when its primary is SIGKILLed; the caller then resubmits
// explicitly to the replacement primary. Native rados clients run alongside.
func TestCephOSDObjectBackoffFailoverIntegration(t *testing.T) {
	if !isReplicatedPGFixtureProfile() || os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("requires an opt-in three-BlueStore replicated-PG object backoff fixture")
	}
	c, _, base, _ := replicatedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Minute)
	t.Cleanup(cancel)
	evidence := objectBackoffEvidence{Profile: os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE"), Objects: map[string]string{}, Placements: map[string]objectBackoffPlacement{},
		Replies: map[string]cephmsgr.OSDReply{}, Errors: map[string]string{}, Native: map[string]objectBackoffNative{}, Snapshots: map[string]mutationSnapshot{}, TimingsMS: map[string]float64{}}
	stopped, paused := map[int32]bool{}, map[int32]bool{}
	var connections []*cephmsgr.OSDConnection
	var pendingCancels []context.CancelFunc
	prepared, scoped := false, false
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 4*time.Minute)
		defer stop()
		dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
		// Publish the receipt even when a cleanup helper stops this goroutine.
		defer func() {
			data, err := json.Marshal(evidence)
			if err == nil {
				err = testcluster.WritePublicEvidence(filepath.Join(dir, "osd-object-backoff-results.json"), data)
			}
			if err != nil {
				t.Error(err)
			}
		}()
		fail := func(label string, err error) {
			if err != nil {
				evidence.CleanupFailures = append(evidence.CleanupFailures, label+": "+err.Error())
				t.Error(label, err)
			}
		}
		for _, cancelPending := range pendingCancels {
			cancelPending()
		}
		for _, o := range connections {
			_ = o.Close()
		}
		for id := range paused {
			if paused[id] {
				fail("resume osd."+strconv.Itoa(int(id)), testcluster.ControlDaemon(cleanup, dir, "resume", "osd", strconv.Itoa(int(id))))
			}
		}
		for id := range stopped {
			if stopped[id] {
				fail("start osd."+strconv.Itoa(int(id)), testcluster.ControlDaemon(cleanup, dir, "start", "osd", strconv.Itoa(int(id))))
			}
		}
		if scoped && !evidence.Restored {
			var restored struct {
				Restored bool `json:"restored"`
			}
			err := objectBackoffControl(cleanup, "restore", map[string]any{}, &restored)
			fail("restore native flags/configuration", err)
			evidence.Restored = err == nil && restored.Restored
		}
		if prepared && (!scoped || evidence.Restored) {
			waitOSDFixtureClean(t, cleanup, base)
			evidence.CleanupFailures = append(evidence.CleanupFailures, "owned-object cleanup started")
			evidence.Cleaned = replicatedPGOwned(t, cleanup, base, "cleanup", omapWriteInput{}).Cleanup.Removed
			evidence.CleanupFailures = evidence.CleanupFailures[:len(evidence.CleanupFailures)-1]
			if !evidence.Cleaned {
				t.Error("native object backoff owned-object cleanup incomplete")
			}
		}
	})

	owned := replicatedPGOwned(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte{0, 255, 'o', 'b', 'j', '-', 'b', 'o'}}, Reserve: 8})
	prepared = true
	if len(owned.Objects) != 9 {
		t.Fatal("object backoff ownership requires one seeded and eight reserved objects", owned.Objects)
	}
	var prep struct {
		Placements map[string]objectBackoffPlacement `json:"placements"`
	}
	scoped = true
	objectBackoffMust(t, ctx, "prepare", map[string]any{"objects": owned.Objects}, &prep)
	evidence.Placements = prep.Placements
	// Choose a primary with at least three owned names (pigeonhole over nine
	// names on three OSDs) as a fallback. Prefer two names in the same PG
	// so the unrelated-object check also exercises hobject range selection.
	// X is the degraded object, Y the other object, X2 the failover object.
	groups := map[int32][]string{}
	for _, name := range owned.Objects {
		p := prep.Placements[name]
		if len(p.Acting) != 3 || p.Acting[0] != p.Primary {
			t.Fatal("native placement is not a healthy three-replica acting set", name, p)
		}
		groups[p.Primary] = append(groups[p.Primary], name)
	}
	var primary int32 = -1
	for id, names := range groups {
		if primary < 0 || len(names) > len(groups[primary]) || len(names) == len(groups[primary]) && id < primary {
			primary = id
		}
	}
	group := groups[primary]
	var x, y string
	for _, name := range group {
		if name == owned.Objects[0] {
			y = name
		}
	}
	for _, name := range group {
		if name == y {
			continue
		}
		if x == "" {
			x = name
		} else if y == "" {
			y = name
		}
	}
	// Native placement is the only selection oracle. Keep the seeded object
	// as Y when it belongs to the first same-PG pair, avoiding an extra put.
samePGPair:
	for i, first := range owned.Objects {
		for _, second := range owned.Objects[i+1:] {
			a, b := prep.Placements[first], prep.Placements[second]
			if a.PGID != "" && a.PGID == b.PGID && a.Primary == b.Primary {
				x, y, primary = first, second, a.Primary
				if x == owned.Objects[0] {
					x, y = y, x
				}
				break samePGPair
			}
		}
	}
	var x2 string
	for _, name := range owned.Objects[1:] {
		if name != x && name != y {
			x2 = name
			break
		}
	}
	if x == "" || y == "" || x2 == "" {
		t.Fatal("owned object selection failed", groups)
	}
	evidence.Objects["degraded"], evidence.Objects["other"], evidence.Objects["failover"] = x, y, x2
	stale := prep.Placements[x].Acting[2]
	baseX := []byte{0, 255, 'd', 'e', 'g', 'r', 'a', 'd', 'e', 'd', 128, 10}
	putNative := func(name string, data []byte) {
		t.Helper()
		objectBackoffMust(t, ctx, "put", map[string]any{"object": name, "data": base64.StdEncoding.EncodeToString(data)}, nil)
	}
	if y != owned.Objects[0] {
		putNative(y, []byte("unrelated-object"))
	}

	// ---- Leaf 1: object-scope BLOCK while a returned replica is stale ----
	objectBackoffMust(t, ctx, "config", map[string]any{"enabled": true}, nil)
	makeDegraded := func(name string, replica int32, data []byte) objectBackoffWait {
		t.Helper()
		var w objectBackoffWait
		objectBackoffDaemon(t, ctx, "stop", replica)
		stopped[replica] = true
		objectBackoffMust(t, ctx, "wait-osd", map[string]any{"osd": replica, "up": false, "object": name}, &w)
		putNative(name, data)
		objectBackoffDaemon(t, ctx, "start", replica)
		stopped[replica] = false
		objectBackoffMust(t, ctx, "wait-osd", map[string]any{"osd": replica, "up": true, "object": name}, &w)
		objectBackoffMust(t, ctx, "wait-degraded", map[string]any{"object": name, "osd": replica}, &w)
		evidence.Degraded = append(evidence.Degraded, w)
		return w
	}
	degraded := makeDegraded(x, stale, baseX)
	if degraded.Placement.Primary != primary || degraded.PG.Degraded == nil || *degraded.PG.Degraded < 1 {
		t.Fatal("native degraded state lacks the selected primary or a degraded object", degraded)
	}
	nativeX := replicatedPGNative(t, ctx, x)
	route := nativeX.Placement
	evidence.Snapshots["degraded-before"] = nativeX.Actual
	if route.ID != primary || !bytes.Equal(nativeX.Actual.Data, baseX) {
		t.Fatal("native degraded object routing/bytes differ", route, nativeX.Actual)
	}
	nativeY := replicatedPGNative(t, ctx, y)
	routeY := nativeY.Placement
	evidence.Snapshots["same-session-other-before"] = nativeY.Actual
	evidence.SameSessionRoutes = map[string]osdFixtureOracle{"blocked": route, "other": routeY}
	evidence.SameSessionOtherSamePG = route.Pool == routeY.Pool && route.PG == routeY.PG
	if routeY.ID != primary || nativeY.Actual.Code != 0 || len(nativeY.Actual.Data) == 0 {
		t.Fatal("unrelated object lacks native bytes on the selected primary", routeY, nativeY.Actual)
	}
	o := replicatedPGConnection(t, c, ctx, route)
	connections = append(connections, o)
	// The product keeps one connection per OSD ID within a Client (OpenOSD
	// returns the held connection), so an independent daemon session needs a
	// second authenticated Client.
	if same := replicatedPGConnection(t, c, ctx, routeY); same != o {
		t.Fatal("OpenOSD to the same OSD returned a distinct connection; update the session model", same, o)
	}
	secondOptions := integrationOptions(t)
	secondOptions.EnableOSD = true
	secondOptions.OSDProfile = replicatedPGFixtureOSDProfile(t)
	c2, err := cephmsgr.Dial(ctx, secondOptions)
	if err != nil {
		t.Fatal("second independent client", err)
	}
	t.Cleanup(func() { c2.Close() })
	o2 := replicatedPGConnection(t, c2, ctx, route)
	connections = append(connections, o2)

	objectBackoffDaemon(t, ctx, "pause", stale)
	paused[stale] = true
	tailGo := []byte{0, 255, 'g', 'o', '-', 'o', 'n', 'c', 'e'}
	original := replicatedPGRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(tailGo)), Data: tailGo})
	original.ClientIncarnation = 81
	original.ModificationTime = cephmsgr.OSDTime{Seconds: 1700002000, Nanoseconds: 111}
	type completion struct {
		reply cephmsgr.OSDReply
		err   error
	}
	pendingCtx, pendingCancel := context.WithCancel(ctx)
	pendingCancels = append(pendingCancels, pendingCancel)
	done := make(chan completion, 1)
	sent := time.Now()
	go func() { r, e := o.Request(pendingCtx, original); done <- completion{r, e} }()
	eventCtx, stopEvent := context.WithTimeout(ctx, 30*time.Second)
	block, err := o.NextBackoff(eventCtx)
	stopEvent()
	blocked := time.Now()
	if err != nil {
		select {
		case r := <-done:
			t.Fatal("no native object BLOCK; the write completed instead", r.reply, r.err, err)
		default:
			t.Fatal("no native object BLOCK", err)
		}
	}
	evidence.Block = block
	evidence.TimingsMS["go_send_to_block"] = float64(blocked.Sub(sent).Microseconds()) / 1000
	if block.Op != cephmsgr.OSDBackoffBlock || block.Pool != uint64(route.Pool) || block.Seed != route.PG || block.Shard != -1 || block.ID == 0 ||
		block.MapEpoch < route.Epoch || block.Source.ID != primary || block.GlobalID != c.Snapshot().GlobalID || block.SessionGeneration == 0 || len(block.RawFront) == 0 {
		t.Fatal("native object BLOCK PG/epoch/session identity differs", block, route)
	}
	if block.Begin.Object != x || block.End.Object != x || !bytes.Equal(block.Begin.Raw, block.End.Raw) || block.Begin.Hash != route.Hash ||
		block.Begin.Pool != route.Pool || block.Begin.Namespace != "" || block.Begin.Max || block.Begin.Snapshot != uint64(1<<64-2) {
		t.Fatal("native BLOCK is not the single head object [X,X]", block.Begin, block.End)
	}
	if err := o.AckBackoff(ctx, block); err != nil {
		t.Fatal("explicit original-epoch ACK of object BLOCK", err)
	}
	// The acknowledged single-object BLOCK remains active while a different
	// object on this exact authenticated connection completes STAT and READ.
	statRequest := replicatedPGRequest(routeY, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat})
	statRequest.ClientIncarnation = 82
	statStarted := time.Now()
	statY, err := o.Request(ctx, statRequest)
	evidence.Replies["same-session-other-object-stat"] = statY
	evidence.Errors["same-session-other-object-stat"] = fmt.Sprint(err)
	evidence.TimingsMS["same-session-other-stat"] = float64(time.Since(statStarted).Microseconds()) / 1000
	if err != nil {
		t.Fatal("same session STAT of an unrelated object during object BLOCK", err)
	}
	checkReplicatedPGReply(t, statY, routeY)
	if len(statY.Operations) != 1 || statY.Operations[0].Code != cephmsgr.OSDStat || len(statY.Operations[0].Data) != 16 ||
		binary.LittleEndian.Uint64(statY.Operations[0].Data) != uint64(len(nativeY.Actual.Data)) ||
		binary.LittleEndian.Uint32(statY.Operations[0].Data[8:]) != nativeY.Actual.Seconds ||
		binary.LittleEndian.Uint32(statY.Operations[0].Data[12:]) != nativeY.Actual.Nanos || statY.UserVersion != nativeY.Actual.Version {
		t.Fatal("same session STAT differs from native unrelated-object size/mtime/version", statY, nativeY.Actual)
	}
	readRequest := replicatedPGRequest(routeY, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(nativeY.Actual.Data))})
	readRequest.ClientIncarnation = 84
	readStarted := time.Now()
	readY, err := o.Request(ctx, readRequest)
	evidence.Replies["same-session-other-object-read"] = readY
	evidence.Errors["same-session-other-object-read"] = fmt.Sprint(err)
	evidence.TimingsMS["same-session-other-read"] = float64(time.Since(readStarted).Microseconds()) / 1000
	if err != nil {
		t.Fatal("same session READ of an unrelated object during object BLOCK", err)
	}
	checkReplicatedPGReply(t, readY, routeY)
	if len(readY.Operations) != 1 || readY.Operations[0].Code != cephmsgr.OSDRead || !bytes.Equal(readY.Operations[0].Data, nativeY.Actual.Data) || readY.UserVersion != nativeY.Actual.Version ||
		statY.GlobalID != block.GlobalID || readY.GlobalID != block.GlobalID || readY.Transaction <= statY.Transaction {
		t.Fatal("same session unrelated-object READ lost native bytes/version or authenticated request identity", readY, statY, nativeY.Actual)
	}
	// This new matching request is known unsent. Session.Call still allocates
	// a submission TID; no wire reply TID or raw reply exists for the refusal.
	blockedStat := replicatedPGRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat})
	blockedStat.ClientIncarnation = 85
	reply, err := o.Request(ctx, blockedStat)
	evidence.Replies["same-session-blocked-object-stat"] = reply
	evidence.Errors["same-session-blocked-object-stat"] = fmt.Sprint(err)
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.Is(err, cephmsgr.ErrOSDBackoff) || errors.As(err, &unknown) || reply.Submission.Transaction <= readY.Transaction ||
		reply.Submission.GlobalID != block.GlobalID || reply.Transaction != 0 || len(reply.RawFront) != 0 ||
		reply.Submission.ClientIncarnation != blockedStat.ClientIncarnation || reply.Submission.MapEpoch != blockedStat.MapEpoch ||
		!reflect.DeepEqual(reply.Submission.Placement, blockedStat.Placement) {
		t.Fatal("matching object on the blocked session was not a known local unsent refusal", reply, err)
	}
	select {
	case completed := <-done:
		t.Fatal("original blocked mutation completed while the stale replica was paused", completed.reply, completed.err)
	default:
	}
	afterY := replicatedPGNative(t, ctx, y)
	evidence.Snapshots["same-session-other-after"] = afterY.Actual
	if !reflect.DeepEqual(afterY.Actual, nativeY.Actual) {
		t.Fatal("same session unrelated-object reads changed native bytes/version/mtime", nativeY.Actual, afterY.Actual)
	}
	// Another session to the same primary has no daemon backoff: an unrelated
	// stat and a read of the degraded (primary-readable) object proceed.
	otherStat, err := o2.Request(ctx, replicatedPGRequest(routeY, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}))
	evidence.Replies["other-session-other-object-stat"] = otherStat
	if err != nil {
		t.Fatal("other session stat of an unrelated object during object BLOCK", err)
	}
	checkReplicatedPGReply(t, otherStat, routeY)
	readX, err := o2.Request(ctx, replicatedPGRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(baseX))}))
	evidence.Replies["other-session-degraded-read"] = readX
	if err != nil || len(readX.Operations) != 1 || !bytes.Equal(readX.Operations[0].Data, baseX) {
		t.Fatal("other session read of the degraded object during BLOCK", readX, err)
	}
	// Native exact-source ACK reception at the PG boundary.
	for deadline := time.Now().Add(15 * time.Second); ; {
		var history struct {
			Acks []objectBackoffAck `json:"acks"`
		}
		objectBackoffMust(t, ctx, "ack-history", map[string]any{"osd": primary, "object": x, "pgid": route.PGID}, &history)
		for i := range history.Acks {
			a := history.Acks[i]
			if a.Client == "client."+strconv.FormatUint(block.GlobalID, 10) && a.ID == block.ID && a.MapEpoch == block.MapEpoch && a.PGID == route.PGID && a.SingleObject && a.ReachedPG {
				evidence.GoAck = &a
			}
		}
		if evidence.GoAck != nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Second)
	}
	if evidence.GoAck == nil {
		t.Fatal("native history lacks the Go original-epoch ACK_BLOCK for the object")
	}
	// Native librados client on the same object, and an unrelated native stat.
	tailNative := []byte("native-once")
	objectBackoffMust(t, ctx, "native-start", map[string]any{"object": x, "label": "block", "data": base64.StdEncoding.EncodeToString(tailNative)}, nil)
	var nativeBlocked objectBackoffNative
	for deadline := time.Now().Add(20 * time.Second); ; {
		var r struct {
			Native objectBackoffNative `json:"native"`
		}
		objectBackoffMust(t, ctx, "native-finish", map[string]any{"wait_seconds": 1}, &r)
		nativeBlocked = r.Native
		if r.Native.Finished {
			t.Fatal("native append completed while the stale replica was frozen", r.Native)
		}
		if len(r.Native.Backoffs) != 0 || time.Now().After(deadline) {
			break
		}
	}
	if len(nativeBlocked.Backoffs) == 0 || nativeBlocked.Backoffs[0].Op != "block" || !nativeBlocked.Backoffs[0].SingleObject || !nativeBlocked.Backoffs[0].BeginHasObject || nativeBlocked.Backoffs[0].PGID != route.PGID {
		t.Fatal("native Objecter did not receive the same single-object BLOCK", nativeBlocked, string(nativeBlocked.Diagnostic))
	}
	objectBackoffMust(t, ctx, "native-stat", map[string]any{"object": y}, &evidence.NativeStat)
	var nativeStat struct {
		ExitCode int `json:"exit_code"`
	}
	if err := json.Unmarshal(evidence.NativeStat, &nativeStat); err != nil || nativeStat.ExitCode != 0 {
		t.Fatal("native stat of an unrelated object during BLOCK", string(evidence.NativeStat), err)
	}
	if hold := 6*time.Second - time.Since(blocked); hold > 0 {
		time.Sleep(hold)
	}
	select {
	case r := <-done:
		t.Fatal("blocked Go write completed before recovery", r.reply, r.err)
	default:
	}
	resumed := time.Now()
	objectBackoffDaemon(t, ctx, "resume", stale)
	paused[stale] = false
	evidence.TimingsMS["block_hold"] = float64(resumed.Sub(blocked).Microseconds()) / 1000
	releaseCtx, stopRelease := context.WithTimeout(ctx, 30*time.Second)
	unblock, err := o.NextBackoff(releaseCtx)
	stopRelease()
	unblocked := time.Now()
	if err != nil {
		t.Fatal("native object UNBLOCK after recovery", err)
	}
	evidence.Unblock = unblock
	evidence.TimingsMS["go_resume_to_unblock"] = float64(unblocked.Sub(resumed).Microseconds()) / 1000
	if unblock.Op != cephmsgr.OSDBackoffUnblock || !unblock.Matched || unblock.ID != block.ID || unblock.Pool != block.Pool || unblock.Seed != block.Seed || unblock.Shard != block.Shard ||
		unblock.MapEpoch < block.MapEpoch || unblock.GlobalID != block.GlobalID || unblock.SessionGeneration != block.SessionGeneration ||
		!bytes.Equal(unblock.Begin.Raw, block.Begin.Raw) || !bytes.Equal(unblock.End.Raw, block.End.Raw) {
		t.Fatal("native object UNBLOCK lost matched control identity", unblock, block)
	}
	var nativeDone struct {
		Native objectBackoffNative `json:"native"`
	}
	objectBackoffMust(t, ctx, "native-finish", map[string]any{"wait_seconds": 30}, &nativeDone)
	evidence.Native["block"] = nativeDone.Native
	n := nativeDone.Native
	if !n.Finished || n.ExitCode == nil || *n.ExitCode != 0 || n.Completed == nil {
		t.Fatal("native append did not complete after UNBLOCK", n)
	}
	evidence.TimingsMS["native_start_to_complete"] = (*n.Completed - n.Started) * 1000
	evidence.TimingsMS["native_resume_to_complete"] = (*n.Completed - float64(resumed.UnixNano())/1e9) * 1000
	var nativeUnblock bool
	for _, line := range n.Backoffs {
		nativeUnblock = nativeUnblock || line.Op == "unblock" && line.SingleObject && line.BeginHasObject && line.PGID == route.PGID
	}
	sends := map[uint64]int{}
	for _, line := range n.Sends {
		if line.PGID == route.PGID {
			sends[line.TID]++
		}
	}
	resent := false
	for _, count := range sends {
		resent = resent || count >= 2
	}
	if !nativeUnblock || !resent {
		t.Fatal("native Objecter did not observe UNBLOCK and resend its held append", n)
	}
	// The daemon dropped the blocked Go write (add_backoff returns without
	// queueing it). The product does not replay it: it stays without reply.
	select {
	case r := <-done:
		t.Fatal("dropped Go write unexpectedly received a reply", r.reply, r.err)
	case <-time.After(3 * time.Second):
	}
	pendingCancel()
	first := <-done
	evidence.Replies["blocked-original"] = first.reply
	evidence.Errors["blocked-original"] = fmt.Sprint(first.err)
	if !errors.As(first.err, &unknown) || !errors.Is(first.err, context.Canceled) || first.reply.Submission.Transaction == 0 || first.reply.Submission.ClientIncarnation != 81 || first.reply.Transaction != 0 {
		t.Fatal("sent and blocked Go write lost its unknown outcome", first.reply, first.err)
	}
	afterNative := replicatedPGNative(t, ctx, x)
	evidence.Snapshots["degraded-after-native"] = afterNative.Actual
	wantNative := append(bytes.Clone(baseX), tailNative...)
	// The rados CLI append loop ends with an empty APPEND (observed as a
	// separate tid), so the native user version may advance by more than one;
	// the exact byte image establishes a single data append.
	if !bytes.Equal(afterNative.Actual.Data, wantNative) || afterNative.Actual.Version <= nativeX.Actual.Version {
		t.Fatal("native append not exactly once, or the dropped Go write executed", afterNative.Actual)
	}
	// Caller-selected first resubmission after UNBLOCK, same session.
	fresh := replicatedPGRequest(afterNative.Placement, original.Operations...)
	fresh.ClientIncarnation = 83
	fresh.ModificationTime = original.ModificationTime
	resubmitted := time.Now()
	freshReply, err := o.Request(ctx, fresh)
	evidence.Replies["resubmitted"] = freshReply
	if err != nil {
		t.Fatal("caller resubmission after object UNBLOCK", err)
	}
	evidence.TimingsMS["go_resubmit_rtt"] = float64(time.Since(resubmitted).Microseconds()) / 1000
	checkReplicatedPGReply(t, freshReply, afterNative.Placement)
	final := replicatedPGNative(t, ctx, x)
	evidence.Snapshots["degraded-final"] = final.Actual
	if !bytes.Equal(final.Actual.Data, append(wantNative, tailGo...)) || final.Actual.Version != afterNative.Actual.Version+1 || freshReply.UserVersion != final.Actual.Version ||
		final.Actual.Seconds != fresh.ModificationTime.Seconds || final.Actual.Nanos != fresh.ModificationTime.Nanoseconds || freshReply.Transaction <= first.reply.Submission.Transaction {
		t.Fatal("explicit Go resubmission did not execute exactly once", freshReply, final.Actual)
	}
	if err := o.AckBackoff(ctx, block); !errors.Is(err, cephmsgr.ErrOSDBackoffIdentity) {
		t.Fatal("released object control retained ACK authority", err)
	}
	t.Logf("object BLOCK pg=%s id=%d epochs=%d/%d object=%s; Go BLOCK %.1fms after send, UNBLOCK %.1fms after resume; native completed %.1fms after resume with resend",
		route.PGID, block.ID, block.MapEpoch, unblock.MapEpoch, x, evidence.TimingsMS["go_send_to_block"], evidence.TimingsMS["go_resume_to_unblock"], evidence.TimingsMS["native_resume_to_complete"])

	// ---- Leaf 2: primary SIGKILL while a Go write is held in flight ----
	objectBackoffMust(t, ctx, "config", map[string]any{"enabled": false}, nil)
	p2 := prep.Placements[x2]
	primary2, stale2 := p2.Primary, p2.Acting[2]
	baseX2 := []byte{0, 255, 'f', 'a', 'i', 'l', 'o', 'v', 'e', 'r'}
	makeDegraded(x2, stale2, baseX2)
	native2 := replicatedPGNative(t, ctx, x2)
	route2 := native2.Placement
	evidence.Snapshots["failover-before"] = native2.Actual
	if route2.ID != primary2 || !bytes.Equal(native2.Actual.Data, baseX2) {
		t.Fatal("native failover object routing/bytes differ", route2, native2.Actual)
	}
	o3 := replicatedPGConnection(t, c, ctx, route2)
	connections = append(connections, o3)
	objectBackoffDaemon(t, ctx, "pause", stale2)
	paused[stale2] = true
	tailGo2 := []byte("go-failover-once")
	original2 := replicatedPGRequest(route2, cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(tailGo2)), Data: tailGo2})
	original2.ClientIncarnation = 91
	original2.ModificationTime = cephmsgr.OSDTime{Seconds: 1700002100, Nanoseconds: 222}
	pending2Ctx, pending2Cancel := context.WithTimeout(ctx, 90*time.Second)
	pendingCancels = append(pendingCancels, pending2Cancel)
	done2 := make(chan completion, 1)
	go func() { r, e := o3.Request(pending2Ctx, original2); done2 <- completion{r, e} }()
	goClient := "client." + strconv.FormatUint(c.Snapshot().GlobalID, 10)
	for deadline := time.Now().Add(20 * time.Second); ; {
		var r struct {
			Ops []objectBackoffOp `json:"ops"`
		}
		objectBackoffMust(t, ctx, "ops", map[string]any{"osd": primary2, "object": x2}, &r)
		evidence.HeldOps = r.Ops
		found := false
		for _, op := range r.Ops {
			found = found || objectBackoffHeldDegraded(op, goClient)
		}
		if found || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	held := false
	for _, op := range evidence.HeldOps {
		held = held || objectBackoffHeldDegraded(op, goClient)
	}
	if !held {
		t.Fatal("native in-flight ops lack the Go write waiting for the degraded object", evidence.HeldOps)
	}
	quiet, stopQuiet := context.WithTimeout(ctx, 2*time.Second)
	_, err = o3.NextBackoff(quiet)
	stopQuiet()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("default configuration unexpectedly delivered a backoff control", err)
	}
	tailNative2 := []byte("native-failover-once")
	objectBackoffMust(t, ctx, "native-start", map[string]any{"object": x2, "label": "failover", "data": base64.StdEncoding.EncodeToString(tailNative2)}, nil)
	for deadline := time.Now().Add(20 * time.Second); ; {
		var r struct {
			Ops []objectBackoffOp `json:"ops"`
		}
		objectBackoffMust(t, ctx, "ops", map[string]any{"osd": primary2, "object": x2}, &r)
		if len(r.Ops) >= 2 {
			evidence.HeldOps = r.Ops
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("native append is not held in flight beside the Go write", r.Ops)
		}
		time.Sleep(500 * time.Millisecond)
	}
	killed := time.Now()
	objectBackoffDaemon(t, ctx, "kill", primary2)
	stopped[primary2] = true
	objectBackoffDaemon(t, ctx, "resume", stale2)
	paused[stale2] = false
	var second completion
	select {
	case second = <-done2:
	case <-time.After(60 * time.Second):
		t.Fatal("in-flight Go write was not released by the primary's death")
	}
	evidence.TimingsMS["go_kill_to_outcome"] = float64(time.Since(killed).Microseconds()) / 1000
	evidence.Replies["failover-original"] = second.reply
	evidence.Errors["failover-original"] = fmt.Sprint(second.err)
	if !errors.As(second.err, &unknown) || second.reply.Submission.Transaction == 0 || second.reply.Submission.ClientIncarnation != 91 || second.reply.Submission.MapEpoch != route2.Epoch ||
		!reflect.DeepEqual(second.reply.Submission.Placement, original2.Placement) || second.reply.Transaction != 0 {
		t.Fatal("in-flight write lost its unknown outcome or original submission tuple", second.reply, second.err)
	}
	afterKill, err := o3.Request(ctx, replicatedPGRequest(route2, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}))
	evidence.Replies["failed-session-request"] = afterKill
	evidence.Errors["failed-session-request"] = fmt.Sprint(err)
	if err == nil {
		t.Fatal("request on the failed primary session unexpectedly succeeded", afterKill)
	}
	objectBackoffMust(t, ctx, "wait-osd", map[string]any{"osd": primary2, "up": false, "object": x2}, &evidence.FailoverRoute)
	newPrimary := evidence.FailoverRoute.Placement.Primary
	if newPrimary == primary2 || newPrimary < 0 || evidence.FailoverRoute.Topology.Epoch <= route2.Epoch {
		t.Fatal("native map did not select a replacement primary on a newer epoch", evidence.FailoverRoute)
	}
	var nativeFailover struct {
		Native objectBackoffNative `json:"native"`
	}
	objectBackoffMust(t, ctx, "native-finish", map[string]any{"wait_seconds": 45}, &nativeFailover)
	evidence.Native["failover"] = nativeFailover.Native
	nf := nativeFailover.Native
	if !nf.Finished || nf.ExitCode == nil || *nf.ExitCode != 0 || nf.Completed == nil {
		t.Fatal("native append did not complete through failover", nf)
	}
	evidence.TimingsMS["native_kill_to_complete"] = (*nf.Completed - float64(killed.UnixNano())/1e9) * 1000
	targets := map[int32]bool{}
	for _, line := range nf.Sends {
		targets[line.OSD] = true
	}
	if !targets[primary2] || !targets[newPrimary] {
		t.Fatal("native Objecter did not resend from the dead primary to its replacement", nf.Sends)
	}
	afterFailover := replicatedPGNative(t, ctx, x2)
	evidence.Snapshots["failover-after-native"] = afterFailover.Actual
	wantNative2 := append(bytes.Clone(baseX2), tailNative2...)
	if afterFailover.Placement.ID != newPrimary || !bytes.Equal(afterFailover.Actual.Data, wantNative2) || afterFailover.Actual.Version <= native2.Actual.Version {
		t.Fatal("native failover state: Go write executed, native append not exactly once, or routing differs", afterFailover)
	}
	route3 := afterFailover.Placement
	o4 := replicatedPGConnection(t, c, ctx, route3)
	connections = append(connections, o4)
	retry := replicatedPGRequest(route3, original2.Operations...)
	retry.ClientIncarnation = 93
	retry.ModificationTime = original2.ModificationTime
	retryReply, err := o4.Request(ctx, retry)
	evidence.Replies["failover-resubmitted"] = retryReply
	if err != nil {
		t.Fatal("explicit resubmission to the replacement primary", err)
	}
	checkReplicatedPGReply(t, retryReply, route3)
	finalFailover := replicatedPGNative(t, ctx, x2)
	evidence.Snapshots["failover-final"] = finalFailover.Actual
	if !bytes.Equal(finalFailover.Actual.Data, append(wantNative2, tailGo2...)) || finalFailover.Actual.Version != afterFailover.Actual.Version+1 || retryReply.UserVersion != finalFailover.Actual.Version ||
		retryReply.MapEpoch < route3.Epoch || route3.Epoch <= route2.Epoch {
		t.Fatal("explicit failover resubmission did not execute exactly once on the new epoch", retryReply, finalFailover.Actual)
	}
	objectBackoffDaemon(t, ctx, "start", primary2)
	stopped[primary2] = false
	var restored struct {
		Restored bool `json:"restored"`
	}
	objectBackoffMust(t, ctx, "restore", map[string]any{}, &restored)
	evidence.Restored = restored.Restored
	if !restored.Restored {
		t.Fatal("native flags/configuration were not restored")
	}
	for label, name := range map[string]string{"degraded-restored": x, "failover-restored": x2} {
		snap := replicatedPGNative(t, ctx, name).Actual
		evidence.Snapshots[label] = snap
		want := evidence.Snapshots["degraded-final"]
		if name == x2 {
			want = finalFailover.Actual
		}
		if !reflect.DeepEqual(snap, want) {
			t.Fatal("returned primary or recovery changed bytes/version (replay or resurrection)", label, snap, want)
		}
	}
	t.Logf("failover: osd.%d killed with Go write held (unknown outcome after %.1fms), new primary osd.%d epoch %d; native resent and completed %.1fms after kill; Go resubmission executed once",
		primary2, evidence.TimingsMS["go_kill_to_outcome"], newPrimary, route3.Epoch, evidence.TimingsMS["native_kill_to_complete"])
}
