package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// The profile must be selected before MON admission because HASHPSPOOL is a
// cluster-wide required bit. The older fixture helper deliberately remains
// unchanged for its v6 profile and cannot be used by this acceptance test.
func replicatedPGFixture(t *testing.T) (*cephmsgr.Client, context.Context, osdFixtureOracle, []byte) {
	t.Helper()
	if os.Getenv("CEPH_MSGR_TEST_OSD") != "1" || os.Getenv("CEPH_MSGR_CONTROL_DIR") == "" || os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "replicated-pg-v8" {
		t.Skip("requires opt-in replicated-pg-v8 OSD fixture")
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	metadata, err := os.ReadFile(filepath.Join(dir, "osd-oracle.json"))
	if err != nil {
		t.Fatal(err)
	}
	var route osdFixtureOracle
	if err := json.Unmarshal(metadata, &route); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "osd-native-read.bin"))
	if err != nil || uint64(len(data)) != route.Size {
		t.Fatal("native replicated-PG baseline", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	if route.Replicas == 3 {
		waitOSDFixtureClean(t, ctx, route)
		fresh := replicatedPGNative(t, ctx, route.Name).Placement
		route = adoptMutationPlacement(t, route, fresh)
		publishReplicaOSDRouting(t, dir, route)
	}
	options := integrationOptions(t)
	options.EnableOSD = true
	options.OSDProfile = cephmsgr.OSDProfileReplicatedPG
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("replicated-PG MON admission", err)
	}
	t.Cleanup(func() { c.Close() })
	if c.Snapshot().OSDTicket.Expires.IsZero() {
		t.Fatal("replicated-PG OSD service ticket missing")
	}
	return c, ctx, route, data
}

// Native observations and mutation construction here are consumer-only helpers.
// No placement, PG folding or retry policy is delegated to the product.
func replicatedPGControl(t *testing.T, ctx context.Context, kind, action, inputName, resultName string, input any, result any) {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	data, err := json.Marshal(input)
	if err != nil || len(data) > 1<<20 {
		t.Fatal("bounded replicated-PG native input", err)
	}
	if err := testcluster.WritePublicEvidence(filepath.Join(dir, inputName), data); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, resultName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := testcluster.ControlDaemon(ctx, dir, "verify", kind, action); err != nil {
		t.Fatal("replicated-PG native oracle", err)
	}
	file, err := os.Open(filepath.Join(dir, resultName))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	data, err = io.ReadAll(io.LimitReader(file, (512<<10)+1))
	if err != nil || len(data) > 512<<10 {
		t.Fatal("bounded replicated-PG native result", err)
	}
	if err := json.Unmarshal(data, result); err != nil {
		t.Fatal(err)
	}
}
func replicatedPGNative(t *testing.T, ctx context.Context, name string) mutationOracle {
	t.Helper()
	var result mutationOracle
	replicatedPGControl(t, ctx, "osd-mutation", "compare", "osd-mutation-input.json", "osd-mutation-result.json", map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}, &result)
	r := result.Placement
	if r.Name != name || r.Namespace != "" || r.ID < 0 || r.Address == "" || r.Epoch == 0 || r.Pool < 0 || r.RawPGID != fmt.Sprintf("%d.%x", r.Pool, r.Hash) || r.PGID != fmt.Sprintf("%d.%x", r.Pool, r.PG) || len(r.Acting) == 0 || r.Acting[0] != r.ID {
		t.Fatal("native raw hash/actual PG/primary identity inconsistent", r)
	}
	return result
}
func replicatedPGOwned(t *testing.T, ctx context.Context, base osdFixtureOracle, mode string, input omapWriteInput) omapWriteReceipt {
	t.Helper()
	var result omapWriteReceipt
	replicatedPGControl(t, ctx, "osd-omap-write", mode, "osd-omap-write-input.json", "osd-omap-write-result.json", input, &result)
	const source = "7f793731f1b39eb4f465e960113d2363c311b964"
	if result.Schema != 1 || result.Commit != source || result.Mode != mode || result.Fixture.ID != base.Pool || result.Fixture.Object != base.Name || !strings.Contains(result.DaemonVersion, source) || !strings.Contains(result.DaemonVersion, "20.2.4") || !result.Scope.Independent || result.Scope.RawWire {
		t.Fatal("replicated-PG native ownership provenance differs", result)
	}
	return result
}
func replicatedPGRequest(route osdFixtureOracle, ops ...cephmsgr.OSDOperation) cephmsgr.OSDRequest {
	r := osdObjectRequest(route, ops...)
	r.Placement = &cephmsgr.OSDReplicatedPlacement{PG: cephmsgr.OSDPG{Pool: uint64(route.Pool), Seed: route.PG}, Primary: route.ID}
	return r
}
func replicatedPGConnection(t *testing.T, c *cephmsgr.Client, ctx context.Context, route osdFixtureOracle) *cephmsgr.OSDConnection {
	t.Helper()
	o, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: route.ID, Address: route.Address})
	if err != nil {
		t.Fatal("explicit replicated-PG connection", err)
	}
	return o
}
func checkReplicatedPGReply(t *testing.T, reply cephmsgr.OSDReply, route osdFixtureOracle) {
	t.Helper()
	want := replicatedPGRequest(route).Placement
	if reply.Code != 0 || reply.Transaction == 0 || reply.GlobalID == 0 || reply.Object != route.Name || reply.PG != want.PG || reply.MapEpoch < route.Epoch || reply.RetryAttempt != 0 || reply.Submission.Transaction != reply.Transaction || reply.Submission.GlobalID != reply.GlobalID || reply.Submission.MapEpoch != route.Epoch || reply.Submission.Attempt != 0 || !reflect.DeepEqual(reply.Submission.Placement, want) {
		t.Fatal("replicated-PG reply lost actual PG/identity/epoch", reply)
	}
}

type replicatedPGCase struct {
	Name       string             `json:"name"`
	Route      osdFixtureOracle   `json:"native_placement"`
	Before     mutationSnapshot   `json:"native_before"`
	After      mutationSnapshot   `json:"native_after"`
	Reply      cephmsgr.OSDReply  `json:"go_reply"`
	Unknown    bool               `json:"unknown_outcome"`
	FreshReply *cephmsgr.OSDReply `json:"fresh_go_reply,omitempty"`
}

func TestCephOSDReplicatedPGIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "replicated-pg-v8" {
		t.Skip("requires explicit replicated-pg-v8 fixture profile")
	}
	c, _, base, original := replicatedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	t.Cleanup(cancel)
	var evidence []replicatedPGCase
	prepared := false
	cleanupOK := false
	t.Cleanup(func() {
		// Preserve partial case receipts even if native cleanup reports Fatal.
		defer func() {
			data, err := json.Marshal(struct {
				Profile string             `json:"profile"`
				Base    osdFixtureOracle   `json:"fixture"`
				Cases   []replicatedPGCase `json:"cases"`
				Cleanup bool               `json:"owned_cleanup"`
			}{"replicated-pg-v8", base, evidence, cleanupOK})
			if err == nil {
				err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-replicated-pg-results.json"), data)
			}
			if err != nil {
				t.Error(err)
			}
		}()
		cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
		defer stop()
		if prepared {
			cleanupOK = replicatedPGOwned(t, cleanup, base, "cleanup", omapWriteInput{}).Cleanup.Removed
			if !cleanupOK {
				t.Error("replicated-PG owned cleanup incomplete")
			}
		}
	})
	t.Run("native fixture stat read preserves state", func(t *testing.T) {
		before := replicatedPGNative(t, ctx, base.Name)
		route := before.Placement
		if before.Actual.Code != 0 || !bytes.Equal(before.Actual.Data, original) || route.Hash == route.PG {
			t.Fatal("fixture lacks distinct raw hash/native bytes", before)
		}
		o := replicatedPGConnection(t, c, ctx, route)
		defer o.Close()
		reply, err := o.Request(ctx, replicatedPGRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(original) + 32)}))
		if err != nil {
			t.Fatal(err)
		}
		checkReplicatedPGReply(t, reply, route)
		if len(reply.Operations) != 2 || len(reply.Operations[0].Data) != 16 || binary.LittleEndian.Uint64(reply.Operations[0].Data) != uint64(len(original)) || binary.LittleEndian.Uint32(reply.Operations[0].Data[8:]) != before.Actual.Seconds || binary.LittleEndian.Uint32(reply.Operations[0].Data[12:]) != before.Actual.Nanos || !bytes.Equal(reply.Operations[1].Data, original) || reply.UserVersion != before.Actual.Version {
			t.Fatal("native stat/read bytes, mtime or version differ", reply, before)
		}
		after := replicatedPGNative(t, ctx, base.Name)
		if !reflect.DeepEqual(before.Actual, after.Actual) {
			t.Fatal("read-only replicated-PG request altered native state")
		}
		evidence = append(evidence, replicatedPGCase{Name: "fixture stat/read", Route: route, Before: before.Actual, After: after.Actual, Reply: reply})
	})
	owned := replicatedPGOwned(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("native owned baseline")}, Reserve: 8})
	prepared = true
	var selected mutationOracle
	for _, name := range owned.Objects {
		candidate := replicatedPGNative(t, ctx, name)
		if candidate.Placement.PG != 0 && candidate.Placement.Hash != candidate.Placement.PG {
			selected = candidate
			break
		}
	}
	if selected.Placement.Name == "" {
		t.Fatal("bounded owned candidates did not provide a nonzero actual PG distinct from raw hash")
	}
	route := selected.Placement
	name := route.Name
	current := []byte{0, 255, 128, 10, 'v', '8', 0, 'P', 'G'}
	t.Run("nonzero actual PG owned write and native readback", func(t *testing.T) {
		before := replicatedPGNative(t, ctx, name)
		o := replicatedPGConnection(t, c, ctx, route)
		defer o.Close()
		request := replicatedPGRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(current)), Data: current})
		request.ClientIncarnation = 61
		request.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000900, Nanoseconds: 123456789}
		reply, err := o.Request(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		checkReplicatedPGReply(t, reply, route)
		after := replicatedPGNative(t, ctx, name)
		if reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || after.Actual.Code != 0 || !bytes.Equal(after.Actual.Data, current) || after.Actual.Version != reply.UserVersion || after.Actual.Version <= before.Actual.Version || after.Actual.Seconds != request.ModificationTime.Seconds || after.Actual.Nanos != request.ModificationTime.Nanoseconds {
			t.Fatal("native owned postcondition differs", reply, after)
		}
		read, err := o.Request(ctx, replicatedPGRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(current) + 32)}))
		if err != nil {
			t.Fatal(err)
		}
		checkReplicatedPGReply(t, read, route)
		if !bytes.Equal(read.Operations[0].Data, current) || read.UserVersion != after.Actual.Version {
			t.Fatal("Go/native owned readback differs")
		}
		unchanged := replicatedPGNative(t, ctx, name)
		if !reflect.DeepEqual(after.Actual, unchanged.Actual) {
			t.Fatal("owned readback altered native state")
		}
		evidence = append(evidence, replicatedPGCase{Name: "owned write/readback", Route: route, Before: before.Actual, After: after.Actual, Reply: reply, FreshReply: &read})
	})
	t.Run("invalid explicit placements never submit", func(t *testing.T) {
		o := replicatedPGConnection(t, c, ctx, route)
		defer o.Close()
		before := replicatedPGNative(t, ctx, name)
		for _, edit := range []func(*cephmsgr.OSDRequest){func(r *cephmsgr.OSDRequest) { r.Placement = nil }, func(r *cephmsgr.OSDRequest) { r.Placement.Primary++ }, func(r *cephmsgr.OSDRequest) { r.Placement.PG.Pool++ }} {
			r := replicatedPGRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 1})
			edit(&r)
			reply, err := o.Request(ctx, r)
			if err == nil || reply.Submission.Transaction != 0 || len(reply.RawFront) != 0 {
				t.Fatal("invalid placement submitted", reply, err)
			}
		}
		after := replicatedPGNative(t, ctx, name)
		if !reflect.DeepEqual(before.Actual, after.Actual) {
			t.Fatal("local placement rejection altered native state")
		}
		evidence = append(evidence, replicatedPGCase{Name: "local placement rejection", Route: route, Before: before.Actual, After: after.Actual})
	})
	for _, closeHandle := range []bool{false, true} {
		caseName := "submitted cancel retains unknown outcome"
		if closeHandle {
			caseName = "submitted Close retains unknown outcome"
		}
		t.Run(caseName, func(t *testing.T) {
			before := replicatedPGNative(t, ctx, name)
			route = before.Placement
			options := integrationOptions(t)
			options.EnableOSD = true
			options.OSDProfile = cephmsgr.OSDProfileReplicatedPG
			var armed atomic.Bool
			var osdDials atomic.Int32
			blocked := make(chan struct{}, 1)
			dial := options.DialContext
			faultDaemon := "osd." + strconv.Itoa(int(route.ID))
			options.DialContext = func(call context.Context, network, endpoint string) (net.Conn, error) {
				conn, err := dial(call, network, endpoint)
				if err != nil {
					return nil, err
				}
				_, daemon, _ := testcluster.SplitPeer(endpoint)
				if daemon != faultDaemon {
					return conn, nil
				}
				osdDials.Add(1)
				return &testcluster.LostReplyConn{Conn: conn, Armed: &armed, Closed: make(chan struct{}), SignalBlocked: func() {
					select {
					case blocked <- struct{}{}:
					default:
					}
				}}, nil
			}
			subject, err := cephmsgr.Dial(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			defer subject.Close()
			o := replicatedPGConnection(t, subject, ctx, route)
			defer o.Close()
			tail := []byte{0, 255, 128, 'o', 'n', 'c', 'e'}
			if closeHandle {
				tail = append(tail, 'C')
			} else {
				tail = append(tail, 'X')
			}
			request := replicatedPGRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail})
			request.ClientIncarnation = 63
			request.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000910, Nanoseconds: 987654321}
			call, cancelCall := context.WithCancel(ctx)
			defer cancelCall()
			type completion struct {
				reply cephmsgr.OSDReply
				err   error
			}
			done := make(chan completion, 1)
			armed.Store(true)
			go func() { r, e := o.Request(call, request); done <- completion{r, e} }()
			select {
			case <-blocked:
			case <-ctx.Done():
				t.Fatal("replicated-PG reply withholding not activated", ctx.Err())
			}
			want := append(bytes.Clone(before.Actual.Data), tail...)
			var executed mutationOracle
			for {
				executed = replicatedPGNative(t, ctx, name)
				if executed.Actual.Code == 0 && bytes.Equal(executed.Actual.Data, want) {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("one append not independently observed", ctx.Err())
				case <-time.After(100 * time.Millisecond):
				}
			}
			if executed.Actual.Version <= before.Actual.Version || executed.Actual.Seconds != request.ModificationTime.Seconds || executed.Actual.Nanos != request.ModificationTime.Nanoseconds {
				t.Fatal("native one-append postcondition missing", executed)
			}
			if closeHandle {
				if err := o.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				cancelCall()
			}
			var outcome completion
			select {
			case outcome = <-done:
			case <-ctx.Done():
				t.Fatal("submitted wait not released", ctx.Err())
			}
			var unknown *cephmsgr.OutcomeUnknownError
			sub := outcome.reply.Submission
			if !errors.As(outcome.err, &unknown) || sub.Transaction == 0 || sub.GlobalID != subject.Snapshot().GlobalID || sub.ClientIncarnation != 63 || sub.MapEpoch != route.Epoch || sub.Attempt != 0 || !reflect.DeepEqual(sub.Placement, request.Placement) || outcome.reply.Transaction != 0 || len(outcome.reply.RawData) != 0 || len(outcome.reply.Operations) != 0 {
				t.Fatal("unknown outcome lost explicit placement/submission", outcome.reply, outcome.err)
			}
			if closeHandle && !errors.Is(outcome.err, cephmsgr.ErrClosed) || !closeHandle && !errors.Is(outcome.err, context.Canceled) {
				t.Fatal("local ending reason lost", outcome.err)
			}
			if osdDials.Load() != 1 {
				t.Fatal("submitted append automatically reconnected", osdDials.Load())
			}
			_ = o.Close()
			armed.Store(false)
			route = executed.Placement
			fresh := replicatedPGConnection(t, subject, ctx, route)
			defer fresh.Close()
			read, err := fresh.Request(ctx, replicatedPGRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(want) + 32)}))
			if err != nil {
				t.Fatal(err)
			}
			checkReplicatedPGReply(t, read, route)
			final := replicatedPGNative(t, ctx, name)
			if read.Transaction <= sub.Transaction || read.UserVersion != executed.Actual.Version || !bytes.Equal(read.Operations[0].Data, want) || !reflect.DeepEqual(final.Actual, executed.Actual) || osdDials.Load() != 2 {
				t.Fatal("fresh explicit session replayed append or changed native state", read, final)
			}
			current = want
			evidence = append(evidence, replicatedPGCase{Name: caseName, Route: route, Before: before.Actual, After: final.Actual, Reply: outcome.reply, Unknown: true, FreshReply: &read})
		})
	}
	t.Logf("replicated-PG v8 exercised native actual PG=%s distinct raw=%s; %d retained case receipts", route.PGID, route.RawPGID, len(evidence))
}
