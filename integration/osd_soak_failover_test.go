package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// soakOracle serializes native oracle and daemon control calls: they share
// fixed request and result files in the control directory.
var soakOracle sync.Mutex

// soakPlace is nativeMutation's placement query without t.Fatal, for workers.
func soakPlace(ctx context.Context, object string) (osdFixtureOracle, error) {
	soakOracle.Lock()
	defer soakOracle.Unlock()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	data, err := json.Marshal(map[string]any{"compare": false, "placement": true, "object": object, "namespace": ""})
	if err != nil {
		return osdFixtureOracle{}, err
	}
	if err = os.WriteFile(filepath.Join(dir, "osd-mutation-input.json"), data, 0600); err != nil {
		return osdFixtureOracle{}, err
	}
	if err = testcluster.ControlDaemon(ctx, dir, "verify", "osd-mutation", "compare"); err != nil {
		return osdFixtureOracle{}, err
	}
	if data, err = os.ReadFile(filepath.Join(dir, "osd-mutation-result.json")); err != nil {
		return osdFixtureOracle{}, err
	}
	var result mutationOracle
	if err = json.Unmarshal(data, &result); err != nil {
		return osdFixtureOracle{}, err
	}
	if result.Placement.Name != object || result.Placement.ID < 0 || result.Placement.Address == "" {
		return osdFixtureOracle{}, fmt.Errorf("native placement differs: %+v", result.Placement)
	}
	return result.Placement, nil
}

// The failover soak keeps four writers running while an OSD of one worker's
// acting set (every other time its primary) is SIGKILLed and restarted every
// cycle. Workers act as a
// caller would: a request without an answer ends unknown and is resolved only
// by explicit resubmission under its original identity to the primary of a
// freshly queried placement; requests that never left are sent again as new
// ones. The replicated PG log answers those resubmissions, so at the end the
// native client must read exactly the bytes and xattr each worker expects.
func TestCephOSDSoakFailoverIntegration(t *testing.T) {
	value := os.Getenv("CEPH_MSGR_TEST_OSD_SOAK")
	if value == "" {
		t.Skip("set CEPH_MSGR_TEST_OSD_SOAK to a duration for the disposable OSD fixture")
	}
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("primary failover needs three BlueStore replicas")
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < 30*time.Second {
		t.Fatal("OSD soak duration must be at least 30s", value)
	}
	fixture, request := osdFixture, osdObjectRequest
	if isReplicatedPGFixtureProfile() {
		fixture, request = replicatedPGFixture, replicatedPGRequest
	}
	_, _, base, _ := fixture(t)
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	ctx, cancel := context.WithTimeout(context.Background(), duration+5*time.Minute)
	defer cancel()
	receipts := newSoakReceipts(t, "failover", duration)
	const workers = 4
	prepared := false
	t.Cleanup(func() {
		if !prepared {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 180*time.Second)
		defer stop()
		started := time.Now()
		result := nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{})
		receipts.write(t, "cleanup", map[string]any{"removed": result.Cleanup.Removed, "objects": result.Cleanup.Objects, "elapsed_ns": time.Since(started).Nanoseconds()})
		if !result.Cleanup.Removed {
			t.Error("owned soak objects were not removed", result.Cleanup)
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("soak")}, Reserve: workers})
	prepared = true
	if len(owned.Objects) != workers+1 {
		t.Fatal("native soak objects differ", owned.Objects)
	}
	names := owned.Objects[1:]
	var appends, reads, unknowns, resubmissions, resent, replacements, attrs, truncations atomic.Int64
	faults := 0
	receipts.counters = func() map[string]int64 {
		return map[string]int64{"appends": appends.Load(), "reads": reads.Load(), "unknowns": unknowns.Load(), "resubmissions": resubmissions.Load(), "resent": resent.Load(), "replacements": replacements.Load(), "xattrs": attrs.Load(), "truncations": truncations.Load(), "faults": int64(faults)}
	}
	type outcome struct {
		data []byte
		attr string
	}
	results := make([]outcome, workers)
	receipts.beginWorkload(t)
	work, stop := context.WithTimeout(ctx, duration)
	defer stop()
	var wg sync.WaitGroup
	defer func() {
		stop()
		if t.Failed() {
			cancel()
		}
		wg.Wait()
		receipts.finishWorkload()
	}()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			defer receipts.workerDone()
			fail := func(format string, args ...any) {
				receipts.failure(fmt.Sprintf("worker %d: "+format, append([]any{w}, args...)...))
				t.Errorf("worker %d: "+format, append([]any{w}, args...)...)
				stop()
			}
			options := integrationOptions(t)
			options.EnableOSD = true
			if isReplicatedPGFixtureProfile() {
				options.OSDProfile = replicatedPGFixtureOSDProfile(t)
			}
			c, err := cephmsgr.Dial(ctx, options)
			if err != nil {
				fail("dial: %v", err)
				return
			}
			defer c.Close()
			var route osdFixtureOracle
			var o *cephmsgr.OSDConnection
			defer func() {
				if o != nil {
					o.Close()
				}
			}()
			// replace closes the current connection, queries placement again and
			// opens the reported primary, retrying while the cluster settles.
			replace := func() bool {
				if o != nil {
					o.Close()
					o = nil
				}
				for attempt := 0; attempt < 60; attempt++ {
					if ctx.Err() != nil {
						fail("placement: %v", ctx.Err())
						return false
					}
					r, err := soakPlace(ctx, names[w])
					if err == nil {
						opened, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: r.ID, Address: r.Address})
						if err == nil {
							route, o = r, opened
							replacements.Add(1)
							return true
						}
					}
					time.Sleep(time.Second)
				}
				fail("no reachable primary for %s", names[w])
				return false
			}
			if !replace() {
				return
			}
			random := rand.New(rand.NewSource(int64(w) + 11))
			stamp := uint32(1700200000)
			// write sends a writing vector until its outcome is known. An
			// unknown outcome is only ever resubmitted under its original
			// identity; a request that was never transmitted is sent anew.
			write := func(ops ...cephmsgr.OSDOperation) bool {
				stamp++
				var original *cephmsgr.OSDSubmission
				for attempt := int32(0); attempt <= 30; attempt++ {
					r := request(route, ops...)
					r.ModificationTime = cephmsgr.OSDTime{Seconds: stamp}
					if original != nil {
						r.Resubmit, r.RetryAttempt = original, attempt
						resubmissions.Add(1)
					}
					call, done := context.WithTimeout(ctx, 6*time.Second)
					reply, err := o.Request(call, r)
					receipts.mutation(reply, err)
					done()
					var unknown *cephmsgr.OutcomeUnknownError
					var server *cephmsgr.OSDError
					switch {
					case err == nil:
						return true
					case errors.As(err, &unknown):
						if original == nil {
							unknowns.Add(1)
							submission := reply.Submission
							original = &submission
						}
					case errors.As(err, &server):
						fail("server refusal: %v", err)
						return false
					default:
						// Not transmitted: nothing executed under this attempt.
						if original == nil {
							resent.Add(1)
						}
					}
					if !replace() {
						return false
					}
				}
				fail("outcome still unknown after 30 resubmissions")
				return false
			}
			var expected []byte
			attr := ""
			for i := 0; work.Err() == nil; i++ {
				time.Sleep(10 * time.Millisecond)
				switch {
				case i%25 == 24:
					read := request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(expected) + 64)})
					call, done := context.WithTimeout(ctx, 6*time.Second)
					reply, err := o.Request(call, read)
					done()
					if err != nil {
						// Reads have no effect; a failed one is simply asked again.
						if !replace() {
							return
						}
						continue
					}
					if !bytes.Equal(reply.Operations[0].Data, expected) {
						fail("read of %d bytes differs from the %d expected", len(reply.Operations[0].Data), len(expected))
						return
					}
					reads.Add(1)
				case i%25 == 12:
					value := strconv.Itoa(i)
					if !write(cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "soak"}, Data: []byte(value)}) {
						return
					}
					attr = value
					attrs.Add(1)
				case len(expected) > 32<<10:
					if !write(cephmsgr.OSDOperation{Code: cephmsgr.OSDTruncate, Offset: 0}) {
						return
					}
					expected = expected[:0]
					truncations.Add(1)
				default:
					b := make([]byte, 1+random.Intn(256))
					random.Read(b)
					if !write(cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(b)), Data: b}) {
						return
					}
					expected = append(expected, b...)
					appends.Add(1)
				}
			}
			results[w] = outcome{expected, attr}
		}(w)
	}

	// Faults run on the test goroutine: one OSD down at a time, alternately the
	// current primary of a worker's object and another acting member,
	// restarted and clean before the next.
	started := time.Now()
	for work.Err() == nil {
		select {
		case <-work.Done():
		case <-time.After(15 * time.Second):
		}
		if work.Err() != nil {
			break
		}
		r, err := soakPlace(ctx, names[faults%workers])
		if err != nil {
			t.Fatal("fault placement", err)
		}
		// Rotate through the acting set: primary kills move the primary,
		// replica kills change the interval under the same primary.
		victim := strconv.Itoa(int(r.Acting[(faults/workers)%len(r.Acting)]))
		if faults%2 == 0 {
			victim = strconv.Itoa(int(r.ID))
		}
		soakOracle.Lock()
		phaseStarted := time.Now()
		err = testcluster.ControlDaemon(ctx, dir, "kill", "osd", victim)
		soakOracle.Unlock()
		receipts.write(t, fmt.Sprintf("fault-%03d-kill", faults+1), map[string]any{"victim": victim, "primary": r.ID, "acting": r.Acting, "elapsed_ns": time.Since(phaseStarted).Nanoseconds(), "passed": err == nil})
		if err != nil {
			receipts.failure("kill osd." + victim + ": " + err.Error())
			t.Fatal("kill", victim, err)
		}
		time.Sleep(12 * time.Second)
		soakOracle.Lock()
		phaseStarted = time.Now()
		err = testcluster.ControlDaemon(ctx, dir, "start", "osd", victim)
		receipts.write(t, fmt.Sprintf("fault-%03d-restart", faults+1), map[string]any{"victim": victim, "elapsed_ns": time.Since(phaseStarted).Nanoseconds(), "passed": err == nil})
		if err == nil {
			phaseStarted = time.Now()
			err = testcluster.ControlDaemon(ctx, dir, "verify", "osd-ready", "clean")
			receipts.write(t, fmt.Sprintf("fault-%03d-clean", faults+1), map[string]any{"victim": victim, "elapsed_ns": time.Since(phaseStarted).Nanoseconds(), "passed": err == nil})
		}
		soakOracle.Unlock()
		if err != nil {
			receipts.failure("restart/clean osd." + victim + ": " + err.Error())
			t.Fatal("restart", victim, err)
		}
		faults++
		t.Logf("elapsed=%s fault %d: osd.%s killed and back clean; appends=%d reads=%d unknown=%d resubmissions=%d resent=%d replacements=%d", time.Since(started).Round(time.Second), faults, victim, appends.Load(), reads.Load(), unknowns.Load(), resubmissions.Load(), resent.Load(), replacements.Load())
	}
	wg.Wait()
	receipts.finishWorkload()
	if t.Failed() {
		return
	}
	waitOSDFixtureClean(t, ctx, base)
	receipts.write(t, "recovery-clean", map[string]any{"passed": true})
	for w, r := range results {
		observed := time.Now()
		observation := nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: names[w]})
		state := observation.State
		got := xattrInventory(state.nativeXattrSnapshot)["soak"]
		if !receipts.observe(t, w, r.data, r.attr, observation, time.Since(observed)) {
			receipts.failure(fmt.Sprintf("worker %d final native bytes/xattr/version differs", w))
			t.Fatalf("worker %d: native object differs: %d bytes vs %d expected, xattr %q vs %q", w, len(state.Data), len(r.data), got, r.attr)
		}
	}
	if faults < 1 || appends.Load() < 100 || reads.Load() < 10 {
		t.Fatalf("insufficient exercise: faults=%d appends=%d reads=%d", faults, appends.Load(), reads.Load())
	}
	t.Logf("failover soak %s: %d OSD kills; appends=%d reads=%d unknown=%d resolved by %d resubmissions, resent=%d, replacements=%d; native bytes and xattrs matched for %d objects", duration, faults, appends.Load(), reads.Load(), unknowns.Load(), resubmissions.Load(), resent.Load(), replacements.Load(), workers)
}
