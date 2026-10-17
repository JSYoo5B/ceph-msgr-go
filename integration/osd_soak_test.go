package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// The soak runs concurrent writers, each with its own client and object, for
// CEPH_MSGR_TEST_OSD_SOAK. Every worker appends, rereads and checks its
// object, sets an xattr, replaces its connection, and sends appends under
// deadlines short enough to end some of them unknown. An unknown append is
// resolved only by explicit resubmission under its original identity on a
// fresh connection; a request that ended before transmission is not counted.
// At the end the native client must see exactly the bytes and xattr each
// worker expects, so no append was lost, duplicated or reordered, and the
// client's goroutines must return to their starting level.
func TestCephOSDSoakIntegration(t *testing.T) {
	value := os.Getenv("CEPH_MSGR_TEST_OSD_SOAK")
	if value == "" {
		t.Skip("set CEPH_MSGR_TEST_OSD_SOAK to a duration for the disposable OSD fixture")
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
	ctx, cancel := context.WithTimeout(context.Background(), duration+3*time.Minute)
	defer cancel()
	const workers = 4
	prepared := false
	t.Cleanup(func() {
		if !prepared {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 60*time.Second)
		defer stop()
		if result := nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}); !result.Cleanup.Removed {
			t.Error("owned soak objects were not removed", result.Cleanup)
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("soak")}, Reserve: workers})
	prepared = true
	if len(owned.Objects) != workers+1 {
		t.Fatal("native soak objects differ", owned.Objects)
	}
	routes := make([]osdFixtureOracle, workers)
	for i := range routes {
		// The reserved names start absent; the first append creates them.
		name := owned.Objects[i+1]
		routes[i] = nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
		if routes[i].Name != name || routes[i].ID < 0 {
			t.Fatal("native soak placement differs", routes[i])
		}
	}
	baseline := runtime.NumGoroutine()
	var appends, reads, attrs, churns, unknowns, resubmissions, unsent, truncations atomic.Int64
	type outcome struct {
		data []byte
		attr string
	}
	results := make([]outcome, workers)
	work, stop := context.WithTimeout(ctx, duration)
	defer stop()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			route := routes[w]
			fail := func(format string, args ...any) {
				t.Errorf("worker %d: "+format, append([]any{w}, args...)...)
				stop()
			}
			// OpenOSD shares one handle per OSD within a client, and Close ends
			// it for every holder; each worker owns a client so its churn and
			// resubmissions stay its own.
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
			open := func() *cephmsgr.OSDConnection {
				o, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: route.ID, Address: route.Address})
				if err != nil {
					fail("open: %v", err)
					return nil
				}
				return o
			}
			o := open()
			if o == nil {
				return
			}
			defer func() {
				if o != nil {
					o.Close()
				}
			}()
			random := rand.New(rand.NewSource(int64(w) + 1))
			var expected []byte
			attr := ""
			stamp := uint32(1700100000)
			build := func(ops ...cephmsgr.OSDOperation) cephmsgr.OSDRequest {
				r := request(route, ops...)
				stamp++
				if ops[0].Code != cephmsgr.OSDRead {
					r.ModificationTime = cephmsgr.OSDTime{Seconds: stamp}
				}
				return r
			}
			chunk := func(i int) []byte {
				b := make([]byte, 1+random.Intn(512))
				random.Read(b)
				b[0] = byte(i)
				return b
			}
			appendOp := func(b []byte) cephmsgr.OSDOperation {
				return cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(b)), Data: b}
			}
			for i := 0; work.Err() == nil; i++ {
				// A steady pace, not a throughput test: the soak is about
				// duration, churn and unknown-outcome resolution.
				time.Sleep(2 * time.Millisecond)
				switch i % 10 {
				case 6:
					reply, err := o.Request(ctx, build(cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(expected) + 64)}))
					var server *cephmsgr.OSDError
					if len(expected) == 0 && errors.As(err, &server) && server.Code == -2 {
						reads.Add(1)
						continue
					}
					if err != nil || !bytes.Equal(reply.Operations[0].Data, expected) {
						fail("read after %d bytes differs: %v", len(expected), err)
						return
					}
					reads.Add(1)
				case 7:
					value := strconv.Itoa(i)
					if _, err := o.Request(ctx, build(cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "soak"}, Data: []byte(value)})); err != nil {
						fail("setxattr: %v", err)
						return
					}
					attr = value
					attrs.Add(1)
				case 8:
					b := chunk(i)
					r := build(appendOp(b))
					short, cancelShort := context.WithTimeout(ctx, time.Duration(1+random.Intn(15))*time.Millisecond)
					reply, err := o.Request(short, r)
					cancelShort()
					var unknown *cephmsgr.OutcomeUnknownError
					switch {
					case err == nil:
						expected = append(expected, b...)
						appends.Add(1)
					case errors.As(err, &unknown):
						unknowns.Add(1)
						// Resolve on a fresh connection under the original identity.
						o.Close()
						if o = open(); o == nil {
							return
						}
						again := r
						again.Resubmit = &reply.Submission
						for attempt := int32(1); ; attempt++ {
							again.RetryAttempt = attempt
							resolved, err := o.Request(ctx, again)
							resubmissions.Add(1)
							if err == nil && resolved.Transaction == reply.Submission.Transaction {
								expected = append(expected, b...)
								appends.Add(1)
								break
							}
							if !errors.As(err, &unknown) || attempt == 5 {
								fail("resubmission attempt %d: %v", attempt, err)
								return
							}
							o.Close()
							if o = open(); o == nil {
								return
							}
						}
					case errors.Is(err, context.DeadlineExceeded):
						// Not an unknown outcome: it ended before transmission,
						// so nothing executed.
						unsent.Add(1)
					default:
						fail("short-deadline append: %v", err)
						return
					}
				case 5:
					// Keep objects small so reads stay cheap and the sparse
					// BlueStore devices never approach their full ratio, which
					// would leave writes unanswered; the reset exercises TRUNCATE.
					if len(expected) < 64<<10 {
						continue
					}
					if _, err := o.Request(ctx, build(cephmsgr.OSDOperation{Code: cephmsgr.OSDTruncate, Offset: 0})); err != nil {
						fail("truncate: %v", err)
						return
					}
					expected = expected[:0]
					truncations.Add(1)
				case 9:
					o.Close()
					if o = open(); o == nil {
						return
					}
					churns.Add(1)
				default:
					b := chunk(i)
					if _, err := o.Request(ctx, build(appendOp(b))); err != nil {
						fail("append: %v", err)
						return
					}
					expected = append(expected, b...)
					appends.Add(1)
				}
			}
			results[w] = outcome{expected, attr}
		}(w)
	}
	started := time.Now()
	ticker := time.NewTicker(30 * time.Second)
	for done := false; !done; {
		select {
		case <-work.Done():
			done = true
		case <-ticker.C:
			t.Logf("elapsed=%s appends=%d reads=%d xattrs=%d truncations=%d churns=%d unknown=%d resubmissions=%d unsent=%d goroutines=%d", time.Since(started).Round(time.Second), appends.Load(), reads.Load(), attrs.Load(), truncations.Load(), churns.Load(), unknowns.Load(), resubmissions.Load(), unsent.Load(), runtime.NumGoroutine())
		}
	}
	ticker.Stop()
	wg.Wait()
	if t.Failed() {
		return
	}
	for w, r := range results {
		state := nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: routes[w].Name}).State
		got := xattrInventory(state.nativeXattrSnapshot)["soak"]
		if !bytes.Equal(state.Data, r.data) || got != r.attr {
			t.Fatalf("worker %d: native object differs: %d bytes vs %d expected, xattr %q vs %q", w, len(state.Data), len(r.data), got, r.attr)
		}
	}
	if appends.Load() < 100 || reads.Load() < 10 || churns.Load() < 10 {
		t.Fatalf("insufficient exercise: appends=%d reads=%d churns=%d", appends.Load(), reads.Load(), churns.Load())
	}
	deadline := time.Now().Add(10 * time.Second)
	for runtime.NumGoroutine() > baseline+4 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > baseline+4 {
		t.Fatalf("goroutines did not return: before=%d after=%d", baseline, after)
	}
	t.Log(fmt.Sprintf("soak %s: appends=%d reads=%d xattrs=%d truncations=%d churns=%d unknown=%d resolved by %d resubmissions, unsent=%d; native bytes and xattrs matched for %d objects", duration, appends.Load(), reads.Load(), attrs.Load(), truncations.Load(), churns.Load(), unknowns.Load(), resubmissions.Load(), unsent.Load(), workers))
}
