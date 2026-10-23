package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Go-originated cache-tier operations on the owned writeback tier. The product
// client sends every CACHE_* request; the closed native observer seeds, reads
// (IGNORE_CACHE STAT+READ, C++ is_dirty) and removes the heads independently.
// These consumer helpers are test-only and nothing here is exported.

type cacheOpGoObservation struct {
	Snapshot struct {
		Base tierOpsState `json:"base"`
		Tier tierOpsState `json:"tier"`
	} `json:"snapshot"`
	Dirty      *tierOpsState `json:"dirty"`
	DirtyPolls []*bool       `json:"dirty_polls"`
}

type cacheOpGoNative struct {
	Source     string                          `json:"source_commit"`
	Checkpoint string                          `json:"checkpoint"`
	Plan       cachePoolPlan                   `json:"plan"`
	Issues     bool                            `json:"issues_cache_operations"`
	Phase      string                          `json:"phase"`
	Error      string                          `json:"error"`
	Placements []cachePoolPlacement            `json:"placements"`
	Seeded     map[string]cacheOpGoObservation `json:"seeded"`
	Labels     map[string]cacheOpGoObservation `json:"labels"`
	Steps      []tierOpsStep                   `json:"steps"`
	Cleanup    struct {
		Attempted bool `json:"attempted"`
		Complete  bool `json:"complete"`
	} `json:"cleanup"`
}

type cacheOpGoRecord struct {
	Label   string              `json:"label"`
	Route   cachePoolPlacement  `json:"native_route"`
	Request cephmsgr.OSDRequest `json:"request"`
	Reply   cephmsgr.OSDReply   `json:"reply"`
	Error   string              `json:"error,omitempty"`
	Unknown bool                `json:"unknown_outcome"`
}

const (
	cacheOpGoFlush  = "go-cache-flush"
	cacheOpGoTry    = "go-cache-try"
	cacheOpGoAbsent = "go-cache-absent"
)

func cacheOpGoReadNative(directory string) (cacheOpGoNative, []byte, error) {
	var value cacheOpGoNative
	raw, err := os.ReadFile(filepath.Join(directory, "osd-cache-op-go-native.json"))
	if err != nil {
		return value, nil, err
	}
	if len(raw) > 512<<10 {
		return value, nil, errors.New("native Go cache-op receipt exceeds bound")
	}
	err = json.Unmarshal(raw, &value)
	return value, raw, err
}

// cacheOpGoRequest builds the native caller's exact request for one opcode.
func cacheOpGoRequest(route cachePoolPlacement, code cephmsgr.OSDOpcode) cephmsgr.OSDRequest {
	r := cephmsgr.OSDRequest{MapEpoch: route.Epoch, Object: cephmsgr.OSDObject{Pool: route.Pool, Name: route.Name, Namespace: route.Namespace, Hash: route.Hash}, Operations: []cephmsgr.OSDOperation{{Code: code}}, IgnoreCache: true}
	switch code {
	case cephmsgr.OSDCacheFlush:
		r.IgnoreOverlay = true
	case cephmsgr.OSDCacheTryFlush, cephmsgr.OSDCacheEvict:
		r.IgnoreOverlay, r.SkipRWLocks = true, true
	case cephmsgr.OSDCachePin, cephmsgr.OSDCacheUnpin:
		// The native-only checkpoint pins with IGNORE_CACHE so an absent head
		// cannot be promoted first; pin and unpin are writes with an mtime.
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000900}
	}
	return r
}

func cacheOpGoSame(a, b tierOpsState, mtime bool) bool {
	return a.Code == b.Code && string(a.Data) == string(b.Data) && a.Size == b.Size && a.Version == b.Version && (!mtime || a.MTime == b.MTime)
}

// cacheOpGoTryFlushOutcome checks the independent native observation of the
// separate try-flush head against the single Go result. Success requires B in
// both pools, a valid final is_dirty read of false and a last bounded poll of
// false. EBUSY stays a signed refusal with unchanged tier bytes and a base
// holding A or B. Internal EAGAIN from the pinned flush handler is consumed by
// execute_ctx without a terminal reply; it is not a Go-visible retry result.
func cacheOpGoTryFlushOutcome(code int32, o cacheOpGoObservation) error {
	var last *bool
	if n := len(o.DirtyPolls); n > 0 {
		last = o.DirtyPolls[n-1]
	}
	switch code {
	case 0:
		if !tierOpsHolds(o.Snapshot.Base, cachePoolChanged) || !tierOpsHolds(o.Snapshot.Tier, cachePoolChanged) {
			return errors.New("successful try-flush did not leave the tier bytes in both pools")
		}
		if o.Dirty == nil || o.Dirty.DirtyResult == nil || *o.Dirty.DirtyResult != 0 || o.Dirty.Dirty == nil || *o.Dirty.Dirty || last == nil || *last {
			return errors.New("successful try-flush left the head dirty or unobserved")
		}
	case -16:
		if !tierOpsHolds(o.Snapshot.Tier, cachePoolChanged) || !(tierOpsHolds(o.Snapshot.Base, cachePoolOriginal) || tierOpsHolds(o.Snapshot.Base, cachePoolChanged)) {
			return fmt.Errorf("try-flush %d changed the tier bytes or left a foreign base", code)
		}
	default:
		return fmt.Errorf("unclassified try-flush result %d", code)
	}
	return nil
}

func TestCephOSDCacheOpGoIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_CACHE_POOLS") != "1" {
		t.Skip("requires isolated cache-pool fixture")
	}
	directory := ordinaryLogFixture(t)
	if os.Getenv("CEPH_MSGR_TEST_OSD") != "1" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" || isReplicatedPGFixtureProfile() {
		t.Fatal("Go cache-op acceptance requires legacy-v6 single BlueStore")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	initial, _, err := cachePoolReadNative(directory)
	if err != nil || initial.Source != mdsFreshSource || initial.Plan.FSID != os.Getenv("CEPH_MSGR_FSID") || initial.Phase != "prepared" {
		t.Fatal("cache-pool fixture preparation incomplete", initial.Phase, err)
	}
	records := []cacheOpGoRecord{}
	controls := []map[string]any{}
	receipt := map[string]any{"source_commit": mdsFreshSource, "plan": initial.Plan, "observer_issues_cache_operations": false}
	control := func(wait context.Context, args ...string) (cacheOpGoNative, error) {
		err := testcluster.RequestControl(wait, directory, "osd-cache-pools", "osd-cache-pools-completed", append([]string{initial.Plan.Token, "go-ops"}, args...)...)
		n, raw, readErr := cacheOpGoReadNative(directory)
		controls = append(controls, map[string]any{"args": args, "native_bytes": len(raw), "native_sha256": fmt.Sprintf("%x", sha256.Sum256(raw)), "phase": n.Phase, "native_error": n.Error})
		if readErr == nil && (!reflect.DeepEqual(n.Plan, initial.Plan) || n.Source != mdsFreshSource || n.Checkpoint != "cache-op-go-observer-v1" || n.Issues) {
			return n, errors.New("foreign native Go cache-op observer response")
		}
		return n, errors.Join(err, readErr)
	}
	var clients []*cephmsgr.Client
	prepared := false
	t.Cleanup(func() {
		for _, c := range clients {
			c.Close()
		}
		if prepared {
			clean, stop := context.WithTimeout(context.Background(), 3*time.Minute)
			defer stop()
			final, err := control(clean, "finish")
			receipt["final_native"] = map[string]any{"phase": final.Phase, "cleanup": final.Cleanup, "steps": len(final.Steps)}
			if err != nil || !final.Cleanup.Complete || final.Phase != "finished" {
				t.Error("owned Go cache-op head cleanup", err, final.Error)
			}
		}
		receipt["requests"], receipt["controls"] = records, controls
		encoded, e := json.MarshalIndent(receipt, "", "  ")
		if e == nil {
			e = testcluster.WritePublicEvidence(filepath.Join(directory, "osd-cache-op-go-results.json"), append(encoded, '\n'))
		}
		if e != nil {
			t.Error(e)
		}
	})
	n, err := control(ctx, "prepare")
	prepared = err == nil || n.Phase != ""
	if err != nil || n.Phase != "prepared" || len(n.Placements) != 6 || len(n.Seeded) != 2 {
		t.Fatal("native Go cache-op preparation", n.Phase, n.Error, err)
	}
	seeded := n.Seeded[cacheOpGoFlush]
	for _, name := range []string{cacheOpGoFlush, cacheOpGoTry} {
		s := n.Seeded[name]
		if !tierOpsHolds(s.Snapshot.Base, cachePoolOriginal) || !tierOpsHolds(s.Snapshot.Tier, cachePoolChanged) || s.Dirty == nil || s.Dirty.Dirty == nil || !*s.Dirty.Dirty {
			t.Fatal("native seeds differ", name, s)
		}
	}
	routes := cachePoolNative{Plan: initial.Plan, Placements: n.Placements}
	route := func(name string) cachePoolPlacement { return cachePoolRoute(t, routes, initial.Plan.Tier, name) }

	options := integrationOptions(t)
	options.EnableOSD, options.EnableOSDCachePools = true, true
	raw, err := os.ReadFile(filepath.Join(directory, "osd-cache-pools-rw.key"))
	if err != nil {
		t.Fatal(err)
	}
	if options.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(raw))); err != nil {
		t.Fatal(err)
	}
	options.Identity = initial.Plan.Clients["rw"]
	rw, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	clients = append(clients, rw)
	receipt["identity"] = rw.Snapshot()

	// send issues one Go request exactly once and checks its signed result,
	// request identity and placement echo. An unknown outcome stops the test;
	// it is never resent.
	send := func(t *testing.T, label, name string, code cephmsgr.OSDOpcode, accept ...int32) cephmsgr.OSDReply {
		t.Helper()
		r := route(name)
		request := cacheOpGoRequest(r, code)
		o, err := rw.OpenOSD(ctx, cephmsgr.OSDTarget{ID: r.ID, Address: r.Address})
		if err != nil {
			t.Fatal("explicit OSD", err)
		}
		reply, err := o.Request(ctx, request)
		entry := cacheOpGoRecord{Label: label, Route: r, Request: request, Reply: reply}
		var unknown *cephmsgr.OutcomeUnknownError
		entry.Unknown = errors.As(err, &unknown)
		if err != nil {
			entry.Error = err.Error()
		}
		records = append(records, entry)
		if entry.Unknown {
			t.Fatal("Go cache operation outcome unknown; not resent", label, err)
		}
		if reply.Submission.Transaction == 0 || reply.Submission.GlobalID != rw.Snapshot().GlobalID || reply.Object != request.Object.Name || reply.PG.Pool != uint64(request.Object.Pool) || reply.PG.Seed != request.Object.Hash || reply.MapEpoch < request.MapEpoch || len(reply.RawFront) == 0 || len(reply.Operations) != 1 {
			t.Fatal("Go cache reply lost identity, placement or its operation", label, reply.Code, err)
		}
		if reply.Code < 0 {
			var remote *cephmsgr.OSDError
			if !errors.As(err, &remote) || remote.Code != reply.Code {
				t.Fatal("signed server refusal lost its raw error", label, reply.Code, err)
			}
		} else if err != nil {
			t.Fatal("successful reply returned error", label, err)
		}
		// The pinned read/no-op/error and transaction-commit paths both add
		// ACK+ONDISK. Retain these raw terminal flags independently of state:
		// identical flags do not show whether an object mutation occurred.
		if reply.Flags&(cephmsgr.OSDReplyAck|cephmsgr.OSDReplyOnDisk) != cephmsgr.OSDReplyAck|cephmsgr.OSDReplyOnDisk {
			t.Fatal("selected native cache terminal reply lacks ACK+ONDISK", label, reply.Flags)
		}
		if op := reply.Operations[0].Result; op != 0 && op != reply.Code {
			t.Fatal("per-op result disagrees with the aggregate", label, op, reply.Code)
		}
		for _, code := range accept {
			if reply.Code == code {
				return reply
			}
		}
		t.Fatalf("%s returned %d, accepted %v", label, reply.Code, accept)
		return reply
	}
	observe := func(t *testing.T, label string) cacheOpGoObservation {
		t.Helper()
		n, err := control(ctx, "observe", label)
		o, ok := n.Labels[label]
		if err != nil || !ok {
			t.Fatal("native observation", label, n.Error, err)
		}
		return o
	}
	dirty := func(o cacheOpGoObservation) *bool {
		if o.Dirty == nil || o.Dirty.DirtyResult == nil || *o.Dirty.DirtyResult != 0 {
			return nil
		}
		return o.Dirty.Dirty
	}

	steps := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"dirty_eviction_is_refused", func(t *testing.T) {
			send(t, "evict-dirty", cacheOpGoFlush, cephmsgr.OSDCacheEvict, -16)
			o := observe(t, "after-evict-dirty")
			if !cacheOpGoSame(o.Snapshot.Tier, seeded.Snapshot.Tier, true) || !cacheOpGoSame(o.Snapshot.Base, seeded.Snapshot.Base, true) || dirty(o) == nil || !*dirty(o) {
				t.Fatal("dirty eviction changed native state", o)
			}
		}},
		{"pinned_head_refuses_flush_and_eviction", func(t *testing.T) {
			send(t, "pin", cacheOpGoFlush, cephmsgr.OSDCachePin, 0)
			send(t, "pinned-flush", cacheOpGoFlush, cephmsgr.OSDCacheFlush, -1)
			send(t, "pinned-try-flush", cacheOpGoFlush, cephmsgr.OSDCacheTryFlush, -1)
			send(t, "pinned-evict", cacheOpGoFlush, cephmsgr.OSDCacheEvict, -1)
			o := observe(t, "pinned")
			// Pin keeps the user version and bytes; its request mtime may apply.
			if !cacheOpGoSame(o.Snapshot.Tier, seeded.Snapshot.Tier, false) || !cacheOpGoSame(o.Snapshot.Base, seeded.Snapshot.Base, true) || dirty(o) == nil || !*dirty(o) {
				t.Fatal("pinned refusals changed native state", o)
			}
			send(t, "unpin", cacheOpGoFlush, cephmsgr.OSDCacheUnpin, 0)
			send(t, "evict-dirty-unpinned", cacheOpGoFlush, cephmsgr.OSDCacheEvict, -16)
			o = observe(t, "unpinned")
			if !cacheOpGoSame(o.Snapshot.Tier, seeded.Snapshot.Tier, false) || !cacheOpGoSame(o.Snapshot.Base, seeded.Snapshot.Base, true) || dirty(o) == nil || !*dirty(o) {
				t.Fatal("unpinned dirty eviction changed native state", o)
			}
		}},
		{"flush_writes_back_and_cleans", func(t *testing.T) {
			send(t, "flush", cacheOpGoFlush, cephmsgr.OSDCacheFlush, 0)
			o := observe(t, "flushed")
			if !tierOpsHolds(o.Snapshot.Base, cachePoolChanged) || !tierOpsHolds(o.Snapshot.Tier, cachePoolChanged) || dirty(o) == nil || *dirty(o) {
				t.Fatal("Go flush did not write back the tier bytes or clear dirty", o)
			}
			receipt["flushed"] = o
		}},
		{"clean_flushes_are_noops", func(t *testing.T) {
			flushed, _ := receipt["flushed"].(cacheOpGoObservation)
			noops := map[string]cacheOpGoObservation{}
			for _, tc := range []struct {
				label, observe string
				code           cephmsgr.OSDOpcode
			}{
				{"flush-clean", "clean-flush", cephmsgr.OSDCacheFlush},
				{"try-flush-clean", "clean-try-flush", cephmsgr.OSDCacheTryFlush},
			} {
				send(t, tc.label, cacheOpGoFlush, tc.code, 0)
				o := observe(t, tc.observe)
				noops[tc.label] = o
				receipt["clean_noops"] = noops
				if !cacheOpGoSame(o.Snapshot.Base, flushed.Snapshot.Base, true) || !cacheOpGoSame(o.Snapshot.Tier, flushed.Snapshot.Tier, true) || dirty(o) == nil || *dirty(o) {
					t.Fatal("clean cache flush changed native bytes/version/mtime/dirty state", tc.label, o)
				}
			}
		}},
		{"try_flush_is_issued_once", func(t *testing.T) {
			reply := send(t, "try-flush", cacheOpGoTry, cephmsgr.OSDCacheTryFlush, 0, -16)
			o := observe(t, "try-flushed")
			receipt["try_flush"] = map[string]any{"code": reply.Code, "dirty_polls": o.DirtyPolls, "reissued": false, "base": o.Snapshot.Base}
			if err := cacheOpGoTryFlushOutcome(reply.Code, o); err != nil {
				t.Fatal(err, o)
			}
		}},
		{"clean_eviction_keeps_the_base", func(t *testing.T) {
			flushed, _ := receipt["flushed"].(cacheOpGoObservation)
			send(t, "evict-clean", cacheOpGoFlush, cephmsgr.OSDCacheEvict, 0)
			o := observe(t, "evicted")
			if o.Snapshot.Tier.Code != -2 || !cacheOpGoSame(o.Snapshot.Base, flushed.Snapshot.Base, true) {
				t.Fatal("Go eviction left the tier head or changed the base", o)
			}
		}},
		{"absent_head_controls", func(t *testing.T) {
			absent := map[string]cacheOpGoObservation{}
			for _, tc := range []struct {
				label string
				code  cephmsgr.OSDOpcode
			}{
				{"absent-flush", cephmsgr.OSDCacheFlush}, {"absent-try-flush", cephmsgr.OSDCacheTryFlush}, {"absent-evict", cephmsgr.OSDCacheEvict},
			} {
				send(t, tc.label, cacheOpGoAbsent, tc.code, 0, -2)
				o := observe(t, tc.label)
				absent[tc.label] = o
				receipt["absent_controls"] = absent
				if o.Snapshot.Base.Code != -2 || o.Snapshot.Tier.Code != -2 {
					t.Fatal("absent cache control created a head", tc.label, o)
				}
			}
			send(t, "absent-pin", cacheOpGoAbsent, cephmsgr.OSDCachePin, -2)
			send(t, "absent-unpin", cacheOpGoAbsent, cephmsgr.OSDCacheUnpin, -2)
			o := observe(t, "absent-final")
			if o.Snapshot.Base.Code != -2 || o.Snapshot.Tier.Code != -2 {
				t.Fatal("absent controls created a head", o)
			}
		}},
	}
	for _, step := range steps {
		if !t.Run(step.name, step.run) {
			return
		}
	}
	summary := map[string]int32{}
	for _, r := range records {
		summary[r.Label] = r.Reply.Code
		t.Logf("go %s tid=%d epoch=%d code=%d op=%d reply_flags=%#x", r.Label, r.Reply.Submission.Transaction, r.Reply.MapEpoch, r.Reply.Code, r.Reply.Operations[0].Result, r.Reply.Flags)
	}
	receipt["result_codes"] = summary
}

func TestCacheOpGoTryFlushOutcome(t *testing.T) {
	state := func(data []byte) tierOpsState {
		return tierOpsState{Data: data, Size: uint64(len(data)), Version: 3}
	}
	flag := func(v bool) *bool { return &v }
	zero := int32(0)
	observation := func(base []byte, dirty bool, polls ...bool) cacheOpGoObservation {
		var o cacheOpGoObservation
		o.Snapshot.Base, o.Snapshot.Tier = state(base), state(cachePoolChanged)
		o.Dirty = &tierOpsState{Dirty: flag(dirty), DirtyResult: &zero}
		for _, p := range polls {
			o.DirtyPolls = append(o.DirtyPolls, flag(p))
		}
		return o
	}
	if err := cacheOpGoTryFlushOutcome(0, observation(cachePoolChanged, false, true, false)); err != nil {
		t.Fatal("clean written-back try-flush refused", err)
	}
	for _, code := range []int32{-16} {
		for _, base := range [][]byte{cachePoolOriginal, cachePoolChanged} {
			if err := cacheOpGoTryFlushOutcome(code, observation(base, true, true, true)); err != nil {
				t.Fatal("in-progress try-flush outcome refused", code, err)
			}
		}
	}
	unread := observation(cachePoolChanged, false, false)
	unread.Dirty.DirtyResult = new(int32)
	*unread.Dirty.DirtyResult = -5
	badRead := observation(cachePoolChanged, false, false)
	badRead.Snapshot.Base.ReadResult = -5
	noPolls := observation(cachePoolChanged, false)
	tierChanged := observation(cachePoolOriginal, true, true)
	tierChanged.Snapshot.Tier = state(cachePoolOriginal)
	for name, c := range map[string]struct {
		code int32
		o    cacheOpGoObservation
	}{
		"internal EAGAIN is not a terminal reply": {-11, observation(cachePoolChanged, false, false)},
		"success with the base still A and clean": {0, observation(cachePoolOriginal, false, false)},
		"success with every poll dirty":           {0, observation(cachePoolChanged, true, true, true)},
		"success with a dirty final read":         {0, observation(cachePoolChanged, true, false)},
		"success with a failed is_dirty read":     {0, unread},
		"success with a failed base read":         {0, badRead},
		"success without a poll":                  {0, noPolls},
		"EBUSY with changed tier bytes":           {-16, tierChanged},
		"unclassified result":                     {-5, observation(cachePoolChanged, false, false)},
	} {
		if cacheOpGoTryFlushOutcome(c.code, c.o) == nil {
			t.Error("contradictory try-flush observation accepted:", name)
		}
	}
}
