package integration_test

import (
	"bytes"
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

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Native-only cache-tier operation checkpoint. These consumer helpers only
// drive the fixture and judge its librados receipt; no product client, opcode
// or admission is involved, and nothing here is exported.

const (
	tierOpsIgnoreCache   = 8
	tierOpsSkipRWLocks   = 16
	tierOpsIgnoreOverlay = 32
	tierOpsFlushFlags    = tierOpsIgnoreCache | tierOpsIgnoreOverlay
	tierOpsTryFlags      = tierOpsIgnoreCache | tierOpsIgnoreOverlay | tierOpsSkipRWLocks
	tierOpsEvictFlags    = tierOpsTryFlags
	tierOpsPinFlags      = tierOpsIgnoreCache
)

type tierOpsState struct {
	Code        int32  `json:"code"`
	Data        []byte `json:"bytes_b64"`
	Size        uint64 `json:"size"`
	Version     uint64 `json:"version"`
	MTime       int64  `json:"mtime"`
	StatResult  int32  `json:"stat_prval"`
	ReadResult  int32  `json:"read_prval"`
	Dirty       *bool  `json:"dirty"`
	DirtyResult *int32 `json:"isdirty_prval"`
	PerOp       *bool  `json:"per_op_result_available"`
}

type tierOpsStep struct {
	Label     string       `json:"label"`
	Role      string       `json:"identity_role"`
	Pool      string       `json:"pool"`
	Namespace string       `json:"namespace"`
	Object    string       `json:"object"`
	Action    string       `json:"action"`
	Flags     int          `json:"api_flags"`
	Wire      int          `json:"wire_flags_source_derived"`
	MayMutate bool         `json:"may_mutate"`
	State     string       `json:"state"`
	Result    tierOpsState `json:"result"`
	Identity  struct {
		Name     string `json:"name"`
		GlobalID uint64 `json:"global_id"`
	} `json:"native_identity"`
}

type tierOpsReceipt struct {
	Schema       int                                `json:"schema_version"`
	Source       string                             `json:"source_commit"`
	Checkpoint   string                             `json:"checkpoint"`
	Plan         cachePoolPlan                      `json:"plan"`
	NativeOnly   bool                               `json:"native_only"`
	Product      bool                               `json:"product_support_claimed"`
	Complete     bool                               `json:"complete"`
	Sequence     bool                               `json:"sequence_complete"`
	Stopped      string                             `json:"stopped"`
	Error        string                             `json:"error"`
	Steps        []tierOpsStep                      `json:"steps"`
	Observations map[string]map[string]tierOpsState `json:"observations"`
	Placements   []cachePoolPlacement               `json:"placements"`
	Cleanup      struct {
		Attempted bool   `json:"attempted"`
		Complete  bool   `json:"complete"`
		Reason    string `json:"reason"`
	} `json:"cleanup"`
	TryFlush struct {
		Code     int32   `json:"code"`
		Definite bool    `json:"definite"`
		Dirty    []*bool `json:"dirty_observations"`
		Reissued bool    `json:"reissued"`
	} `json:"try_flush"`
	API struct {
		Symbols map[string]string `json:"cxx_symbols"`
		Flags   map[string]int    `json:"operation_flags"`
	} `json:"native_api"`
}

type tierOpsExpectation struct {
	action, role string
	flags        int
	code         int32
}

// Source-derived expectations, checked against the observed native receipt.
var tierOpsExpected = map[string]tierOpsExpectation{
	"evict-dirty":                  {"cache-evict", "rw", tierOpsEvictFlags, -16},
	"scoped-ro-evict-dirty":        {"cache-evict", "ro", tierOpsEvictFlags, -16},
	"scoped-base-evict-tier":       {"cache-evict", "base", tierOpsEvictFlags, -1},
	"scoped-ro-pin-refused":        {"cache-pin", "ro", tierOpsPinFlags, -1},
	"scoped-base-pin-refused":      {"cache-pin", "base", tierOpsPinFlags, -1},
	"pin":                          {"cache-pin", "rw", tierOpsPinFlags, 0},
	"pinned-flush":                 {"cache-flush", "rw", tierOpsFlushFlags, -1},
	"pinned-try-flush":             {"cache-try-flush", "rw", tierOpsTryFlags, -1},
	"pinned-evict":                 {"cache-evict", "rw", tierOpsEvictFlags, -1},
	"wrong-lock-flush-skiprwlocks": {"cache-flush", "rw", tierOpsFlushFlags | tierOpsSkipRWLocks, -22},
	"wrong-lock-try-flush-locked":  {"cache-try-flush", "rw", tierOpsTryFlags &^ tierOpsSkipRWLocks, -22},
	"scoped-ro-unpin-refused":      {"cache-unpin", "ro", tierOpsPinFlags, -1},
	"unpin":                        {"cache-unpin", "rw", tierOpsPinFlags, 0},
	"evict-dirty-unpinned":         {"cache-evict", "rw", tierOpsEvictFlags, -16},
	"flush":                        {"cache-flush", "rw", tierOpsFlushFlags, 0},
	"evict-clean":                  {"cache-evict", "rw", tierOpsEvictFlags, 0},
	"ordinary-read-after-evict":    {"snapshot", "rw", 0, 0},
	"absent-flush":                 {"cache-flush", "rw", tierOpsFlushFlags, -2},
	"absent-try-flush":             {"cache-try-flush", "rw", tierOpsTryFlags, -2},
	"absent-evict":                 {"cache-evict", "rw", tierOpsEvictFlags, -2},
	"absent-pin":                   {"cache-pin", "rw", tierOpsPinFlags, -2},
	"absent-unpin":                 {"cache-unpin", "rw", tierOpsPinFlags, -2},
}

func tierOpsWire(flags int) int {
	wire := 0
	for bit, value := range map[int]int{tierOpsIgnoreCache: 0x8000, tierOpsSkipRWLocks: 0x10000, tierOpsIgnoreOverlay: 0x20000} {
		if flags&bit != 0 {
			wire |= value
		}
	}
	return wire
}

func tierOpsHolds(s tierOpsState, data []byte) bool {
	return s.Code == 0 && s.StatResult == 0 && s.ReadResult == 0 && bytes.Equal(s.Data, data) && s.Size == uint64(len(data)) && s.Version > 0
}

func tierOpsSame(a, b tierOpsState) bool {
	return a.Code == b.Code && bytes.Equal(a.Data, b.Data) && a.Size == b.Size && a.Version == b.Version && a.MTime == b.MTime
}

func tierOpsDirty(t *testing.T, steps map[string]tierOpsStep, label string, want bool) {
	t.Helper()
	s, ok := steps[label]
	if !ok || s.Action != "is-dirty" || s.Role != "observer" || s.Flags != tierOpsIgnoreCache || s.Result.Code != 0 || s.Result.DirtyResult == nil || *s.Result.DirtyResult != 0 || s.Result.Dirty == nil || *s.Result.Dirty != want {
		t.Fatal("native is_dirty observation differs", label, want, s.Result)
	}
}

func validateTierOpsReceipt(t *testing.T, n tierOpsReceipt, plan cachePoolPlan) {
	t.Helper()
	if n.Schema != 1 || n.Source != mdsFreshSource || n.Checkpoint != "cache-tier-ops-native-v1" || !reflect.DeepEqual(n.Plan, plan) || !n.NativeOnly || n.Product {
		t.Fatal("foreign or product-claiming tier-ops receipt", n.Checkpoint, n.NativeOnly, n.Product)
	}
	if n.Stopped != "" || n.Error != "" || !n.Sequence || !n.Cleanup.Attempted || !n.Cleanup.Complete || !n.Complete {
		t.Fatal("native tier-ops checkpoint incomplete", n.Stopped, n.Error, n.Cleanup)
	}
	if len(n.API.Symbols) != 13 || !strings.HasSuffix(n.API.Symbols["operate_read"], "4listEi") || !strings.HasSuffix(n.API.Symbols["operate_write"], "20ObjectWriteOperationEi") {
		t.Fatal("native C++ entry points differ", n.API.Symbols)
	}
	wantFlags := map[string]int{"cache-flush": tierOpsFlushFlags, "cache-try-flush": tierOpsTryFlags, "cache-evict": tierOpsEvictFlags, "cache-pin": tierOpsPinFlags, "cache-unpin": tierOpsPinFlags}
	if !reflect.DeepEqual(n.API.Flags, wantFlags) {
		t.Fatal("operation flag layering differs", n.API.Flags)
	}
	if len(n.Placements) != 6 {
		t.Fatal("tier-ops placements incomplete", len(n.Placements))
	}
	for _, p := range n.Placements {
		if (p.PoolName != plan.Base && p.PoolName != plan.Tier) || p.Namespace != plan.Namespace || p.PrimaryID != 0 || p.Shard != -1 || p.Epoch == 0 || !strings.HasPrefix(p.Address, "v2:") || !strings.HasPrefix(p.Name, "tier-ops-") {
			t.Fatal("tier-ops placement differs", p)
		}
	}
	steps := map[string]tierOpsStep{}
	identities := map[string]string{"observer": "client.test", "rw": plan.Clients["rw"], "ro": plan.Clients["ro"], "base": plan.Clients["base"]}
	for _, s := range n.Steps {
		if _, dup := steps[s.Label]; dup {
			t.Fatal("repeated native step", s.Label)
		}
		steps[s.Label] = s
		if s.State != "completed" || s.Namespace != plan.Namespace || s.Wire != tierOpsWire(s.Flags) || s.Identity.Name != identities[s.Role] || s.Identity.GlobalID == 0 {
			t.Fatal("native step state, flags or identity differs", s.Label, s.State, s.Flags, s.Wire, s.Identity)
		}
		if strings.HasPrefix(s.Action, "cache-") && (s.Pool != plan.Tier || s.Role == "observer" || !s.MayMutate || s.Result.PerOp == nil || *s.Result.PerOp) {
			t.Fatal("cache operation left the tier, used the observer or claimed per-op results", s.Label)
		}
	}
	for label, want := range tierOpsExpected {
		s, ok := steps[label]
		if !ok || s.Action != want.action || s.Role != want.role || s.Flags != want.flags || s.Result.Code != want.code {
			t.Errorf("native %s: action=%s role=%s flags=%d code=%d; expected %+v", label, s.Action, s.Role, s.Flags, s.Result.Code, want)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
	obs := func(label string) map[string]tierOpsState {
		o, ok := n.Observations[label]
		if !ok {
			t.Fatal("missing native observation", label)
		}
		return o
	}
	for _, name := range []string{"tier-ops-flush", "tier-ops-try"} {
		seeded := obs("seeded-" + name)
		if !tierOpsHolds(seeded["base"], cachePoolOriginal) || !tierOpsHolds(seeded["tier"], cachePoolChanged) {
			t.Fatal("seeded base/tier bytes differ", name)
		}
		tierOpsDirty(t, steps, "seeded-dirty-"+name, true)
	}
	seeded := obs("seeded-tier-ops-flush")
	for _, label := range []string{"after-evict-dirty", "pinned", "unpinned"} {
		o := obs(label)
		// Pin/unpin keep the user version but take the request mtime.
		if !tierOpsSame(o["base"], seeded["base"]) || !tierOpsHolds(o["tier"], cachePoolChanged) || o["tier"].Version != seeded["tier"].Version {
			t.Fatal("refused cache operation changed base or tier", label)
		}
	}
	tierOpsDirty(t, steps, "pinned-dirty", true)
	flushed := obs("flushed")
	if !tierOpsHolds(flushed["base"], cachePoolChanged) || !tierOpsHolds(flushed["tier"], cachePoolChanged) {
		t.Fatal("blocking flush did not leave B in base and tier", flushed)
	}
	tierOpsDirty(t, steps, "flushed-dirty", false)
	evicted := obs("evicted")
	if evicted["tier"].Code != -2 || !tierOpsSame(evicted["base"], flushed["base"]) {
		t.Fatal("clean eviction did not remove only the tier head", evicted)
	}
	if !tierOpsHolds(steps["ordinary-read-after-evict"].Result, cachePoolChanged) || !steps["ordinary-read-after-evict"].MayMutate {
		t.Fatal("ordinary tier read did not return flushed base bytes")
	}
	promoted := obs("promoted")
	if !tierOpsHolds(promoted["tier"], cachePoolChanged) || !tierOpsSame(promoted["base"], flushed["base"]) {
		t.Fatal("promotion after eviction differs", promoted)
	}
	tierOpsDirty(t, steps, "promoted-dirty", false)
	try := steps["try-flush"]
	if try.Action != "cache-try-flush" || try.Role != "rw" || try.Flags != tierOpsTryFlags || try.Object != "tier-ops-try" || n.TryFlush.Code != try.Result.Code || !n.TryFlush.Definite || n.TryFlush.Reissued || len(n.TryFlush.Dirty) == 0 {
		t.Fatal("try-flush receipt differs", try.Result.Code, n.TryFlush)
	}
	tried := obs("try-flushed")
	last := n.TryFlush.Dirty[len(n.TryFlush.Dirty)-1]
	switch try.Result.Code {
	case 0:
		if last == nil || *last || !tierOpsHolds(tried["base"], cachePoolChanged) || !tierOpsHolds(tried["tier"], cachePoolChanged) {
			t.Fatal("successful try-flush did not clean the head and write B to base", tried)
		}
	case -11, -16:
		// Retained as its own outcome; the controller does not reissue it.
		if !tierOpsHolds(tried["tier"], cachePoolChanged) {
			t.Fatal("unsuccessful try-flush changed tier bytes")
		}
	default:
		t.Fatal("unclassified try-flush result", try.Result.Code)
	}
	for _, label := range []string{"absent-initial", "absent-final"} {
		o := obs(label)
		if o["base"].Code != -2 || o["tier"].Code != -2 {
			t.Fatal("absent control head exists", label)
		}
	}
	for _, pool := range []string{"tier", "base"} {
		for _, name := range []string{"tier-ops-flush", "tier-ops-try", "tier-ops-absent"} {
			if s := steps["cleanup-absent-"+pool+"-"+name]; s.Result.Code != -2 || s.Flags != tierOpsIgnoreCache {
				t.Fatal("owned tier-ops head remains", pool, name)
			}
		}
	}
}

func TestCephOSDCacheTierOpsIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_CACHE_POOLS") != "1" {
		t.Skip("requires isolated cache-pool fixture")
	}
	directory := ordinaryLogFixture(t)
	if os.Getenv("CEPH_MSGR_TEST_OSD") != "1" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" || isReplicatedPGFixtureProfile() {
		t.Fatal("native tier-ops checkpoint requires legacy-v6 single BlueStore")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	initial, _, err := cachePoolReadNative(directory)
	if err != nil || initial.Source != mdsFreshSource || initial.Plan.FSID != os.Getenv("CEPH_MSGR_FSID") || initial.Phase != "prepared" {
		t.Fatal("cache-pool fixture not prepared", initial.Phase, err)
	}
	control := func(wait context.Context, action string) error {
		return testcluster.RequestControl(wait, directory, "osd-cache-pools", "osd-cache-pools-completed", initial.Plan.Token, action)
	}
	results := map[string]any{"source_commit": mdsFreshSource, "plan": initial.Plan, "native_only": true, "product_support_claimed": false}
	t.Cleanup(func() {
		clean, stop := context.WithTimeout(context.Background(), 3*time.Minute)
		defer stop()
		finishErr := control(clean, "finish")
		final, _, readErr := cachePoolReadNative(directory)
		var fixture struct {
			Finished bool           `json:"finished"`
			Selected bool           `json:"tier_ops_selected"`
			Cleanup  map[string]any `json:"cleanup"`
			Error    string         `json:"cleanup_error"`
		}
		raw, fixtureErr := os.ReadFile(filepath.Join(directory, "osd-cache-pools-fixture.json"))
		if fixtureErr == nil {
			fixtureErr = json.Unmarshal(raw, &fixture)
		}
		results["finish"] = map[string]any{"native_phase": final.Phase, "native_cleanup_complete": final.CleanupComplete, "fixture_finished": fixture.Finished, "fixture_cleanup": fixture.Cleanup, "fixture_cleanup_error": fixture.Error}
		if err := errors.Join(finishErr, readErr, fixtureErr); err != nil || !final.CleanupComplete || final.Phase != "finished" || !fixture.Finished || !fixture.Selected {
			t.Error("owned cache-pool cleanup", err, final.Phase, fixture.Finished)
		}
		for _, gate := range []string{"owned_pools_absent", "owned_rules_absent", "owned_clients_absent", "pool_delete_override_absent"} {
			if fixture.Cleanup[gate] != true {
				t.Error("fixture cleanup gate not confirmed", gate)
			}
		}
		encoded, e := json.MarshalIndent(results, "", "  ")
		if e == nil {
			e = testcluster.WritePublicEvidence(filepath.Join(directory, "osd-cache-tier-ops-results.json"), append(encoded, '\n'))
		}
		if e != nil {
			t.Error(e)
		}
	})
	controlErr := control(ctx, "tier-ops")
	raw, err := os.ReadFile(filepath.Join(directory, "osd-cache-tier-ops-native.json"))
	if err != nil || len(raw) > 1<<20 {
		t.Fatal("native tier-ops receipt", controlErr, err)
	}
	results["native_sha256"], results["native_bytes"] = fmt.Sprintf("%x", sha256.Sum256(raw)), len(raw)
	var n tierOpsReceipt
	if err := json.Unmarshal(raw, &n); err != nil {
		t.Fatal(err)
	}
	codes := map[string]int32{}
	for _, s := range n.Steps {
		if strings.HasPrefix(s.Action, "cache-") {
			codes[s.Label] = s.Result.Code
		}
	}
	results["cache_operation_codes"], results["try_flush"], results["stopped"] = codes, n.TryFlush, n.Stopped
	if controlErr != nil {
		t.Fatal("tier-ops control", controlErr, n.Stopped, n.Error)
	}
	validateTierOpsReceipt(t, n, initial.Plan)
	results["validated"] = true
}
