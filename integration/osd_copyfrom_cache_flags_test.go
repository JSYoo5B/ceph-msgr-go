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

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

type cacheCopyFlagsPair struct {
	Base tierOpsState `json:"base"`
	Tier tierOpsState `json:"tier"`
}

type cacheCopyFlagsNative struct {
	Source      string                        `json:"source_commit"`
	Checkpoint  string                        `json:"checkpoint"`
	Plan        cachePoolPlan                 `json:"plan"`
	Issues      bool                          `json:"issues_copy_or_cache_operations"`
	Phase       string                        `json:"phase"`
	Error       string                        `json:"error"`
	Placements  []cachePoolPlacement          `json:"placements"`
	Seeded      map[string]cacheCopyFlagsPair `json:"seeded"`
	SeededDirty tierOpsState                  `json:"seeded_dirty"`
	Labels      []struct {
		Label  string             `json:"label"`
		Object string             `json:"object"`
		States cacheCopyFlagsPair `json:"states"`
	} `json:"labels"`
	Cleanup struct {
		Attempted bool `json:"attempted"`
		Complete  bool `json:"complete"`
	} `json:"cleanup"`
}

type cacheCopyFlagsRecord struct {
	Cell    string              `json:"cell"`
	Request cephmsgr.OSDRequest `json:"request"`
	Reply   cephmsgr.OSDReply   `json:"reply"`
	Error   string              `json:"error,omitempty"`
	Before  cacheCopyFlagsPair  `json:"destination_before"`
	After   cacheCopyFlagsPair  `json:"destination_after"`
}

const (
	cacheCopyDirty = "go-copy-dirty"
	// cacheCopyFixedMTime is the observer's seed mtime
	// (tools/osd_cache_tier_ops_oracle.py), which a copy carries over.
	cacheCopyFixedMTime = 1700000800
)

func cacheCopyFlagsReadNative(directory string) (cacheCopyFlagsNative, []byte, error) {
	var value cacheCopyFlagsNative
	raw, err := os.ReadFile(filepath.Join(directory, "osd-cache-copy-flags-native.json"))
	if err != nil {
		return value, nil, err
	}
	if len(raw) > 1<<20 {
		return value, nil, errors.New("native copy cache-flag receipt exceeds bound")
	}
	err = json.Unmarshal(raw, &value)
	return value, raw, err
}

// TestCephOSDCopyFromCacheFlagsIntegration sends COPY_FROM and COPY_FROM2
// with SourceIgnoreCache and SourceIgnoreOverlay from Go (identity scoped to
// the base pool) into destinations in the base pool of the owned writeback
// tier, from sources the closed native observer seeded: one head dirty in
// the tier (tier B over base A) and, per opcode, one head in the base alone.
// The destination OSD reads the source with its own Objecter, and
// _copy_some turns the copy flags into that read's IGNORE_CACHE and
// IGNORE_OVERLAY (ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc).
//
// Without an overlay: a copy of the dirty tier head gets B with or without
// IGNORE_CACHE; a tier miss with IGNORE_CACHE is -ENOENT with per-op
// -EINPROGRESS and promotes nothing (maybe_handle_cache returns early), while
// the flagless copy of the same miss promotes A into the tier (writeback mode
// without a hit set) and copies it; a copy of the base head gets A with or
// without IGNORE_OVERLAY. With the base pool's overlay set to the tier, the
// flagless copy of the base head is redirected to the tier and gets B
// (Objecter::_calc_target, src/osdc/Objecter.cc), and IGNORE_OVERLAY, alone or
// with IGNORE_CACHE, still reads the base and gets A. Every success gives the
// destination the source's bytes, size, user version and mtime; every
// failure leaves its Go-seeded sentinel unchanged; the native observer reads
// with IGNORE_CACHE and IGNORE_OVERLAY, so it names the pool it reads and
// promotes nothing. The copies do not change the dirty source. The overlay is
// removed before the checked finish. Not verified: other profiles, EC tiers,
// hit sets and the tier agent, a proxied copy read, flags without the opt-in
// live (refused before submission, unit-tested), FLUSH and RWORDERED
// (refused, see internal/osd/copyfrom.go).
//
//scenario:row osd-cache-pools.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-osdcopyfromcacheflags key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 osd_cache_pools=1 capabilities=exclusive-osd-cache-pools
//scenario:covers CLIENT to OSD
//fixture:isolated sets and removes the base pool overlay of the cache-pool fixture and promotes owned heads into its tier
func TestCephOSDCopyFromCacheFlagsIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_CACHE_POOLS") != "1" {
		t.Skip("requires isolated cache-pool fixture")
	}
	directory := ordinaryLogFixture(t)
	if os.Getenv("CEPH_MSGR_TEST_OSD") != "1" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" || isReplicatedPGFixtureProfile() {
		t.Fatal("copy cache-flag acceptance requires legacy-v6 single BlueStore")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	initial, _, err := cachePoolReadNative(directory)
	if err != nil || initial.Source != mdsFreshSource || initial.Plan.FSID != os.Getenv("CEPH_MSGR_FSID") || initial.Phase != "prepared" {
		t.Fatal("cache-pool fixture preparation incomplete", initial.Phase, err)
	}
	plan := initial.Plan
	records := []cacheCopyFlagsRecord{}
	controls := []map[string]any{}
	receipt := map[string]any{"source_commit": mdsFreshSource, "plan": plan}
	control := func(wait context.Context, args ...string) (cacheCopyFlagsNative, error) {
		err := testcluster.RequestControl(wait, directory, "osd-cache-pools", "osd-cache-pools-completed", append([]string{plan.Token, "copy-flags"}, args...)...)
		n, raw, readErr := cacheCopyFlagsReadNative(directory)
		controls = append(controls, map[string]any{"args": args, "native_bytes": len(raw), "native_sha256": fmt.Sprintf("%x", sha256.Sum256(raw)), "phase": n.Phase, "native_error": n.Error, "error": fmt.Sprint(err)})
		if readErr == nil && (!reflect.DeepEqual(n.Plan, plan) || n.Source != mdsFreshSource || n.Checkpoint != "cache-copy-flags-observer-v1" || n.Issues) {
			return n, errors.New("foreign native copy cache-flag observer response")
		}
		return n, errors.Join(err, readErr)
	}
	var clients []*cephmsgr.Client
	prepared, overlay := false, false
	t.Cleanup(func() {
		for _, c := range clients {
			c.Close()
		}
		clean, stop := context.WithTimeout(context.Background(), 3*time.Minute)
		defer stop()
		if overlay {
			if _, err := control(clean, "overlay-remove"); err != nil {
				t.Error("remove the base pool overlay", err)
			}
		}
		if prepared {
			final, err := control(clean, "finish")
			receipt["final_native"] = map[string]any{"phase": final.Phase, "cleanup": final.Cleanup}
			if err != nil || !final.Cleanup.Complete || final.Phase != "finished" {
				t.Error("owned copy cache-flag head cleanup", err, final.Error)
			}
		}
		receipt["requests"], receipt["controls"] = records, controls
		encoded, e := json.MarshalIndent(receipt, "", "  ")
		if e == nil {
			e = testcluster.WritePublicEvidence(filepath.Join(directory, "osd-cache-copy-flags-results.json"), append(encoded, '\n'))
		}
		if e != nil {
			t.Error(e)
		}
	})
	n, err := control(ctx, "prepare")
	prepared = err == nil || n.Phase != ""
	if err != nil || n.Phase != "prepared" || len(n.Placements) != 10 || len(n.Seeded) != 5 {
		t.Fatal("native copy cache-flag preparation", n.Phase, n.Error, err)
	}
	dirty := n.Seeded[cacheCopyDirty]
	if !tierOpsHolds(dirty.Base, cachePoolOriginal) || !tierOpsHolds(dirty.Tier, cachePoolChanged) || n.SeededDirty.Dirty == nil || !*n.SeededDirty.Dirty ||
		dirty.Base.MTime != cacheCopyFixedMTime || dirty.Tier.MTime != cacheCopyFixedMTime {
		t.Fatal("native dirty source differs", dirty, n.SeededDirty)
	}
	misses := []string{"go-copy-miss-0", "go-copy-miss-1"}
	for _, name := range misses {
		if s := n.Seeded[name]; !tierOpsHolds(s.Base, cachePoolOriginal) || s.Tier.Code != -2 {
			t.Fatal("native miss source differs", name, s)
		}
	}
	routes := cachePoolNative{Plan: plan, Placements: n.Placements}
	route := func(pool, name string) cachePoolPlacement { return cachePoolRoute(t, routes, pool, name) }
	baseID, tierID := route(plan.Base, cacheCopyDirty).Pool, route(plan.Tier, cacheCopyDirty).Pool
	observe := func(t *testing.T, name string) cacheCopyFlagsPair {
		t.Helper()
		n, err := control(ctx, "observe", name)
		if err != nil || len(n.Labels) == 0 || n.Labels[len(n.Labels)-1].Object != name {
			t.Fatal("native observation", name, n.Error, err)
		}
		return n.Labels[len(n.Labels)-1].States
	}

	options := integrationOptions(t)
	options.EnableOSD, options.EnableOSDCachePools = true, true
	key, err := os.ReadFile(filepath.Join(directory, "osd-cache-pools-base.key"))
	if err != nil {
		t.Fatal(err)
	}
	if options.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(key))); err != nil {
		t.Fatal(err)
	}
	options.Identity = plan.Clients["base"]
	client, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	clients = append(clients, client)
	receipt["identity"] = client.Snapshot()

	type copyCell struct {
		name        string
		tier        bool // source in the tier pool, else the base pool
		object      string
		ignoreCache bool
		ignoreOver  bool
		aggregate   int32
		want        tierOpsState // the copied source on success
	}
	sentinel := uint32(0)
	run := func(t *testing.T, code cephmsgr.OSDOpcode, dst string, tc copyCell) {
		t.Helper()
		r := route(plan.Base, dst)
		o, err := client.OpenOSD(ctx, cephmsgr.OSDTarget{ID: r.ID, Address: r.Address})
		if err != nil {
			t.Fatal("explicit OSD", err)
		}
		defer o.Close()
		object := cephmsgr.OSDObject{Pool: r.Pool, Name: r.Name, Namespace: r.Namespace, Hash: r.Hash}
		label := []byte(fmt.Sprintf("sentinel-%x-%s", code, tc.name))
		sentinel++
		seed := cephmsgr.OSDRequest{MapEpoch: r.Epoch, Object: object, ModificationTime: cephmsgr.OSDTime{Seconds: 1700098000 + sentinel},
			Operations: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDWriteFull, Length: uint64(len(label)), Data: label}}}
		if reply, err := o.Request(ctx, seed); err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("destination seed", tc.name, reply, err)
		}
		before := observe(t, dst)
		if !tierOpsHolds(before.Base, label) || before.Tier.Code != -2 || before.Base.MTime != int64(1700098000+sentinel) {
			t.Fatal("native destination sentinel differs", tc.name, before)
		}
		pool := baseID
		if tc.tier {
			pool = tierID
		}
		in := cephmsgr.OSDCopyFromInput{SourcePool: pool, SourceName: tc.object, SourceNamespace: plan.Namespace, SourceIgnoreCache: tc.ignoreCache, SourceIgnoreOverlay: tc.ignoreOver}
		request := cephmsgr.OSDRequest{MapEpoch: r.Epoch, Object: object, ModificationTime: cephmsgr.OSDTime{Seconds: 1700099000},
			Operations: []cephmsgr.OSDOperation{{Code: code, CopyFrom: in}}}
		reply, err := o.Request(ctx, request)
		entry := cacheCopyFlagsRecord{Cell: fmt.Sprintf("%x/%s", code, tc.name), Request: request, Reply: reply, Before: before}
		if err != nil {
			entry.Error = err.Error()
		}
		entry.After = observe(t, dst)
		records = append(records, entry)
		t.Logf("%s: aggregate=%d tid=%d uv=%d epoch=%d err=%v", entry.Cell, reply.Code, reply.Transaction, reply.UserVersion, reply.MapEpoch, err)
		var server *cephmsgr.OSDError
		var unknown *cephmsgr.OutcomeUnknownError
		if errors.As(err, &unknown) || tc.aggregate == 0 && (err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0) ||
			tc.aggregate != 0 && (!errors.As(err, &server) || server.Code != tc.aggregate) {
			t.Fatalf("%s: copy result %+v %v", tc.name, reply, err)
		}
		perOp := int32(0)
		if tc.aggregate != 0 {
			perOp = -115
		}
		if reply.Code != tc.aggregate || reply.Transaction == 0 || reply.Submission.GlobalID != client.Snapshot().GlobalID || reply.Object != dst ||
			len(reply.Operations) != 1 || reply.Operations[0].Code != code || reply.Operations[0].Result != perOp {
			t.Fatalf("%s: raw receipt %+v", tc.name, reply)
		}
		after := entry.After
		if after.Tier.Code != -2 {
			t.Fatalf("%s: the destination appeared in the tier: %+v", tc.name, after.Tier)
		}
		if tc.aggregate != 0 {
			if !tierOpsSame(before.Base, after.Base) {
				t.Fatalf("%s: a failed copy changed the destination: %+v", tc.name, after.Base)
			}
			return
		}
		if !tierOpsHolds(after.Base, tc.want.Data) || after.Base.Version != tc.want.Version || reply.UserVersion != after.Base.Version || after.Base.MTime != tc.want.MTime {
			t.Fatalf("%s: native destination %+v is not the copied source %+v", tc.name, after.Base, tc.want)
		}
	}
	codes := []cephmsgr.OSDOpcode{cephmsgr.OSDCopyFrom, cephmsgr.OSDCopyFrom2}
	destinations := []string{"go-copy-dst-0", "go-copy-dst-1"}
	t.Run("no-overlay", func(t *testing.T) {
		for i, code := range codes {
			dst, miss := destinations[i], misses[i]
			for _, tc := range []copyCell{
				{name: "tier-dirty", tier: true, object: cacheCopyDirty, want: dirty.Tier},
				{name: "tier-dirty-ignore-cache", tier: true, object: cacheCopyDirty, ignoreCache: true, want: dirty.Tier},
				{name: "tier-miss-ignore-cache", tier: true, object: miss, ignoreCache: true, aggregate: -2},
			} {
				run(t, code, dst, tc)
			}
			if s := observe(t, miss); s.Tier.Code != -2 || !tierOpsSame(s.Base, n.Seeded[miss].Base) {
				t.Fatal("an IGNORE_CACHE copy promoted the miss or changed its base", s)
			}
			run(t, code, dst, copyCell{name: "tier-miss-promote", tier: true, object: miss, want: n.Seeded[miss].Base})
			if s := observe(t, miss); !tierOpsHolds(s.Tier, cachePoolOriginal) || s.Tier.Version != n.Seeded[miss].Base.Version || !tierOpsSame(s.Base, n.Seeded[miss].Base) {
				t.Fatal("the flagless copy did not promote the base head into the tier", s)
			}
			for _, tc := range []copyCell{
				{name: "base", object: cacheCopyDirty, want: dirty.Base},
				{name: "base-ignore-overlay", object: cacheCopyDirty, ignoreOver: true, want: dirty.Base},
			} {
				run(t, code, dst, tc)
			}
		}
	})
	if t.Failed() {
		return
	}
	overlay = true
	if _, err := control(ctx, "overlay-set"); err != nil {
		t.Fatal("set the base pool overlay", err)
	}
	t.Run("overlay", func(t *testing.T) {
		for i, code := range codes {
			for _, tc := range []copyCell{
				{name: "overlay-base", object: cacheCopyDirty, want: dirty.Tier},
				{name: "overlay-base-ignore-overlay", object: cacheCopyDirty, ignoreOver: true, want: dirty.Base},
				{name: "overlay-base-ignore-both", object: cacheCopyDirty, ignoreOver: true, ignoreCache: true, want: dirty.Base},
			} {
				run(t, code, destinations[i], tc)
			}
		}
	})
	if _, err := control(ctx, "overlay-remove"); err != nil {
		t.Fatal("remove the base pool overlay", err)
	}
	overlay = false
	if s := observe(t, cacheCopyDirty); !tierOpsSame(s.Base, dirty.Base) || !tierOpsSame(s.Tier, dirty.Tier) || !bytes.Equal(s.Tier.Data, cachePoolChanged) {
		t.Fatal("the copies changed the dirty source", s)
	}
}
