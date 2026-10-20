package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
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

var cachePoolOriginal = []byte("base\x00\xff\x80\ncache-proof")
var cachePoolChanged = []byte("tier\x00\x80\xff\nreplacement")

type cachePoolPlan struct {
	Source    string            `json:"source_commit"`
	FSID      string            `json:"fsid"`
	Token     string            `json:"token"`
	Base      string            `json:"base"`
	Tier      string            `json:"tier"`
	Namespace string            `json:"namespace"`
	Clients   map[string]string `json:"clients"`
}
type cachePoolState struct {
	Code       int32  `json:"code"`
	Data       []byte `json:"bytes_b64"`
	Size       uint64 `json:"size"`
	Version    uint64 `json:"version"`
	MTime      int64  `json:"mtime"`
	StatResult int32  `json:"stat_prval"`
	ReadResult int32  `json:"read_prval"`
}
type cachePoolPair struct{ Base, Tier cachePoolState }
type cachePoolCaseEvidence struct {
	GoAfter             cachePoolPair  `json:"go_after"`
	GoCompared          cachePoolPair  `json:"go_after_native_comparison"`
	NativeResult        cachePoolState `json:"native_result"`
	NativeAfter         cachePoolPair  `json:"native_after"`
	GoFollowup          cachePoolState `json:"go_followup"`
	NativeFollowup      cachePoolState `json:"native_followup"`
	GoAfterFollowup     cachePoolPair  `json:"go_after_followup"`
	NativeAfterFollowup cachePoolPair  `json:"native_after_followup"`
	Complete            bool           `json:"complete"`
}
type cachePoolPlacement struct {
	osdFixtureOracle
	PoolName string `json:"pool_name"`
}
type cachePoolNative struct {
	Source           string                           `json:"source_commit"`
	Plan             cachePoolPlan                    `json:"plan"`
	Phase            string                           `json:"phase"`
	Complete         bool                             `json:"complete"`
	CleanupComplete  bool                             `json:"cleanup_complete"`
	CasesComplete    bool                             `json:"cases_complete"`
	Placements       []cachePoolPlacement             `json:"placements"`
	ForeignPlacement cachePoolPlacement               `json:"foreign_placement"`
	Baseline         map[string]cachePoolPair         `json:"baseline"`
	Cases            map[string]cachePoolCaseEvidence `json:"cases"`
	Steps            []json.RawMessage                `json:"steps"`
}
type cachePoolGoRecord struct {
	Case    string              `json:"case"`
	Route   cachePoolPlacement  `json:"native_route"`
	Request cephmsgr.OSDRequest `json:"request"`
	Reply   cephmsgr.OSDReply   `json:"reply"`
	Error   string              `json:"error,omitempty"`
	Unknown bool                `json:"unknown_outcome"`
}

func cachePoolReadNative(directory string) (cachePoolNative, []byte, error) {
	var value cachePoolNative
	raw, err := os.ReadFile(filepath.Join(directory, "osd-cache-pools-native.json"))
	if err != nil {
		return value, nil, err
	}
	if len(raw) > 1<<20 {
		return value, nil, errors.New("native cache receipt exceeds bound")
	}
	err = json.Unmarshal(raw, &value)
	return value, raw, err
}

func cachePoolRoute(t *testing.T, n cachePoolNative, pool, name string) cachePoolPlacement {
	t.Helper()
	var matches []cachePoolPlacement
	for _, p := range n.Placements {
		if p.PoolName == pool && p.Name == name {
			matches = append(matches, p)
		}
	}
	if len(matches) != 1 {
		t.Fatal("missing or ambiguous native placement", pool, name)
	}
	p := matches[0]
	if p.Pool < 0 || p.Namespace != n.Plan.Namespace || p.ID != 0 || p.PrimaryID != 0 || p.Shard != -1 || p.PoolType != 1 || p.Replicas != 1 || p.MinSize != 1 || p.PG != 0 || p.Epoch == 0 || len(p.Acting) != 1 || p.Acting[0] != 0 || !reflect.DeepEqual(p.Acting, p.Up) || !strings.HasPrefix(p.Address, "v2:") || p.RawPGID != fmt.Sprintf("%d.%x", p.Pool, p.Hash) || p.PGID != fmt.Sprintf("%d.0", p.Pool) {
		t.Fatal("owned replicated primary placement differs", p)
	}
	return p
}

func cachePoolRequest(route cachePoolPlacement, which string) cephmsgr.OSDRequest {
	op, mode, _ := strings.Cut(which, "-")
	operation := cephmsgr.OSDOperation{}
	switch op {
	case "stat":
		operation.Code = cephmsgr.OSDStat
	case "read":
		operation.Code = cephmsgr.OSDRead
		operation.Length = 4096
	case "writefull":
		operation.Code = cephmsgr.OSDWriteFull
		operation.Length = uint64(len(cachePoolChanged))
		operation.Data = cachePoolChanged
	case "delete":
		operation.Code = cephmsgr.OSDDelete
	default:
		panic("closed cache operation")
	}
	r := cephmsgr.OSDRequest{MapEpoch: route.Epoch, Object: cephmsgr.OSDObject{Pool: route.Pool, Name: route.Name, Namespace: route.Namespace, Hash: route.Hash}, Operations: []cephmsgr.OSDOperation{operation}, IgnoreCache: mode == "ignore"}
	if op == "writefull" || op == "delete" {
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000800}
	}
	return r
}

func cachePoolStateEqual(a, b cachePoolState) bool {
	return a.Code == b.Code && (a.Code < 0 || a.Size == b.Size && a.MTime == b.MTime && bytes.Equal(a.Data, b.Data))
}
func cachePoolOwnUnchanged(t *testing.T, label string, before, after cachePoolState) {
	t.Helper()
	if !cachePoolStateEqual(before, after) || before.Code == 0 && before.Version != after.Version {
		t.Fatal("owned object changed", label, before, after)
	}
}
func cachePoolCompare(t *testing.T, which string, r cephmsgr.OSDReply, baseline cachePoolPair, p cachePoolCaseEvidence, nativeBefore cachePoolPair) {
	t.Helper()
	op, mode, _ := strings.Cut(which, "-")
	want := int32(0)
	if (op == "read" || op == "stat") && mode == "ignore" {
		want = -2
	}
	if r.Code != want || p.NativeResult.Code != want || !p.Complete || len(r.Operations) != 1 {
		t.Fatal("Go/native standalone result differs", which, r.Code, p.NativeResult.Code)
	}
	if r.Operations[0].Result != want && !(want < 0 && r.Operations[0].Result == 0) {
		t.Fatal("signed per-op result differs", which, r.Operations[0].Result, want)
	}
	if !cachePoolStateEqual(p.GoCompared.Base, p.NativeAfter.Base) || !cachePoolStateEqual(p.GoCompared.Tier, p.NativeAfter.Tier) {
		t.Fatal("paired standalone operations produced different states", which, p.GoCompared, p.NativeAfter)
	}
	cachePoolOwnUnchanged(t, which+" Go base", baseline.Base, p.GoCompared.Base)
	cachePoolOwnUnchanged(t, which+" native base", nativeBefore.Base, p.NativeAfter.Base)
	if want == 0 {
		if r.UserVersion == 0 || p.NativeResult.Version == 0 {
			t.Fatal("successful operation omitted user version", which)
		}
		switch op {
		case "read":
			if !bytes.Equal(r.Operations[0].Data, cachePoolOriginal) || !bytes.Equal(p.NativeResult.Data, cachePoolOriginal) || p.NativeResult.ReadResult != 0 {
				t.Fatal("standalone read bytes/prval differ")
			}
		case "stat":
			data := r.Operations[0].Data
			if len(data) != 16 || binary.LittleEndian.Uint64(data) != uint64(len(cachePoolOriginal)) || binary.LittleEndian.Uint32(data[8:]) != 1700000800 || binary.LittleEndian.Uint32(data[12:]) != 0 || p.NativeResult.Size != uint64(len(cachePoolOriginal)) || p.NativeResult.MTime != 1700000800 || p.NativeResult.StatResult != 0 {
				t.Fatal("standalone stat size/mtime differs")
			}
		case "writefull":
			if p.GoCompared.Tier.Code != 0 || !bytes.Equal(p.GoCompared.Tier.Data, cachePoolChanged) || r.UserVersion != p.GoCompared.Tier.Version || r.UserVersion <= baseline.Tier.Version {
				t.Fatal("writefull bytes or own object version differs")
			}
			if p.NativeResult.Version != p.NativeAfter.Tier.Version || p.NativeResult.Version <= nativeBefore.Tier.Version {
				t.Fatal("native writefull differs from its own object version")
			}
		case "delete":
			if p.GoCompared.Tier.Code != -2 || r.UserVersion <= baseline.Tier.Version {
				t.Fatal("delete local state/own version differs")
			}
			if p.NativeResult.Version <= nativeBefore.Tier.Version {
				t.Fatal("native delete did not advance its own version")
			}
		}
		if op == "read" || op == "stat" {
			own := baseline.Base.Version
			if p.GoCompared.Tier.Code == 0 {
				own = p.GoCompared.Tier.Version
			}
			if r.UserVersion != own {
				t.Fatal("Go reply version differs from its own native object observation", r.UserVersion, own)
			}
			nativeOwn := nativeBefore.Base.Version
			if p.NativeAfter.Tier.Code == 0 {
				nativeOwn = p.NativeAfter.Tier.Version
			}
			if p.NativeResult.Version != nativeOwn {
				t.Fatal("native reply version differs from its own object observation", p.NativeResult.Version, nativeOwn)
			}
		}
	}
	if op == "delete" {
		follow := int32(-2)
		if mode == "ignore" {
			follow = 0
		}
		if p.GoFollowup.Code != follow || p.NativeFollowup.Code != follow || !cachePoolStateEqual(p.GoAfterFollowup.Tier, p.NativeAfterFollowup.Tier) {
			t.Fatal("subsequent native reads disagree with bounded whiteout behavior", which, p.GoFollowup.Code, p.NativeFollowup.Code)
		}
		if follow == 0 && (!bytes.Equal(p.GoFollowup.Data, cachePoolOriginal) || !bytes.Equal(p.NativeFollowup.Data, cachePoolOriginal)) {
			t.Fatal("base bytes not preserved after ignore delete")
		}
		cachePoolOwnUnchanged(t, which+" Go base after followup", baseline.Base, p.GoAfterFollowup.Base)
		cachePoolOwnUnchanged(t, which+" native base after followup", nativeBefore.Base, p.NativeAfterFollowup.Base)
	}
}

func TestCephOSDIgnoreCacheIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_CACHE_POOLS") != "1" {
		t.Skip("requires isolated cache-pool fixture")
	}
	directory := ordinaryLogFixture(t)
	if os.Getenv("CEPH_MSGR_TEST_OSD") != "1" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" || isReplicatedPGFixtureProfile() {
		t.Fatal("live cache acceptance requires legacy-v6 single BlueStore")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	initial, initialRaw, err := cachePoolReadNative(directory)
	if err != nil || initial.Source != mdsFreshSource || initial.Plan.FSID != os.Getenv("CEPH_MSGR_FSID") || initial.Phase != "prepared" || len(initial.Baseline) != 16 || len(initial.Placements) != 32 {
		t.Fatal("native cache preparation incomplete", initial.Phase, err)
	}
	records := []cachePoolGoRecord{}
	controls := []map[string]any{}
	receipt := map[string]any{"source_commit": mdsFreshSource, "plan": initial.Plan, "initial_native_phase": initial.Phase, "native_default_osd_refusal": "synthetic handshake evidence only; public default Dial is refused first by MON"}
	receipt["prepared_native_sha256"], receipt["prepared_native_bytes"] = fmt.Sprintf("%x", sha256.Sum256(initialRaw)), len(initialRaw)
	control := func(wait context.Context, action, which string) (cachePoolNative, error) {
		args := []string{initial.Plan.Token, action}
		if which != "" {
			args = append(args, which)
		}
		err := testcluster.RequestControl(wait, directory, "osd-cache-pools", "osd-cache-pools-completed", args...)
		n, raw, readErr := cachePoolReadNative(directory)
		entry := map[string]any{"action": action, "case": which, "native_bytes": len(raw), "native_sha256": fmt.Sprintf("%x", sha256.Sum256(raw)), "phase": n.Phase}
		if which != "" {
			entry["case_evidence"] = n.Cases[which]
		}
		controls = append(controls, entry)
		if !reflect.DeepEqual(n.Plan, initial.Plan) || n.Source != mdsFreshSource {
			return n, errors.New("foreign native cache control response")
		}
		return n, errors.Join(err, readErr)
	}
	var clients []*cephmsgr.Client
	t.Cleanup(func() {
		for _, c := range clients {
			c.Close()
		}
		clean, stop := context.WithTimeout(context.Background(), 3*time.Minute)
		defer stop()
		final, err := control(clean, "finish", "")
		receipt["final_native"] = map[string]any{"phase": final.Phase, "complete": final.Complete, "cases_complete": final.CasesComplete, "cleanup_complete": final.CleanupComplete}
		if err != nil || !final.CleanupComplete {
			t.Error("scoped cache cleanup", err)
		}
		if !t.Failed() && !final.Complete {
			t.Error("native matrix did not complete all selected cases")
		}
		receipt["requests"], receipt["controls"] = records, controls
		encoded, e := json.MarshalIndent(receipt, "", "  ")
		if e == nil {
			e = testcluster.WritePublicEvidence(filepath.Join(directory, "osd-cache-pools-results.json"), append(encoded, '\n'))
		}
		if e != nil {
			t.Error(e)
		}
	})
	if !t.Run("default_client_refuses_native_MON_cache_feature", func(t *testing.T) {
		options := integrationOptions(t)
		options.EnableOSD = true
		local, stop := context.WithTimeout(ctx, 15*time.Second)
		defer stop()
		c, err := cephmsgr.Dial(local, options)
		if c != nil {
			c.Close()
		}
		if !errors.Is(err, cephmsgr.ErrUnsupportedFeatures) || !strings.Contains(err.Error(), fmt.Sprintf("peer requires 0x%016x", uint64(1)<<35)) {
			t.Fatal("default MON admission was not explicitly refused", err)
		}
		receipt["default_MON_refusal"] = err.Error()
	}) {
		return
	}
	dialRole := func(t *testing.T, role string) *cephmsgr.Client {
		t.Helper()
		options := integrationOptions(t)
		options.EnableOSD = true
		options.EnableOSDCachePools = true
		raw, err := os.ReadFile(filepath.Join(directory, "osd-cache-pools-"+role+".key"))
		if err != nil {
			t.Fatal(err)
		}
		options.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(raw)))
		if err != nil {
			t.Fatal(err)
		}
		options.Identity = initial.Plan.Clients[role]
		c, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, c)
		return c
	}
	rw := dialRole(t, "rw")
	if err := rw.WaitMgrReady(ctx); err != nil {
		t.Fatal(err)
	}
	before := rw.Snapshot()
	receipt["primary_before"] = before
	logs, err := rw.WatchLogs(ctx, cephmsgr.LogOptions{StartVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	adminOptions := integrationOptions(t)
	adminOptions.EnableOSD = true
	adminOptions.EnableOSDCachePools = true
	admin, err := cephmsgr.Dial(ctx, adminOptions)
	if err != nil {
		t.Fatal(err)
	}
	clients = append(clients, admin)
	if !t.Run("opted_private_MON_and_MGR_commands_preserve_identity", func(t *testing.T) {
		ping, err := rw.PingMonitor(ctx, "b")
		var parsed struct {
			Status struct {
				Name string `json:"name"`
				Map  struct {
					FSID string `json:"fsid"`
				} `json:"monmap"`
			} `json:"mon_status"`
		}
		if err != nil || json.Unmarshal(ping, &parsed) != nil || parsed.Status.Name != "b" || parsed.Status.Map.FSID != initial.Plan.FSID {
			t.Fatal("private ping", err)
		}
		refused, err := rw.MonTellTo(ctx, "b", cephmsgr.Command{JSON: json.RawMessage(`{"prefix":"version","format":"json"}`)})
		var remote *cephmsgr.CommandError
		if !errors.As(err, &remote) || remote.Code != -13 || refused.Code != -13 || refused.Message != "insufficient caps" {
			t.Fatal("scoped MON read identity lost its known Tell refusal", refused, err)
		}
		receipt["scoped_private_tell_refusal"] = refused
		// Monitor::handle_tell_command requires read, write and execute even
		// for version. Keep the subject's read grant and use the separate
		// opted-in admin identity for this positive daemon-local command.
		tell, err := admin.MonTellTo(ctx, "b", cephmsgr.Command{JSON: json.RawMessage(`{"prefix":"version","format":"json"}`)})
		if err != nil || tell.Code != 0 || len(tell.Data) == 0 {
			t.Fatal("private MON tell", tell.Code, err)
		}
		mgr, err := rw.MgrCommand(ctx, cephmsgr.Command{JSON: json.RawMessage(`{"prefix":"pg stat","format":"json"}`)})
		if err != nil || mgr.Code != 0 || !json.Valid(mgr.Data) {
			t.Fatal("MGR raw read command", mgr.Code, err)
		}
		receipt["private_ping_raw"], receipt["private_tell"], receipt["manager_command"] = json.RawMessage(ping), tell, mgr
	}) {
		return
	}
	subject := func(t *testing.T, c *cephmsgr.Client, which string, r cephmsgr.OSDRequest, route cachePoolPlacement) cephmsgr.OSDReply {
		t.Helper()
		o, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: route.ID, Address: route.Address})
		if err != nil {
			t.Fatal("explicit OSD", err)
		}
		reply, err := o.Request(ctx, r)
		entry := cachePoolGoRecord{Case: which, Route: route, Request: r, Reply: reply}
		var unknown *cephmsgr.OutcomeUnknownError
		entry.Unknown = errors.As(err, &unknown)
		if err != nil {
			entry.Error = err.Error()
		}
		records = append(records, entry)
		if entry.Unknown || reply.Submission.Transaction == 0 || reply.Submission.GlobalID != c.Snapshot().GlobalID || reply.Object != r.Object.Name || reply.PG.Pool != uint64(r.Object.Pool) || reply.PG.Seed != r.Object.Hash || reply.MapEpoch < r.MapEpoch || len(reply.RawFront) == 0 {
			t.Fatal("subject lost raw identity/placement or outcome unknown", which, reply.Code, err)
		}
		if reply.Code >= 0 && err != nil {
			t.Fatal("successful reply returned error", err)
		}
		if reply.Code < 0 {
			var remote *cephmsgr.OSDError
			if !errors.As(err, &remote) || remote.Code != reply.Code {
				t.Fatal("signed server refusal lost its raw error", reply.Code, err)
			}
		}
		return reply
	}
	for _, op := range []string{"stat", "read", "writefull", "delete"} {
		for _, mode := range []string{"normal", "ignore"} {
			which := op + "-" + mode
			if !t.Run(which, func(t *testing.T) {
				route := cachePoolRoute(t, initial, initial.Plan.Tier, which+"-go")
				r := cachePoolRequest(route, which)
				reply := subject(t, rw, which, r, route)
				if _, err := control(ctx, "observe", which); err != nil {
					t.Fatal("native Go readback", err)
				}
				n, err := control(ctx, "native", which)
				if err != nil {
					t.Fatal("independent native counterpart", err)
				}
				cachePoolCompare(t, which, reply, initial.Baseline[which+"-go"], n.Cases[which], initial.Baseline[which+"-native"])
			}) {
				return
			}
		}
	}
	if !t.Run("scoped_pool_namespace_and_readonly_refusals_preserve_state", func(t *testing.T) {
		ro, base := dialRole(t, "ro"), dialRole(t, "base")
		route := cachePoolRoute(t, initial, initial.Plan.Tier, "writefull-ignore-go")
		before, err := control(ctx, "observe", "writefull-ignore")
		if err != nil {
			t.Fatal(err)
		}
		cases := []struct {
			name  string
			c     *cephmsgr.Client
			r     cephmsgr.OSDRequest
			route cachePoolPlacement
		}{
			{"read-only-write", ro, cachePoolRequest(route, "writefull-ignore"), route},
			{"base-only-tier-read", base, cachePoolRequest(route, "read-ignore"), route},
			{"wrong-namespace", rw, cachePoolRequest(route, "read-ignore"), route},
		}
		cases[2].r.Object.Namespace = "cache-foreign"
		baseRoute := cachePoolRoute(t, initial, initial.Plan.Base, "writefull-ignore-go")
		roRead := subject(t, ro, "read-only-read", cachePoolRequest(route, "read-ignore"), route)
		if roRead.Code != 0 || len(roRead.Operations) != 1 || !bytes.Equal(roRead.Operations[0].Data, cachePoolChanged) {
			t.Fatal("scoped readonly bytes differ")
		}
		baseRead := subject(t, base, "base-only-base-read", cachePoolRequest(baseRoute, "read-ignore"), baseRoute)
		if baseRead.Code != 0 || len(baseRead.Operations) != 1 || !bytes.Equal(baseRead.Operations[0].Data, cachePoolOriginal) {
			t.Fatal("scoped base-only bytes differ")
		}
		foreign := initial.ForeignPlacement
		if foreign.Pool != route.Pool || foreign.PoolName != initial.Plan.Tier || foreign.Name != route.Name || foreign.Namespace != "cache-foreign" || foreign.ID != route.ID || foreign.PrimaryID != 0 || foreign.Epoch == 0 || foreign.Address != route.Address {
			t.Fatal("foreign namespace placement differs", foreign)
		}
		cases[2].route = foreign
		cases[2].r = cachePoolRequest(foreign, "read-ignore")

		cases = append(cases, struct {
			name  string
			c     *cephmsgr.Client
			r     cephmsgr.OSDRequest
			route cachePoolPlacement
		}{"tier-only-base-read", rw, cachePoolRequest(baseRoute, "read-ignore"), baseRoute})
		for _, item := range cases {
			r := subject(t, item.c, item.name, item.r, item.route)
			if r.Code != -1 || len(r.Operations) != 1 || r.Operations[0].Result != -1 && r.Operations[0].Result != 0 {
				t.Fatal("expected raw EPERM", item.name, r.Code)
			}
		}
		after, err := control(ctx, "observe", "writefull-ignore")
		if err != nil {
			t.Fatal(err)
		}
		cachePoolOwnUnchanged(t, "permission probes tier", before.Cases["writefull-ignore"].GoAfter.Tier, after.Cases["writefull-ignore"].GoAfter.Tier)
		cachePoolOwnUnchanged(t, "permission probes base", before.Cases["writefull-ignore"].GoAfter.Base, after.Cases["writefull-ignore"].GoAfter.Base)
	}) {
		return
	}
	t.Run("primary_channel_survives_all_cache_operations", func(t *testing.T) {
		// The publication uses the separate owned admin identity, not a widened
		// permission grant on the pool-scoped RW subject.
		sentinel := fmt.Sprintf("ceph-msgr-cache-pools-%d", time.Now().UnixNano())
		publishLogOnce(t, ctx, admin, sentinel)
		waitLogEntry(t, ctx, logs, sentinel)
		after := rw.Snapshot()
		receipt["primary_after"] = after
		if after.GlobalID != before.GlobalID || after.FSID != before.FSID || after.Monitor.Endpoint != before.Monitor.Endpoint || after.Manager.Endpoint != before.Manager.Endpoint || !after.Monitor.Ready || !after.Manager.Ready || after.AuthRejection != nil {
			t.Fatal("cache/private operations changed primary identity", after)
		}
		receipt["subscription_survived"] = true
	})
}
