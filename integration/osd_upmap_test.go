package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

type nativeUpmapPlan struct {
	PoolName string                      `json:"pool_name"`
	PoolID   int64                       `json:"pool_id"`
	OSDs     []int32                     `json:"osd_ids"`
	Objects  map[string]string           `json:"objects"`
	Baseline map[string]osdFixtureOracle `json:"baseline"`
	Targets  map[string][]int32          `json:"targets"`
}
type nativeUpmapObservation struct {
	Route osdFixtureOracle    `json:"route"`
	Query json.RawMessage     `json:"pg_query"`
	Pool  json.RawMessage     `json:"pool"`
	State nativeXattrSnapshot `json:"object_state"`
}
type nativeUpmapScope struct {
	Schema       int                               `json:"schema_version"`
	Source       string                            `json:"source_commit"`
	Token        string                            `json:"token"`
	FSID         string                            `json:"fsid"`
	Phase        string                            `json:"phase"`
	Epoch        uint32                            `json:"native_epoch"`
	Plan         nativeUpmapPlan                   `json:"plan"`
	Observations map[string]nativeUpmapObservation `json:"observations"`
	Baseline     nativeUpmapObservation            `json:"baseline_fixture"`
	Finished     bool                              `json:"finished"`
	Cleanup      *struct {
		Restored          bool     `json:"overrides_restored"`
		ObjectsRemoved    bool     `json:"objects_removed"`
		PoolRemoved       bool     `json:"pool_removed"`
		Retained          bool     `json:"retained_for_fixture_teardown"`
		BaselineUnchanged bool     `json:"baseline_unchanged"`
		Errors            []string `json:"errors"`
	} `json:"cleanup,omitempty"`
	Retained []struct {
		Name   string `json:"name"`
		ID     int64  `json:"id"`
		Reason string `json:"reason"`
	} `json:"retained_resources,omitempty"`
	PreCleanup *nativeUpmapScope `json:"pre_cleanup,omitempty"`
	Unresolved string            `json:"unresolved_mutation"`
}
type upmapControlEvidence struct {
	Action     string             `json:"action"`
	Raw        []byte             `json:"raw_response"`
	Scope      nativeUpmapScope   `json:"decoded_response"`
	Error      string             `json:"error,omitempty"`
	AuthBefore *upmapAuthEvidence `json:"auth_before,omitempty"`
	AuthAfter  *upmapAuthEvidence `json:"auth_after,omitempty"`
}
type upmapAuthEvidence struct {
	ObservedAt   time.Time                     `json:"observed_at"`
	GlobalID     uint64                        `json:"global_id"`
	MonitorReady bool                          `json:"monitor_ready"`
	AuthTicket   cephmsgr.TicketState          `json:"auth_ticket"`
	OSDTicket    cephmsgr.TicketState          `json:"osd_ticket"`
	Rejection    *cephmsgr.AuthenticationError `json:"rejection,omitempty"`
}

func upmapAuthObservation(c *cephmsgr.Client) *upmapAuthEvidence {
	s := c.Snapshot()
	return &upmapAuthEvidence{time.Now().UTC(), s.GlobalID, s.Monitor.Ready, s.AuthTicket, s.OSDTicket, s.AuthRejection}
}

type upmapGoEvidence struct {
	Stage   string              `json:"stage"`
	Route   osdFixtureOracle    `json:"native_route"`
	Target  cephmsgr.OSDTarget  `json:"explicit_target"`
	Request cephmsgr.OSDRequest `json:"request"`
	Reply   cephmsgr.OSDReply   `json:"reply"`
	Error   string              `json:"error,omitempty"`
	Unknown bool                `json:"outcome_unknown"`
}

func upmapControl(ctx context.Context, action string) (upmapControlEvidence, error) {
	result := upmapControlEvidence{Action: action}
	switch action {
	case "prepare", "items", "restore-items", "full", "restore-full", "observe", "finish":
	default:
		return result, errors.New("unsupported upmap control action")
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if err := testcluster.WritePublicEvidence(filepath.Join(dir, "osd-upmap-input.json"), []byte("{}")); err != nil {
		return result, err
	}
	path := filepath.Join(dir, "osd-upmap-result.json")
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	call, stop := context.WithTimeout(ctx, 160*time.Second)
	defer stop()
	controlErr := testcluster.ControlDaemon(call, dir, "verify", "osd-upmap", action)
	file, err := os.Open(path)
	if err == nil {
		result.Raw, err = io.ReadAll(io.LimitReader(file, (512<<10)+1))
		_ = file.Close()
		if len(result.Raw) > 512<<10 {
			err = errors.New("native upmap response exceeds bound")
		}
		if err == nil {
			err = json.Unmarshal(result.Raw, &result.Scope)
		}
	}
	if controlErr != nil {
		err = controlErr
	}
	if err != nil {
		result.Error = err.Error()
		return result, err
	}
	return result, nil
}
func upmapStableRoute(a, b osdFixtureOracle) bool {
	return a.Pool == b.Pool && a.Name == b.Name && a.Namespace == b.Namespace && a.Hash == b.Hash && a.PG == b.PG && a.RawPGID == b.RawPGID && a.PGID == b.PGID && a.ID == b.ID && a.Address == b.Address && a.Replicas == b.Replicas && a.MinSize == b.MinSize && reflect.DeepEqual(a.Acting, b.Acting) && reflect.DeepEqual(a.Up, b.Up)
}
func upmapObjectIdentity(a, b osdFixtureOracle) bool {
	return a.Pool == b.Pool && a.Name == b.Name && a.Namespace == b.Namespace && a.Hash == b.Hash && a.PG == b.PG && a.RawPGID == b.RawPGID && a.PGID == b.PGID
}
func checkUpmapObservation(t *testing.T, o nativeUpmapObservation, pool int64, name string, replicas int, epoch uint32, poolName string) {
	t.Helper()
	r := o.Route
	s := o.State
	if r.Pool != pool || r.Name != name || r.Namespace != "" || r.PG >= 8 || r.RawPGID != fmt.Sprintf("%d.%x", pool, r.Hash) || r.PGID != fmt.Sprintf("%d.%x", pool, r.PG) || r.Epoch != epoch || epoch == 0 || r.Replicas != replicas || r.MinSize != 2 || len(r.Acting) != replicas || !reflect.DeepEqual(r.Acting, r.Up) || r.ID != r.Acting[0] || s.Code != 0 || s.Version == 0 || s.Seconds < 0 || s.Seconds > int64(^uint32(0)) || s.Nanos < 0 || s.Nanos >= 1e9 || r.Size != uint64(len(s.Data)) || r.Seconds != uint32(s.Seconds) || r.Nanoseconds != uint32(s.Nanos) {
		t.Fatal("native upmap object/route/state tuple differs", name, r.Pool, r.PGID, r.Epoch, s.Code, s.Version)
	}
	seen := map[int32]bool{}
	for _, id := range r.Acting {
		if id < 0 || id > 2 || seen[id] {
			t.Fatal("native upmap acting membership differs", r.Acting)
		}
		seen[id] = true
	}
	if !strings.HasPrefix(r.Address, "v2:") {
		t.Fatal("native upmap endpoint is not v2", r.Address)
	}
	endpoint, nonce, ok := strings.Cut(strings.TrimPrefix(r.Address, "v2:"), "/")
	if !ok {
		t.Fatal("native upmap endpoint lacks nonce", r.Address)
	}
	if _, err := netip.ParseAddrPort(endpoint); err != nil {
		t.Fatal("native upmap endpoint", err)
	}
	if _, err := strconv.ParseUint(nonce, 10, 32); err != nil {
		t.Fatal("native upmap nonce", err)
	}
	var query struct {
		State string `json:"state"`
		Epoch uint32 `json:"epoch"`
		Info  struct {
			PG string `json:"pgid"`
		} `json:"info"`
		Acting, Up []int32
	}
	if err := json.Unmarshal(o.Query, &query); err != nil {
		t.Fatal("native upmap PG query", err)
	}
	flags := strings.Split(query.State, "+")
	active, clean := false, false
	for _, f := range flags {
		active = active || f == "active"
		clean = clean || f == "clean"
	}
	if !active || !clean || query.Epoch != epoch || query.Info.PG != r.PGID || !reflect.DeepEqual(query.Acting, r.Acting) || !reflect.DeepEqual(query.Up, r.Up) {
		t.Fatal("native upmap applied PG epoch/clean membership differs", query.State, query.Epoch, r.PGID)
	}
	if replicas == 2 {
		var p struct {
			ID         int64  `json:"pool"`
			Name       string `json:"pool_name"`
			Type, Size int
			MinSize    int `json:"min_size"`
			Flags      uint64
			Mode       string `json:"pg_autoscale_mode"`
			Num        int    `json:"pg_num"`
			Target     int    `json:"pg_num_target"`
			Pending    int    `json:"pg_num_pending"`
			PGP        int    `json:"pg_placement_num"`
			PGPTarget  int    `json:"pg_placement_num_target"`
		}
		if err := json.Unmarshal(o.Pool, &p); err != nil || p.ID != pool || p.Name != poolName || p.Type != 1 || p.Size != 2 || p.MinSize != 2 || p.Flags&1 == 0 || p.Mode != "off" || p.Num != 8 || p.Target != 8 || p.Pending != 8 || p.PGP != 8 || p.PGPTarget != 8 {
			t.Fatal("native owned upmap pool policy differs", p, err)
		}
	}
}
func checkUpmapScope(t *testing.T, s nativeUpmapScope, prepared *nativeUpmapScope, base osdFixtureOracle, before mutationSnapshot, authenticatedFSID string) {
	t.Helper()
	token, err := hex.DecodeString(s.Token)
	if err != nil || len(token) != 16 || s.Token != strings.ToLower(s.Token) || s.Schema != 1 || s.Source != "7f793731f1b39eb4f465e960113d2363c311b964" || len(s.FSID) != 36 || s.FSID != authenticatedFSID || s.Finished || s.Plan.PoolID == base.Pool || s.Plan.PoolID < 0 || s.Plan.PoolName != "ceph-msgr-upmap-"+s.Token || !reflect.DeepEqual(s.Plan.OSDs, []int32{0, 1, 2}) || len(s.Plan.Objects) != 2 || len(s.Plan.Baseline) != 2 || len(s.Plan.Targets) != 2 || len(s.Observations) != 2 {
		t.Fatal("native upmap scope identity or bounded plan differs", s.Schema, s.Token, s.Phase)
	}
	if prepared != nil && (s.Token != prepared.Token || s.FSID != prepared.FSID || !reflect.DeepEqual(s.Plan, prepared.Plan)) {
		t.Fatal("native upmap scope or frozen plan changed")
	}
	for _, slot := range []string{"items", "full"} {
		if s.Plan.Objects[slot] != "owned-upmap-"+slot {
			t.Fatal("upmap native fixed owned head differs", slot, s.Plan.Objects[slot])
		}
		o, ok := s.Observations[slot]
		if !ok {
			t.Fatal("native upmap observation missing", slot)
		}
		checkUpmapObservation(t, o, s.Plan.PoolID, s.Plan.Objects[slot], 2, s.Epoch, s.Plan.PoolName)
		if !upmapObjectIdentity(o.Route, s.Plan.Baseline[slot]) {
			t.Fatal("upmap changed raw hash/actual PG or object identity", slot)
		}
	}
	checkUpmapObservation(t, s.Baseline, base.Pool, base.Name, 3, s.Epoch, "")
	if !upmapStableRoute(s.Baseline.Route, base) || s.Baseline.State.Code != int(before.Code) || !bytes.Equal(s.Baseline.State.Data, before.Data) || s.Baseline.State.Version != before.Version || s.Baseline.State.Seconds != int64(before.Seconds) || s.Baseline.State.Nanos != int64(before.Nanos) {
		t.Fatal("upmap changed original three-replica fixture tuple or state")
	}
	if prepared != nil && !reflect.DeepEqual(s.Baseline.State, prepared.Baseline.State) {
		t.Fatal("upmap changed original native fixture metadata")
	}
}
func checkUpmapRestored(t *testing.T, s nativeUpmapScope, prepared nativeUpmapScope, states map[string]nativeXattrSnapshot) {
	t.Helper()
	for _, slot := range []string{"items", "full"} {
		o := s.Observations[slot]
		if !upmapStableRoute(o.Route, prepared.Plan.Baseline[slot]) || !reflect.DeepEqual(o.State, states[slot]) {
			t.Fatal("owned upmap restoration changed current object or original membership", slot)
		}
	}
}

// This is caller-side acceptance for an exclusively owned auxiliary pool. It
// never calculates CRUSH placement, decodes inner maps or replays a mutation.
func TestCephOSDUpmapIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "replicated-pg-v8" || os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("requires opt-in three-BlueStore replicated-pg-v8 native upmap fixture")
	}
	c, _, base, _ := replicatedPGFixture(t)
	globalID := c.Snapshot().GlobalID
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	original := replicatedPGNative(t, ctx, base.Name)
	base = original.Placement
	var controls []upmapControlEvidence
	var requests []upmapGoEvidence
	var prepared, latest, finish nativeUpmapScope
	attempted, finishAttempted, cleaned := false, false, false
	cleanupError := ""
	control := func(call context.Context, action string) (nativeUpmapScope, error) {
		before := upmapAuthObservation(c)
		record, err := upmapControl(call, action)
		record.AuthBefore, record.AuthAfter = before, upmapAuthObservation(c)
		controls = append(controls, record)
		return record.Scope, err
	}
	t.Cleanup(func() {
		defer func() {
			data, err := json.Marshal(struct {
				Scope        string                 `json:"scope"`
				Controls     []upmapControlEvidence `json:"controls"`
				Requests     []upmapGoEvidence      `json:"go_requests"`
				Cleaned      bool                   `json:"cleaned"`
				CleanupError string                 `json:"cleanup_error"`
			}{"owned auxiliary two-replica pool only; retained empty pool belongs to fixture teardown; caller placement and one explicit APPEND per mapped phase; no automatic replay", controls, requests, cleaned, cleanupError})
			if err == nil && len(data) <= 8<<20 {
				err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-upmap-results.json"), data)
			} else if err == nil {
				err = errors.New("bounded upmap evidence exceeded")
			}
			if err != nil {
				t.Error(err)
			}
		}()
		if attempted && !finishAttempted {
			finishAttempted = true
			cleanup, stop := context.WithTimeout(context.Background(), 180*time.Second)
			defer stop()
			var err error
			finish, err = control(cleanup, "finish")
			if err != nil {
				cleanupError = err.Error()
				t.Error("native owned upmap finish unconfirmed", err)
			}
			cleaned = err == nil && finish.Finished && finish.Cleanup != nil && finish.Cleanup.Restored && finish.Cleanup.ObjectsRemoved && finish.Cleanup.BaselineUnchanged && !finish.Cleanup.PoolRemoved && finish.Cleanup.Retained && len(finish.Cleanup.Errors) == 0
			if !cleaned {
				t.Error("native upmap overrides/heads cleanup incomplete; retained resources and raw receipts preserved")
			}
		}
	})
	attempted = true
	var err error
	prepared, err = control(ctx, "prepare")
	if err != nil {
		t.Fatal("prepare native owned upmap scope", err)
	}
	checkUpmapScope(t, prepared, nil, base, original.Actual, c.Snapshot().FSID)
	if prepared.Phase != "prepare" {
		t.Fatal("native upmap preparation phase differs", prepared.Phase)
	}
	states := map[string]nativeXattrSnapshot{}
	for _, slot := range []string{"items", "full"} {
		o := prepared.Observations[slot]
		want := append([]byte{0, 255, 128}, []byte("owned-upmap-"+slot+"\x00")...)
		if !bytes.Equal(o.State.Data, want) {
			t.Fatal("native owned upmap seed differs", slot)
		}
		states[slot] = o.State
		b := prepared.Plan.Baseline[slot]
		target := prepared.Plan.Targets[slot]
		if len(b.Acting) != 2 || len(target) != 2 || !upmapStableRoute(b, o.Route) {
			t.Fatal("native frozen upmap baseline/target differs", slot)
		}
		spare := target[1]
		if slot == "full" {
			spare = target[0]
		}
		if spare < 0 || spare > 2 || spare == b.Acting[0] || spare == b.Acting[1] || slot == "items" && target[0] != b.Acting[1] || slot == "full" && target[1] != b.ID {
			t.Fatal("native upmap plan is not the fixed membership/primary replacement", slot, b.Acting, target)
		}
	}
	submit := func(t *testing.T, call context.Context, stage string, o *cephmsgr.OSDConnection, route osdFixtureOracle, request cephmsgr.OSDRequest) cephmsgr.OSDReply {
		r, e := o.Request(call, request)
		var unknown *cephmsgr.OutcomeUnknownError
		proof := upmapGoEvidence{Stage: stage, Route: route, Target: cephmsgr.OSDTarget{ID: route.ID, Address: route.Address}, Request: request, Reply: r, Unknown: errors.As(e, &unknown)}
		if e != nil {
			proof.Error = e.Error()
		}
		requests = append(requests, proof)
		if e != nil {
			t.Fatal("caller-targeted upmap request failed; original submission retained and never replayed", stage, proof.Unknown, r.Submission.Transaction, r.Submission.MapEpoch, e)
		}
		checkReplicatedPGReply(t, r, route)
		if r.Submission.GlobalID != globalID || r.Submission.ClientIncarnation != request.ClientIncarnation || r.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("upmap reply incarnation/completion differs", stage, r.Submission, r.Flags)
		}
		return r
	}
	read := func(t *testing.T, stage string, ob nativeUpmapObservation) {
		call, stop := context.WithTimeout(ctx, 20*time.Second)
		defer stop()
		r := ob.Route
		o := replicatedPGConnection(t, c, call, r)
		defer o.Close()
		req := replicatedPGRequest(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(ob.State.Data) + 64)})
		req.ClientIncarnation = 129
		reply := submit(t, call, stage, o, r, req)
		if len(reply.Operations) != 2 || reply.Operations[0].Result != 0 || reply.Operations[1].Result != 0 || len(reply.Operations[0].Data) != 16 || binary.LittleEndian.Uint64(reply.Operations[0].Data) != uint64(len(ob.State.Data)) || binary.LittleEndian.Uint32(reply.Operations[0].Data[8:]) != uint32(ob.State.Seconds) || binary.LittleEndian.Uint32(reply.Operations[0].Data[12:]) != uint32(ob.State.Nanos) || !bytes.Equal(reply.Operations[1].Data, ob.State.Data) || reply.UserVersion != ob.State.Version || !bytes.Equal(reply.RawData, append(bytes.Clone(reply.Operations[0].Data), reply.Operations[1].Data...)) {
			t.Fatal("caller-targeted upmap STAT/READ differs from native bytes/version/mtime", stage, reply.UserVersion, ob.State.Version)
		}
	}
	for _, slot := range []string{"items", "full"} {
		read(t, "baseline-"+slot, prepared.Observations[slot])
	}
	latest = prepared
	for index, slot := range []string{"items", "full"} {
		t.Run(slot, func(t *testing.T) {
			mapped, e := control(ctx, slot)
			if e != nil {
				t.Fatal("native upmap transition", slot, e)
			}
			checkUpmapScope(t, mapped, &prepared, base, original.Actual, c.Snapshot().FSID)
			if mapped.Phase != slot || mapped.Epoch <= latest.Epoch {
				t.Fatal("native upmap phase/map epoch did not advance", slot, mapped.Phase, mapped.Epoch, latest.Epoch)
			}
			for _, other := range []string{"items", "full"} {
				if !reflect.DeepEqual(mapped.Observations[other].State, states[other]) {
					t.Fatal("native remap changed owned state before Go mutation", other)
				}
			}
			selected := mapped.Observations[slot]
			before := prepared.Plan.Baseline[slot]
			expected := prepared.Plan.Targets[slot]
			if slot == "items" {
				expected = []int32{before.Acting[0], expected[1]}
			}
			if !reflect.DeepEqual(selected.Route.Acting, expected) || !reflect.DeepEqual(selected.Route.Up, expected) || selected.Route.ID != expected[0] || slot == "items" && selected.Route.ID != before.ID || slot == "full" && selected.Route.ID == before.ID {
				t.Fatal("native actual remap does not match explicit membership/primary plan", slot, selected.Route.Acting, expected)
			}
			read(t, slot+"-mapped", selected)
			call, stop := context.WithTimeout(ctx, 20*time.Second)
			defer stop()
			o := replicatedPGConnection(t, c, call, selected.Route)
			defer o.Close()
			tail := append([]byte{0, 255, 128}, []byte("upmap-"+slot+"-once-"+prepared.Token+"\x00")...)
			request := replicatedPGRequest(selected.Route, cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail})
			request.ClientIncarnation = uint32(131 + index*2)
			request.ModificationTime = cephmsgr.OSDTime{Seconds: uint32(1700001300 + index), Nanoseconds: uint32(123456789 + index)}
			reply := submit(t, call, slot+"-append-once", o, selected.Route, request)
			observed, e := control(ctx, "observe")
			if e != nil {
				t.Fatal("independent native after explicit APPEND", e)
			}
			checkUpmapScope(t, observed, &prepared, base, original.Actual, c.Snapshot().FSID)
			if observed.Phase != slot || observed.Epoch < mapped.Epoch || !upmapStableRoute(observed.Observations[slot].Route, selected.Route) {
				t.Fatal("native upmap observation changed mapped tuple", slot)
			}
			after := observed.Observations[slot].State
			want := append(bytes.Clone(states[slot].Data), tail...)
			// User versions are PG-log based, so another owned head in the same PG
			// can make the value jump. Compare this head's reply to this head's native
			// result; exact bytes and one submitted sentinel establish the postcondition.
			if len(reply.Operations) != 1 || reply.Operations[0].Result != 0 || len(reply.RawData) != 0 || after.Version <= states[slot].Version || after.Version != reply.UserVersion || !bytes.Equal(after.Data, want) || bytes.Count(after.Data, tail) != 1 || after.Seconds != int64(request.ModificationTime.Seconds) || after.Nanos != int64(request.ModificationTime.Nanoseconds) || !reflect.DeepEqual(after.Attributes, states[slot].Attributes) {
				t.Fatal("native explicit APPEND bytes/version/mtime differ", slot, after.Version, reply.UserVersion)
			}
			other := "items"
			if slot == "items" {
				other = "full"
			}
			if !reflect.DeepEqual(observed.Observations[other].State, states[other]) {
				t.Fatal("APPEND changed the other owned head", other)
			}
			states[slot] = after
			restored, e := control(ctx, "restore-"+slot)
			if e != nil {
				t.Fatal("native owned override restore", slot, e)
			}
			checkUpmapScope(t, restored, &prepared, base, original.Actual, c.Snapshot().FSID)
			if restored.Phase != "restore-"+slot || restored.Epoch <= observed.Epoch {
				t.Fatal("native override removal epoch/phase differs", slot)
			}
			checkUpmapRestored(t, restored, prepared, states)
			for _, head := range []string{"items", "full"} {
				read(t, slot+"-restored-"+head, restored.Observations[head])
			}
			latest = restored
		})
		if t.Failed() {
			return
		}
	}
	finishAttempted = true
	finish, err = control(ctx, "finish")
	if err != nil {
		cleanupError = err.Error()
		t.Fatal("native owned upmap scope finish", err)
	}
	if finish.Schema != prepared.Schema || finish.Source != prepared.Source || finish.Token != prepared.Token || finish.FSID != prepared.FSID || !reflect.DeepEqual(finish.Plan, prepared.Plan) || finish.Phase != "finish" || !finish.Finished || finish.Cleanup == nil || !finish.Cleanup.Restored || !finish.Cleanup.ObjectsRemoved || finish.Cleanup.PoolRemoved || !finish.Cleanup.Retained || !finish.Cleanup.BaselineUnchanged || len(finish.Cleanup.Errors) != 0 || finish.Unresolved != "" || len(finish.Retained) != 1 || finish.Retained[0].Name != prepared.Plan.PoolName || finish.Retained[0].ID != prepared.Plan.PoolID || finish.Retained[0].Reason == "" || finish.PreCleanup == nil {
		t.Fatal("upmap cleanup/explicit retained pool receipt incomplete", finish.Phase, finish.Cleanup, finish.Unresolved)
	}
	checkUpmapScope(t, *finish.PreCleanup, &prepared, base, original.Actual, c.Snapshot().FSID)
	checkUpmapRestored(t, *finish.PreCleanup, prepared, states)
	cleaned = true
}
