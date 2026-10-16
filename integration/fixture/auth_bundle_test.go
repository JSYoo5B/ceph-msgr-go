package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

const authBundleTestSecret = "AQBnever-log-this-key-material=="

type fakeAuthBundleSources struct {
	mu        sync.Mutex
	state     managerPreparationMap
	stateNext *managerPreparationMap
	started   map[string]string
	tell      ceph.NativeCommandResult
	tellErr   error
	calls     []string
	block     bool
}

func (f *fakeAuthBundleSources) record(call string) {
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.mu.Unlock()
}
func (f *fakeAuthBundleSources) wait(ctx context.Context) error {
	if f.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return ctx.Err()
}
func (f *fakeAuthBundleSources) AdminSocket(ctx context.Context, daemon string, args ...string) ([]byte, error) {
	f.record("admin " + daemon + " " + strings.Join(args, " "))
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	name := strings.TrimPrefix(daemon, "mon.")
	switch args[0] {
	case "mon_status":
		return []byte(`{"name":"` + name + `","rank":0,"state":"leader","election_epoch":12,"quorum":[0,1,2],"monmap":{"epoch":5,"auth_epoch":3,"fsid":"f5"}}`), nil
	case "sessions":
		if name == "a" {
			return []byte(`[{"entity_name":"mgr.a","con_type":"mgr","global_id":4161},{"entity_name":"mgr.a","con_type":"client","global_id":4179},{"entity_name":"mgr.b","con_type":"mgr","global_id":4170}]`), nil
		}
		return []byte(`[]`), nil
	}
	return nil, errors.New("invalid command")
}
func (f *fakeAuthBundleSources) ManagerMap(ctx context.Context) (managerPreparationMap, error) {
	f.record("mgr dump")
	if err := f.wait(ctx); err != nil {
		return managerPreparationMap{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	state := f.state
	if f.stateNext != nil {
		f.state = *f.stateNext
	}
	return state, nil
}
func (f *fakeAuthBundleSources) QuorumStatus(ctx context.Context) (ceph.QuorumStatus, error) {
	f.record("quorum")
	if err := f.wait(ctx); err != nil {
		return ceph.QuorumStatus{}, err
	}
	return ceph.QuorumStatus{QuorumNames: []string{"a", "b", "c"}}, nil
}
func (f *fakeAuthBundleSources) OwnedManagers() []string { return []string{"a", "b"} }
func (f *fakeAuthBundleSources) ContainerInstance(ctx context.Context, kind, name string) (string, string, error) {
	f.record("container " + kind + "." + name)
	if err := f.wait(ctx); err != nil {
		return "", "", err
	}
	return "cid-" + kind + "-" + name, f.started[kind+"."+name], nil
}
func (f *fakeAuthBundleSources) TellVersion(ctx context.Context, target string, timeout time.Duration, output int) (ceph.NativeCommandResult, error) {
	f.record("tell " + strings.Join(authBundleControlArgv(target), " "))
	if err := f.wait(ctx); err != nil {
		return ceph.NativeCommandResult{}, err
	}
	return f.tell, f.tellErr
}
func (f *fakeAuthBundleSources) Redact(ctx context.Context, data []byte) ([]byte, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return []byte(strings.ReplaceAll(string(data), authBundleTestSecret, "[REDACTED]")), nil
}

func newFakeAuthBundleSources() *fakeAuthBundleSources {
	return &fakeAuthBundleSources{
		state:   gateState("a", 4161, "b", 4170),
		started: map[string]string{"mgr.a": "2026-10-16T01:00:00Z", "mgr.b": "2026-10-16T01:00:01Z", "mon.a": "t0", "mon.b": "t0", "mon.c": "t0"},
		tell:    ceph.NativeCommandResult{Stdout: []byte(`{"version":"ceph version 20.2.4","release":"tentacle"}`)},
	}
}

func authBundleTestRequest() authBundleRequest {
	secretID := uint64(2)
	return authBundleRequest{
		Label:      "auth-epoch",
		Provenance: authBundleProvenance{Producer: "integration/fixture", Subject: "cephmsgr cold client", Run: "TestCephServiceKeyEpochIntegration"},
		Expected:   authBundleExpected{FSID: "f5", Monitors: []string{"a", "b", "c"}, Manager: "a", GID: 4161},
		Rejection: authBundleRejection{Run: "run-1", Cycle: 2, Attempt: 1, At: time.Date(2026, 10, 16, 1, 0, 5, 0, time.UTC), Target: "mgr.a", TargetGID: 4161,
			ClientID: 4400, SecretID: &secretID, TicketLabel: "hmac-cold-2", Result: -13, Error: "authentication rejected: method 2 code -13"},
		Tickets: []authBundleTicket{{Run: "run-1", Cycle: 1, Service: 16, SecretID: 2, Label: "hmac-cold-1"}, {Run: "run-1", Cycle: 2, Service: 16, SecretID: 2, Label: "hmac-cold-2"}},
	}
}

func TestAuthFirstFailureBundleCollectsWithoutChangingTheRejection(t *testing.T) {
	sources := newFakeAuthBundleSources()
	request := authBundleTestRequest()
	recorded := request.Rejection
	bundle := collectAuthFirstFailureBundle(context.Background(), sources, "7", request, time.Minute, time.Second)
	verdict := bundle.Verdict
	if !verdict.Complete || !bundle.CollectionComplete || verdict.Target != gateSlotRegistered || !verdict.ControlsSucceeded || verdict.Result != -13 ||
		verdict.GenerationEquality != authGenerationUnavailable || bundle.GenerationEquality != authGenerationUnavailable {
		t.Fatalf("verdict %+v problems %v", verdict, verdict.Problems)
	}
	if bundle.Rejection.At != recorded.At || bundle.Rejection.Error != recorded.Error || *bundle.Rejection.SecretID != 2 || bundle.Rejection.Result != -13 {
		t.Fatal("the rejection changed", bundle.Rejection)
	}
	if bundle.Expected.ContainerID != "cid-mgr-a" || bundle.Expected.StartedAt != "2026-10-16T01:00:00Z" || sources.calls[0] != "container mgr.a" {
		t.Fatal("target container identity was not frozen first", bundle.Expected, sources.calls)
	}
	if len(bundle.Monitors) != 3 || bundle.Monitors[0].AuthEpoch == nil || *bundle.Monitors[0].AuthEpoch != 3 || bundle.Monitors[0].StartedAt != "t0" || len(bundle.Quorum) != 3 {
		t.Fatal("MON observations", bundle.Monitors, bundle.Quorum)
	}
	a := bundle.Managers[0]
	if a.Name != "a" || !a.Active || a.GID != 4161 || !slices.Equal(a.BeaconGlobalIDs, []uint64{4161}) || !slices.Equal(a.ModuleGlobalIDs, []uint64{4179}) {
		t.Fatal("MGR session split", a)
	}
	if len(bundle.Controls) != 1 || bundle.Controls[0].Target != "mgr.a" || bundle.Release != "ceph version 20.2.4 tentacle" {
		t.Fatal("controls", bundle.Controls, bundle.Release)
	}
	if !slices.Equal(verdict.ReusedSecretIDs, []uint64{2}) {
		t.Fatal("secret IDs shared across cycles must be reported, not equated", verdict.ReusedSecretIDs)
	}
	// Only read-only observations and the fixed control ran: no wipe, no
	// config change, no debug level, no authentication retry.
	for _, call := range sources.calls {
		if strings.Contains(call, "debug") || strings.Contains(call, "config") || strings.Contains(call, "wipe") || strings.Contains(call, "auth") {
			t.Fatal("collection ran a mutating or secret-revealing command", call)
		}
	}
	data, err := json.Marshal(bundle)
	if err != nil || checkAuthBundleSecrecy(data, authBundleTestSecret) != nil {
		t.Fatal("secrecy check", err)
	}
	if checkAuthBundleSecrecy(data, "hmac-cold-2") == nil {
		t.Fatal("secrecy check ignored a given secret")
	}
	if checkAuthBundleSecrecy([]byte(strings.Replace(string(data), `"schema"`, `"debug_auth":"x","schema"`, 1))) == nil {
		t.Fatal("secrecy check accepted a field beyond the schema")
	}
}

func TestAuthFirstFailureBundlePartialCollectionsKeepTheRejection(t *testing.T) {
	request := authBundleTestRequest()
	starved := newFakeAuthBundleSources()
	starved.block = true
	bundle := collectAuthFirstFailureBundle(context.Background(), starved, "1", request, 20*time.Millisecond, time.Second)
	if bundle.Verdict.Complete || bundle.CollectionComplete || bundle.Rejection.Result != -13 || bundle.Verdict.Target != queryUnavailable ||
		!slices.ContainsFunc(bundle.Queries, func(q authBundleQuery) bool { return q.Status == queryTimedOut }) {
		t.Fatalf("starved budget: %+v %+v", bundle.Verdict, bundle.Queries)
	}
	for _, query := range bundle.Queries {
		if query.Status == queryComplete {
			t.Fatal("a query completed after the budget ended", query)
		}
		if strings.Contains(query.Error, "deadline") && query.Error != authTextWithheld {
			t.Fatal("unredacted text kept after the budget", query)
		}
	}
	truncated := newFakeAuthBundleSources()
	truncated.tell = ceph.NativeCommandResult{Stdout: []byte(`{"vers`), StdoutTruncated: true}
	bundle = collectAuthFirstFailureBundle(context.Background(), truncated, "2", request, time.Minute, time.Second)
	if bundle.Verdict.Complete || bundle.Verdict.ControlsSucceeded || bundle.Rejection.Result != -13 ||
		!slices.ContainsFunc(bundle.Queries, func(q authBundleQuery) bool { return q.Status == queryTruncated && strings.HasPrefix(q.Name, "tell ") }) {
		t.Fatalf("truncated control: %+v %+v", bundle.Verdict, bundle.Queries)
	}
	timedOut := newFakeAuthBundleSources()
	timedOut.tell = ceph.NativeCommandResult{TimedOut: true, Stderr: []byte("key " + authBundleTestSecret)}
	bundle = collectAuthFirstFailureBundle(context.Background(), timedOut, "3", request, time.Minute, time.Second)
	data, _ := json.Marshal(bundle)
	if !slices.ContainsFunc(bundle.Queries, func(q authBundleQuery) bool {
		return q.Status == queryTimedOut && strings.Contains(q.Excerpt, "[REDACTED]")
	}) ||
		strings.Contains(string(data), authBundleTestSecret) {
		t.Fatalf("timed out control not recorded redacted: %+v", bundle.Queries)
	}
}

func TestClassifyAuthBundleTargetAndRefusedClient(t *testing.T) {
	request := authBundleTestRequest()
	changed := newFakeAuthBundleSources()
	changed.state = gateState("a", 4200, "b", 4170)
	bundle := collectAuthFirstFailureBundle(context.Background(), changed, "4", request, time.Minute, time.Second)
	if bundle.Verdict.Target != "changed" || bundle.Verdict.Complete {
		t.Fatal("re-registered target", bundle.Verdict)
	}
	absent := newFakeAuthBundleSources()
	absent.state = gateState("b", 4170, "", 0)
	bundle = collectAuthFirstFailureBundle(context.Background(), absent, "5", request, time.Minute, time.Second)
	if bundle.Verdict.Target != gateSlotAbsent {
		t.Fatal("absent target", bundle.Verdict)
	}
	moved := newFakeAuthBundleSources()
	next := gateState("a", 4161, "b", 4180)
	moved.stateNext = &next
	bundle = collectAuthFirstFailureBundle(context.Background(), moved, "6", request, time.Minute, time.Second)
	if bundle.Verdict.Complete || !slices.ContainsFunc(bundle.Verdict.Problems, func(p string) bool { return strings.Contains(p, "mgr.b changed during the collection") }) {
		t.Fatal("MGR change during the collection", bundle.Verdict)
	}
	for refused, want := range map[uint64]string{4161: gateRefusalRegistered, 4179: gateRefusalModuleClient, 9: "unknown"} {
		request := authBundleTestRequest()
		request.Rejection.RefusedGlobalID = &refused
		bundle := collectAuthFirstFailureBundle(context.Background(), newFakeAuthBundleSources(), "8", request, time.Minute, time.Second)
		if bundle.Verdict.RefusedClient != want {
			t.Fatal("refused client", refused, bundle.Verdict.RefusedClient)
		}
	}
	bundle.GenerationEquality = "equal"
	if verdict := classifyAuthBundle(bundle); verdict.Complete || verdict.GenerationEquality != authGenerationUnavailable {
		t.Fatal("claimed generation equality accepted", verdict)
	}
	bundle.GenerationEquality = authGenerationUnavailable
	bundle.Rejection = authBundleRejection{}
	if verdict := classifyAuthBundle(bundle); verdict.Complete {
		t.Fatal("bundle without a rejection is complete")
	}
}

func TestAuthBundleRequestIsStrictAndOwned(t *testing.T) {
	request := authBundleTestRequest()
	data, _ := json.Marshal(request)
	encoded := base64.RawURLEncoding.EncodeToString(data)
	decoded, err := decodeAuthBundleRequest(encoded)
	if err != nil || decoded.Rejection.TicketLabel != "hmac-cold-2" || strings.ContainsAny(encoded, " \n") {
		t.Fatal("decode", err)
	}
	for name, bad := range map[string]string{
		"empty":         "",
		"not base64":    "%%%",
		"unknown field": base64.RawURLEncoding.EncodeToString([]byte(`{"label":"auth-epoch","key":"x"}`)),
		"oversized":     strings.Repeat("A", authBundleRequestLimit+1),
	} {
		if _, err := decodeAuthBundleRequest(bad); err == nil {
			t.Fatal(name, "accepted")
		}
	}
	p := profile{authEpoch: "1", fsid: "f5"}
	monitors, managers := []string{"c", "b", "a"}, []string{"a", "b"}
	if err := validateAuthBundleRequest(request, p, monitors, managers); err != nil {
		t.Fatal(err)
	}
	short := request
	short.Label = "short-tickets"
	if validateAuthBundleRequest(short, p, monitors, managers) == nil || validateAuthBundleRequest(short, profile{shortTickets: "1", fsid: "f5"}, monitors, managers) != nil {
		t.Fatal("label must match the fixture profile")
	}
	for name, mutate := range map[string]func(*authBundleRequest){
		"other FSID":       func(r *authBundleRequest) { r.Expected.FSID = "other" },
		"other MONs":       func(r *authBundleRequest) { r.Expected.Monitors = []string{"a", "b"} },
		"unowned MGR":      func(r *authBundleRequest) { r.Expected.Manager = "x"; r.Rejection.Target = "mgr.x" },
		"other target":     func(r *authBundleRequest) { r.Rejection.Target = "mgr.b" },
		"caller container": func(r *authBundleRequest) { r.Expected.ContainerID = "abc" },
		"unknown label":    func(r *authBundleRequest) { r.Label = "ordinary" },
	} {
		changed := authBundleTestRequest()
		mutate(&changed)
		if validateAuthBundleRequest(changed, p, monitors, managers) == nil {
			t.Fatal(name, "accepted")
		}
	}
	if argv := strings.Join(authBundleControlArgv("mgr.a"), " "); argv != "ceph --connect-timeout 5 tell mgr.a version" {
		t.Fatal("native control changed", argv)
	}
}
