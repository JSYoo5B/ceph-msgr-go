package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

type tellVersion struct {
	Version     string `json:"version"`
	Release     string `json:"release"`
	ReleaseType string `json:"release_type"`
}

func TestCephTellIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" || os.Getenv("CEPH_MSGR_TEST_MGR_COUNT") != "2" || os.Getenv("CEPH_MSGR_TEST_MAPPED_IPV6") == "1" {
		t.Skip("requires the ordinary two-MGR disposable fixture")
	}
	for _, name := range []string{"CEPH_MSGR_TEST_EXPIRE_TICKETS", "CEPH_MSGR_TEST_IDLE_SESSIONS", "CEPH_MSGR_TEST_AUTH_EPOCH", "CEPH_MSGR_TEST_SHORT_TICKETS"} {
		if os.Getenv(name) == "1" {
			t.Skip("uses an independent ordinary authentication fixture")
		}
	}
	if os.Getenv("CEPH_MSGR_TEST_MODE_REJECTION") != "" {
		t.Skip("requires secure MON and MGR listeners")
	}
	oracles := make(map[string]tellVersion)
	for _, role := range []string{"mon", "mgr"} {
		data, err := os.ReadFile(filepath.Join(control, "tell-oracle-"+role+".json"))
		var version tellVersion
		if err != nil || json.Unmarshal(data, &version) != nil || !strings.HasPrefix(version.Version, "20.2.") || version.Release != "tentacle" || version.ReleaseType == "" {
			t.Fatal("independent native tell version oracle failed", role, err)
		}
		oracles[role] = version
	}
	c, ctx := fixtureClient(t, time.Minute)
	versionCommand := cephmsgr.Command{JSON: []byte(`{"prefix":"version","format":"json"}`)}
	checkVersion := func(role string, result cephmsgr.Result, err error) {
		t.Helper()
		var version tellVersion
		if err != nil || result.Code != 0 || json.Unmarshal(result.Data, &version) != nil || version != oracles[role] {
			t.Fatalf("%s tell disagrees with native daemon version: code=%d bytes=%d err=%v version=%+v", role, result.Code, len(result.Data), err, version)
		}
	}
	for _, target := range []struct {
		role string
		call func(context.Context, cephmsgr.Command) (cephmsgr.Result, error)
	}{{"mon", c.MonTell}, {"mgr", c.MgrTell}} {
		result, err := target.call(ctx, versionCommand)
		checkVersion(target.role, result, err)
		result, err = target.call(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"__ceph_msgr_missing_tell__","format":"json"}`)})
		var server *cephmsgr.CommandError
		var auth *cephmsgr.AuthenticationError
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &server) || errors.As(err, &auth) || errors.As(err, &unknown) || result.Code != -22 || server.Code != result.Code || server.Message != result.Message || result.Message == "" {
			t.Fatal("unknown daemon-local command lost server result", target.role, result.Code, err)
		}
		result, err = target.call(ctx, versionCommand)
		checkVersion(target.role, result, err)
	}
	// Dispatch normal commands and daemon-local tells concurrently on the same
	// authenticated MON/MGR sessions, using each request's own transaction.
	type response struct {
		role   string
		tell   bool
		result cephmsgr.Result
		err    error
	}
	responses := make(chan response, 4)
	for _, target := range []struct {
		role string
		tell bool
		call func(context.Context, cephmsgr.Command) (cephmsgr.Result, error)
		json string
	}{
		{"mon", false, c.MonCommand, `{"prefix":"status","format":"json"}`},
		{"mgr", false, c.MgrCommand, `{"prefix":"pg stat","format":"json"}`},
		{"mon", true, c.MonTell, string(versionCommand.JSON)},
		{"mgr", true, c.MgrTell, string(versionCommand.JSON)},
	} {
		go func() {
			result, err := target.call(ctx, cephmsgr.Command{JSON: []byte(target.json)})
			responses <- response{target.role, target.tell, result, err}
		}()
	}
	for range cap(responses) {
		select {
		case got := <-responses:
			if got.tell {
				checkVersion(got.role, got.result, got.err)
			} else if got.err != nil || got.result.Code != 0 || !json.Valid(got.result.Data) {
				t.Fatal("ordinary command mixed with tell failed", got.role, got.result.Code, got.err)
			}
		case <-ctx.Done():
			t.Fatal("mixed tell/command callers did not finish", ctx.Err())
		}
	}
	readonlyOptions := integrationOptions(t)
	key, err := os.ReadFile(filepath.Join(control, "readonly.key"))
	if err != nil {
		t.Fatal(err)
	}
	readonlyOptions.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(key)))
	if err != nil {
		t.Fatal(err)
	}
	readonlyOptions.Identity = "client.readonly"
	readonly, err := cephmsgr.Dial(ctx, readonlyOptions)
	if err != nil {
		t.Fatal("authenticate read-only tell client", err)
	}
	t.Cleanup(func() { readonly.Close() })
	for _, call := range []func(context.Context, cephmsgr.Command) (cephmsgr.Result, error){readonly.MonTell, readonly.MgrTell} {
		result, err := call(ctx, versionCommand)
		var server *cephmsgr.CommandError
		var auth *cephmsgr.AuthenticationError
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &server) || errors.As(err, &auth) || errors.As(err, &unknown) || result.Code != -13 || server.Code != result.Code || server.Message != result.Message || result.Message == "" {
			t.Fatal("tell capability denial lost authenticated command result", result.Code, err)
		}
	}
	for _, mgr := range []bool{false, true} {
		if err := fixtureRecoveryRead(ctx, readonly, mgr); err != nil {
			t.Fatal("read-only ordinary command after tell denial", mgr, err)
		}
	}
	state := readonly.Snapshot()
	if state.AuthRejection != nil || !state.Monitor.Ready || !state.Manager.Ready || state.GlobalID == 0 {
		t.Fatal("tell capability denial corrupted authenticated state", state)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	for _, call := range []func(context.Context, cephmsgr.Command) (cephmsgr.Result, error){c.MonTell, c.MgrTell} {
		var unknown *cephmsgr.OutcomeUnknownError
		if _, err := call(ctx, versionCommand); !errors.Is(err, cephmsgr.ErrClosed) || errors.As(err, &unknown) {
			t.Fatal("tell after Close lost known non-execution", err)
		}
	}
	t.Log("native CLI MON/MGR versions, daemon-local errors, mixed command/tell concurrency, capability denial and Close passed")
}
