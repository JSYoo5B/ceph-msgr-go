package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// A started read can cross renewal rejection. Preserve its uncertainty while
// requiring every endpoint's failure to report the independently observed code.
func matchesAuthenticationRejection(err error, method uint32, code int32) bool {
	switch cause := err.(type) {
	case *cephmsgr.AuthenticationError:
		return cause != nil && cause.Method == method && cause.Code == code
	case interface{ Unwrap() []error }:
		children := cause.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !matchesAuthenticationRejection(child, method, code) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return matchesAuthenticationRejection(cause.Unwrap(), method, code)
	}
	return false
}

func TestAuthenticationRejectionOracle(t *testing.T) {
	rejection := func() error { return &cephmsgr.AuthenticationError{Method: 2, Code: -13} }
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{"explicit", rejection(), true},
		{"wrapped", fmt.Errorf("authentication: %w", rejection()), true},
		{"inflight endpoint rejection", &cephmsgr.OutcomeUnknownError{Cause: errors.Join(rejection(), rejection(), rejection())}, true},
		{"nested inflight endpoint rejection", fmt.Errorf("MON read: %w", &cephmsgr.OutcomeUnknownError{Cause: errors.Join(fmt.Errorf("first endpoint: %w", rejection()), fmt.Errorf("next endpoint: %w", rejection()))}), true},
		{"nil", nil, false},
		{"no auth rejection", io.EOF, false},
		{"missing cause", &cephmsgr.OutcomeUnknownError{}, false},
		{"different code", errors.Join(rejection(), &cephmsgr.AuthenticationError{Method: 2, Code: -1}), false},
		{"different method", errors.Join(rejection(), &cephmsgr.AuthenticationError{Method: 1, Code: -13}), false},
		{"joined protocol failure", &cephmsgr.OutcomeUnknownError{Cause: errors.Join(rejection(), cephmsgr.ErrMalformedMessage)}, false},
		{"joined crypto failure", &cephmsgr.OutcomeUnknownError{Cause: errors.Join(rejection(), errors.New("cipher authentication failure"))}, false},
		{"joined transport failure", &cephmsgr.OutcomeUnknownError{Cause: errors.Join(rejection(), io.EOF)}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := matchesAuthenticationRejection(test.err, 2, -13); got != test.want {
				t.Fatal("incorrect authentication rejection classification", got, test.want, test.err)
			}
		})
	}
}

func TestCephRevokedCredentialRecoveryIntegration(t *testing.T) {
	admin, ctx := fixtureClient(t, time.Minute)
	encoded, err := os.ReadFile(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "revocable.key"))
	if err != nil {
		t.Fatal(err)
	}
	options := integrationOptions(t)
	options.Identity = "client.revocable"
	options.Key, err = cephmsgr.ParseKey(string(bytes.TrimSpace(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.MgrCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"pg stat"}`)}); err != nil {
		t.Fatal("MGR before key revocation", err)
	}
	keyring := append([]byte("[client.revocable]\n\tkey = "), bytes.TrimSpace(encoded)...)
	keyring = append(keyring, []byte("\n\tcaps mon = \"allow *\"\n\tcaps mgr = \"allow *\"\n")...)
	restored := false
	t.Cleanup(func() {
		if restored {
			return
		}
		restore, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		if _, err := admin.MonCommand(restore, cephmsgr.Command{JSON: []byte(`{"prefix":"auth import"}`), Input: keyring}); err != nil {
			t.Error("restore fixture credential after revocation", err)
		}
	})
	if _, err := admin.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"auth rm","entity":"client.revocable"}`)}); err != nil {
		t.Fatal("remove fixture credential", err)
	}
	// Independently observe the current daemon's explicit rejection. It is
	// distinct from ticket expiration or a locally canceled discovery wait.
	var server *cephmsgr.AuthenticationError
	for {
		fresh, rejected := cephmsgr.Dial(ctx, options)
		if fresh != nil {
			fresh.Close()
		} else {
			if !errors.As(rejected, &server) || server.Method != 2 || server.Code >= 0 || !matchesAuthenticationRejection(rejected, server.Method, server.Code) {
				t.Fatal("fresh connection did not report explicit server rejection", rejected)
			}
			break
		}
		// The committed removal can reach followers after the command
		// response. A still-authenticated follower is not a rejection.
		select {
		case <-ctx.Done():
			t.Fatal("key removal did not reach authentication servers", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Logf("revoked credential: method=%d code=%d", server.Method, server.Code)
	// The short TTL triggers real background reauthentication. No fixture
	// code changes the client's published tickets or forces the renewal.
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var inflightRejections int
	for {
		_, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"status"}`)})
		if err != nil {
			if !matchesAuthenticationRejection(err, server.Method, server.Code) {
				t.Fatal("renewal rejection changed the server result", err)
			}
			var unknown *cephmsgr.OutcomeUnknownError
			if !errors.As(err, &unknown) {
				break // A subsequent call observes rejection before admission.
			}
			inflightRejections++
		}
		select {
		case <-deadline.C:
			t.Fatal("background authentication rejection never reached new MON calls", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
	t.Logf("renewal rejection: %d in-flight reads; subsequent MON admission rejected", inflightRejections)
	_, err = c.MgrCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"pg stat"}`)})
	var unknown *cephmsgr.OutcomeUnknownError
	if !matchesAuthenticationRejection(err, server.Method, server.Code) || errors.As(err, &unknown) {
		t.Fatal("new MGR call lost the MON authentication rejection", err)
	}
	rejectedState := c.Snapshot()
	if rejectedState.AuthRejection == nil || rejectedState.AuthRejection.Code != server.Code || rejectedState.AuthRejection.Method != server.Method || rejectedState.Monitor.Ready || rejectedState.Manager.Ready || rejectedState.Closed {
		t.Fatal("public state did not preserve genuine renewal rejection", rejectedState)
	}
	for _, wait := range []func(context.Context) error{c.WaitMonReady, c.WaitMgrReady} {
		var unknown *cephmsgr.OutcomeUnknownError
		if err := wait(ctx); !matchesAuthenticationRejection(err, server.Method, server.Code) || errors.As(err, &unknown) {
			t.Fatal("preparation lost the actual authentication rejection", err)
		}
	}
	// Restore the same fixture key through an independently authenticated
	// administrator. The rejected client must recover without being rebuilt.
	if _, err := admin.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"auth import"}`), Input: keyring}); err != nil {
		t.Fatal("restore fixture credential", err)
	}
	restored = true
	for {
		_, err = c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"status"}`)})
		if err == nil {
			break
		}
		if !matchesAuthenticationRejection(err, server.Method, server.Code) {
			t.Fatal("unexpected recovery error after restoring credential", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("restored credential did not recover", ctx.Err())
		case <-ticker.C:
		}
	}
	if _, err := c.MgrCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"pg stat"}`)}); err != nil {
		t.Fatal("MGR after credential restoration", err)
	}
	recoveredState := waitClientState(t, c, ctx, func(s cephmsgr.State) bool { return s.Monitor.Ready && s.Manager.Ready })
	if recoveredState.AuthRejection != nil || !recoveredState.Monitor.Ready || !recoveredState.Manager.Ready || recoveredState.Closed || recoveredState.GlobalID != rejectedState.GlobalID {
		t.Fatal("public state did not recover with the restored credential", recoveredState)
	}
	for _, wait := range []func(context.Context) error{c.WaitMonReady, c.WaitMgrReady} {
		if err := wait(ctx); err != nil {
			t.Fatal("preparation after real credential restoration", err)
		}
	}
}
