package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// A started read can cross renewal rejection. Preserve its uncertainty while
// requiring matching explicit rejection. Other endpoint failures may be transport
// losses or setup deadlines; they must not mask protocol or cryptographic errors.
func matchesAuthenticationRejection(err error, method uint32, code int32) bool {
	allowed := []error{context.DeadlineExceeded} // An individual endpoint's setup bound.
	var collect func(error)
	collect = func(err error) {
		switch cause := err.(type) {
		case *cephmsgr.AuthenticationError:
			if cause != nil && cause.Method == method && cause.Code == code {
				allowed = append(allowed, cause)
			}
		case interface{ Unwrap() []error }:
			for _, child := range cause.Unwrap() {
				collect(child)
			}
		case interface{ Unwrap() error }:
			collect(cause.Unwrap())
		}
	}
	collect(err)
	return len(allowed) > 1 && testcluster.IsRecoveryError(err, allowed...)
}

type authOracleNetError struct{}

func (authOracleNetError) Error() string   { return "not a transport failure" }
func (authOracleNetError) Timeout() bool   { return true }
func (authOracleNetError) Temporary() bool { return true }

func TestAuthenticationRejectionOracle(t *testing.T) {
	rejection := func() error { return &cephmsgr.AuthenticationError{Method: 2, Code: -13} }
	transport := &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{"explicit", rejection(), true},
		{"wrapped", fmt.Errorf("authentication: %w", rejection()), true},
		{"inflight endpoint rejection", &cephmsgr.OutcomeUnknownError{Cause: errors.Join(rejection(), rejection(), rejection())}, true},
		{"nested inflight endpoint rejection", fmt.Errorf("MON read: %w", &cephmsgr.OutcomeUnknownError{Cause: errors.Join(fmt.Errorf("first endpoint: %w", rejection()), fmt.Errorf("next endpoint: %w", rejection()))}), true},
		{"endpoint EOF and explicit rejection", errors.Join(io.EOF, rejection()), true},
		{"endpoint deadline and explicit rejection", errors.Join(context.DeadlineExceeded, rejection()), true},
		{"nested inflight auth and transport", fmt.Errorf("MON read: %w", &cephmsgr.OutcomeUnknownError{Cause: errors.Join(fmt.Errorf("first endpoint: %w", rejection()), errors.Join(io.EOF, transport, context.DeadlineExceeded))}), true},
		{"nil", nil, false},
		{"no auth rejection", io.EOF, false},
		{"transport only", errors.Join(io.EOF, transport, context.DeadlineExceeded), false},
		{"missing cause", &cephmsgr.OutcomeUnknownError{}, false},
		{"different code", errors.Join(rejection(), &cephmsgr.AuthenticationError{Method: 2, Code: -1}), false},
		{"different method", errors.Join(rejection(), &cephmsgr.AuthenticationError{Method: 1, Code: -13}), false},
		{"joined protocol failure", &cephmsgr.OutcomeUnknownError{Cause: errors.Join(rejection(), cephmsgr.ErrMalformedMessage)}, false},
		{"joined crypto failure", &cephmsgr.OutcomeUnknownError{Cause: errors.Join(rejection(), msgr.ErrAuthentication)}, false},
		{"joined unknown failure", &cephmsgr.OutcomeUnknownError{Cause: errors.Join(rejection(), errors.New("unrecognized failure"))}, false},
		{"transport cannot mask protocol", errors.Join(rejection(), transport, cephmsgr.ErrMalformedMessage), false},
		{"net operation wraps unknown", errors.Join(rejection(), &net.OpError{Err: errors.New("unrecognized failure")}), false},
		{"net operation wraps crypto", errors.Join(rejection(), &net.OpError{Err: msgr.ErrAuthentication}), false},
		{"net error spoof", errors.Join(rejection(), authOracleNetError{}), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := matchesAuthenticationRejection(test.err, 2, -13); got != test.want {
				t.Fatalf("incorrect authentication rejection classification: got=%t want=%t error=%q", got, test.want, fmt.Sprint(test.err))
			}
		})
	}
}

func TestCephRevokedCredentialRecoveryIntegration(t *testing.T) {
	admin, ctx := fixtureClient(t, time.Minute)
	encoded, err := os.ReadFile(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "revocable.key"))
	if err != nil {
		t.Fatalf("read fixture credential: %q", fmt.Sprint(err))
	}
	options := integrationOptions(t)
	options.Identity = "client.revocable"
	options.Key, err = cephmsgr.ParseKey(string(bytes.TrimSpace(encoded)))
	if err != nil {
		t.Fatalf("parse fixture credential: %q", fmt.Sprint(err))
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatalf("initial revocable client authentication: %q", fmt.Sprint(err))
	}
	defer c.Close()
	if _, err := c.MgrCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"pg stat"}`)}); err != nil {
		t.Fatalf("MGR before key revocation: %q", fmt.Sprint(err))
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
			t.Errorf("restore fixture credential after revocation: %q", fmt.Sprint(err))
		}
	})
	if _, err := admin.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"auth rm","entity":"client.revocable"}`)}); err != nil {
		t.Fatalf("remove fixture credential: %q", fmt.Sprint(err))
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
				t.Fatalf("fresh connection did not report explicit server rejection: %q", fmt.Sprint(rejected))
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
				t.Fatalf("renewal rejection changed the server result: %q", fmt.Sprint(err))
			}
			var unknown *cephmsgr.OutcomeUnknownError
			if !errors.As(err, &unknown) {
				break // A subsequent call observes rejection before admission.
			}
			inflightRejections++
		}
		select {
		case <-deadline.C:
			t.Fatalf("background authentication rejection never reached new MON calls: %q", fmt.Sprint(err))
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
	t.Logf("renewal rejection: %d in-flight reads; subsequent MON admission rejected", inflightRejections)
	_, err = c.MgrCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"pg stat"}`)})
	var unknown *cephmsgr.OutcomeUnknownError
	if !matchesAuthenticationRejection(err, server.Method, server.Code) || errors.As(err, &unknown) {
		t.Fatalf("new MGR call lost the MON authentication rejection: %q", fmt.Sprint(err))
	}
	rejectedState := c.Snapshot()
	if rejectedState.AuthRejection == nil || rejectedState.AuthRejection.Code != server.Code || rejectedState.AuthRejection.Method != server.Method || rejectedState.Monitor.Ready || rejectedState.Manager.Ready || rejectedState.Closed {
		t.Fatal("public state did not preserve genuine renewal rejection", rejectedState)
	}
	for _, wait := range []func(context.Context) error{c.WaitMonReady, c.WaitMgrReady} {
		var unknown *cephmsgr.OutcomeUnknownError
		if err := wait(ctx); !matchesAuthenticationRejection(err, server.Method, server.Code) || errors.As(err, &unknown) {
			t.Fatalf("preparation lost the actual authentication rejection: %q", fmt.Sprint(err))
		}
	}
	// Restore the same fixture key through an independently authenticated
	// administrator. The rejected client must recover without being rebuilt.
	if _, err := admin.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"auth import"}`), Input: keyring}); err != nil {
		t.Fatalf("restore fixture credential: %q", fmt.Sprint(err))
	}
	restored = true
	for {
		_, err = c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"status"}`)})
		if err == nil {
			break
		}
		if !matchesAuthenticationRejection(err, server.Method, server.Code) {
			t.Fatalf("unexpected recovery error after restoring credential: %q", fmt.Sprint(err))
		}
		select {
		case <-ctx.Done():
			t.Fatal("restored credential did not recover", ctx.Err())
		case <-ticker.C:
		}
	}
	if _, err := c.MgrCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"pg stat"}`)}); err != nil {
		t.Fatalf("MGR after credential restoration: %q", fmt.Sprint(err))
	}
	recoveredState := waitClientState(t, c, ctx, func(s cephmsgr.State) bool { return s.Monitor.Ready && s.Manager.Ready })
	if recoveredState.AuthRejection != nil || !recoveredState.Monitor.Ready || !recoveredState.Manager.Ready || recoveredState.Closed || recoveredState.GlobalID != rejectedState.GlobalID {
		t.Fatal("public state did not recover with the restored credential", recoveredState)
	}
	for _, wait := range []func(context.Context) error{c.WaitMonReady, c.WaitMgrReady} {
		if err := wait(ctx); err != nil {
			t.Fatalf("preparation after real credential restoration: %q", fmt.Sprint(err))
		}
	}
}
