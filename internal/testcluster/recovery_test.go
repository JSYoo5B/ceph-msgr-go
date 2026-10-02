package testcluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

type timeoutOnlyError struct{}

func (timeoutOnlyError) Error() string   { return "unrecognized timeout" }
func (timeoutOnlyError) Timeout() bool   { return true }
func (timeoutOnlyError) Temporary() bool { return true }

type misleadingRecoveryError struct{}

func (misleadingRecoveryError) Error() string        { return "unrecognized error with custom Is" }
func (misleadingRecoveryError) Is(target error) bool { return target == io.EOF }

type cyclicRecoveryError struct{}

func (*cyclicRecoveryError) Error() string   { return "cyclic cause" }
func (e *cyclicRecoveryError) Unwrap() error { return e }

type recoveryServerError struct{ code int }

func (e recoveryServerError) Error() string { return fmt.Sprintf("server error %d", e.code) }

func TestIsRecoveryErrorRequiresEveryCauseToBeAllowed(t *testing.T) {
	op := &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}
	auth := &cephx.AuthenticationError{Method: 2, Code: -13}
	changed := errors.New("active manager changed")
	badJoin := errors.Join(io.EOF, msgr.ErrFrame)
	unknown := func(cause error) error { return &session.OutcomeUnknownError{Cause: cause} }
	for _, test := range []struct {
		name            string
		err             error
		allowed         []error
		want            bool
		formerlyAllowed bool
	}{
		{name: "nil"},
		{name: "EOF", err: io.EOF, want: true},
		{name: "truncated transport", err: io.ErrUnexpectedEOF, want: true},
		{name: "closed socket", err: net.ErrClosed, want: true},
		{name: "wrapped transport", err: fmt.Errorf("handshake: %w", io.EOF), want: true},
		{name: "uncertain transport", err: unknown(op), want: true},
		{name: "syscall transport", err: op, want: true},
		{name: "raw syscall", err: syscall.ECONNRESET},
		{name: "zero syscall", err: &net.OpError{Err: syscall.Errno(0)}, formerlyAllowed: true},
		{name: "DNS transport", err: &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "fixture.invalid", IsNotFound: true}}, want: true},
		{name: "raw DNS", err: &net.DNSError{Err: "no such host", Name: "fixture.invalid", IsNotFound: true}},
		{name: "empty DNS cause", err: &net.OpError{Err: &net.DNSError{}}, formerlyAllowed: true},
		{name: "DNS wraps protocol", err: &net.OpError{Err: &net.DNSError{Err: "lookup failed", UnwrapErr: msgr.ErrFrame}}, formerlyAllowed: true},
		{name: "socket deadline", err: &net.OpError{Err: os.ErrDeadlineExceeded}, want: true},
		{name: "raw OS deadline", err: os.ErrDeadlineExceeded},
		{name: "deadline requires policy", err: context.DeadlineExceeded},
		{name: "allowed deadline", err: unknown(context.DeadlineExceeded), allowed: []error{context.DeadlineExceeded}, want: true},
		{name: "network deadline requires policy", err: &net.OpError{Err: context.DeadlineExceeded}, formerlyAllowed: true},
		{name: "allowed network deadline", err: &net.OpError{Err: context.DeadlineExceeded}, allowed: []error{context.DeadlineExceeded}, want: true},
		{name: "cancel requires policy", err: context.Canceled},
		{name: "allowed cancel", err: fmt.Errorf("setup: %w", context.Canceled), allowed: []error{context.Canceled}, want: true},
		{name: "retirement requires policy", err: session.ErrRetired},
		{name: "allowed retirement", err: unknown(session.ErrRetired), allowed: []error{session.ErrRetired}, want: true},
		{name: "allowed keepalive", err: unknown(session.ErrKeepaliveTimeout), allowed: []error{session.ErrKeepaliveTimeout}, want: true},
		{name: "allowed manager change", err: unknown(changed), allowed: []error{changed}, want: true},
		{name: "Close requires policy", err: session.ErrClosed},
		{name: "allowed Close", err: unknown(session.ErrClosed), allowed: []error{session.ErrClosed}, want: true},
		{name: "unknown protocol", err: unknown(msgr.ErrFrame), formerlyAllowed: true},
		{name: "unknown crypto", err: unknown(msgr.ErrAuthentication), formerlyAllowed: true},
		{name: "unknown auth", err: unknown(auth), formerlyAllowed: true},
		{name: "server rejection", err: recoveryServerError{code: -22}},
		{name: "unknown server rejection", err: unknown(recoveryServerError{code: -22}), formerlyAllowed: true},
		{name: "unknown nil cause", err: unknown(nil), formerlyAllowed: true},
		{name: "arbitrary net error", err: timeoutOnlyError{}},
		{name: "net operation wraps arbitrary", err: &net.OpError{Err: errors.New("unrecognized failure")}, formerlyAllowed: true},
		{name: "net operation wraps protocol", err: &net.OpError{Err: fmt.Errorf("decode: %w", msgr.ErrFrame)}, formerlyAllowed: true},
		{name: "nil operation cause", err: &net.OpError{}, formerlyAllowed: true},
		{name: "typed nil operation", err: (*net.OpError)(nil)},
		{name: "typed nil unknown", err: (*session.OutcomeUnknownError)(nil), formerlyAllowed: true},
		{name: "custom Is hides failure", err: misleadingRecoveryError{}, formerlyAllowed: true},
		{name: "joined transport and protocol", err: errors.Join(op, msgr.ErrFrame), formerlyAllowed: true},
		{name: "joined transport and auth", err: errors.Join(op, auth), formerlyAllowed: true},
		{name: "joined transport and crypto", err: errors.Join(io.EOF, msgr.ErrAuthentication), formerlyAllowed: true},
		{name: "joined transport and server rejection", err: errors.Join(op, recoveryServerError{code: -22}), formerlyAllowed: true},
		{name: "nested uncertain bad join", err: unknown(fmt.Errorf("setup: %w", errors.Join(io.EOF, auth))), formerlyAllowed: true},
		{name: "allowed sentinel cannot hide join", err: errors.Join(context.DeadlineExceeded, msgr.ErrFrame), allowed: []error{context.DeadlineExceeded}},
		{name: "allowed outer join cannot hide cause", err: badJoin, allowed: []error{badJoin}, formerlyAllowed: true},
		{name: "all joined causes allowed", err: unknown(errors.Join(op, session.ErrRetired)), allowed: []error{session.ErrRetired}, want: true},
		{name: "network policy does not leak across join", err: errors.Join(op, syscall.ECONNRESET), formerlyAllowed: true},
		{name: "network join checks every cause", err: &net.OpError{Err: errors.Join(syscall.ECONNRESET, msgr.ErrFrame)}, formerlyAllowed: true},
		{name: "cycle rejected", err: &cyclicRecoveryError{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := IsRecoveryError(test.err, test.allowed...); got != test.want {
				t.Fatalf("accepted=%v, want %v", got, test.want)
			}
			if test.formerlyAllowed {
				// The old recovery-read oracle accepted any unknown outcome or
				// net.OpError; its errors.Is checks could also hide joined causes.
				var uncertain *session.OutcomeUnknownError
				var network *net.OpError
				old := errors.As(test.err, &uncertain) || errors.As(test.err, &network) || errors.Is(test.err, io.EOF) || errors.Is(test.err, io.ErrUnexpectedEOF) || errors.Is(test.err, net.ErrClosed)
				if !old || test.want {
					t.Fatal("regression case does not demonstrate an old false positive")
				}
			}
		})
	}
}
