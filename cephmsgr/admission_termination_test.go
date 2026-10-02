package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

func admissionTerminationSession(t *testing.T) *session.Session {
	t.Helper()
	conn, peer := net.Pipe()
	s := session.New(&session.Transport{Conn: conn}, session.Config{}, nil, nil)
	t.Cleanup(func() { s.Fail(ErrClosed); peer.Close(); s.Wait() })
	return s
}

func TestMonitorAdmissionReadyDoesNotTerminateSession(t *testing.T) {
	s := admissionTerminationSession(t)
	ready := make(chan struct{})
	close(ready)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waitMonitorAdmission(ctx, s, ready); err != nil {
		t.Fatal("verified monitor did not finish admission", err)
	}
	select {
	case <-s.Done():
		t.Fatal("successful admission terminated its session", s.Err())
	default:
	}
}

func TestMonitorAdmissionPreservesProtocolFailureWhenContextAlsoEnds(t *testing.T) {
	for _, end := range []string{"canceled", "deadline"} {
		t.Run(end, func(t *testing.T) {
			s := admissionTerminationSession(t)
			protocol := fmt.Errorf("%w: invalid MGR map: %w", msgr.ErrFrame, io.ErrUnexpectedEOF)
			s.Fail(protocol)
			var ctx context.Context
			var cancel context.CancelFunc
			if end == "deadline" {
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			} else {
				ctx, cancel = context.WithCancel(context.Background())
				cancel()
			}
			defer cancel()
			ready := make(chan struct{})
			// Both terminal events are ready before admission resumes. The
			// result must retain the established cause whichever select wins.
			for i := 0; i < 64; i++ {
				if err := waitMonitorAdmission(ctx, s, ready); err != protocol || !errors.Is(err, msgr.ErrFrame) || !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatal("admission replaced its established protocol failure", i, err)
				}
			}
		})
	}
}

func TestMonitorAdmissionContextTerminationPrecedesLaterProtocolFailure(t *testing.T) {
	s := admissionTerminationSession(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := waitMonitorAdmission(ctx, s, make(chan struct{}))
	if !errors.Is(err, context.Canceled) {
		t.Fatal("admission did not return its canceled context", err)
	}
	// A reader may finish decoding just after cancellation wins admission.
	// It must not establish a different session cause in that interval.
	s.Fail(fmt.Errorf("%w: invalid MGR map: %w", msgr.ErrFrame, io.ErrUnexpectedEOF))
	if s.Err() != err || !errors.Is(s.Err(), context.Canceled) {
		t.Fatal("late protocol failure replaced admission cancellation", s.Err(), err)
	}
}
