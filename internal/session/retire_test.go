package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestRetirementRejectsLateAdmissionAndDrainsAcceptedRequest(t *testing.T) {
	s, reader, writer := testSession(t)
	s.Start()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	accepted := make(chan error, 1)
	go func() {
		_, err := s.Call(ctx, msgr.MessageData{Type: msgr.MonCommandMessage})
		accepted <- err
	}()
	request := readRequest(t, reader)
	s.Retire()
	_, err := s.Call(ctx, msgr.MessageData{Type: msgr.MonCommandMessage})
	var unknown *OutcomeUnknownError
	if !errors.Is(err, ErrRetired) || errors.As(err, &unknown) {
		t.Fatal("late admission was not rejected before transmission", err)
	}
	idle := make(chan error, 1)
	go func() { idle <- s.WaitIdle(ctx) }()
	select {
	case err := <-idle:
		t.Fatal("retirement forgot an accepted request", err)
	default:
	}
	reply(t, writer, request, 1)
	if err := <-accepted; err != nil {
		t.Fatal("retirement interrupted an accepted request", err)
	}
	if err := <-idle; err != nil {
		t.Fatal("retirement did not drain", err)
	}
	s.Fail(ErrRetired)
	_, err = s.Call(ctx, msgr.MessageData{Type: msgr.MonCommandMessage})
	if !errors.Is(err, ErrRetired) || errors.As(err, &unknown) {
		t.Fatal("retired session lost its admission rejection", err)
	}
}
