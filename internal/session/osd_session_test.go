package session

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestOSDTransactionsSurviveNewSessionsAndDispatchReplies(t *testing.T) {
	var ids atomic.Uint64
	for attempt := 0; attempt < 2; attempt++ {
		s, r, w := testSession(t)
		s.config.NextTransaction = func() (uint64, error) { return ids.Add(1), nil }
		s.Start()
		result := make(chan error, 1)
		go func() {
			m, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.OSDOpMessage})
			if err == nil && m.Type != msgr.OSDOpReplyMessage {
				err = errors.New("OSD reply lost")
			}
			result <- err
		}()
		request := readRequest(t, r)
		if request.Transaction != uint64(attempt+1) {
			t.Fatal("OSD transaction identity restarted", request.Transaction)
		}
		request.Type, request.Sequence = msgr.OSDOpReplyMessage, 1
		if err := w.Write(request.Frame()); err != nil {
			t.Fatal(err)
		}
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		s.Fail(ErrClosed)
		s.Wait()
	}
}

func TestOSDTransactionSourceFailureDoesNotAdmitRequest(t *testing.T) {
	s, _, _ := testSession(t)
	s.config.NextTransaction = func() (uint64, error) { return 0, nil }
	if _, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.OSDOpMessage}); err == nil || len(s.pending) != 0 || len(s.queue) != 0 {
		t.Fatal("invalid transaction reached admission", err)
	}
}
