package session

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

type cleanupConn struct {
	net.Conn
	entered, release chan struct{}
	once             sync.Once
}

func (c *cleanupConn) Close() error {
	c.once.Do(func() {
		c.Conn.Close()
		close(c.entered)
		<-c.release
	})
	return nil
}

func TestWaitIncludesConnectionAndCallbackCleanup(t *testing.T) {
	for _, owner := range []string{"before Start", "external Fail", "reader Fail"} {
		t.Run(owner, func(t *testing.T) {
			conn, peer := net.Pipe()
			closeRelease, callbackRelease := make(chan struct{}), make(chan struct{})
			var closeOnce, callbackOnce sync.Once
			releaseClose := func() { closeOnce.Do(func() { close(closeRelease) }) }
			releaseCallback := func() { callbackOnce.Do(func() { close(callbackRelease) }) }
			held := &cleanupConn{Conn: conn, entered: make(chan struct{}), release: closeRelease}
			callbackEntered, failed := make(chan struct{}), make(chan struct{})
			s := New(&Transport{Conn: held, Reader: msgr.NewReader(held, 0), Writer: msgr.NewWriter(held, 0)}, Config{WriteTimeout: time.Second}, nil, func(error) {
				close(callbackEntered)
				<-callbackRelease
			})
			t.Cleanup(func() { releaseClose(); releaseCallback(); s.Fail(ErrClosed); peer.Close(); s.Wait() })
			if owner != "before Start" {
				s.Start()
			}
			if owner == "reader Fail" {
				peer.Close()
			} else {
				go func() { s.Fail(io.EOF); close(failed) }()
			}
			await := func(ch <-chan struct{}) {
				t.Helper()
				select {
				case <-ch:
				case <-time.After(time.Second):
					t.Fatal("cleanup did not reach the barrier")
				}
			}
			await(held.entered)
			// Repeated failure must neither replace the original cause nor
			// wait on the owner that is still finishing connection cleanup.
			repeated := make(chan struct{})
			go func() { s.Fail(ErrClosed); close(repeated) }()
			await(repeated)
			if !errors.Is(s.Err(), io.EOF) {
				t.Fatal("repeated Fail replaced the original cause", s.Err())
			}
			waited := make(chan struct{})
			go func() { s.Wait(); close(waited) }()
			assertWaiting := func(stage string) {
				t.Helper()
				select {
				case <-waited:
					t.Errorf("Wait returned before %s cleanup", stage)
				case <-time.After(20 * time.Millisecond):
				}
			}
			assertWaiting("connection")
			releaseClose()
			await(callbackEntered)
			assertWaiting("callback")
			releaseCallback()
			await(waited)
			if owner != "reader Fail" {
				await(failed)
			}
			// Fail before Start seals admission; starting this failed session
			// later must not introduce workers after Wait has returned.
			s.Start()
			s.Wait()
		})
	}
}

func TestWaitBeforeStartWaitsForSessionTermination(t *testing.T) {
	conn, peer := net.Pipe()
	s := New(&Transport{Conn: conn, Reader: msgr.NewReader(conn, 0), Writer: msgr.NewWriter(conn, 0)}, Config{WriteTimeout: time.Second}, nil, nil)
	t.Cleanup(func() { s.Fail(ErrClosed); peer.Close(); s.Wait() })
	waited := make(chan struct{})
	go func() { s.Wait(); close(waited) }()
	select {
	case <-waited:
		t.Error("Wait returned before an unstarted session terminated")
	case <-time.After(20 * time.Millisecond):
	}
	s.Start()
	peer.Close()
	select {
	case <-waited:
	case <-time.After(time.Second):
		t.Fatal("Wait did not finish after started workers terminated")
	}
}
