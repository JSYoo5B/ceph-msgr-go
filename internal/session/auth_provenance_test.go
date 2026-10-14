package session

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

type observedFixtureAuth struct {
	fixtureAuth
	writes []struct {
		tag      msgr.Tag
		bytes    int
		complete bool
	}
}

func (a *observedFixtureAuth) AuthWriteObserved(tag msgr.Tag, n int, complete bool) {
	a.writes = append(a.writes, struct {
		tag      msgr.Tag
		bytes    int
		complete bool
	}{tag, n, complete})
}

type authWriteFaultConn struct {
	net.Conn
	writes  int
	partial bool
}

func (c *authWriteFaultConn) Write(p []byte) (int, error) {
	c.writes++
	// Actual third protocol write is AUTH_REQUEST, after banner and HELLO.
	if c.writes == 3 {
		if !c.partial {
			return 0, io.ErrClosedPipe
		}
		n, err := c.Conn.Write(p[:7])
		if err != nil {
			return n, err
		}
		return n, io.ErrClosedPipe
	}
	return c.Conn.Write(p)
}

func TestAuthProvenanceReportsActualCompletePartialAndZeroWrites(t *testing.T) {
	for _, mode := range []string{"complete", "partial", "zero"} {
		t.Run(mode, func(t *testing.T) {
			client, peer := net.Pipe()
			t.Cleanup(func() { client.Close(); peer.Close() })
			auth := &observedFixtureAuth{fixtureAuth: fixtureAuthData()}
			peerDone := make(chan error, 1)
			go func() {
				peerDone <- handshakePeer(peer, auth.fixtureAuth, handshakePeerConfig{mode: 2, peerRole: 16, serverID: 77, serverFlags: 1})
			}()
			var conn net.Conn = client
			if mode != "complete" {
				conn = &authWriteFaultConn{Conn: client, partial: mode == "partial"}
			}
			tr, err := Handshake(context.Background(), conn, msgr.Address{Type: 2}, 16, 77, auth, SecureMode, 4096, time.Second)
			if mode == "complete" {
				if err != nil {
					t.Fatal(err)
				}
				tr.Conn.Close()
			} else if !errors.Is(err, io.ErrClosedPipe) {
				t.Fatal("lost actual transport-write error", err)
			}
			if len(auth.writes) != 1 {
				t.Fatal("wrong auth frame-write event count")
			}
			got := auth.writes[0]
			wantBytes := 53
			if mode == "partial" {
				wantBytes = 7
			}
			if mode == "zero" {
				wantBytes = 0
			}
			if got.tag != msgr.AuthRequest || got.bytes != wantBytes || got.complete != (mode == "complete") {
				t.Fatal("observer did not report actual local protocol write", got)
			}
			select {
			case <-peerDone:
			case <-time.After(3 * time.Second):
				t.Fatal("synthetic peer did not terminate")
			}
		})
	}
}

func TestAuthProvenanceCompletedWriteDoesNotImplyCanceledHandshakeAcceptance(t *testing.T) {
	client, peer := net.Pipe()
	release := make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	held := clearingDeadlineConn{Conn: client, clearing: make(chan struct{}), release: release}
	t.Cleanup(func() { finish(); client.Close(); peer.Close() })
	auth := &observedFixtureAuth{fixtureAuth: fixtureAuthData()}
	peerDone := make(chan error, 1)
	go func() {
		peerDone <- handshakePeer(peer, auth.fixtureAuth, handshakePeerConfig{mode: 2, peerRole: 16, serverID: 77, serverFlags: 1})
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	returned := make(chan error, 1)
	go func() {
		_, err := Handshake(ctx, held, msgr.Address{Type: 2}, 16, 77, auth, SecureMode, 4096, 30*time.Second)
		returned <- err
	}()
	select {
	case <-held.clearing:
	case <-time.After(3 * time.Second):
		t.Fatal("handshake did not reach cancellation gate")
	}
	cancel()
	finish()
	select {
	case err := <-returned:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("canceled handshake was accepted", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handshake did not terminate")
	}
	if len(auth.writes) != 1 || !auth.writes[0].complete || auth.writes[0].bytes != 53 {
		t.Fatal("cancel race lost prior complete auth write")
	}
	select {
	case <-peerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("synthetic peer did not terminate")
	}
}
