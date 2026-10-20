package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

func TestFreshMDSHandshakeNegativeRankClosesBeforeAuthentication(t *testing.T) {
	client, peer := net.Pipe()
	defer peer.Close()
	// A nil authenticator is safe here only because invalid rank admission
	// must close before reading, writing or starting authentication.
	tr, err := HandshakeFreshMDS(context.Background(), client, msgr.Address{Type: 2}, -1, nil, SecureMode, 4096, time.Second)
	if tr != nil || err == nil || !strings.Contains(err.Error(), "nonnegative active rank") {
		t.Fatal("negative rank was admitted", err)
	}
	var one [1]byte
	if n, err := peer.Read(one[:]); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatal("invalid rank did not close without traffic", n, err)
	}
}

func TestFreshMDSHandshakeCancellationBeforeBanner(t *testing.T) {
	client, peer := net.Pipe()
	defer peer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tr, err := HandshakeFreshMDS(ctx, client, msgr.Address{Type: 2}, 0, nil, SecureMode, 4096, time.Second)
	if tr != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled setup returned a transport or lost its cause", err)
	}
	var one [1]byte
	if _, err := peer.Read(one[:]); !errors.Is(err, io.EOF) {
		t.Fatal("canceled setup retained its connection", err)
	}
}

func TestFreshMDSHandshakeCancellationAndTimeoutBeforeIdentification(t *testing.T) {
	for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
		for _, timeout := range []bool{false, true} {
			t.Run(fmt.Sprintf("mode%d/timeout%t", mode, timeout), func(t *testing.T) {
				auth, fixture, err := freshMDSAuthorizer(cephx.AES256K, mode)
				if err != nil {
					t.Fatal(err)
				}
				client, peer := net.Pipe()
				t.Cleanup(func() { client.Close(); peer.Close() })
				entered := make(chan struct{})
				cfg := freshMDSPeerDefaults()
				cfg.beforeServerIdent = func(r *msgr.Reader, _ *msgr.Writer) error {
					close(entered)
					return freshMDSNoTraffic(r)
				}
				peerDone := make(chan error, 1)
				go func() { peerDone <- freshMDSPeer(peer, fixture, mode, cfg) }()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				duration := time.Second
				if timeout {
					duration = 150 * time.Millisecond
				}
				finished := make(chan error, 1)
				go func() {
					tr, err := HandshakeFreshMDS(ctx, client, msgr.Address{Type: 2}, 0, auth, mode, 4096, duration)
					if tr != nil {
						tr.Conn.Close()
						err = errors.New("unfinished identification returned a transport")
					}
					finished <- err
				}()
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("setup did not reach the deterministic identification barrier")
				}
				if !timeout {
					cancel()
				}
				select {
				case err := <-finished:
					if timeout {
						var network net.Error
						if !errors.As(err, &network) || !network.Timeout() {
							t.Fatal("setup deadline lost its transport timeout", err)
						}
					} else if !errors.Is(err, context.Canceled) {
						t.Fatal("setup cancellation lost its context cause", err)
					}
				case <-time.After(time.Second):
					t.Fatal("fresh MDS setup did not stop")
				}
				if err := <-peerDone; err != nil {
					t.Fatal("canceled setup retried or emitted application traffic", err)
				}
			})
		}
	}
}

func TestFreshMDSHandshakeDoesNotReuseCookieAcrossAttempts(t *testing.T) {
	var previous uint64
	for range 2 {
		auth, fixture, err := freshMDSAuthorizer(cephx.AES256K, CRCMode)
		if err != nil {
			t.Fatal(err)
		}
		client, peer := net.Pipe()
		cfg := freshMDSPeerDefaults()
		seen := make(chan uint64, 1)
		cfg.clientIdent = func(_ msgr.Address, _, _, _, cookie uint64) error { seen <- cookie; return nil }
		done := make(chan error, 1)
		go func() { done <- freshMDSPeer(peer, fixture, CRCMode, cfg) }()
		tr, err := HandshakeFreshMDS(context.Background(), client, msgr.Address{Type: 2}, 0, auth, CRCMode, 4096, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		tr.Conn.Close()
		cookie := <-seen
		if cookie == 0 || cookie == previous {
			t.Fatal("fresh attempt reused its previous cookie")
		}
		previous = cookie
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
