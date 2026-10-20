package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// RADOS_BACKOFF shares bit 21/incarnation 2 with RESEND_ON_SPLIT,
// OSDMAP_PG_UPMAP and CRUSH_CHOOSE_ARGS. Test the peer requirement at the
// actual role-4 handshake boundary; decoding controls cannot justify this bit.
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/include/ceph_features.h#L114
func TestOSDHandshakeRejectsCombinedBackoffRequirementBeforeApplication(t *testing.T) {
	const backoffMask = uint64(1<<21 | 1<<57)
	client, peer := net.Pipe()
	auth := fixtureAuthData()
	done := make(chan error, 1)
	advertised := make(chan uint64, 1)
	go func() {
		done <- handshakePeer(peer, auth, handshakePeerConfig{
			mode: 2, peerRole: 4, serverFlags: 1,
			serverSupported: Features | 1<<8 | 1<<9 | backoffMask,
			serverRequired:  RequiredFeatures | backoffMask,
			identFeatures: func(bits uint64) error {
				advertised <- bits
				return nil
			},
			afterIdent: func(_ net.Conn, r *msgr.Reader, _ *msgr.Writer) error {
				f, err := r.Read()
				if err == nil {
					return fmt.Errorf("client sent frame %d after unsupported OSD feature requirement", f.Tag)
				}
				if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
					return err
				}
				return nil
			},
		})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	tr, err := Handshake(ctx, client, msgr.Address{Type: 2}, 4, 0, auth, SecureMode, 4096, time.Second)
	if tr != nil {
		tr.Conn.Close()
		t.Fatal("unsupported backoff feature produced a usable OSD transport")
	}
	if !errors.Is(err, msgr.ErrFeatures) {
		t.Fatal("combined backoff requirement did not fail feature admission", err)
	}
	select {
	case bits := <-advertised:
		if bits&backoffMask == backoffMask {
			t.Fatal("OSD client advertised the unsupported combined feature")
		}
	case <-ctx.Done():
		t.Fatal("peer did not observe OSD identification", ctx.Err())
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("client failed to close before application traffic", err)
		}
	case <-ctx.Done():
		t.Fatal("peer did not finish rejected handshake", ctx.Err())
	}
}
