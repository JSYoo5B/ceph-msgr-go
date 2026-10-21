package session

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

func TestHandshakeMissingFeaturesPreservesPeerMask(t *testing.T) {
	for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
		t.Run(fmt.Sprintf("mode-%d", mode), func(t *testing.T) {
			missing := uint64(0x8002000000000000)
			err := completeHandshakeRefusal(t, mode, handshakePeerConfig{missingFeatures: &missing})
			if !errors.Is(err, msgr.ErrFeatures) || !strings.Contains(err.Error(), "peer requires 0x8002000000000000") {
				t.Fatalf("complete peer refusal lost feature cause or full mask: %v", err)
			}
		})
	}
}

func TestHandshakeRejectsUnofferedCompressionSelection(t *testing.T) {
	for _, test := range []struct {
		name   string
		mode   ConnectionMode
		method msgr.CompressionAlgorithm
	}{
		{"CRC-zlib", CRCMode, msgr.CompAlgZlib},
		{"secure-zstd", SecureMode, msgr.CompAlgZstd},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := completeHandshakeRefusal(t, test.mode, handshakePeerConfig{selectedCompression: test.method})
			want := fmt.Sprintf("server selected unsupported compression method %d", test.method)
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("complete unsupported selection was not explicitly refused: %v", err)
			}
		})
	}
}

// Require successful transmission of the complete peer response as well as
// refusal. EOF, a timeout or a malformed frame alone cannot satisfy these tests.
func completeHandshakeRefusal(t *testing.T, mode ConnectionMode, cfg handshakePeerConfig) error {
	t.Helper()
	client, peer := net.Pipe()
	defer client.Close()
	auth := fixtureAuthData()
	cfg.mode, cfg.offeredMode = uint32(mode), mode
	done := make(chan error, 1)
	go func() { done <- handshakePeer(peer, auth, cfg) }()
	transport, err := Handshake(context.Background(), client,
		msgr.Address{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")},
		1, 0, auth, mode, 4096, time.Second)
	if transport != nil {
		transport.Conn.Close()
		t.Error("terminal peer refusal published a transport")
	}
	if peerErr := <-done; peerErr != nil {
		t.Fatalf("peer did not transmit the complete refusal: %v", peerErr)
	}
	if err == nil {
		t.Fatal("complete peer refusal was accepted")
	}
	return err
}
