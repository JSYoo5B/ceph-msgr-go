package session

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestOSDHandshakeBindsZeroIDAndNegotiatesImplementedFeatures(t *testing.T) {
	for _, test := range []struct {
		name         string
		id, required uint64
		wantError    bool
	}{
		{"osd-zero", 0, RequiredFeatures, false},
		{"different-id", 1, RequiredFeatures, true},
		{"unsupported-crush", 0, RequiredFeatures | 1<<18, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, peer := net.Pipe()
			auth := fixtureAuthData()
			done := make(chan error, 1)
			go func() {
				done <- handshakePeer(peer, auth, handshakePeerConfig{mode: 2, peerRole: 4, serverFlags: 1,
					serverSupported: Features | 1<<8 | 1<<9, serverRequired: test.required,
					identFeatures: func(bits uint64) error {
						if bits != Features|1<<8|1<<9 || bits&monAdmissionFeatures != 0 {
							return errors.New("OSD advertised unsupported admission or object features")
						}
						return nil
					}})
			}()
			tr, err := Handshake(context.Background(), client, msgr.Address{Type: 2}, 4, test.id, auth, SecureMode, 4096, time.Second)
			if (err != nil) != test.wantError {
				t.Fatal("OSD identity or feature admission", err)
			}
			if tr != nil {
				if tr.Features != Features|1<<8|1<<9 {
					t.Fatal("negotiated feature intersection lost", tr.Features)
				}
				tr.Conn.Close()
			}
			if test.name == "unsupported-crush" && !errors.Is(err, msgr.ErrFeatures) {
				t.Fatal("unsupported feature error classification", err)
			}
			// The peer waits for the next frame; connection cleanup ends its wait.
			<-done
		})
	}
}
