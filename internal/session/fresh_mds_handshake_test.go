package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// All optional policy seams are deliberately enabled. The MDS entry point must
// retain its fixed masks regardless of authentication wrappers or OSD options.
type mixedFreshMDSAuth struct{ ServiceAuth }

func (mixedFreshMDSAuth) mdsMapsMON() bool        { return true }
func (mixedFreshMDSAuth) replicatedPGV8MON() bool { return true }

func TestFreshMDSHandshakeAuthorizerAndFraming(t *testing.T) {
	for _, kind := range []uint16{cephx.AES, cephx.AES256K} {
		for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
			for _, rank := range []int32{0, 3} {
				t.Run(fmt.Sprintf("key%d/mode%d/rank%d", kind, mode, rank), func(t *testing.T) {
					auth, fixture, err := freshMDSAuthorizer(kind, mode)
					if err != nil {
						t.Fatal(err)
					}
					auth.ReplicatedPGV8, auth.ReplicatedPGReplyV8 = true, true
					client, peer := net.Pipe()
					t.Cleanup(func() { client.Close(); peer.Close() })
					cfg := freshMDSPeerDefaults()
					cfg.rank = uint64(rank)
					cfg.supported |= monAdmissionFeatures | replicatedPGReplyV8Features | 1<<29 | 1<<49
					cfg.clientIdent = func(address msgr.Address, _ uint64, _, _, cookie uint64) error {
						if address.Type != 2 || address.Endpoint.Port() != 0 || cookie == 0 {
							return errors.New("invalid fresh client address or cookie")
						}
						return nil
					}
					payload := []byte{1, 2, 3, 4, 5, 6, 7, 8}
					cfg.afterIdent = func(r *msgr.Reader, w *msgr.Writer) error {
						f, err := r.Read()
						if err != nil {
							return err
						}
						if f.Tag != msgr.Keepalive || len(f.Segments) != 1 || !bytes.Equal(f.Segments[0], payload) {
							return errors.New("fresh connection sent application traffic instead of keepalive")
						}
						return w.Write(msgr.Frame{Tag: msgr.KeepaliveAck, Segments: f.Segments})
					}
					done := make(chan error, 1)
					go func() { done <- freshMDSPeer(peer, fixture, mode, cfg) }()
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					tr, err := HandshakeFreshMDS(ctx, client, msgr.Address{Type: 2}, rank, mixedFreshMDSAuth{auth}, mode, 4096, time.Second)
					if err != nil || tr == nil {
						t.Fatal("fresh MDS handshake failed", err)
					}
					if tr.GlobalID != 42 || tr.Features != Features {
						t.Fatal("fresh MDS admitted unrelated policy or lost identity", tr.GlobalID, tr.Features)
					}
					cancel() // Setup cancellation must not close the admitted connection.
					if err := tr.Writer.Write(msgr.Frame{Tag: msgr.Keepalive, Segments: [][]byte{payload}}); err != nil {
						t.Fatal("admitted connection did not outlive setup context", err)
					}
					f, err := tr.Reader.Read()
					if err != nil || f.Tag != msgr.KeepaliveAck || !bytes.Equal(f.Segments[0], payload) {
						t.Fatal("post-authentication framing failed", err)
					}
					if err := <-done; err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func TestFreshMDSHandshakeRejectsUnimplementedAdmission(t *testing.T) {
	type testCase struct {
		name string
		rank int32
		edit func(*freshMDSPeerConfig)
		want error
	}
	cases := []testCase{
		{"rank_zero_is_checked", 0, func(c *freshMDSPeerConfig) { c.rank = 1 }, nil},
		{"rank_mismatch", 1, func(c *freshMDSPeerConfig) { c.rank = 0 }, nil},
		{"gid_is_not_rank", 0, func(c *freshMDSPeerConfig) { c.rank = 42 }, nil},
		{"zero_cookie", 0, func(c *freshMDSPeerConfig) { c.cookie = 0 }, msgr.ErrFeatures},
		{"lossy_server", 0, func(c *freshMDSPeerConfig) { c.flags = 1 }, msgr.ErrFeatures},
		{"unknown_server_flags", 0, func(c *freshMDSPeerConfig) { c.flags = 2 }, msgr.ErrFeatures},
		{"mixed_server_flags", 0, func(c *freshMDSPeerConfig) { c.flags = 3 }, msgr.ErrFeatures},
		{"missing_MSG_ADDR2", 0, func(c *freshMDSPeerConfig) { c.supported &^= 1 << 59 }, msgr.ErrFeatures},
		{"empty_addresses", 0, func(c *freshMDSPeerConfig) {
			c.addresses = func(msgr.Address) []msgr.Address { return nil }
		}, msgr.ErrAuthentication},
		{"different_address", 0, func(c *freshMDSPeerConfig) {
			c.addresses = func(a msgr.Address) []msgr.Address {
				a.Endpoint = netip.MustParseAddrPort("192.0.2.99:6800")
				return []msgr.Address{a}
			}
		}, msgr.ErrAuthentication},
		{"different_nonce", 0, func(c *freshMDSPeerConfig) {
			c.addresses = func(a msgr.Address) []msgr.Address { a.Nonce++; return []msgr.Address{a} }
		}, msgr.ErrAuthentication},
		{"authorizer_reply_nonce", 0, func(c *freshMDSPeerConfig) { c.badNonce = true }, cephx.ErrIntegrity},
		{"transcript_signature", 0, func(c *freshMDSPeerConfig) { c.badSignature = true }, msgr.ErrAuthentication},
	}
	for _, role := range []uint8{1, 4, 16} {
		cases = append(cases, testCase{fmt.Sprintf("wrong_role%d", role), 0, func(c *freshMDSPeerConfig) { c.role = role }, nil})
	}
	for _, feature := range []uint{18, 21, 25, 29, 30, 41, 48, 49, 56, 58} {
		cases = append(cases, testCase{fmt.Sprintf("unsupported_required%d", feature), 0,
			func(c *freshMDSPeerConfig) { c.required |= uint64(1) << feature; c.supported |= uint64(1) << feature }, msgr.ErrFeatures})
	}
	for _, truncated := range []int{1, 8, 32} {
		cases = append(cases, testCase{fmt.Sprintf("truncated_ident%d", truncated), 0,
			func(c *freshMDSPeerConfig) { c.truncateIdent = truncated }, msgr.ErrFrame})
	}
	for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("mode%d/%s", mode, tc.name), func(t *testing.T) {
				auth, fixture, err := freshMDSAuthorizer(cephx.AES256K, mode)
				if err != nil {
					t.Fatal(err)
				}
				client, peer := net.Pipe()
				t.Cleanup(func() { client.Close(); peer.Close() })
				cfg := freshMDSPeerDefaults()
				tc.edit(&cfg)
				done := make(chan error, 1)
				go func() { done <- freshMDSPeer(peer, fixture, mode, cfg) }()
				tr, err := HandshakeFreshMDS(context.Background(), client, msgr.Address{Type: 2}, tc.rank, auth, mode, 4096, time.Second)
				if tr != nil || err == nil || (tc.want != nil && !errors.Is(err, tc.want)) {
					t.Error("invalid admission returned a transport or wrong failure class", err)
				}
				if cfg.truncateIdent > 0 && !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Error("complete truncated ident lost decode cause", err)
				}
				if err := <-done; err != nil {
					t.Fatal("peer observed traffic after refusal", err)
				}
			})
		}
	}
}

func TestFreshMDSHandshakeRefusesEveryRecoveryControl(t *testing.T) {
	for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
		for _, tag := range []msgr.Tag{msgr.SessionReconnect, msgr.SessionReset, msgr.SessionRetry, msgr.SessionRetryGlobal, msgr.SessionReconnectOK, msgr.Wait} {
			t.Run(fmt.Sprintf("mode%d/tag%d", mode, tag), func(t *testing.T) {
				auth, fixture, err := freshMDSAuthorizer(cephx.AES256K, mode)
				if err != nil {
					t.Fatal(err)
				}
				client, peer := net.Pipe()
				t.Cleanup(func() { client.Close(); peer.Close() })
				cfg := freshMDSPeerDefaults()
				cfg.identTag, cfg.identPayload = tag, make([]byte, 8)
				done := make(chan error, 1)
				go func() { done <- freshMDSPeer(peer, fixture, mode, cfg) }()
				tr, err := HandshakeFreshMDS(context.Background(), client, msgr.Address{Type: 2}, 0, auth, mode, 4096, time.Second)
				if tr != nil || !errors.Is(err, msgr.ErrFrame) {
					t.Fatal("recovery control was accepted", err)
				}
				if err := <-done; err != nil {
					t.Fatal("client responded to recovery control", err)
				}
			})
		}
	}
}

func TestFreshMDSHandshakePinsModeAndSecretRequirements(t *testing.T) {
	for _, kind := range []uint16{cephx.AES, cephx.AES256K} {
		for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
			for _, legacy := range []bool{false, true} {
				t.Run(fmt.Sprintf("key%d/mode%d/legacy%t", kind, mode, legacy), func(t *testing.T) {
					auth, fixture, err := freshMDSAuthorizer(kind, mode)
					if err != nil {
						t.Fatal(err)
					}
					client, peer := net.Pipe()
					t.Cleanup(func() { client.Close(); peer.Close() })
					cfg := freshMDSPeerDefaults()
					cfg.legacyReply = legacy
					if !legacy {
						cfg.selected = SecureMode
						if mode == SecureMode {
							cfg.selected = CRCMode
						}
					}
					done := make(chan error, 1)
					go func() { done <- freshMDSPeer(peer, fixture, mode, cfg) }()
					tr, err := HandshakeFreshMDS(context.Background(), client, msgr.Address{Type: 2}, 0, auth, mode, 4096, time.Second)
					if tr != nil {
						tr.Conn.Close()
					}
					if legacy && mode == CRCMode {
						if err != nil || tr == nil {
							t.Fatal("CRC rejected valid secret-free authorizer v1 reply", err)
						}
					} else if legacy {
						if tr != nil || !errors.Is(err, cephx.ErrIntegrity) {
							t.Fatal("secure mode accepted a secret-free authorizer reply", err)
						}
					} else if tr != nil || err == nil || !strings.Contains(err.Error(), "server selected connection mode") {
						t.Fatal("fresh MDS silently changed connection mode", err)
					}
					if err := <-done; err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func TestFreshMDSEstablishedSessionRefusesRecoveryWithoutApplicationTraffic(t *testing.T) {
	for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
		for _, tag := range []msgr.Tag{msgr.SessionReconnect, msgr.SessionReset, msgr.SessionRetry, msgr.SessionRetryGlobal, msgr.SessionReconnectOK, msgr.Wait} {
			t.Run(fmt.Sprintf("mode%d/tag%d", mode, tag), func(t *testing.T) {
				auth, fixture, err := freshMDSAuthorizer(cephx.AES256K, mode)
				if err != nil {
					t.Fatal(err)
				}
				client, peer := net.Pipe()
				t.Cleanup(func() { client.Close(); peer.Close() })
				cfg := freshMDSPeerDefaults()
				cfg.afterIdent = func(r *msgr.Reader, w *msgr.Writer) error {
					if err := w.Write(msgr.Frame{Tag: tag, Segments: [][]byte{make([]byte, 8)}}); err != nil {
						return err
					}
					return freshMDSNoTraffic(r)
				}
				done := make(chan error, 1)
				go func() { done <- freshMDSPeer(peer, fixture, mode, cfg) }()
				tr, err := HandshakeFreshMDS(context.Background(), client, msgr.Address{Type: 2}, 0, auth, mode, 4096, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				s := New(tr, Config{}, func(msgr.MessageData) error {
					return errors.New("unexpected MDS application message")
				}, nil)
				s.Start()
				select {
				case <-s.Done():
				case <-time.After(time.Second):
					s.Fail(ErrClosed)
					t.Fatal("established MDS tried to handle recovery")
				}
				s.Wait()
				if !errors.Is(s.Err(), msgr.ErrFrame) {
					t.Fatal("recovery refusal lost its frame error", s.Err())
				}
				if err := <-done; err != nil {
					t.Fatal("established MDS emitted OPEN or recovery traffic", err)
				}
			})
		}
	}
}

func TestFreshMDSDoesNotChangeOriginalHandshakeLossyPolicy(t *testing.T) {
	for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
		client, peer := net.Pipe()
		fixture := fixtureAuthData()
		if mode == CRCMode {
			fixture.secret = nil
		}
		done := make(chan error, 1)
		go func() {
			done <- handshakePeer(peer, fixture, handshakePeerConfig{
				mode: uint32(mode), offeredMode: mode, peerRole: 2, serverFlags: 0,
				afterIdent: func(_ net.Conn, r *msgr.Reader, _ *msgr.Writer) error { return freshMDSNoTraffic(r) },
			})
		}()
		tr, err := Handshake(context.Background(), client, msgr.Address{Type: 2}, 2, 0, fixture, mode, 4096, time.Second)
		if tr != nil || !errors.Is(err, msgr.ErrFeatures) {
			t.Fatal("ordinary Handshake admitted stateful ServerIdent", err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
