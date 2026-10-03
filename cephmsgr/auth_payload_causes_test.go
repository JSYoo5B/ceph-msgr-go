package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestPublicAuthenticationPlaintextCauses(t *testing.T) {
	for _, test := range []struct {
		name  string
		role  uint8
		tag   msgr.Tag
		field string
	}{
		{"mon-ticket-version", 1, msgr.AuthDone, "ticket-version"},
		{"mon-ticket-key", 1, msgr.AuthDone, "ticket-key"},
		{"mon-ticket-validity", 1, msgr.AuthDone, "ticket-validity"},
		{"mon-ticket-blob-version", 1, msgr.AuthDone, "ticket-blob-version"},
		{"mgr-challenge-version", 16, msgr.AuthReplyMore, "challenge-version"},
		{"mgr-authorizer-version", 16, msgr.AuthDone, "authorizer-version"},
		{"mgr-authorizer-nonce", 16, msgr.AuthDone, "authorizer-nonce"},
		{"mgr-wrong-nonce", 16, msgr.AuthDone, "wrong-nonce"},
		{"mgr-valid", 16, msgr.AuthDone, "valid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			options := mockOptions(t, 20, [16]byte{1})
			var monDials, mgrDials, monCommands, mgrCommands, mutations atomic.Uint32
			var peers sync.WaitGroup
			fixtureErrors := make(chan error, 1)
			var c *Client
			t.Cleanup(func() {
				cancel()
				if c != nil {
					c.Close()
				}
				peers.Wait()
			})
			options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				cfg := peerConfig{fsid: [16]byte{1}, release: 20, role: 1}
				dials, commands := &monDials, &monCommands
				if strings.HasSuffix(endpoint, ":6800") {
					cfg.role, cfg.id = 16, 99
					dials, commands = &mgrDials, &mgrCommands
				}
				if dials.Add(1) != 1 {
					return nil, errors.New("unexpected authentication retry")
				}
				cfg.command = func(msgr.MessageData) { commands.Add(1) }
				if cfg.role == test.role {
					cfg.payload = func(tag msgr.Tag, payload []byte) []byte {
						if tag != test.tag {
							return payload
						}
						mutations.Add(1)
						changed, err := authenticationPlaintextPayload(test.field, tag, payload)
						if err != nil {
							select {
							case fixtureErrors <- err:
							default:
							}
							return payload
						}
						return changed
					}
				}
				client, peer := net.Pipe()
				peers.Add(1)
				go func() { defer peers.Done(); mockDaemon(peer, cfg) }()
				return client, nil
			}
			var err error
			c, err = Dial(ctx, options)
			if test.role == 1 {
				if c != nil {
					t.Fatal("malformed MON authentication returned a client")
				}
			} else {
				if err != nil {
					t.Fatal("healthy primary bootstrap failed", err)
				}
				before := c.snapshotAuth()
				result, callErr := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"pg stat"}`)})
				err = callErr
				if !reflect.DeepEqual(c.snapshotAuth(), before) {
					t.Fatal("MGR setup changed primary identity or tickets")
				}
				if test.field == "valid" && (err != nil || result.Code != 0 || !bytes.Equal(result.Data, []byte(`{"ok":true}`))) {
					t.Fatal("valid resealed authorizer reply did not complete command", result.Code, err)
				}
				if test.field != "valid" && (mgrCommands.Load() != 0 || len(result.Data) != 0) {
					t.Fatal("rejected MGR setup submitted an application command")
				}
				if monCommands.Load() != 0 {
					t.Fatal("MGR setup transmitted a MON command")
				}
				if _, primaryErr := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); primaryErr != nil {
					t.Fatal("MGR refusal damaged healthy primary", primaryErr)
				}
			}
			select {
			case fixtureErr := <-fixtureErrors:
				t.Fatal("payload mutation failed", fixtureErr)
			default:
			}
			var unknown *OutcomeUnknownError
			if errors.As(err, &unknown) {
				t.Fatal("pre-command authentication refusal became an unknown outcome", err)
			}
			if test.field == "wrong-nonce" {
				if !errors.Is(err, cephx.ErrIntegrity) || errors.Is(err, ErrMalformedMessage) || errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatal("complete wrong nonce lost genuine integrity classification", err)
				}
			} else if test.field != "valid" {
				if !errors.Is(err, io.ErrUnexpectedEOF) || !errors.Is(err, ErrMalformedMessage) || errors.Is(err, cephx.ErrIntegrity) || errors.Is(err, cephx.ErrTicket) || errors.Is(err, wire.ErrVersion) || retryableSetup(err) {
					t.Error("complete cryptographically valid frame lost plaintext decode cause", err)
				}
			}
			if monDials.Load() != 1 || mutations.Load() != 1 {
				t.Fatal("authentication failed to run once without retry", monDials.Load(), mutations.Load())
			}
			if test.role == 1 && (mgrDials.Load() != 0 || monCommands.Load() != 0 || mgrCommands.Load() != 0) {
				t.Fatal("MON refusal admitted a daemon command or MGR connection")
			}
			if test.role == 16 && mgrDials.Load() != 1 {
				t.Fatal("MGR authentication was retried", mgrDials.Load())
			}
			if test.field == "valid" && mgrCommands.Load() != 1 {
				t.Fatal("valid MGR command was not transmitted exactly once", mgrCommands.Load())
			}
		})
	}
}

// Rewrite a complete peer frame while preserving every outer length and the
// CephX envelope/integrity. This fixture uses the existing synthetic peer; it
// is a public error-propagation regression, not a real-Ceph protocol oracle.
func authenticationPlaintextPayload(field string, tag msgr.Tag, payload []byte) ([]byte, error) {
	key, _ := mockCredential()
	d := wire.NewDecoder(payload)
	var globalID uint64
	var mode uint32
	if tag == msgr.AuthDone {
		globalID, mode = d.U64(), d.U32()
	}
	inner := d.Bytes()
	if err := d.Done(); err != nil {
		return nil, err
	}
	if strings.HasPrefix(field, "ticket-") {
		p := wire.NewDecoder(inner)
		method, status := p.U16(), p.U32()
		version, count, service, keyVersion := p.U8(), p.U32(), p.U32(), p.U8()
		ciphertext := p.Bytes()
		encrypted, blob := p.Bool(), p.Bytes()
		connection, extra := p.Bytes(), p.Bytes()
		if err := p.Done(); err != nil {
			return nil, err
		}
		if count != 1 || service != cephx.ServiceAuth || encrypted {
			return nil, errors.New("unexpected synthetic MON ticket structure")
		}
		plain, err := key.value.Open(4, ciphertext)
		if err != nil {
			return nil, err
		}
		switch field {
		case "ticket-version":
			plain = nil
		case "ticket-key":
			plain = plain[:1]
		case "ticket-validity":
			plain = plain[:len(plain)-8]
		case "ticket-blob-version":
			blob = nil
		}
		ciphertext, err = key.value.Seal(4, plain)
		if err != nil {
			return nil, err
		}
		e := wire.Encoder{}
		e.U16(method)
		e.U32(status)
		e.U8(version)
		e.U32(count)
		e.U32(service)
		e.U8(keyVersion)
		e.Bytes(ciphertext)
		e.U8(0)
		e.Bytes(blob)
		e.Bytes(connection)
		e.Bytes(extra)
		inner = e.Data
	} else {
		ciphertext := inner
		if tag == msgr.AuthDone {
			p := wire.NewDecoder(inner)
			ciphertext = p.Bytes()
			if err := p.Done(); err != nil {
				return nil, err
			}
		}
		usage := uint32(0x12)
		if tag == msgr.AuthReplyMore {
			usage = 0x11
		}
		plain, err := key.value.Open(usage, ciphertext)
		if err != nil {
			return nil, err
		}
		switch field {
		case "challenge-version", "authorizer-version":
			plain = nil
		case "authorizer-nonce":
			plain = plain[:1]
		case "wrong-nonce":
			binary.LittleEndian.PutUint64(plain[1:9], binary.LittleEndian.Uint64(plain[1:9])+1)
		}
		ciphertext, err = key.value.Seal(usage, plain)
		if err != nil {
			return nil, err
		}
		inner = ciphertext
		if tag == msgr.AuthDone {
			e := wire.Encoder{}
			e.Bytes(ciphertext)
			inner = e.Data
		}
	}
	e := wire.Encoder{}
	if tag == msgr.AuthDone {
		e.U64(globalID)
		e.U32(mode)
	}
	e.Bytes(inner)
	return e.Data, nil
}
