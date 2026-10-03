package cephx

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestAuthenticationLengthErrorsPreserveCauseAndState(t *testing.T) {
	for _, kind := range []uint16{AES, AES256K} {
		name := "aes256k"
		key := rfcKey(t)
		if kind == AES {
			name = "aes"
			key = Key{kind: AES, secret: bytes.Repeat([]byte{0x51}, 16)}
		}
		t.Run(name, func(t *testing.T) {
			for _, length := range []struct {
				name string
				size uint32
				want error
			}{
				{"limit", wire.DefaultLimit + 1, wire.ErrLimit},
				{"truncated", 1, io.ErrUnexpectedEOF},
			} {
				t.Run(length.name, func(t *testing.T) {
					encoded := wire.Encoder{}
					encoded.U32(length.size)
					control := wire.NewDecoder(encoded.Data)
					control.Bytes()
					if !errors.Is(control.Err(), length.want) {
						t.Fatalf("invalid decoder control: %v", control.Err())
					}
					for _, field := range []string{"ticket-key", "connection-secret", "authorizer-reply", "plaintext-ticket-blob", "encrypted-ticket-blob"} {
						t.Run(field, func(t *testing.T) {
							c, _ := NewClient("client.length-test", key)
							c.GlobalID = 42
							c.Tickets[ServiceAuth] = Ticket{Key: key, SecretID: 7, Blob: []byte("previous auth"), Expires: time.Now().Add(time.Minute)}
							c.Tickets[ServiceMgr] = Ticket{Key: key, SecretID: 9, Blob: []byte("previous mgr"), Expires: time.Now().Add(time.Minute)}
							before := make(map[uint32]Ticket, len(c.Tickets))
							for service, ticket := range c.Tickets {
								before[service] = ticket
							}
							var err error
							if field == "authorizer-reply" {
								a := &Authorizer{ticket: c.Tickets[ServiceMgr], nonce: 123}
								beforeAuthorizer := *a
								var secret []byte
								secret, err = a.Finish(encoded.Data, true)
								if secret != nil || !reflect.DeepEqual(*a, beforeAuthorizer) {
									t.Fatal("failed authorizer changed state or returned secret")
								}
							} else {
								p := wire.Encoder{}
								p.U16(0x100)
								p.U32(0)
								if field == "connection-secret" {
									p.Raw(testTickets(t, key, key, ServiceAuth, nil))
									p.Bytes(encoded.Data)
									p.Bytes(nil)
								} else {
									p.U8(1)
									p.U32(1)
									p.U32(ServiceAuth)
									p.U8(1)
									if field != "ticket-key" {
										payload := wire.Encoder{}
										payload.U8(1)
										payload.Raw(encodeTestKey(key))
										payload.U32(60)
										payload.U32(0)
										ciphertext, sealErr := key.Seal(4, payload.Data)
										if sealErr != nil {
											t.Fatal(sealErr)
										}
										p.Bytes(ciphertext)
										if field == "encrypted-ticket-blob" {
											p.U8(1)
										} else {
											p.U8(0)
										}
									}
									p.Raw(encoded.Data)
								}
								var session Key
								var secret []byte
								session, secret, err = c.Finish(99, p.Data, true)
								if session.Type() != 0 || secret != nil {
									t.Fatal("failed authentication returned credentials")
								}
							}
							if !errors.Is(err, length.want) || errors.Is(err, ErrIntegrity) || errors.Is(err, wire.ErrVersion) {
								t.Errorf("length cause lost: want %v, got %v", length.want, err)
							}
							if c.GlobalID != 42 || !reflect.DeepEqual(c.Tickets, before) {
								t.Fatal("failed authentication published identity or tickets")
							}
						})
					}
				})
			}
		})
	}
}

func TestAuthenticationLengthChecksKeepCryptoAndVersionErrors(t *testing.T) {
	for _, kind := range []uint16{AES, AES256K} {
		key := rfcKey(t)
		if kind == AES {
			key = Key{kind: AES, secret: bytes.Repeat([]byte{0x51}, 16)}
		}
		c, _ := NewClient("client.length-test", key)
		good := testAuthReply(t, key, key, key, nil)
		if _, _, err := c.Finish(42, good, true); err != nil {
			t.Fatal("valid authentication", err)
		}
		before := make(map[uint32]Ticket, len(c.Tickets))
		for service, ticket := range c.Tickets {
			before[service] = ticket
		}
		bad := append([]byte(nil), good...)
		bad[6+5+5+4] ^= 1 // Complete, length-valid encrypted ticket key.
		_, _, err := c.Finish(99, bad, true)
		if !errors.Is(err, ErrIntegrity) || errors.Is(err, wire.ErrLimit) || errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal("crypto rejection reclassified", err)
		}
		// This ciphertext verifies, but its decrypted key structure version is
		// unknown. It must keep the existing encoding-version classification.
		ciphertext, err := key.Seal(4, []byte{2})
		if err != nil {
			t.Fatal(err)
		}
		p := wire.Encoder{}
		p.U16(0x100)
		p.U32(0)
		p.U8(1)
		p.U32(1)
		p.U32(ServiceAuth)
		p.U8(1)
		p.Bytes(ciphertext)
		_, _, err = c.Finish(99, p.Data, true)
		if !errors.Is(err, wire.ErrVersion) || errors.Is(err, wire.ErrLimit) || errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal("encoding version reclassified", err)
		}
		// An encoded empty ciphertext has no decoder error. Crypto validation,
		// rather than a fabricated truncated-length error, must reject it.
		a := &Authorizer{ticket: c.Tickets[ServiceMgr]}
		p = wire.Encoder{}
		p.Bytes(nil)
		_, err = a.Finish(p.Data, true)
		if !errors.Is(err, ErrIntegrity) || errors.Is(err, wire.ErrLimit) || errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal("empty ciphertext reclassified", err)
		}
		if c.GlobalID != 42 || !reflect.DeepEqual(c.Tickets, before) {
			t.Fatal("invalid auth control published identity or tickets")
		}
	}
}
