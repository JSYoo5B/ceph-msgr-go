package cephx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func plaintextTestBytes(p, value []byte) []byte {
	p = binary.LittleEndian.AppendUint32(p, uint32(len(value)))
	return append(p, value...)
}

func plaintextTestKeyPayload(key Key) []byte {
	// Independent CephX key payload: version1, type16, creation utime_t8,
	// key length16/raw key, then validity utime_t8. No key encoder is used.
	p := binary.LittleEndian.AppendUint16([]byte{1}, key.kind)
	p = append(p, make([]byte, 8)...)
	p = binary.LittleEndian.AppendUint16(p, uint16(len(key.secret)))
	p = append(p, key.secret...)
	p = binary.LittleEndian.AppendUint32(p, 60)
	return binary.LittleEndian.AppendUint32(p, 0)
}

func plaintextTestBlob() []byte {
	p := binary.LittleEndian.AppendUint64([]byte{1}, 7)
	return plaintextTestBytes(p, []byte("opaque"))
}

func plaintextTestTicket(t *testing.T, key Key, service uint32, payload, blob []byte, encrypted bool) []byte {
	t.Helper()
	ciphertext, err := key.Seal(4, payload)
	if err != nil {
		t.Fatal(err)
	}
	p := binary.LittleEndian.AppendUint32([]byte{1}, 1)
	p = binary.LittleEndian.AppendUint32(p, service)
	p = append(p, 1)
	p = plaintextTestBytes(p, ciphertext)
	if encrypted {
		p = append(p, 1)
		blob, err = key.Seal(5, plaintextTestBytes(nil, blob))
		if err != nil {
			t.Fatal(err)
		}
	} else {
		p = append(p, 0)
	}
	return plaintextTestBytes(p, blob)
}

func plaintextTestAuthReply(t *testing.T, key Key, service uint32, payload, blob []byte, encrypted bool) []byte {
	t.Helper()
	record := plaintextTestTicket(t, key, service, payload, blob, encrypted)
	auth, extra := record, []byte(nil)
	if service == ServiceMgr {
		auth = plaintextTestTicket(t, key, ServiceAuth, plaintextTestKeyPayload(key), plaintextTestBlob(), false)
		extra = record
	}
	ciphertext, err := key.Seal(3, plaintextTestBytes(nil, bytes.Repeat([]byte{0x39}, 40)))
	if err != nil {
		t.Fatal(err)
	}
	p := binary.LittleEndian.AppendUint16(nil, 0x100)
	p = binary.LittleEndian.AppendUint32(p, 0)
	p = append(p, auth...)
	p = plaintextTestBytes(p, plaintextTestBytes(nil, ciphertext))
	return plaintextTestBytes(p, extra)
}

func plaintextTestClient(t *testing.T, key Key) *Client {
	t.Helper()
	c, err := NewClient("client.plaintext-test", key)
	if err != nil {
		t.Fatal(err)
	}
	c.GlobalID = 42
	previous := Ticket{Key: key, SecretID: 5, Blob: []byte("previous"), Expires: time.Unix(100, 0)}
	c.Tickets[ServiceAuth], c.Tickets[ServiceMgr] = previous, previous
	return c
}

func plaintextTestTicketsSnapshot(c *Client) map[uint32]Ticket {
	before := make(map[uint32]Ticket, len(c.Tickets))
	for service, ticket := range c.Tickets {
		ticket.Blob = append([]byte(nil), ticket.Blob...)
		before[service] = ticket
	}
	return before
}

func TestAuthenticationPlaintextErrorsPreserveCauseAndState(t *testing.T) {
	for _, cipher := range []struct {
		name string
		kind uint16
		size int
	}{{"aes", AES, 16}, {"aes256k", AES256K, 32}} {
		t.Run(cipher.name, func(t *testing.T) {
			key := Key{kind: cipher.kind, secret: bytes.Repeat([]byte{0x51}, cipher.size)}
			for _, field := range []string{"key-version", "key-body", "key-validity", "blob-version"} {
				for _, service := range []struct {
					name string
					id   uint32
				}{{"auth", ServiceAuth}, {"mgr", ServiceMgr}} {
					for _, encrypted := range []bool{false, true} {
						if encrypted && field != "blob-version" {
							continue
						}
						name := field + "/" + service.name
						if encrypted {
							name += "/encrypted-blob"
						}
						t.Run(name, func(t *testing.T) {
							c := plaintextTestClient(t, key)
							before := plaintextTestTicketsSnapshot(c)
							payload, blob := plaintextTestKeyPayload(key), plaintextTestBlob()
							switch field {
							case "key-version":
								payload = nil
							case "key-body":
								payload = []byte{1, byte(cipher.kind), 0}
							case "key-validity":
								payload = payload[:len(payload)-8]
							case "blob-version":
								blob = nil
							}
							session, secret, err := c.Finish(99, plaintextTestAuthReply(t, key, service.id, payload, blob, encrypted))
							if !errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrTicket) || errors.Is(err, wire.ErrVersion) || errors.Is(err, ErrIntegrity) {
								t.Fatal("authenticated truncated plaintext lost its decode cause", err)
							}
							if session.Type() != 0 || secret != nil || c.GlobalID != 42 || !reflect.DeepEqual(c.Tickets, before) {
								t.Fatal("failed plaintext parsing published credentials or auth state")
							}
						})
					}
				}
			}
			for _, field := range []string{"challenge-version", "reply-version", "reply-nonce"} {
				t.Run(field, func(t *testing.T) {
					c := plaintextTestClient(t, key)
					beforeTickets := plaintextTestTicketsSnapshot(c)
					a := &Authorizer{ticket: c.Tickets[ServiceMgr], base: []byte{1, 2, 3}, nonce: 123}
					before := *a
					before.base = append([]byte(nil), a.base...)
					plain, usage := []byte(nil), uint32(0x12)
					if field == "challenge-version" {
						usage = 0x11
					} else if field == "reply-nonce" {
						plain = []byte{2, 124}
					}
					ciphertext, err := key.Seal(usage, plain)
					if err != nil {
						t.Fatal(err)
					}
					var output []byte
					if field == "challenge-version" {
						output, err = a.Payload(ciphertext)
					} else {
						output, err = a.Finish(plaintextTestBytes(nil, ciphertext))
					}
					if !errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, wire.ErrVersion) || errors.Is(err, ErrIntegrity) {
						t.Fatal("authenticated truncated authorizer plaintext lost its cause", err)
					}
					if output != nil || !reflect.DeepEqual(*a, before) || c.GlobalID != 42 || !reflect.DeepEqual(c.Tickets, beforeTickets) {
						t.Fatal("failed authorizer parsing published state, secret or proof")
					}
				})
			}
		})
	}
}

func TestAuthenticationPlaintextChecksKeepSemanticErrors(t *testing.T) {
	for _, cipher := range []struct {
		name string
		kind uint16
		size int
	}{{"aes", AES, 16}, {"aes256k", AES256K, 32}} {
		t.Run(cipher.name, func(t *testing.T) {
			key := Key{kind: cipher.kind, secret: bytes.Repeat([]byte{0x51}, cipher.size)}
			good, blob := plaintextTestKeyPayload(key), plaintextTestBlob()
			for _, service := range []uint32{ServiceAuth, ServiceMgr} {
				for _, encrypted := range []bool{false, true} {
					c := plaintextTestClient(t, key)
					if session, secret, err := c.Finish(99, plaintextTestAuthReply(t, key, service, good, blob, encrypted)); err != nil || session.Type() != cipher.kind || len(secret) != 40 || c.GlobalID != 99 {
						t.Fatal("full independent plaintext vector failed", err)
					}
				}
			}
			zeroValidity := append([]byte(nil), good...)
			clear(zeroValidity[len(zeroValidity)-8:])
			for _, control := range []struct {
				payload, blob []byte
				want          error
			}{{[]byte{2}, blob, wire.ErrVersion}, {good, []byte{2}, wire.ErrVersion}, {zeroValidity, blob, ErrTicket}} {
				c := plaintextTestClient(t, key)
				before := plaintextTestTicketsSnapshot(c)
				session, secret, err := c.Finish(99, plaintextTestAuthReply(t, key, ServiceAuth, control.payload, control.blob, false))
				if !errors.Is(err, control.want) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, wire.ErrLimit) || session.Type() != 0 || secret != nil || c.GlobalID != 42 || !reflect.DeepEqual(c.Tickets, before) {
					t.Fatal("complete invalid ticket changed its semantic class or auth state", err)
				}
			}
			a := &Authorizer{ticket: Ticket{Key: key}, nonce: 123}
			challenge := binary.LittleEndian.AppendUint64([]byte{1}, 456)
			ciphertext, err := key.Seal(0x11, challenge)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := a.Payload(ciphertext); err != nil {
				t.Fatal("full authorizer challenge failed", err)
			}
			reply := plaintextTestBytes(binary.LittleEndian.AppendUint64([]byte{2}, 124), bytes.Repeat([]byte{0x39}, 40))
			ciphertext, err = key.Seal(0x12, reply)
			if err != nil {
				t.Fatal(err)
			}
			if secret, err := a.Finish(plaintextTestBytes(nil, ciphertext)); err != nil || len(secret) != 40 {
				t.Fatal("full authorizer reply failed", err)
			}
			wrongNonce := append([]byte(nil), reply...)
			wrongNonce[1]++
			for _, control := range []struct {
				usage uint32
				plain []byte
				want  error
			}{{0x11, []byte{2}, wire.ErrVersion}, {0x12, []byte{3}, ErrIntegrity}, {0x12, wrongNonce, ErrIntegrity}} {
				ciphertext, err := key.Seal(control.usage, control.plain)
				if err != nil {
					t.Fatal(err)
				}
				before := *a
				var output []byte
				if control.usage == 0x11 {
					output, err = a.Payload(ciphertext)
				} else {
					output, err = a.Finish(plaintextTestBytes(nil, ciphertext))
				}
				if !errors.Is(err, control.want) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, wire.ErrLimit) || output != nil || !reflect.DeepEqual(*a, before) {
					t.Fatal("complete invalid authorizer plaintext changed its semantic class or state", err)
				}
			}
			// Keep the outer length exact while corrupting the ciphertext itself.
			if cipher.kind == AES {
				ciphertext = append(ciphertext, 0) // Invalid CBC block alignment.
			} else {
				ciphertext[len(ciphertext)-1] ^= 1 // Invalid authenticated tag.
			}
			if secret, err := a.Finish(plaintextTestBytes(nil, ciphertext)); !errors.Is(err, ErrIntegrity) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, wire.ErrLimit) || secret != nil {
				t.Fatal("ciphertext tampering was reclassified as an inner decoding error", err)
			}
		})
	}
}
