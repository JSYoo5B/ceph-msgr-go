package cephx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func plaintextFuzzControl(mode uint8, key Key) []byte {
	switch mode {
	case 0, 1:
		return plaintextTestKeyPayload(key)
	case 2, 3:
		return plaintextTestBlob()
	case 4:
		return binary.LittleEndian.AppendUint64([]byte{1}, 456)
	default:
		return plaintextTestBytes(binary.LittleEndian.AppendUint64([]byte{2}, 124), bytes.Repeat([]byte{0x39}, 40))
	}
}

func plaintextFuzzSnapshot(c *Client) Client {
	before := *c
	before.Key.secret = append([]byte(nil), c.Key.secret...)
	before.Tickets = plaintextTestTicketsSnapshot(c)
	for service, ticket := range before.Tickets {
		ticket.Key.secret = append([]byte(nil), ticket.Key.secret...)
		before.Tickets[service] = ticket
	}
	return before
}

// FuzzAuthenticationPlaintextPublication authenticates the fuzzed bytes before
// interpreting their inner structure. It complements ciphertext corruption
// fuzzing, which ordinarily stops at crypto validation. Six routes exercise
// AUTH/MGR ticket keys, encrypted AUTH/MGR blobs, MGR challenges and replies.
func FuzzAuthenticationPlaintextPublication(f *testing.F) {
	for cipher := uint8(0); cipher < 2; cipher++ {
		key := Key{kind: AES, secret: bytes.Repeat([]byte{0x51}, 16)}
		if cipher != 0 {
			key = Key{kind: AES256K, secret: bytes.Repeat([]byte{0x51}, 32)}
		}
		for mode := uint8(0); mode < 6; mode++ {
			selector := mode<<1 | cipher
			good := plaintextFuzzControl(mode, key)
			f.Add(selector, good)
			f.Add(selector, []byte{}) // Missing key/blob/challenge/reply version.
			if mode <= 1 {
				f.Add(selector, []byte{1, byte(key.kind), 0}) // Short key body.
				f.Add(selector, good[:len(good)-8])           // Missing validity.
				zeroValidity := append([]byte(nil), good...)
				clear(zeroValidity[len(zeroValidity)-8:])
				f.Add(selector, zeroValidity)
			}
			version := byte(2)
			if mode == 5 {
				version = 3
				f.Add(selector, []byte{2, 124}) // Short authorizer nonce.
				wrongNonce := append([]byte(nil), good...)
				wrongNonce[1]++
				f.Add(selector, wrongNonce)
			}
			f.Add(selector, []byte{version})
			// The high selector bit opts into CRC without changing the six
			// authenticated plaintext routes or the selected CephX key type.
			crcSelector := selector | 0x80
			f.Add(crcSelector, good)
			f.Add(crcSelector, []byte{})
			if mode == 5 {
				f.Add(crcSelector, binary.LittleEndian.AppendUint64([]byte{1}, 124))
				f.Add(crcSelector, plaintextTestBytes(binary.LittleEndian.AppendUint64([]byte{2}, 124), nil))
				f.Add(crcSelector, binary.LittleEndian.AppendUint64([]byte{1}, 125))
				f.Add(crcSelector, []byte{1, 124}) // Short CRC nonce-only reply.
			}
		}
	}
	f.Fuzz(func(t *testing.T, selector uint8, payload []byte) {
		// A length field can still claim any uint32 size, but Seal and all
		// independently assembled outer envelopes stay explicitly bounded.
		if len(payload) > 8<<10 {
			t.Skip()
		}
		mode := (selector >> 1 & 0x3f) % 6
		requireConnectionSecret := selector&0x80 == 0
		key := Key{kind: AES, secret: bytes.Repeat([]byte{0x51}, 16)}
		if selector&1 != 0 {
			key = Key{kind: AES256K, secret: bytes.Repeat([]byte{0x51}, 32)}
		}
		c := plaintextTestClient(t, key)
		before := plaintextFuzzSnapshot(c)
		input := append([]byte(nil), payload...)
		var session Key
		var output []byte
		var err error
		if mode < 4 {
			service := ServiceAuth
			if mode&1 != 0 {
				service = ServiceMgr
			}
			body, blob := plaintextTestKeyPayload(key), plaintextTestBlob()
			if mode < 2 {
				body = payload
			} else {
				blob = payload
			}
			connectionSecret := bytes.Repeat([]byte{0x39}, 40)
			if !requireConnectionSecret {
				connectionSecret = nil
			}
			reply := plaintextTestAuthReplyWithSecret(t, key, service, body, blob, mode >= 2, connectionSecret)
			session, output, err = c.Finish(99, reply, requireConnectionSecret)
			if err != nil {
				if session.Type() != 0 || output != nil || !reflect.DeepEqual(*c, before) {
					t.Fatal("failed plaintext authentication published credentials or client state")
				}
			} else {
				auth, exists := c.Tickets[ServiceAuth]
				if !exists || !reflect.DeepEqual(auth.Key, session) || c.GlobalID != 99 || session.Type() == 0 || !bytes.Equal(output, connectionSecret) || c.Name != before.Name || !reflect.DeepEqual(c.Key, before.Key) {
					t.Fatal("successful plaintext authentication lost its identity or complete secret")
				}
			}
		} else {
			a := &Authorizer{ticket: c.Tickets[ServiceMgr], base: []byte{1, 2, 3}, nonce: 123}
			beforeAuthorizer := *a
			beforeAuthorizer.base = append([]byte(nil), a.base...)
			beforeAuthorizer.ticket = before.Tickets[ServiceMgr]
			usage := uint32(0x11)
			if mode == 5 {
				usage = 0x12
			}
			ciphertext, sealErr := key.Seal(usage, payload)
			if sealErr != nil {
				t.Fatal(sealErr)
			}
			if mode == 4 {
				output, err = a.Payload(ciphertext)
			} else {
				output, err = a.Finish(plaintextTestBytes(nil, ciphertext), requireConnectionSecret)
			}
			if !reflect.DeepEqual(*c, before) || !reflect.DeepEqual(*a, beforeAuthorizer) || err != nil && output != nil {
				t.Fatal("authorizer plaintext parsing mutated state or published partial output")
			}
			if err == nil && (mode == 4 && len(output) == 0 || mode == 5 && len(output) < 40 && (requireConnectionSecret || len(output) != 0)) {
				t.Fatal("successful authorizer parsing did not return a complete proof or secret")
			}
		}
		if !bytes.Equal(payload, input) {
			t.Fatal("plaintext parsing modified caller-owned fuzz input")
		}
		good := plaintextFuzzControl(mode, key)
		if bytes.Equal(payload, good) {
			if err != nil {
				t.Fatal("full independent plaintext control was rejected", err)
			}
			if mode < 4 {
				service := ServiceAuth
				if mode&1 != 0 {
					service = ServiceMgr
				}
				ticket, exists := c.Tickets[service]
				if !exists || ticket.SecretID != 7 || ticket.Key.Type() != key.Type() || !bytes.Equal(ticket.Blob, []byte("opaque")) {
					t.Fatal("full independent plaintext control did not publish its complete ticket")
				}
			} else if mode == 5 && !bytes.Equal(output, bytes.Repeat([]byte{0x39}, 40)) {
				t.Fatal("full independent authorizer control returned the wrong secret")
			}
		}
		// Check only known independent controls; do not duplicate the decoder
		// to predict how arbitrary plaintext fields should be classified.
		version, want := byte(2), wire.ErrVersion
		if mode == 5 {
			version, want = 3, ErrIntegrity
			wrongNonce := append([]byte(nil), good...)
			wrongNonce[1]++
			if bytes.Equal(payload, wrongNonce) && (!errors.Is(err, ErrIntegrity) || errors.Is(err, io.ErrUnexpectedEOF)) {
				t.Fatal("complete wrong nonce was reclassified", err)
			}
			if !requireConnectionSecret {
				crcNonceOnly := binary.LittleEndian.AppendUint64([]byte{1}, 124)
				crcEmptyV2 := plaintextTestBytes(binary.LittleEndian.AppendUint64([]byte{2}, 124), nil)
				if (bytes.Equal(payload, crcNonceOnly) || bytes.Equal(payload, crcEmptyV2)) && (err != nil || len(output) != 0) {
					t.Fatal("valid CRC reply without connection secret was rejected", err)
				}
				crcWrongNonce := binary.LittleEndian.AppendUint64([]byte{1}, 125)
				if bytes.Equal(payload, crcWrongNonce) && !errors.Is(err, ErrIntegrity) {
					t.Fatal("CRC nonce-only reply bypassed nonce verification", err)
				}
			}
		}
		if bytes.Equal(payload, []byte{version}) && (!errors.Is(err, want) || errors.Is(err, io.ErrUnexpectedEOF)) {
			t.Fatal("complete unsupported version was reclassified", err)
		}
	})
}
