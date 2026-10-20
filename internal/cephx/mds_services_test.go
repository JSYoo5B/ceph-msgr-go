package cephx

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Source-derived synthetic exchanges only: pinned Ceph v20.2.4 commit
// 7f793731f1b39eb4f465e960113d2363c311b964, include/msgr.h:92,
// auth/cephx/CephxClientHandler.cc:61-97, CephxProtocol.h:177-202,
// 324-361,511-570 (LGPL-2.1). No source implementation copied. These tests
// do not establish native MDS authentication or a filesystem client path.
func mdsTestKey(kind uint16, fill byte) Key {
	size := 16
	if kind == AES256K {
		size = 32
	}
	return Key{kind: kind, secret: bytes.Repeat([]byte{fill}, size)}
}

func mdsAuthorizerProof(t *testing.T, a *Authorizer, mds Key, challenge []byte, wantChallenge bool, wantResponse uint64) []byte {
	t.Helper()
	payload, err := a.Payload(challenge)
	if err != nil {
		t.Fatal(err)
	}
	d := wire.NewDecoder(payload)
	if d.U8() != 1 || d.U64() != 42 || d.U32() != ServiceMDS || d.U8() != 1 || d.U64() != 7 || !bytes.Equal(d.Bytes(), []byte("opaque ticket")) {
		t.Fatal("MDS authorizer service, identity or ticket changed")
	}
	proof := d.Bytes()
	if err := d.Done(); err != nil {
		t.Fatal(err)
	}
	plain, err := mds.Open(0x10, proof)
	if err != nil {
		t.Fatal("MDS authorizer used another service key", err)
	}
	d = wire.NewDecoder(plain)
	if d.U8() != 2 || d.U64() != a.nonce || d.Bool() != wantChallenge || d.U64() != wantResponse || d.Done() != nil {
		t.Fatal("MDS authorizer v2 challenge proof differs")
	}
	return proof
}

func TestMDSOptionalServiceAuthenticationRenewalAndAuthorizer(t *testing.T) {
	for _, kind := range []uint16{AES, AES256K} {
		for _, secure := range []bool{false, true} {
			for _, withOSD := range []bool{false, true} {
				t.Run(fmt.Sprintf("key%d/secure%t/osd%t", kind, secure, withOSD), func(t *testing.T) {
					key, auth, mds, mgr, osdKey := mdsTestKey(kind, 0x51), mdsTestKey(kind, 0x61), mdsTestKey(kind, 0x71), mdsTestKey(kind, 0x81), mdsTestKey(kind, 0x91)
					services := ServiceAuth | ServiceMgr | ServiceMDS
					if withOSD {
						services |= ServiceOSD
					}
					c, err := NewClient("client.mds-services", key, services)
					if err != nil {
						t.Fatal(err)
					}
					assertServiceRequest(t, c, services)
					parts := [][]byte{testTickets(t, auth, mds, ServiceMDS, nil), testTickets(t, auth, mgr, ServiceMgr, nil)}
					if withOSD {
						parts = append(parts, testTickets(t, auth, osdKey, ServiceOSD, nil))
					}
					var secret []byte
					if secure {
						secret = bytes.Repeat([]byte{0x39}, 40)
					}
					connection := connectionSecretCiphertext(t, auth, plaintextTestBytes(nil, secret))
					response := authReplyWithConnection(t, key, auth, nil, connection, joinedTicketReplies(parts...))
					if _, got, err := c.Finish(42, response, secure); err != nil || !bytes.Equal(got, secret) || len(c.Tickets) != len(parts)+1 {
						t.Fatal("MDS initial service exchange failed", err)
					}
					a, err := c.Authorizer(ServiceMDS)
					if err != nil || !reflect.DeepEqual(a.Key(), mds) {
						t.Fatal("MDS authorizer selected another key", err)
					}
					proof := mdsAuthorizerProof(t, a, mds, nil, false, 0)
					for _, wrong := range []Key{mgr, osdKey} {
						if _, err := wrong.Open(0x10, proof); err == nil {
							t.Fatal("MDS proof verified with another service key")
						}
					}
					challenge := wire.Encoder{}
					challenge.U8(1)
					challenge.U64(57)
					encryptedChallenge, err := mds.Seal(0x11, challenge.Data)
					if err != nil {
						t.Fatal(err)
					}
					mdsAuthorizerProof(t, a, mds, encryptedChallenge, true, 58)
					wrongChallenge, err := mgr.Seal(0x11, challenge.Data)
					if err != nil {
						t.Fatal(err)
					}
					if payload, err := a.Payload(wrongChallenge); err == nil || payload != nil {
						t.Fatal("MDS challenge accepted MGR encryption")
					}
					for _, valid := range []bool{true, false} {
						reply := wire.Encoder{}
						reply.U8(2)
						nonce := a.nonce + 1
						if !valid {
							nonce++
						}
						reply.U64(nonce)
						reply.Bytes(secret)
						cipher, err := mds.Seal(0x12, reply.Data)
						if err != nil {
							t.Fatal(err)
						}
						envelope := wire.Encoder{}
						envelope.Bytes(cipher)
						got, err := a.Finish(envelope.Data, secure)
						if valid {
							if err != nil || !bytes.Equal(got, secret) {
								t.Fatal("MDS service reply failed", err)
							}
						} else if !errors.Is(err, ErrIntegrity) || got != nil {
							t.Fatal("MDS service reply accepted wrong nonce", err)
						}
					}
					if other, err := c.Authorizer(ServiceMgr); err != nil || !reflect.DeepEqual(other.Key(), mgr) {
						t.Fatal("MDS selection changed MGR authorizer", err)
					}
					if withOSD {
						if other, err := c.Authorizer(ServiceOSD); err != nil || !reflect.DeepEqual(other.Key(), osdKey) {
							t.Fatal("MDS selection changed OSD authorizer", err)
						}
					} else if other, err := c.Authorizer(ServiceOSD); !errors.Is(err, ErrTicket) || other != nil {
						t.Fatal("MDS selection implicitly enabled OSD")
					}
					before := plaintextFuzzSnapshot(c)
					nextAuth, nextMDS, nextMgr, nextOSD := mdsTestKey(kind, 0xa1), mdsTestKey(kind, 0xb1), mdsTestKey(kind, 0xc1), mdsTestKey(kind, 0xd1)
					oldAuth, oldMDS, oldMgr := c.Tickets[ServiceAuth], c.Tickets[ServiceMDS], c.Tickets[ServiceMgr]
					nextMDSReply := testTickets(t, nextAuth, nextMDS, ServiceMDS, &oldMDS)
					connection = connectionSecretCiphertext(t, nextAuth, plaintextTestBytes(nil, secret))
					badMgr := plaintextTestTicket(t, nextAuth, ServiceMgr, []byte{2}, plaintextTestBlob(), false)
					response = authReplyWithConnection(t, key, nextAuth, &oldAuth, connection, joinedTicketReplies(nextMDSReply, badMgr))
					if session, got, err := c.Finish(99, response, secure); !errors.Is(err, wire.ErrVersion) || session.Type() != 0 || got != nil || !reflect.DeepEqual(*c, before) {
						t.Fatal("bad later service ticket published partial MDS renewal", err)
					}
					parts = [][]byte{nextMDSReply, testTickets(t, nextAuth, nextMgr, ServiceMgr, &oldMgr)}
					if withOSD {
						oldOSD := c.Tickets[ServiceOSD]
						parts = append(parts, testTickets(t, nextAuth, nextOSD, ServiceOSD, &oldOSD))
					}
					response = authReplyWithConnection(t, key, nextAuth, &oldAuth, connection, joinedTicketReplies(parts...))
					if _, got, err := c.Finish(42, response, secure); err != nil || !bytes.Equal(got, secret) || !reflect.DeepEqual(c.Tickets[ServiceMDS].Key, nextMDS) || c.Services() != services {
						t.Fatal("MDS renewal lost key or immutable selection", err)
					}
					assertServiceRequest(t, c, services)
					renewed, err := c.Authorizer(ServiceMDS)
					if err != nil || !reflect.DeepEqual(renewed.Key(), nextMDS) || !reflect.DeepEqual(a.Key(), mds) {
						t.Fatal("MDS renewal mutated an existing authorizer", err)
					}
					ticket := c.Tickets[ServiceMDS]
					ticket.Expires = time.Now().Add(-time.Second)
					c.Tickets[ServiceMDS] = ticket
					if expired, err := c.Authorizer(ServiceMDS); !errors.Is(err, ErrTicket) || expired != nil {
						t.Fatal("expired MDS ticket produced authorizer", err)
					}
				})
			}
		}
	}
}
