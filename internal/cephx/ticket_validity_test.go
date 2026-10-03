package cephx

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Pinned Tentacle CephxProtocol.cc accepts nonzero utime_t validity, including
// a fractional second. These are encrypted codec tests; they do not establish
// that the connection coordinator's renewal schedule supports subsecond TTLs.
func validityTicketReply(t testing.TB, encryption, session Key, service, sec, ns uint32) []byte {
	t.Helper()
	key := wire.Encoder{}
	key.U8(1)
	key.Raw(encodeTestKey(session))
	key.U32(sec)
	key.U32(ns)
	encrypted, err := encryption.Seal(4, key.Data)
	if err != nil {
		t.Fatal(err)
	}
	blob := wire.Encoder{}
	blob.U8(1)
	blob.U64(7)
	blob.Bytes([]byte("opaque ticket"))
	reply := wire.Encoder{}
	reply.U8(1)
	reply.U32(1)
	reply.U32(service)
	reply.U8(1)
	reply.Bytes(encrypted)
	reply.U8(0)
	reply.Bytes(blob.Data)
	return reply.Data
}

func TestPositiveTicketValidityCodec(t *testing.T) {
	for _, kind := range []uint16{AES, AES256K} {
		key := Key{kind: kind, secret: bytes.Repeat([]byte{0x41}, 16)}
		if kind == AES256K {
			key.secret = bytes.Repeat([]byte{0x51}, 32)
		}
		t.Run(key.String(), func(t *testing.T) {
			for _, service := range []struct {
				name string
				id   uint32
			}{{"auth", ServiceAuth}, {"mgr", ServiceMgr}} {
				for _, validity := range []time.Duration{time.Nanosecond, 500 * time.Millisecond, time.Second - time.Nanosecond, time.Second, time.Duration(^uint32(0))*time.Second + time.Second - time.Nanosecond} {
					t.Run(service.name+"/"+validity.String(), func(t *testing.T) {
						now := time.Unix(42, 0)
						d := wire.NewDecoder(validityTicketReply(t, key, key, service.id, uint32(validity/time.Second), uint32(validity%time.Second)))
						tickets, err := parseReplies(d, key, nil, now)
						if err != nil {
							t.Fatalf("positive %v validity rejected: %v", validity, err)
						}
						if err := d.Done(); err != nil {
							t.Fatal(err)
						}
						ticket := tickets[service.id]
						if len(tickets) != 1 || ticket.SecretID != 7 || !bytes.Equal(ticket.Blob, []byte("opaque ticket")) || ticket.Key.Type() != kind {
							t.Fatal("ticket fields changed", ticket)
						}
						if !ticket.Expires.Equal(now.Add(validity)) || !ticket.RenewAfter.Equal(now.Add(validity-validity/4)) {
							t.Fatal("ticket validity changed", ticket.Expires, ticket.RenewAfter)
						}
					})
				}
			}
		})
	}
}

func TestInvalidTicketValidityDoesNotPublishAuthentication(t *testing.T) {
	for _, kind := range []uint16{AES, AES256K} {
		key := Key{kind: kind, secret: bytes.Repeat([]byte{0x41}, 16)}
		if kind == AES256K {
			key.secret = bytes.Repeat([]byte{0x51}, 32)
		}
		t.Run(key.String(), func(t *testing.T) {
			for _, field := range []struct {
				name    string
				sec, ns uint32
			}{{"zero", 0, 0}, {"fractional-overflow", 0, 1_000_000_000}, {"integer-overflow", 1, ^uint32(0)}} {
				for _, service := range []struct {
					name string
					id   uint32
				}{{"auth", ServiceAuth}, {"mgr", ServiceMgr}} {
					t.Run(field.name+"/"+service.name, func(t *testing.T) {
						c, err := NewClient("client.validity", key)
						if err != nil {
							t.Fatal(err)
						}
						c.GlobalID = 42
						previous := Ticket{Key: key, SecretID: 3, Blob: []byte("previous proof"), Expires: time.Unix(100, 0), RenewAfter: time.Unix(90, 0)}
						c.Tickets[ServiceAuth], c.Tickets[ServiceMgr] = previous, previous
						before := map[uint32]Ticket{ServiceAuth: previous, ServiceMgr: previous}
						authReply := validityTicketReply(t, key, key, ServiceAuth, 60, 0)
						mgrReply := validityTicketReply(t, key, key, ServiceMgr, 60, 0)
						invalid := validityTicketReply(t, key, key, service.id, field.sec, field.ns)
						if service.id == ServiceAuth {
							authReply = invalid
						} else {
							mgrReply = invalid
						}
						secret := wire.Encoder{}
						secret.Bytes(bytes.Repeat([]byte{0x17}, 40))
						encrypted, err := key.Seal(3, secret.Data)
						if err != nil {
							t.Fatal(err)
						}
						container := wire.Encoder{}
						container.Bytes(encrypted)
						reply := wire.Encoder{}
						reply.U16(0x100)
						reply.U32(0)
						reply.Raw(authReply)
						reply.Bytes(container.Data)
						reply.Bytes(mgrReply)
						if _, _, err := c.Finish(99, reply.Data, true); !errors.Is(err, ErrTicket) {
							t.Fatal("invalid validity accepted or misclassified", err)
						}
						if c.GlobalID != 42 || !reflect.DeepEqual(c.Tickets, before) {
							t.Fatal("invalid validity published authentication state")
						}
					})
				}
			}
		})
	}
}
