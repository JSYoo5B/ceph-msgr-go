package cephx

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

const (
	ServiceMgr  uint32 = 0x10
	ServiceAuth uint32 = 0x20
)

var ErrTicket = errors.New("cephx: missing, expired or malformed ticket")

type Ticket struct {
	Key                 Key
	SecretID            uint64
	Blob                []byte
	Expires, RenewAfter time.Time
}

func (t Ticket) encode(e *wire.Encoder) { e.U8(1); e.U64(t.SecretID); e.Bytes(t.Blob) }

// Client is owned by one connection coordinator. Ticket updates are atomic on
// successful parsing, including renewals encrypted with the previous auth key.
type Client struct {
	Name     string
	Key      Key
	GlobalID uint64
	Tickets  map[uint32]Ticket
}

func NewClient(name string, key Key) (*Client, error) {
	if !strings.HasPrefix(name, "client.") || len(name) <= 7 || len(name) > 1024 || strings.ContainsAny(name, "\x00\r\n") {
		return nil, errors.New("cephx: identity must be client.<id>")
	}
	if key.Type() != AES && key.Type() != AES256K {
		return nil, ErrKey
	}
	return &Client{Name: name, Key: key, Tickets: make(map[uint32]Ticket)}, nil
}
func (c *Client) Initial() []byte {
	e := wire.Encoder{}
	e.U8(10)
	e.U32(8)
	e.String(strings.TrimPrefix(c.Name, "client."))
	e.U64(c.GlobalID)
	return e.Data
}
func randomUint64() (uint64, error) {
	var p [8]byte
	_, err := rand.Read(p[:])
	return binary.LittleEndian.Uint64(p[:]), err
}
func (c *Client) Challenge(p []byte) ([]byte, error) {
	d := wire.NewDecoder(p)
	v := d.U8()
	challenge := d.U64()
	if err := d.Done(); err != nil {
		return nil, err
	}
	if v != 1 {
		return nil, wire.ErrVersion
	}
	nonce, err := randomUint64()
	if err != nil {
		return nil, err
	}
	return c.authRequest(challenge, nonce)
}
func (c *Client) authRequest(challenge, nonce uint64) ([]byte, error) {
	b := wire.Encoder{}
	b.U64(challenge)
	b.U64(nonce)
	var proof []byte
	if c.Key.Type() == AES {
		ciphertext, err := c.Key.Seal(0, b.Data)
		if err != nil {
			return nil, err
		}
		e := wire.Encoder{}
		e.Bytes(ciphertext)
		proof = e.Data
	} else {
		proof = c.Key.Signature(b.Data)
	}
	var sum uint64
	for pos := 0; pos+8 <= len(proof); pos += 8 {
		sum ^= binary.LittleEndian.Uint64(proof[pos:])
	}
	e := wire.Encoder{}
	e.U16(0x100)
	e.U8(3)
	e.U64(nonce)
	e.U64(sum)
	c.Tickets[ServiceAuth].encode(&e)
	e.U32(ServiceAuth | ServiceMgr)
	return e.Data, nil
}

func parseReplies(d *wire.Decoder, secret Key, old map[uint32]Ticket, now time.Time) (map[uint32]Ticket, error) {
	if d.U8() != 1 {
		d.Fail(wire.ErrVersion)
	}
	n := d.Count(6, 32)
	out := make(map[uint32]Ticket, len(old)+n)
	for typ, t := range old {
		out[typ] = t
	}
	seen := make(map[uint32]bool)
	for i := 0; i < n && d.Err() == nil; i++ {
		typ := d.U32()
		if typ == 0 || typ&(typ-1) != 0 || typ > ServiceAuth || seen[typ] {
			return nil, ErrTicket
		}
		seen[typ] = true
		if d.U8() != 1 {
			return nil, wire.ErrVersion
		}
		plain, err := secret.Open(4, d.Bytes())
		if err != nil {
			return nil, err
		}
		td := wire.NewDecoder(plain)
		if td.U8() != 1 {
			return nil, wire.ErrVersion
		}
		key := DecodeKey(td)
		sec, ns := td.U32(), td.U32()
		if ns >= 1_000_000_000 || sec == 0 {
			return nil, ErrTicket
		}
		if err := td.Done(); err != nil {
			return nil, err
		}
		encrypted := d.Bool()
		blob := d.Bytes()
		if encrypted {
			previous, ok := old[typ]
			if !ok {
				return nil, ErrTicket
			}
			plain, err := previous.Key.Open(5, blob)
			if err != nil {
				return nil, err
			}
			bd := wire.NewDecoder(plain)
			blob = bd.Bytes()
			if err := bd.Done(); err != nil {
				return nil, err
			}
		}
		bd := wire.NewDecoder(blob)
		if bd.U8() != 1 {
			return nil, wire.ErrVersion
		}
		sid := bd.U64()
		opaque := bd.Bytes()
		if err := bd.Done(); err != nil {
			return nil, err
		}
		validity := time.Duration(sec)*time.Second + time.Duration(ns)
		out[typ] = Ticket{Key: key, SecretID: sid, Blob: append([]byte(nil), opaque...), Expires: now.Add(validity), RenewAfter: now.Add(validity - validity/4)}
	}
	return out, d.Err()
}

// Finish verifies the MON auth reply and extracts the per-connection secret.
// globalID is published only after every ticket and encrypted field verifies.
func (c *Client) Finish(globalID uint64, p []byte) (Key, []byte, error) {
	d := wire.NewDecoder(p)
	typ, status := d.U16(), int32(d.U32())
	if status != 0 {
		return Key{}, nil, fmt.Errorf("cephx: server authentication code %d", status)
	}
	if typ != 0x100 {
		return Key{}, nil, wire.ErrVersion
	}
	now := time.Now()
	tickets, err := parseReplies(d, c.Key, c.Tickets, now)
	if err != nil {
		return Key{}, nil, err
	}
	auth, ok := tickets[ServiceAuth]
	if !ok {
		return Key{}, nil, ErrTicket
	}
	connection, extra := d.Bytes(), d.Bytes()
	if err := d.Done(); err != nil {
		return Key{}, nil, err
	}
	cd := wire.NewDecoder(connection)
	plain, err := auth.Key.Open(3, cd.Bytes())
	if err != nil {
		return Key{}, nil, err
	}
	if err := cd.Done(); err != nil {
		return Key{}, nil, err
	}
	sd := wire.NewDecoder(plain)
	secret := sd.Bytes()
	if err := sd.Done(); err != nil {
		return Key{}, nil, err
	}
	if len(secret) < 40 {
		return Key{}, nil, ErrIntegrity
	}
	if len(extra) > 0 {
		ed := wire.NewDecoder(extra)
		tickets, err = parseReplies(ed, auth.Key, tickets, now)
		if err != nil {
			return Key{}, nil, err
		}
		if err := ed.Done(); err != nil {
			return Key{}, nil, err
		}
	}
	c.GlobalID, c.Tickets = globalID, tickets
	return auth.Key, append([]byte(nil), secret...), nil
}

type Authorizer struct {
	ticket Ticket
	base   []byte
	nonce  uint64
}

func (c *Client) Authorizer(service uint32) (*Authorizer, error) {
	t, ok := c.Tickets[service]
	if !ok || !time.Now().Before(t.Expires) {
		return nil, ErrTicket
	}
	nonce, err := randomUint64()
	if err != nil {
		return nil, err
	}
	e := wire.Encoder{}
	e.U8(1)
	e.U64(c.GlobalID)
	e.U32(service)
	t.encode(&e)
	return &Authorizer{ticket: t, base: e.Data, nonce: nonce}, nil
}
func (a *Authorizer) Key() Key { return a.ticket.Key }
func (a *Authorizer) Payload(challenge []byte) ([]byte, error) {
	e := wire.Encoder{}
	e.U8(2)
	e.U64(a.nonce)
	if len(challenge) == 0 {
		e.U8(0)
		e.U64(0)
	} else {
		plain, err := a.ticket.Key.Open(0x11, challenge)
		if err != nil {
			return nil, err
		}
		d := wire.NewDecoder(plain)
		if d.U8() != 1 {
			return nil, wire.ErrVersion
		}
		nonce := d.U64()
		if err := d.Done(); err != nil {
			return nil, err
		}
		e.U8(1)
		e.U64(nonce + 1)
	}
	p, err := a.ticket.Key.Seal(0x10, e.Data)
	if err != nil {
		return nil, err
	}
	out := wire.Encoder{}
	out.Raw(a.base)
	out.Bytes(p)
	return out.Data, nil
}
func (a *Authorizer) Finish(p []byte) ([]byte, error) {
	d := wire.NewDecoder(p)
	plain, err := a.ticket.Key.Open(0x12, d.Bytes())
	if err != nil {
		return nil, err
	}
	if err := d.Done(); err != nil {
		return nil, err
	}
	r := wire.NewDecoder(plain)
	if r.U8() != 2 || r.U64() != a.nonce+1 {
		return nil, ErrIntegrity
	}
	secret := r.Bytes()
	if err := r.Done(); err != nil {
		return nil, err
	}
	if len(secret) < 40 {
		return nil, ErrIntegrity
	}
	return secret, nil
}
