package cephx

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"io"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Service-side verification of an incoming CephX service authorizer, the
// accepting half of Authorizer (ceph/ceph/blob/v20.2.4/src/auth/cephx/CephxProtocol.cc
// cephx_verify_authorizer, CephxAuthorizeHandler.cc and MonClient.cc
// handle_auth_request, which a daemon's messenger uses as its auth server).
//
// An authorizer is: version byte 1 (AUTH_MODE_AUTHORIZER), the presented
// global ID, the service ID, a CephXTicketBlob (version 1, secret ID, the
// CephXServiceTicketInfo encrypted with the rotating service secret of that
// ID under TICKET_INFO usage 0x30), then a length-prefixed CephXAuthorize
// (version 2: nonce, have_challenge, server_challenge_plus_one) encrypted
// with the ticket's session key under AUTHORIZE usage 0x10. The ticket info
// is version 1, an AuthTicket (version 2: EntityName as u32 type and string
// ID, global ID, a retired u64 auid, created and expires utime_t, an
// AuthCapsInfo of version 1, allow_all byte and caps bufferlist, and u32
// flags) and the session key.
//
// The exchange has exactly two rounds, as stock MonClient permits: the first
// authorizer is answered with a fresh CephXAuthorizeChallenge (version 1,
// a random server challenge) encrypted under AUTHORIZE_CHALLENGE 0x11 and
// sent raw in AUTH_REPLY_MORE; the second must carry have_challenge and
// server_challenge_plus_one, and is answered with a length-prefixed
// CephXAuthorizeReply encrypted under AUTHORIZE_REPLY 0x12: version 2 with
// nonce_plus_one and a random connection secret in secure mode (64 bytes,
// AuthConnectionMeta::get_connection_secret_length), version 1 with
// nonce_plus_one only in CRC mode. A first authorizer that already claims a
// challenge is answered with a fresh challenge too; any further round is
// refused.
//
// Stricter than native, and stated here because a stock client never
// trips it: the second authorizer must repeat the first one's plaintext
// prefix (global ID, service, ticket) and nonce byte for byte, so a
// response cannot be grafted from another handshake or another ticket; the
// authorizer and every nested encoding must have no trailing bytes; and the
// ticket's caps bufferlist must hold exactly one encoded string (or be empty).
// Like native, the ticket's own expiry is not checked: acceptance of an
// expired service secret ends when the rotating set no longer holds its ID.
//
// Only the presented secret ID is looked up, through the caller's exact-ID
// lookup. The all-ones secret ID, which native code resolves through the
// service's own keyring entry instead of the rotating set, is a separate
// profile without evidence here and is refused; an unknown ID never selects
// another available key.

// AuthorizerRefusal names why an incoming service authorizer was refused.
// It describes the failed check without key, ticket or proof material.
type AuthorizerRefusal string

const (
	RefusedMalformed        AuthorizerRefusal = "malformed authorizer"
	RefusedVersion          AuthorizerRefusal = "unsupported authorizer version"
	RefusedService          AuthorizerRefusal = "authorizer for another service"
	RefusedGeneralSecret    AuthorizerRefusal = "all-ones secret ID is not admitted"
	RefusedUnknownSecret    AuthorizerRefusal = "unknown service secret ID"
	RefusedTicket           AuthorizerRefusal = "ticket does not decrypt or decode"
	RefusedGlobalID         AuthorizerRefusal = "presented global ID differs from the ticket"
	RefusedAuthorize        AuthorizerRefusal = "authorize request does not decrypt or decode"
	RefusedChallenge        AuthorizerRefusal = "challenge response does not match this handshake"
	RefusedDifferentRequest AuthorizerRefusal = "second round presents a different ticket or nonce"
	RefusedRounds           AuthorizerRefusal = "too many authorizer rounds"
	RefusedCaps             AuthorizerRefusal = "malformed authenticated caps"
)

// AuthorizerError is a refused service authorizer.
type AuthorizerError struct{ Reason AuthorizerRefusal }

func (e *AuthorizerError) Error() string {
	return "cephx: service authorizer refused: " + string(e.Reason)
}

func refuse(reason AuthorizerRefusal) error { return &AuthorizerError{Reason: reason} }

// ServiceKeyLookup returns the rotating secret of exactly secretID for the
// verifier's service, or false. It must not substitute another key.
type ServiceKeyLookup func(secretID uint64) (Key, bool)

const (
	usageTicketInfo         uint32 = 0x30
	usageAuthorize          uint32 = 0x10
	usageAuthorizeChallenge uint32 = 0x11
	usageAuthorizeReply     uint32 = 0x12
	// secureConnectionSecret is get_connection_secret_length for secure mode.
	secureConnectionSecret = 64
	// maxCapsBytes bounds an authenticated caps bufferlist.
	maxCapsBytes = 64 << 10
	maxEntityID  = 1024
)

// PeerTicket is the authenticated content of an accepted service ticket.
// It holds no key material. Caps is the decoded caps text and AllowAll is
// kept separately: an allow_all ticket may carry no text at all. Neither is
// interpreted here; authorization is the caller's.
type PeerTicket struct {
	EntityType       uint32
	EntityID         string
	GlobalID         uint64
	SecretID         uint64
	Created, Expires time.Time
	AllowAll         bool
	Caps             string
	Flags            uint32
}

// ServiceVerifier verifies the authorizer exchange of one incoming
// connection. It is owned by that connection's handshake and holds its
// challenge state; it is not safe for concurrent use.
type ServiceVerifier struct {
	service      uint32
	lookup       ServiceKeyLookup
	secretLength int
	random       io.Reader

	rounds     int
	prefix     []byte
	nonce      uint64
	challenge  uint64
	challenged bool

	ticket     PeerTicket
	sessionKey Key
	secret     []byte
	done       bool
}

// NewServiceVerifier verifies authorizers for service (the accepting
// daemon's entity type) with lookup. secure selects the secure-mode reply
// with a connection secret; CRC mode returns the nonce-only reply.
func NewServiceVerifier(service uint32, lookup ServiceKeyLookup, secure bool) *ServiceVerifier {
	v := &ServiceVerifier{service: service, lookup: lookup, random: rand.Reader}
	if secure {
		v.secretLength = secureConnectionSecret
	}
	return v
}

// Verify handles one authorizer round. It returns the AUTH_REPLY_MORE
// payload (the raw encrypted challenge) with done false, or the AUTH_DONE
// payload with done true once the challenge response verified. A refusal is
// an *AuthorizerError; nothing is published by a refused round.
func (v *ServiceVerifier) Verify(payload []byte) (reply []byte, done bool, err error) {
	if v.done || v.rounds >= 2 {
		return nil, false, refuse(RefusedRounds)
	}
	v.rounds++
	d := wire.NewDecoder(payload)
	version := d.U8()
	globalID := d.U64()
	service := d.U32()
	ticketVersion := d.U8()
	secretID := d.U64()
	blob := d.Bytes()
	if d.Err() != nil {
		return nil, false, refuse(RefusedMalformed)
	}
	if version != 1 || ticketVersion != 1 {
		return nil, false, refuse(RefusedVersion)
	}
	prefix := payload[:len(payload)-d.Remaining()]
	if service != v.service {
		return nil, false, refuse(RefusedService)
	}
	if secretID == ^uint64(0) {
		return nil, false, refuse(RefusedGeneralSecret)
	}
	serviceKey, ok := v.lookup(secretID)
	if !ok {
		return nil, false, refuse(RefusedUnknownSecret)
	}
	ticket, sessionKey, err := decodeTicketInfo(serviceKey, blob)
	if err != nil {
		return nil, false, err
	}
	ticket.SecretID = secretID
	if ticket.GlobalID != globalID {
		return nil, false, refuse(RefusedGlobalID)
	}
	ciphertext := d.Bytes()
	if err := d.Done(); err != nil {
		return nil, false, refuse(RefusedMalformed)
	}
	plain, err := sessionKey.Open(usageAuthorize, ciphertext)
	if err != nil {
		return nil, false, refuse(RefusedAuthorize)
	}
	a := wire.NewDecoder(plain)
	authorizeVersion := a.U8()
	nonce := a.U64()
	haveChallenge := a.Bool()
	plusOne := a.U64()
	if a.Done() != nil || authorizeVersion != 2 {
		return nil, false, refuse(RefusedAuthorize)
	}
	if v.rounds == 1 {
		// Native answers every first round with a fresh challenge, also one
		// whose authorize claims a challenge it was never given.
		v.prefix, v.nonce = bytes.Clone(prefix), nonce
		var p [8]byte
		if _, err := io.ReadFull(v.random, p[:]); err != nil {
			return nil, false, err
		}
		v.challenge, v.challenged = binary.LittleEndian.Uint64(p[:]), true
		e := wire.Encoder{}
		e.U8(1)
		e.U64(v.challenge)
		challenge, err := sessionKey.Seal(usageAuthorizeChallenge, e.Data)
		if err != nil {
			return nil, false, err
		}
		return challenge, false, nil
	}
	if !bytes.Equal(prefix, v.prefix) || nonce != v.nonce {
		return nil, false, refuse(RefusedDifferentRequest)
	}
	if !v.challenged || !haveChallenge || plusOne != v.challenge+1 {
		return nil, false, refuse(RefusedChallenge)
	}
	e := wire.Encoder{}
	if v.secretLength == 0 {
		e.U8(1)
		e.U64(nonce + 1)
	} else {
		secret := make([]byte, v.secretLength)
		if _, err := io.ReadFull(v.random, secret); err != nil {
			return nil, false, err
		}
		e.U8(2)
		e.U64(nonce + 1)
		e.Bytes(secret)
		v.secret = secret
	}
	sealed, err := sessionKey.Seal(usageAuthorizeReply, e.Data)
	if err != nil {
		return nil, false, err
	}
	out := wire.Encoder{}
	out.Bytes(sealed)
	v.ticket, v.sessionKey, v.done = ticket, sessionKey, true
	return out.Data, true, nil
}

// Ticket returns the authenticated ticket after a completed exchange.
func (v *ServiceVerifier) Ticket() (PeerTicket, bool) { return v.ticket, v.done }

// SessionKey is the ticket's session key, which signs the Messenger
// transcripts, after a completed exchange.
func (v *ServiceVerifier) SessionKey() Key { return v.sessionKey }

// ConnectionSecret is the secure-mode connection secret sent in the reply;
// empty in CRC mode.
func (v *ServiceVerifier) ConnectionSecret() []byte { return bytes.Clone(v.secret) }

func decodeTicketInfo(serviceKey Key, blob []byte) (PeerTicket, Key, error) {
	plain, err := serviceKey.Open(usageTicketInfo, blob)
	if err != nil {
		return PeerTicket{}, Key{}, refuse(RefusedTicket)
	}
	d := wire.NewDecoder(plain)
	infoVersion := d.U8()
	ticketVersion := d.U8()
	var t PeerTicket
	t.EntityType = d.U32()
	id := d.Bytes()
	t.GlobalID = d.U64()
	d.U64() // retired auid
	createdSec, createdNsec := d.U32(), d.U32()
	expiresSec, expiresNsec := d.U32(), d.U32()
	capsVersion := d.U8()
	allowAll := d.U8()
	caps := d.Bytes()
	t.Flags = d.U32()
	sessionKey := DecodeKey(d)
	if d.Done() != nil || infoVersion != 1 || ticketVersion != 2 || capsVersion != 1 || allowAll > 1 || len(id) > maxEntityID ||
		createdNsec >= 1_000_000_000 || expiresNsec >= 1_000_000_000 {
		return PeerTicket{}, Key{}, refuse(RefusedTicket)
	}
	t.Created = time.Unix(int64(createdSec), int64(createdNsec)).UTC()
	t.Expires = time.Unix(int64(expiresSec), int64(expiresNsec)).UTC()
	t.EntityID, t.AllowAll = string(id), allowAll == 1
	if len(caps) > maxCapsBytes {
		return PeerTicket{}, Key{}, refuse(RefusedCaps)
	}
	if len(caps) > 0 {
		c := wire.NewDecoder(caps)
		text := c.Bytes()
		if c.Done() != nil {
			return PeerTicket{}, Key{}, refuse(RefusedCaps)
		}
		t.Caps = string(text)
	}
	return t, sessionKey, nil
}
