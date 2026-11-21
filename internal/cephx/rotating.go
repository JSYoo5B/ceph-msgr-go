package cephx

import (
	"errors"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Rotating service secrets of a daemon principal.
//
// A daemon asks the MON for its service's rotating secrets with
// CEPHX_GET_ROTATING_KEY inside an MAuth message on the authenticated MON
// session (ceph/ceph/blob/v20.2.4/src/auth/cephx/CephxClientHandler.cc
// build_rotating_request). The MON answers with a CephX response header and
// a bufferlist holding the RotatingSecrets encrypted under the principal's
// own key with ROTATING_SECRET usage (CephxServiceHandler.cc and
// KeyServer::get_rotating_encrypted). RotatingSecrets is a version byte, a
// map of secret ID to ExpiringCryptoKey (version byte, CryptoKey,
// utime_t expiration) and max_ver (ceph/ceph/blob/v20.2.4/src/auth/Auth.h,
// Auth.cc). The native set holds three keys: previous, current and next.

const (
	// requestAuthSessionKey and requestRotatingKey are the CephX request
	// types of CephxProtocol.h (CEPHX_GET_AUTH_SESSION_KEY, _ROTATING_KEY).
	requestAuthSessionKey uint16 = 0x0100
	requestRotatingKey    uint16 = 0x0400
	usageRotatingSecret   uint32 = 0x20
	// rotatingKeyCount is Auth.h KEY_ROTATE_NUM.
	rotatingKeyCount = 3
	// maxRotatingKeys bounds a decoded set. The native set never exceeds
	// KEY_ROTATE_NUM; a few more are tolerated, never an unbounded map.
	maxRotatingKeys = 16
)

// ErrRotating marks a rotating-secret reply that is not a CEPHX_GET_ROTATING_KEY
// response or whose encrypted set does not decode.
var ErrRotating = errors.New("cephx: invalid rotating secrets reply")

// RotatingRequest is the MAuth auth_payload of a rotating-secret request: the
// CephX request header alone.
func RotatingRequest() []byte {
	e := wire.Encoder{}
	e.U16(requestRotatingKey)
	return e.Data
}

// ResponseType returns the CephX request type a successful MAuthReply
// result_bl answers. Replies on one MON session arrive in request order; the
// type lets a caller check that the oldest outstanding request is the one
// answered.
func ResponseType(resultBL []byte) (uint16, error) {
	d := wire.NewDecoder(resultBL)
	typ := d.U16()
	d.U32()
	return typ, d.Err()
}

// RotatingKey is one expiring service secret. Key is secret material; it is
// never printed (Key.String hides it).
type RotatingKey struct {
	ID      uint64
	Key     Key
	Expires time.Time
}

// RotatingSecrets is one complete decoded set, in ascending ID order.
// Values are immutable once published; Keys is never modified in place.
type RotatingSecrets struct {
	Keys       []RotatingKey
	MaxVersion uint64
}

// DecodeRotatingReply verifies a CEPHX_GET_ROTATING_KEY reply (the MAuthReply
// result_bl) with the principal's key. The whole set decodes or nothing is
// returned. A non-zero CephX status is an AuthenticationError.
func DecodeRotatingReply(resultBL []byte, principal Key) (RotatingSecrets, error) {
	d := wire.NewDecoder(resultBL)
	typ, status := d.U16(), int32(d.U32())
	ciphertext := d.Bytes()
	if err := d.Err(); err != nil {
		return RotatingSecrets{}, err
	}
	if typ != requestRotatingKey {
		return RotatingSecrets{}, ErrRotating
	}
	if status != 0 {
		return RotatingSecrets{}, &AuthenticationError{Method: 2, Code: status}
	}
	if err := d.Done(); err != nil {
		return RotatingSecrets{}, err
	}
	plain, err := principal.Open(usageRotatingSecret, ciphertext)
	if err != nil {
		return RotatingSecrets{}, err
	}
	p := wire.NewDecoder(plain)
	if p.U8() != 1 {
		p.Fail(wire.ErrVersion)
	}
	// ID, version byte, minimum CryptoKey (type, created, length) and
	// expiration.
	n := p.Count(8+1+12+8, maxRotatingKeys)
	out := RotatingSecrets{Keys: make([]RotatingKey, 0, n)}
	for i := 0; i < n && p.Err() == nil; i++ {
		id := p.U64()
		if p.U8() != 1 {
			p.Fail(wire.ErrVersion)
		}
		key := DecodeKey(p)
		sec, ns := p.U32(), p.U32()
		if p.Err() != nil {
			break
		}
		if ns >= 1_000_000_000 || (len(out.Keys) > 0 && out.Keys[len(out.Keys)-1].ID >= id) {
			p.Fail(ErrRotating)
			break
		}
		out.Keys = append(out.Keys, RotatingKey{ID: id, Key: key, Expires: time.Unix(int64(sec), int64(ns))})
	}
	out.MaxVersion = p.U64()
	if err := p.Done(); err != nil {
		return RotatingSecrets{}, err
	}
	if len(out.Keys) > 0 && out.Keys[len(out.Keys)-1].ID > out.MaxVersion {
		return RotatingSecrets{}, ErrRotating
	}
	return out, nil
}

// NeedsRefresh is MonClient::_check_auth_rotating's rule
// (ceph/ceph/blob/v20.2.4/src/mon/MonClient.cc): refresh when the set holds
// fewer than three keys, or when the middle ("current") key expires at or
// before now - min(30 s, ttl/4), where ttl is the cluster's
// auth_service_ticket_ttl. The cutoff lies in the past: a refresh is not
// requested before the middle key has expired. The send-rate limit is the
// caller's.
func (r RotatingSecrets) NeedsRefresh(now time.Time, ttl time.Duration) bool {
	if len(r.Keys) < rotatingKeyCount {
		return true
	}
	cutoff := now.Add(-min(30*time.Second, ttl/4))
	return !r.Keys[1].Expires.After(cutoff)
}

// RefreshAt is the earliest time NeedsRefresh can become true for this set.
func (r RotatingSecrets) RefreshAt(ttl time.Duration) time.Time {
	if len(r.Keys) < rotatingKeyCount {
		return time.Time{}
	}
	return r.Keys[1].Expires.Add(min(30*time.Second, ttl/4))
}

// Lookup returns the key of exactly this secret ID, as
// RotatingKeyRing::get_service_secret looks up an ID in the service's set
// (ceph/ceph/blob/v20.2.4/src/auth/RotatingKeyRing.cc). Expiry is not
// checked here: key lookup and ticket validity are separate checks.
func (r RotatingSecrets) Lookup(id uint64) (Key, bool) {
	i, found := slices.BinarySearchFunc(r.Keys, id, func(k RotatingKey, id uint64) int {
		switch {
		case k.ID < id:
			return -1
		case k.ID > id:
			return 1
		}
		return 0
	})
	if !found {
		return Key{}, false
	}
	return r.Keys[i].Key, true
}
