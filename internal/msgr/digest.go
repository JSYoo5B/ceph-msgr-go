package msgr

import (
	"bytes"
	"fmt"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

const MgrDigestMessage uint16 = 0x705

// Digest holds the two raw bufferlists in a MON's MMgrDigest. The returned
// slices own their memory; JSON interpretation belongs to the caller.
type Digest struct {
	MonStatus []byte
	Health    []byte
}

// DigestSubscribe adds continuous health/MON-status delivery while retaining
// map subscriptions. Start zero requests a fresh digest; it is not a cursor.
func DigestSubscribe() MessageData {
	return subscribe(0, 0, "mgrdigest", 0)
}

// DecodeDigest reads the front-only MMgrDigest payload in wire order: MON
// status followed by health detail. The logical frame limit includes the
// 41-byte Messenger header. Failure returns no partial digest.
func DecodeDigest(m MessageData, limit uint32) (digest Digest, err error) {
	defer func() {
		if err != nil {
			digest = Digest{}
			err = fmt.Errorf("%w: invalid MON digest: %w", ErrFrame, err)
		}
	}()
	if m.Type != MgrDigestMessage {
		return Digest{}, ErrFrame
	}
	if m.Version < 1 || m.CompatVersion > 1 || m.CompatVersion > m.Version {
		return Digest{}, wire.ErrVersion
	}
	if uint64(len(m.Front))+uint64(len(m.Middle))+uint64(len(m.Data))+41 > uint64(limit) {
		return Digest{}, wire.ErrLimit
	}
	if len(m.Middle) != 0 || len(m.Data) != 0 {
		return Digest{}, ErrFrame
	}
	d := wire.NewDecoderLimit(m.Front, limit)
	monStatus, health := d.Bytes(), d.Bytes()
	if err := d.Done(); err != nil {
		return Digest{}, err
	}
	return Digest{MonStatus: bytes.Clone(monStatus), Health: bytes.Clone(health)}, nil
}
