// Package session coordinates authenticated msgr2.1 client connections.
package session

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"slices"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Transport and map encodings used by MON/MGR sessions. Do not substitute
// CEPH_FEATURES_ALL: that also promises unsupported OSD message codecs.
const Features uint64 = 1<<1 | 1<<2 | 1<<4 | 1<<5 | 1<<15 | 1<<23 | 1<<28 | 1<<42 | 1<<57 | 1<<59 | 1<<61
const RequiredFeatures uint64 = 1 << 59 // MSG_ADDR2
const sessionFlagLossy uint64 = 1       // CEPH_MSG_CONNECT_LOSSY

// Ceph MON admission requires these CRUSH-generation bits from every CLIENT,
// even clients that only issue commands. These bits satisfy MON admission
// only; they do not imply OSDMap subscriptions or CRUSH placement support
// and are never copied to OSD connections.
const monAdmissionFeatures uint64 = 1<<18 | 1<<25 | 1<<41 | 1<<48 | 1<<58

func featuresForRole(role uint8) uint64 {
	if role == 1 {
		return Features | monAdmissionFeatures
	}
	if role == 4 {
		// Implemented OSD object-locator and PGID64 encodings. In particular,
		// omit CRUSH, upmap, resend-on-split and new OSD reply encoding bits.
		return Features | 1<<8 | 1<<9
	}
	return Features
}

// ConnectionMode is the selected msgr2 wire mode. Only one mode is offered;
// authentication may not substitute a different mode.
type ConnectionMode uint32

const (
	CRCMode    ConnectionMode = 1
	SecureMode ConnectionMode = 2
)

type Authenticator interface {
	Initial() ([]byte, error)
	More([]byte) ([]byte, error)
	Done(uint64, []byte, bool) (cephx.Key, []byte, error)
}
type MonAuth struct{ Client *cephx.Client }

func (a MonAuth) Initial() ([]byte, error)      { return a.Client.Initial(), nil }
func (a MonAuth) More(p []byte) ([]byte, error) { return a.Client.Challenge(p) }
func (a MonAuth) Done(id uint64, p []byte, requireSecret bool) (cephx.Key, []byte, error) {
	return a.Client.Finish(id, p, requireSecret)
}

type ServiceAuth struct{ Authorizer *cephx.Authorizer }

func (a ServiceAuth) Initial() ([]byte, error)      { return a.Authorizer.Payload(nil) }
func (a ServiceAuth) More(p []byte) ([]byte, error) { return a.Authorizer.Payload(p) }
func (a ServiceAuth) Done(_ uint64, p []byte, requireSecret bool) (cephx.Key, []byte, error) {
	secret, err := a.Authorizer.Finish(p, requireSecret)
	return a.Authorizer.Key(), secret, err
}

func authPayloadError(err error) error {
	// Authenticators can also fail while generating local proofs. Mark only
	// peer encoding failures whose EOF could otherwise resemble network loss;
	// explicit rejection and local/cryptographic errors retain their classes.
	var rejection *cephx.AuthenticationError
	if !errors.As(err, &rejection) && (errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, wire.ErrVersion)) {
		return fmt.Errorf("%w: %w", msgr.ErrFrame, err)
	}
	return err
}

type recordingReader struct {
	io.Reader
	active bool
	data   bytes.Buffer
	err    error
}

func (r *recordingReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.active {
		remaining := (1 << 20) - r.data.Len()
		if len(p) > remaining {
			if remaining == 0 {
				r.err = wire.ErrLimit
				return 0, r.err
			}
			// io.ReadFull discards errors when n fills its request. Read only
			// bytes that fit, then fail the next read before consuming any
			// bytes omitted from the signed authentication transcript.
			p = p[:remaining]
		}
	}
	n, err := r.Reader.Read(p)
	if r.active && n > 0 {
		r.data.Write(p[:n])
	}
	return n, err
}

type recordingWriter struct {
	io.Writer
	active bool
	data   bytes.Buffer
}

func (w *recordingWriter) Write(p []byte) (int, error) {
	if w.active && w.data.Len()+len(p) > 1<<20 {
		return 0, wire.ErrLimit
	}
	n, err := w.Writer.Write(p)
	if w.active && n > 0 {
		w.data.Write(p[:n])
	}
	return n, err
}

type Transport struct {
	Conn     net.Conn
	Reader   *msgr.Reader
	Writer   *msgr.Writer
	GlobalID uint64
	Features uint64 // Intersection of local and peer Messenger feature sets.
}

// Handshake takes ownership of conn, closing it on failure. The context and
// deadline govern only setup; a successful connection has its deadline cleared.
func Handshake(ctx context.Context, conn net.Conn, target msgr.Address, role uint8, expectedID uint64, auth Authenticator, mode ConnectionMode, limit uint32, timeout time.Duration) (_ *Transport, err error) {
	if target.Endpoint.IsValid() {
		// Retain the target's wire family, including mapped IPv6. IPv6 scope
		// is carried by ScopeID rather than a Go address zone.
		target.Endpoint = netip.AddrPortFrom(target.Endpoint.Addr().WithZone(""), target.Endpoint.Port())
	}
	stage := "banner"
	cancelDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(cancelDone)
		conn.Close()
	})
	stopped := false
	defer func() {
		// A completed refusal must not become a retryable timeout while Close
		// finishes its cleanup. Network errors still map to a context that ends
		// during cleanup, including a socket deadline preceding its timer.
		preserveFailure := err != nil && ctx.Err() == nil && !setupTransportError(err, true)
		if !stopped && !stop() {
			// A repeated Conn.Close need not wait for another Close's cleanup.
			// Keep cancellation-owned cleanup within this handshake's lifetime.
			<-cancelDone
		}
		if err != nil {
			conn.Close()
		}
		if ctx.Err() != nil && !preserveFailure {
			err = ctx.Err()
			conn.Close()
		}
		if err != nil {
			err = fmt.Errorf("ceph messenger %s: %w", stage, err)
		}
	}()
	if mode != SecureMode && mode != CRCMode {
		return nil, errors.New("ceph messenger: invalid connection mode")
	}
	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err = conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	rx := &recordingReader{Reader: conn, active: true}
	tx := &recordingWriter{Writer: conn, active: true}
	r, w := msgr.NewReader(rx, limit), msgr.NewWriter(tx, limit)
	local := msgr.Banner{Supported: msgr.Revision1 | msgr.Compression, Required: msgr.Revision1}
	if err = msgr.WriteFull(tx, local.Encode()); err != nil {
		return nil, err
	}
	peer, err := msgr.ReadBanner(rx, local)
	if err != nil {
		return nil, err
	}
	write := func(tag msgr.Tag, p []byte) error { return w.Write(msgr.Frame{Tag: tag, Segments: [][]byte{p}}) }
	stage = "hello"
	hello := wire.Encoder{}
	hello.U8(8)
	target.Encode(&hello)
	if err = write(msgr.Hello, hello.Data); err != nil {
		return nil, err
	}
	f, err := r.Read()
	if err != nil {
		return nil, err
	}
	if f.Tag != msgr.Hello || len(f.Segments) != 1 {
		return nil, msgr.ErrFrame
	}
	hd := wire.NewDecoder(f.Segments[0])
	peerRole := hd.U8()
	myAddr := msgr.DecodeAddress(hd)
	if err = hd.Done(); err != nil {
		return nil, fmt.Errorf("%w: %w", msgr.ErrFrame, err)
	}
	if peerRole != role {
		return nil, errors.New("ceph messenger: unexpected daemon role")
	}
	stage = "authentication"
	payload, err := auth.Initial()
	if err != nil {
		return nil, err
	}
	authReq := wire.Encoder{}
	authReq.U32(2)
	authReq.U32(1)
	authReq.U32(uint32(mode))
	authReq.Bytes(payload)
	if err = write(msgr.AuthRequest, authReq.Data); err != nil {
		return nil, err
	}
	var sessionKey cephx.Key
	var secret []byte
	var globalID uint64
	authenticated := false
	for round := 0; round < 8; round++ {
		f, err = r.Read()
		if err != nil {
			return nil, err
		}
		if len(f.Segments) != 1 {
			return nil, msgr.ErrFrame
		}
		d := wire.NewDecoder(f.Segments[0])
		switch f.Tag {
		case msgr.AuthReplyMore:
			payload = d.Bytes()
			if err = d.Done(); err != nil {
				return nil, fmt.Errorf("%w: %w", msgr.ErrFrame, err)
			}
			reply, err := auth.More(payload)
			if err != nil {
				return nil, authPayloadError(err)
			}
			e := wire.Encoder{}
			e.Bytes(reply)
			if err = write(msgr.AuthRequestMore, e.Data); err != nil {
				return nil, err
			}
		case msgr.AuthBadMethod:
			method, code := d.U32(), int32(d.U32())
			for field := 0; field < 2; field++ {
				n := d.Count(4, 32) // allowed methods and allowed connection modes
				for i := 0; i < n; i++ {
					d.U32()
				}
			}
			if err := d.Done(); err != nil {
				return nil, fmt.Errorf("%w: %w", msgr.ErrFrame, err)
			}
			return nil, &cephx.AuthenticationError{Method: method, Code: code}
		case msgr.AuthDone:
			globalID = d.U64()
			selectedMode := d.U32()
			payload = d.Bytes()
			if err = d.Done(); err != nil {
				return nil, fmt.Errorf("%w: %w", msgr.ErrFrame, err)
			}
			if selectedMode != uint32(mode) {
				return nil, fmt.Errorf("ceph messenger: server selected connection mode %d, requested %d", selectedMode, mode)
			}
			sessionKey, secret, err = auth.Done(globalID, payload, mode == SecureMode)
			if err != nil {
				return nil, authPayloadError(err)
			}
			authenticated = true
		default:
			return nil, fmt.Errorf("%w: tag %d during authentication", msgr.ErrFrame, f.Tag)
		}
		if f.Tag == msgr.AuthDone {
			break
		}
	}
	if !authenticated || (mode == SecureMode && len(secret) < 40) {
		return nil, errors.New("ceph messenger: incomplete authentication")
	}
	stage = "signature"
	if mode == SecureMode {
		if err = r.EnableSecure(secret[:16], secret[16:28]); err != nil {
			return nil, err
		}
		if err = w.EnableSecure(secret[:16], secret[28:40]); err != nil {
			return nil, err
		}
	}
	// CRC keeps the plaintext frame codec, but still authenticates both
	// transcripts using the CephX session key before admitting a session.
	sig := sessionKey.Signature(rx.data.Bytes())
	expectedSig := sessionKey.Signature(tx.data.Bytes())
	rx.active, tx.active = false, false
	if err = write(msgr.AuthSignature, sig); err != nil {
		return nil, err
	}
	f, err = r.Read()
	if err != nil {
		return nil, err
	}
	if f.Tag != msgr.AuthSignature || len(f.Segments) != 1 || !hmac.Equal(expectedSig, f.Segments[0]) {
		return nil, msgr.ErrAuthentication
	}
	rx.data.Reset()
	tx.data.Reset()
	if peer.Supported&msgr.Compression != 0 {
		stage = "compression negotiation"
		// Negotiate no compression. This implements the negotiation feature,
		// while advertising no compression algorithms or compressed payloads.
		e := wire.Encoder{}
		e.U8(0)
		e.U32(0)
		if err = write(msgr.CompressionRequest, e.Data); err != nil {
			return nil, err
		}
		f, err = r.Read()
		if err != nil {
			return nil, err
		}
		if f.Tag != msgr.CompressionDone || len(f.Segments) != 1 {
			return nil, msgr.ErrFrame
		}
		d := wire.NewDecoder(f.Segments[0])
		enabled := d.Bool()
		d.U32()
		if err = d.Done(); err != nil {
			return nil, fmt.Errorf("%w: %w", msgr.ErrFrame, err)
		}
		if enabled {
			return nil, errors.New("ceph messenger: compression unsupported")
		}
	}
	var random [8]byte
	stage = "session identification"
	if _, err = rand.Read(random[:]); err != nil {
		return nil, err
	}
	myAddr.Type, myAddr.Nonce = 2, binary.LittleEndian.Uint32(random[:4])
	if myAddr.Endpoint.IsValid() {
		myAddr.Endpoint = netip.AddrPortFrom(myAddr.Endpoint.Addr(), 0)
	}
	ident := wire.Encoder{}
	msgr.EncodeAddresses(&ident, []msgr.Address{myAddr})
	target.Encode(&ident)
	ident.U64(globalID)
	ident.U64(binary.LittleEndian.Uint64(random[:]))
	supportedFeatures := featuresForRole(role)
	ident.U64(supportedFeatures)
	ident.U64(RequiredFeatures)
	ident.U64(sessionFlagLossy)
	ident.U64(0) // no client cookie: fresh lossy recovery only
	if err = write(msgr.ClientIdent, ident.Data); err != nil {
		return nil, err
	}
	f, err = r.Read()
	if err != nil {
		return nil, err
	}
	if len(f.Segments) != 1 {
		return nil, msgr.ErrFrame
	}
	if f.Tag == msgr.IdentMissingFeatures {
		d := wire.NewDecoder(f.Segments[0])
		missing := d.U64()
		if err := d.Done(); err != nil {
			return nil, fmt.Errorf("%w: %w", msgr.ErrFrame, err)
		}
		return nil, fmt.Errorf("%w: peer requires 0x%016x", msgr.ErrFeatures, missing)
	}
	if f.Tag != msgr.ServerIdent {
		return nil, fmt.Errorf("%w: expected server ident, got %d", msgr.ErrFrame, f.Tag)
	}
	id := wire.NewDecoder(f.Segments[0])
	serverAddresses := msgr.DecodeAddresses(id)
	serverID := id.U64()
	id.U64()
	supported, required := id.U64(), id.U64()
	flags := id.U64()
	id.U64()
	if err = id.Done(); err != nil {
		return nil, fmt.Errorf("%w: %w", msgr.ErrFrame, err)
	}
	if !slices.Contains(serverAddresses, target) {
		return nil, fmt.Errorf("%w: server identification does not include target address", msgr.ErrAuthentication)
	}
	if (role == 4 || expectedID != 0) && serverID != expectedID {
		return nil, errors.New("ceph messenger: unexpected daemon ID")
	}
	if RequiredFeatures&^supported != 0 || required&^supportedFeatures != 0 {
		return nil, msgr.ErrFeatures
	}
	// SERVER_IDENT is authoritative about the selected policy. Reliable
	// sessions require cookie-based resumption and message replay, which this
	// command-only client does not implement. Unknown flags are unsupported.
	if flags != sessionFlagLossy {
		return nil, fmt.Errorf("%w: unsupported session flags %#x", msgr.ErrFeatures, flags)
	}
	stopped = stop()
	if !stopped || ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err = conn.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	return &Transport{Conn: conn, Reader: r, Writer: w, GlobalID: globalID, Features: supported & supportedFeatures}, nil
}
