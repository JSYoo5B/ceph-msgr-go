package session

import (
	"context"
	"crypto/hmac"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Accepting msgr2.1 handshake of a daemon role (protocol stage 3).
//
// Accept is the server half of handshake, translated from the accepting
// path of ceph/ceph/blob/v20.2.4/src/msg/async/ProtocolV2.cc
// (_handle_peer_banner_payload, handle_hello, handle_auth_request,
// _handle_auth_request, finish_auth, handle_auth_signature,
// handle_compression_request, handle_client_ident, handle_reconnect and
// send_server_ident) for the one policy implemented: a stateless (lossy)
// server, as src/mgr/DaemonServer.cc gives OSD peers of a MGR
// (Policy::stateless_server(CEPH_FEATURE_SERVER_LUMINOUS)) and, by default,
// its clients (Policy::stateless_server(0)); each admitted peer type has its
// own required mask.
//
// Frame order on the accepting side: banner out, peer banner in, HELLO out
// (this entity type and the peer's socket address, type msgr2, nonce 0),
// peer HELLO in, AUTH_REQUEST in, AUTH_REPLY_MORE out and AUTH_REQUEST_MORE
// in (the CephX challenge), AUTH_DONE out (the ticket's global ID, the
// selected mode, the authorizer reply), the secure frame mode installed in
// the accepting direction (read nonce from bytes 28..40 of the connection
// secret, write nonce from 16..28), AUTH_SIGNATURE out over the received
// transcript and in over the sent one, COMPRESSION_REQUEST in and
// COMPRESSION_DONE out when the peer banner offers compression,
// CLIENT_IDENT in and SERVER_IDENT out. Every byte from the banners through
// AUTH_DONE is recorded, within 1 MiB per direction, for the signatures.
//
// Refusals: a method other than CephX or a peer offering no mode this
// acceptor allows gets AUTH_BAD_METHOD with -EOPNOTSUPP, a refused
// authorizer AUTH_BAD_METHOD with -EACCES (both with allowed method CephX
// and the one allowed mode), a CLIENT_IDENT lacking a required feature
// IDENT_MISSING_FEATURES with the missing mask, and SESSION_RECONNECT, which
// a lossy server never resumes, SESSION_RESET with full reset. Native code
// writes each of these and returns to reading; so does Accept, which then
// ends the handshake with that refusal when the peer closes, sends anything
// else or the deadline passes. A CLIENT_IDENT without addresses or whose
// target is not one of this acceptor's addresses ends the handshake without
// a frame, as native does. Compression is never selected: COMPRESSION_DONE
// always declines, which is also the stock daemon default.
//
// Stricter than native, for identity binding: the peer's HELLO entity type
// must be an admitted peer type, the authenticated ticket must name that
// entity type, an OSD's ticket ID must be the decimal of its CLIENT_IDENT
// numeric ID and a client's CLIENT_IDENT numeric ID must be its ticket's
// global ID, and a CLIENT_IDENT requiring a feature this acceptor does not
// support is refused without a frame (native ignores the peer's required
// mask). The caller's authorization runs last, before SERVER_IDENT; a denial
// ends the handshake without a frame. No transport is published before
// SERVER_IDENT is written.

// AcceptPolicy is the fixed policy of one accepting endpoint.
type AcceptPolicy struct {
	// Self is this endpoint's entity type (HELLO), ServerID its numeric
	// name (SERVER_IDENT gid) and Addresses the exact address vector a
	// CLIENT_IDENT target must belong to and SERVER_IDENT carries.
	Self      uint8
	ServerID  uint64
	Addresses []msgr.Address
	// Peers maps each admitted peer entity type to the SERVER_IDENT feature
	// masks of its policy; every required mask must include MSG_ADDR2 and
	// lie within its supported mask.
	Peers map[uint8]AcceptPeerFeatures
	Mode  ConnectionMode
	// Service and Keys verify the authorizer: the accepting service and its
	// exact-ID rotating secret lookup.
	Service uint32
	Keys    cephx.ServiceKeyLookup
	// HandshakeLimit bounds every frame before publication; MessageLimit
	// replaces it on the published transport.
	HandshakeLimit, MessageLimit uint32
	// GlobalSeq allocates the SERVER_IDENT global sequence once the peer is
	// admitted; it identifies the published connection.
	GlobalSeq func() uint64
	// Authorize is the caller's admission decision on the authenticated and
	// identified peer. It runs on the handshake goroutine without locks; a
	// non-nil error refuses the peer. ctx ends with the handshake.
	Authorize func(context.Context, AcceptedPeer) error
	// newVerifier replaces cephx.NewServiceVerifier in tests.
	newVerifier func(service uint32, keys cephx.ServiceKeyLookup, secure bool) serviceVerifier
}

type serviceVerifier interface {
	Verify([]byte) ([]byte, bool, error)
	Ticket() (cephx.PeerTicket, bool)
	SessionKey() cephx.Key
	ConnectionSecret() []byte
}

// AcceptPeerFeatures are the SERVER_IDENT supported and required masks sent
// to one admitted peer type.
type AcceptPeerFeatures struct{ Supported, Required uint64 }

// AcceptedPeer is what an accepted handshake established about its peer.
type AcceptedPeer struct {
	PeerType uint8
	// SeenAs is this endpoint's address as the peer's HELLO reported it.
	SeenAs msgr.Address
	Mode   ConnectionMode
	Ticket cephx.PeerTicket
	// Addresses, Target, PeerID, PeerGlobalSeq, PeerSupported,
	// PeerRequired, PeerFlags and PeerCookie are the CLIENT_IDENT fields.
	Addresses                   []msgr.Address
	Target                      msgr.Address
	PeerID, PeerGlobalSeq       uint64
	PeerSupported, PeerRequired uint64
	PeerFlags, PeerCookie       uint64
	// ServerGlobalSeq is the SERVER_IDENT global sequence; Features the
	// negotiated intersection of PeerSupported and the policy's Supported.
	ServerGlobalSeq uint64
	Features        uint64
}

// AcceptStage names the handshake step an accepted connection stopped in.
type AcceptStage string

const (
	AcceptBanner         AcceptStage = "banner"
	AcceptHello          AcceptStage = "hello"
	AcceptAuthentication AcceptStage = "authentication"
	AcceptSignature      AcceptStage = "signature"
	AcceptCompression    AcceptStage = "compression negotiation"
	AcceptIdentification AcceptStage = "session identification"
	AcceptAuthorization  AcceptStage = "authorization"
)

// AcceptError is the single terminal cause of a refused or failed accepted
// handshake. Refused reports a deliberate refusal of this acceptor (a
// refusal frame or an identity, feature or authorization check), as opposed
// to a transport failure, timeout or malformed peer input.
type AcceptError struct {
	Stage   AcceptStage
	Refused bool
	Err     error
}

func (e *AcceptError) Error() string {
	kind := "failed"
	if e.Refused {
		kind = "refused"
	}
	return fmt.Sprintf("ceph messenger accept %s %s: %v", e.Stage, kind, e.Err)
}
func (e *AcceptError) Unwrap() error { return e.Err }

// ErrPeerIdentity refuses a peer whose HELLO, ticket and CLIENT_IDENT do not
// name the same admitted entity.
var ErrPeerIdentity = errors.New("ceph messenger: peer identity does not match its authentication")

// ErrModeRefused refuses a peer offering no connection mode this endpoint
// allows, or another authentication method than CephX.
var ErrModeRefused = errors.New("ceph messenger: no acceptable authentication method or connection mode")

const (
	errnoEACCES     int32 = -13
	errnoEOPNOTSUPP int32 = -95
	maxAcceptFrames       = 8
)

// Accept takes ownership of conn, closing it on failure, and runs the
// accepting handshake under policy. ctx and timeout govern only the
// handshake; a published transport has its deadline cleared and its owner
// closes it. Failures are *AcceptError, returned with the peer fields
// established before the failure (for a refusal record; never a session).
func Accept(ctx context.Context, conn net.Conn, policy AcceptPolicy, timeout time.Duration) (_ *Transport, _ AcceptedPeer, err error) {
	stage := AcceptBanner
	refused := false
	ctx, cancelHandshake := context.WithCancel(ctx)
	defer cancelHandshake()
	cancelDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(cancelDone)
		conn.Close()
	})
	stopped := false
	defer func() {
		if !stopped && !stop() {
			<-cancelDone
		}
		if err != nil {
			conn.Close()
			if ctx.Err() != nil && !refused {
				err = ctx.Err()
			}
			var accept *AcceptError
			if !errors.As(err, &accept) {
				err = &AcceptError{Stage: stage, Refused: refused, Err: err}
			}
		}
	}()
	if policy.Mode != SecureMode && policy.Mode != CRCMode {
		return nil, AcceptedPeer{}, errors.New("ceph messenger: invalid connection mode")
	}
	for _, masks := range policy.Peers {
		if masks.Required&RequiredFeatures != RequiredFeatures || masks.Required&^masks.Supported != 0 {
			return nil, AcceptedPeer{}, errors.New("ceph messenger: invalid accept policy feature mask")
		}
	}
	if len(policy.Peers) == 0 || len(policy.Addresses) == 0 || policy.Keys == nil || policy.GlobalSeq == nil || policy.Authorize == nil {
		return nil, AcceptedPeer{}, errors.New("ceph messenger: incomplete accept policy")
	}
	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err = conn.SetDeadline(deadline); err != nil {
		return nil, AcceptedPeer{}, err
	}
	a := &acceptor{policy: policy, rx: &recordingReader{Reader: conn, active: true}}
	a.tx = &recordingWriter{Writer: conn, active: true}
	a.r, a.w = msgr.NewReader(a.rx, policy.HandshakeLimit), msgr.NewWriter(a.tx, policy.HandshakeLimit)
	a.peer.Mode = policy.Mode
	if tcp, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		endpoint := tcp.AddrPort()
		a.socketPeer = msgr.Address{Type: 2, Endpoint: netip.AddrPortFrom(endpoint.Addr().Unmap(), endpoint.Port())}
	}
	if err = a.run(ctx, &stage, &refused); err != nil {
		return nil, a.peer, err
	}
	stopped = stop()
	if !stopped || ctx.Err() != nil {
		return nil, AcceptedPeer{}, ctx.Err()
	}
	if err = conn.SetDeadline(time.Time{}); err != nil {
		return nil, AcceptedPeer{}, err
	}
	a.r.SetLimit(policy.MessageLimit)
	a.w.SetLimit(policy.MessageLimit)
	return &Transport{Conn: conn, Reader: a.r, Writer: a.w, GlobalID: a.peer.Ticket.GlobalID, Features: a.peer.Features}, a.peer, nil
}

type acceptor struct {
	policy     AcceptPolicy
	masks      AcceptPeerFeatures // the admitted peer type's
	rx         *recordingReader
	tx         *recordingWriter
	r          *msgr.Reader
	w          *msgr.Writer
	socketPeer msgr.Address
	peer       AcceptedPeer
	compress   bool
	verifier   serviceVerifier
}

func (a *acceptor) write(tag msgr.Tag, p []byte) error {
	return a.w.Write(msgr.Frame{Tag: tag, Segments: [][]byte{p}})
}

// read returns the next single-segment control frame.
func (a *acceptor) read() (msgr.Frame, error) {
	f, err := a.r.Read()
	if err != nil {
		return f, err
	}
	if len(f.Segments) != 1 {
		return f, msgr.ErrFrame
	}
	return f, nil
}

// awaitClose follows a refusal frame: native keeps reading after writing
// it, and the stock peer closes. Whatever ends the wait, the refusal is the
// connection's terminal cause.
func (a *acceptor) awaitClose(refusal error) error {
	a.r.Read()
	return refusal
}

func (a *acceptor) run(ctx context.Context, stage *AcceptStage, refused *bool) error {
	local := msgr.Banner{Supported: msgr.Revision1 | msgr.Compression, Required: msgr.Revision1}
	if err := msgr.WriteFull(a.tx, local.Encode()); err != nil {
		return err
	}
	banner, err := msgr.ReadBanner(a.rx, local)
	if err != nil {
		return err
	}
	a.compress = banner.Supported&msgr.Compression != 0
	*stage = AcceptHello
	if err := a.write(msgr.Hello, a.helloPayload()); err != nil {
		return err
	}
	f, err := a.read()
	if err != nil {
		return err
	}
	if f.Tag != msgr.Hello {
		return fmt.Errorf("%w: expected hello, got tag %d", msgr.ErrFrame, f.Tag)
	}
	d := wire.NewDecoder(f.Segments[0])
	a.peer.PeerType = d.U8()
	a.peer.SeenAs = msgr.DecodeAddress(d)
	if err := d.Done(); err != nil {
		return fmt.Errorf("%w: %w", msgr.ErrFrame, err)
	}
	masks, admitted := a.policy.Peers[a.peer.PeerType]
	if !admitted {
		*refused = true
		return fmt.Errorf("%w: peer entity type %d is not admitted", ErrPeerIdentity, a.peer.PeerType)
	}
	a.masks = masks
	*stage = AcceptAuthentication
	if err := a.authenticate(refused); err != nil {
		return err
	}
	*stage = AcceptSignature
	if err := a.verifyTranscripts(); err != nil {
		return err
	}
	if a.compress {
		*stage = AcceptCompression
		if err := a.declineCompression(); err != nil {
			return err
		}
	}
	*stage = AcceptIdentification
	if err := a.identify(refused); err != nil {
		return err
	}
	*stage = AcceptAuthorization
	if err := a.policy.Authorize(ctx, a.peer); err != nil {
		*refused = true
		return err
	}
	*stage = AcceptIdentification
	a.peer.ServerGlobalSeq = a.policy.GlobalSeq()
	return a.write(msgr.ServerIdent, a.serverIdentPayload())
}

// The payload encoders below are the acceptor's control frames, compared
// with natively assembled frames in accept_native_test.go.

// helloPayload is this entity type and the peer's socket address.
func (a *acceptor) helloPayload() []byte {
	e := wire.Encoder{}
	e.U8(a.policy.Self)
	a.socketPeer.Encode(&e)
	return e.Data
}

func (a *acceptor) serverIdentPayload() []byte {
	e := wire.Encoder{}
	msgr.EncodeAddresses(&e, a.policy.Addresses)
	e.U64(a.policy.ServerID)
	e.U64(a.peer.ServerGlobalSeq)
	e.U64(a.masks.Supported)
	e.U64(a.masks.Required)
	e.U64(sessionFlagLossy)
	e.U64(0) // a lossy server keeps no session cookie
	return e.Data
}

// badMethodPayload allows CephX and the one allowed mode.
func (a *acceptor) badMethodPayload(method uint32, code int32) []byte {
	e := wire.Encoder{}
	e.U32(method)
	e.U32(uint32(code))
	e.U32(1)
	e.U32(2)
	e.U32(1)
	e.U32(uint32(a.policy.Mode))
	return e.Data
}

func authReplyMorePayload(reply []byte) []byte {
	e := wire.Encoder{}
	e.Bytes(reply)
	return e.Data
}

func authDonePayload(globalID uint64, mode ConnectionMode, reply []byte) []byte {
	e := wire.Encoder{}
	e.U64(globalID)
	e.U32(uint32(mode))
	e.Bytes(reply)
	return e.Data
}

func identMissingFeaturesPayload(missing uint64) []byte {
	e := wire.Encoder{}
	e.U64(missing)
	return e.Data
}

// sessionResetPayload is a full reset.
func sessionResetPayload() []byte { return []byte{1} }

// compressionDonePayload declines: not compressed, method none.
func compressionDonePayload() []byte { return []byte{0, 0, 0, 0, 0} }

func (a *acceptor) badMethod(method uint32, code int32) error {
	return a.write(msgr.AuthBadMethod, a.badMethodPayload(method, code))
}

// authenticate runs the CephX authorizer exchange through AUTH_DONE.
func (a *acceptor) authenticate(refused *bool) error {
	newVerifier := a.policy.newVerifier
	if newVerifier == nil {
		newVerifier = func(service uint32, keys cephx.ServiceKeyLookup, secure bool) serviceVerifier {
			return cephx.NewServiceVerifier(service, keys, secure)
		}
	}
	a.verifier = newVerifier(a.policy.Service, a.policy.Keys, a.policy.Mode == SecureMode)
	for round := 0; round < 2; round++ {
		f, err := a.read()
		if err != nil {
			return err
		}
		d := wire.NewDecoder(f.Segments[0])
		var payload []byte
		switch {
		case round == 0 && f.Tag == msgr.AuthRequest:
			method := d.U32()
			n := d.Count(4, 32)
			modes := make([]uint32, 0, n)
			for i := 0; i < n; i++ {
				modes = append(modes, d.U32())
			}
			payload = d.Bytes()
			if err := d.Done(); err != nil {
				return fmt.Errorf("%w: %w", msgr.ErrFrame, err)
			}
			if method != 2 || !slices.Contains(modes, uint32(a.policy.Mode)) {
				*refused = true
				if err := a.badMethod(method, errnoEOPNOTSUPP); err != nil {
					return err
				}
				return a.awaitClose(fmt.Errorf("%w: method %d, modes %v", ErrModeRefused, method, modes))
			}
		case round == 1 && f.Tag == msgr.AuthRequestMore:
			payload = d.Bytes()
			if err := d.Done(); err != nil {
				return fmt.Errorf("%w: %w", msgr.ErrFrame, err)
			}
		default:
			return fmt.Errorf("%w: tag %d during authentication", msgr.ErrFrame, f.Tag)
		}
		reply, done, err := a.verifier.Verify(payload)
		if err != nil {
			var refusal *cephx.AuthorizerError
			if !errors.As(err, &refusal) {
				return err
			}
			*refused = true
			if err := a.badMethod(2, errnoEACCES); err != nil {
				return err
			}
			return a.awaitClose(err)
		}
		if !done {
			if err := a.write(msgr.AuthReplyMore, authReplyMorePayload(reply)); err != nil {
				return err
			}
			continue
		}
		ticket, _ := a.verifier.Ticket()
		if err := a.write(msgr.AuthDone, authDonePayload(ticket.GlobalID, a.policy.Mode, reply)); err != nil {
			return err
		}
		a.peer.Ticket = ticket
		return nil
	}
	return fmt.Errorf("%w: authentication did not complete", msgr.ErrFrame)
}

// verifyTranscripts installs the selected mode in the accepting direction
// and exchanges AUTH_SIGNATURE.
func (a *acceptor) verifyTranscripts() error {
	key := a.verifier.SessionKey()
	if a.policy.Mode == SecureMode {
		secret := a.verifier.ConnectionSecret()
		if len(secret) < 40 {
			return errors.New("ceph messenger: incomplete connection secret")
		}
		if err := a.r.EnableSecure(secret[:16], secret[28:40]); err != nil {
			return err
		}
		if err := a.w.EnableSecure(secret[:16], secret[16:28]); err != nil {
			return err
		}
	}
	sig := key.Signature(a.rx.data.Bytes())
	expected := key.Signature(a.tx.data.Bytes())
	a.rx.active, a.tx.active = false, false
	a.rx.data.Reset()
	a.tx.data.Reset()
	if err := a.write(msgr.AuthSignature, sig); err != nil {
		return err
	}
	f, err := a.read()
	if err != nil {
		return err
	}
	if f.Tag != msgr.AuthSignature || !hmac.Equal(expected, f.Segments[0]) {
		return msgr.ErrAuthentication
	}
	return nil
}

func (a *acceptor) declineCompression() error {
	f, err := a.read()
	if err != nil {
		return err
	}
	if f.Tag != msgr.CompressionRequest {
		return fmt.Errorf("%w: expected compression request, got tag %d", msgr.ErrFrame, f.Tag)
	}
	d := wire.NewDecoder(f.Segments[0])
	d.Bool()
	n := d.Count(4, 32)
	for i := 0; i < n; i++ {
		d.U32()
	}
	if err := d.Done(); err != nil {
		return fmt.Errorf("%w: %w", msgr.ErrFrame, err)
	}
	return a.write(msgr.CompressionDone, compressionDonePayload())
}

// identify reads CLIENT_IDENT, answering SESSION_RECONNECT with a full
// SESSION_RESET and a missing required feature with IDENT_MISSING_FEATURES,
// within maxAcceptFrames frames.
func (a *acceptor) identify(refused *bool) error {
	var refusal error
	for range maxAcceptFrames {
		f, err := a.read()
		if err != nil {
			if refusal != nil {
				return refusal
			}
			return err
		}
		d := wire.NewDecoder(f.Segments[0])
		switch f.Tag {
		case msgr.SessionReconnect:
			msgr.DecodeAddresses(d)
			for range 5 {
				d.U64() // client and server cookies, global, connect and message sequences
			}
			if err := d.Done(); err != nil {
				return fmt.Errorf("%w: %w", msgr.ErrFrame, err)
			}
			if err := a.write(msgr.SessionReset, sessionResetPayload()); err != nil {
				return err
			}
			continue
		case msgr.ClientIdent:
		default:
			if refusal != nil {
				return refusal
			}
			return fmt.Errorf("%w: tag %d during identification", msgr.ErrFrame, f.Tag)
		}
		p := &a.peer
		p.Addresses = msgr.DecodeAddresses(d)
		p.Target = msgr.DecodeAddress(d)
		p.PeerID, p.PeerGlobalSeq = d.U64(), d.U64()
		p.PeerSupported, p.PeerRequired = d.U64(), d.U64()
		p.PeerFlags, p.PeerCookie = d.U64(), d.U64()
		if err := d.Done(); err != nil {
			return fmt.Errorf("%w: %w", msgr.ErrFrame, err)
		}
		if len(p.Addresses) == 0 || p.Addresses[0] == (msgr.Address{}) {
			return fmt.Errorf("%w: client identification has no address", msgr.ErrFrame)
		}
		if !slices.Contains(a.policy.Addresses, p.Target) {
			*refused = true
			return fmt.Errorf("%w: client identification targets %v, not this endpoint", ErrPeerIdentity, p.Target.Endpoint)
		}
		if missing := a.masks.Required &^ p.PeerSupported; missing != 0 {
			*refused = true
			refusal = fmt.Errorf("%w: peer lacks 0x%016x", msgr.ErrFeatures, missing)
			if err := a.write(msgr.IdentMissingFeatures, identMissingFeaturesPayload(missing)); err != nil {
				return err
			}
			continue
		}
		if unsupported := p.PeerRequired &^ a.masks.Supported; unsupported != 0 {
			*refused = true
			return fmt.Errorf("%w: peer requires unsupported 0x%016x", msgr.ErrFeatures, unsupported)
		}
		if err := a.checkIdentity(); err != nil {
			*refused = true
			return err
		}
		p.Features = p.PeerSupported & a.masks.Supported
		return nil
	}
	if refusal != nil {
		return refusal
	}
	return fmt.Errorf("%w: identification did not complete", msgr.ErrFrame)
}

// checkIdentity binds the HELLO entity type, the authenticated ticket and
// the CLIENT_IDENT numeric name to one entity: an OSD's numeric name is its
// ID (the ticket's osd.<id>), a client's its global ID (a native client
// names itself client.<global ID> once authenticated).
func (a *acceptor) checkIdentity() error {
	p := &a.peer
	if p.Ticket.EntityType != uint32(p.PeerType) {
		return fmt.Errorf("%w: ticket entity type %d, HELLO %d", ErrPeerIdentity, p.Ticket.EntityType, p.PeerType)
	}
	switch p.PeerType {
	case uint8(cephx.ServiceOSD):
		id, err := strconv.ParseUint(p.Ticket.EntityID, 10, 32)
		if err != nil || strconv.FormatUint(id, 10) != p.Ticket.EntityID || id != p.PeerID {
			return fmt.Errorf("%w: ticket osd.%s, identification %d", ErrPeerIdentity, p.Ticket.EntityID, p.PeerID)
		}
	case entityClient:
		if p.PeerID != p.Ticket.GlobalID {
			return fmt.Errorf("%w: ticket global ID %d, identification %d", ErrPeerIdentity, p.Ticket.GlobalID, p.PeerID)
		}
	default:
		return fmt.Errorf("%w: entity type %d has no identity binding", ErrPeerIdentity, p.PeerType)
	}
	return nil
}

// entityClient is CEPH_ENTITY_TYPE_CLIENT.
const entityClient = 0x08

// MgrOSDPeerFeatures is what a MGR acceptor advertises to an OSD peer in
// SERVER_IDENT: the base Features, SERVER_LUMINOUS (21), which it requires,
// and the three bits a stock OSD requires of its MGR (src/ceph_osd.cc
// osd_required: UID 0, PGID64 9, OSDENC 13). OSDENC and PGID64 make the OSD
// encode its MGR messages in their current form, the only form a decoder of
// this repository reads (internal/msgr pgstats.go); UID carries no field of
// its own. Native MGR
// advertises CEPH_FEATURES_ALL; promising every daemon encoding is not done
// here.
const MgrOSDPeerFeatures uint64 = Features | 1<<0 | 1<<9 | 1<<13 | 1<<21

// MgrOSDPeerRequiredFeatures is the native required mask DaemonServer sends
// an OSD: CEPH_FEATURE_SERVER_LUMINOUS (bit 21 alone; the call site uses the
// FEATURE macro, not FEATUREMASK, so bit 57 is not added) plus ProtocolV2's
// msgr2_required MSG_ADDR2 (bit 59), 0x0800000000200000.
const MgrOSDPeerRequiredFeatures uint64 = 1<<21 | RequiredFeatures

// MgrClientPeerFeatures is what a MGR acceptor advertises to a client
// peer: the OSD peer's mask plus OSDREPLYMUX (12), which librados requires
// of every server it talks to (src/librados/RadosClient.cc,
// lossy_client(CEPH_FEATURE_OSDREPLYMUX)). The bit only selects how an
// OSD's MOSDOpReply carries per-operation data, which a MGR never sends;
// advertising it to a client is admission only. The MGR messages a client
// sends (MMgrOpen, MMgrReport, MMgrClose, the two command families) and the
// replies sent to it have no feature-dependent encoding.
const MgrClientPeerFeatures uint64 = MgrOSDPeerFeatures | 1<<12

// MgrClientPeerRequiredFeatures is the native required mask DaemonServer
// sends a client: its default Policy::stateless_server(0) requires nothing
// beyond ProtocolV2's MSG_ADDR2.
const MgrClientPeerRequiredFeatures uint64 = RequiredFeatures
