package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// MGR daemon-role acceptor (protocol stage 3, SPEC.md "Protocol stages").
//
// A MgrListener accepts msgr2.1 connections from stock OSDs and clients on
// a listener the consumer bound, as the MGR its MgrDaemon is: it verifies
// each peer's CephX MGR service authorizer against the daemon's rotating
// MGR secrets (exact secret ID), answers the native challenge,
// authenticates both transcripts, identifies itself as MGR(global ID) with
// the advertised address vector, and publishes an authenticated per-peer
// session. It decides nothing about the peer: the consumer's Authorize
// hook admits or refuses each authenticated peer (deny by default), and
// what the messages mean, every reply and every role decision stay with
// the consumer. The wire contract and its refusals are internal/session
// Accept (accept.go) and internal/cephx ServiceVerifier (service.go);
// native policy: ceph/ceph/blob/v20.2.4/src/mgr/DaemonServer.cc
// (stateless_server policy for OSDs requiring SERVER_LUMINOUS, the default
// stateless_server(0) for clients, ms_handle_fast_authentication caps
// handling) and ceph/ceph/blob/v20.2.4/src/mon/MonClient.cc
// handle_auth_request.
//
// OSD and CLIENT peers are admitted, in the one ConnectionMode selected,
// and no compression is negotiated. MON and MDS peers, which native MGR
// also accepts, are refused at HELLO; stock MONs do connect to the active
// MGR (their MgrClient), so their refusals appear in the listener's
// records. A peer session is lossy, like native: its loss ends it, and a
// stock peer reconnects on its own as a new session with a new Generation;
// nothing is resumed or replayed.
//
// Inbound messages are delivered in order with their raw segments; the
// MGR daemon messages are also decoded (mgr_peer_messages.go), and a
// malformed one ends the session as native decode failure does. Outbound,
// a peer is sent only what the consumer asks for: MMgrConfigure
// (MgrPeer.Configure), the MMgrClose acknowledgement (MgrPeerClose) and
// command replies (MgrPeerCommand), each through a reply handle bound to
// its connection.

// ErrMgrPeerOverflow ends a MgrPeer whose undelivered inbound messages
// exceeded its bounds. Accepted messages still drain before it.
var ErrMgrPeerOverflow = errors.New("ceph: MGR peer message buffer exceeded")

// ErrMgrPeerDenied is the cause recorded for a peer the Authorize hook
// refused, or every peer when no hook is set.
var ErrMgrPeerDenied = errors.New("ceph: MGR peer not authorized")

// MgrListenerOptions configures MgrDaemon.Listen.
type MgrListenerOptions struct {
	// Listener is a TCP listener the consumer bound; Listen owns it from
	// then on and Close closes it.
	Listener net.Listener
	// Addresses is the address vector this MGR advertises in its beacon.
	// SERVER_IDENT carries it exactly, so a stock OSD keeps the session
	// across maps that publish the same vector, and a peer's CLIENT_IDENT
	// target must be one of its entries. Every entry is a msgr2 address with
	// an endpoint. That the entries reach Listener is the consumer's.
	Addresses []ManagerAddress
	// ConnectionMode is the one mode accepted; a peer offering only the
	// other one is refused with AUTH_BAD_METHOD.
	ConnectionMode ConnectionMode
	// Authorize admits an authenticated, identified peer before
	// SERVER_IDENT is sent. A nil hook refuses every peer. It runs on that
	// peer's handshake goroutine with a context ending with the handshake
	// (timeout or Close); Close waits for it to return.
	Authorize func(context.Context, MgrPeerIdentity) error
	// MaxHandshakes bounds concurrent handshakes (default 16, at most
	// 1024); a connection beyond it is closed at once. HandshakeTimeout
	// bounds one handshake (default 10 s).
	MaxHandshakes    int
	HandshakeTimeout time.Duration
	// MaxPeers bounds published sessions not yet closed, accepted or not
	// (default 256, at most 65536). MaxPendingPeers bounds published
	// sessions waiting for Accept (default 16, at most MaxPeers). A peer
	// beyond either is refused before SERVER_IDENT, at authorization.
	MaxPeers        int
	MaxPendingPeers int
	// MaxBufferedMessages and MaxBufferedBytes bound one peer's undelivered
	// inbound messages (defaults 64 and the frame limit); see
	// ErrMgrPeerOverflow.
	MaxBufferedMessages int
	MaxBufferedBytes    int
	// MaxFrameSize bounds one frame of a published session (default the
	// daemon's); frames before publication are bounded to 64 KiB.
	MaxFrameSize      uint32
	KeepaliveInterval time.Duration
	KeepaliveTimeout  time.Duration
}

// MgrPeerCaps are the MGR caps of the peer's authenticated service ticket.
// AllowAll is the ticket's allow_all flag, kept apart from Text, the caps
// string; neither is interpreted here.
type MgrPeerCaps struct {
	AllowAll bool
	Text     string
}

// MgrPeerIdentity is what one accepted connection established. It holds no
// key or ticket material.
type MgrPeerIdentity struct {
	// Generation identifies the connection: the SERVER_IDENT global
	// sequence, increasing over the daemon's accepted connections. Zero
	// before SERVER_IDENT (in Authorize).
	Generation uint64
	// Entity is the authenticated principal (osd.<id>) and GlobalID its
	// global ID, both from the verified ticket; SecretID is the rotating MGR
	// secret ID the ticket was sealed with and TicketExpires its expiry.
	Entity        string
	GlobalID      uint64
	SecretID      uint64
	TicketExpires time.Time
	Caps          MgrPeerCaps
	// MgrGlobalID is this daemon's global ID, sent as the SERVER_IDENT
	// numeric name.
	MgrGlobalID uint64
	// The peer's CLIENT_IDENT: its address vector, the target it dialed,
	// numeric name (an OSD's ID, a client's global ID; either is bound to
	// the ticket), global sequence, feature masks, flags and cookie.
	Addresses     []ManagerAddress
	Target        ManagerAddress
	PeerID        uint64
	PeerGlobalSeq uint64
	PeerSupported uint64
	PeerRequired  uint64
	PeerFlags     uint64
	PeerCookie    uint64
	// Features is the negotiated set; Remote the socket peer.
	Features       uint64
	ConnectionMode ConnectionMode
	Remote         netip.AddrPort
}

// MgrInboundMessage is one message received from a peer, with owned copies
// of its segments. Exactly one of the decoded fields is set for a MGR
// daemon message of its type (mgr_peer_messages.go); every other type is
// delivered raw only.
type MgrInboundMessage struct {
	Sequence, Transaction                  uint64
	Type, Priority, Version, CompatVersion uint16
	Front, Middle, Data                    []byte

	Open    *MgrDaemonOpen
	Update  *MgrDaemonUpdate
	Report  *MgrPeerReport
	PGStats *MgrPGStats
	Close   *MgrPeerClose
	Command *MgrPeerCommand
}

// MgrListenerRefusal is one connection that did not become a session.
// Refused distinguishes a deliberate refusal from a failure (timeout,
// transport loss or malformed input). Reason is the cause's text, which
// carries no key material; Entity is set once a ticket verified.
type MgrListenerRefusal struct {
	// Sequence numbers refusals and failures from 1 in listener order.
	Sequence uint64
	Time     time.Time
	Remote   netip.AddrPort
	Stage    string
	Refused  bool
	Reason   string
	Entity   string
	Err      error
}

// MgrListenerState is a local view without I/O.
type MgrListenerState struct {
	Closed       bool
	Err          error
	Bound        net.Addr
	Advertised   []ManagerAddress
	Handshakes   int
	Peers        int
	Pending      int
	Accepted     uint64 // Published sessions.
	Refused      uint64 // Deliberate refusals, including MaxHandshakes overflow.
	Failed       uint64 // Handshakes ending otherwise.
	Recent       []MgrListenerRefusal
	LastAccepted MgrPeerIdentity
}

const maxRecentRefusals = 32

// entityTypeClient is CEPH_ENTITY_TYPE_CLIENT.
const entityTypeClient = 0x08

// MgrListener accepts OSD sessions for one MgrDaemon. Its methods are safe
// for concurrent use.
type MgrListener struct {
	daemon   *MgrDaemon
	options  MgrListenerOptions
	listener net.Listener
	policy   session.AcceptPolicy
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	mu         sync.Mutex
	changed    chan struct{}
	closed     bool
	failure    error
	handshakes int
	reserved   int // publication slots held by handshakes past authorization
	peers      map[*MgrPeer]struct{}
	pending    []*MgrPeer
	accepted   uint64
	refused    uint64
	failed     uint64
	recent     []MgrListenerRefusal
	last       MgrPeerIdentity
}

// Listen starts accepting OSD sessions on options.Listener as this daemon.
// The daemon's rotating MGR secrets verify each authorizer, so a peer can
// be admitted only once WaitRotatingKeys has returned a set holding its
// ticket's secret ID. Closing the daemon closes the listener and waits for
// its workers: a Listen racing the daemon's Close or failure either returns
// ErrClosed or a listener that the daemon has closed, never one whose
// worker outlives the daemon's Close.
func (d *MgrDaemon) Listen(options MgrListenerOptions) (*MgrListener, error) {
	if options.Listener == nil {
		return nil, errors.New("ceph: MGR listener requires a bound listener")
	}
	fail := func(err error) (*MgrListener, error) {
		options.Listener.Close()
		return nil, err
	}
	mode := options.ConnectionMode.sessionMode()
	if mode == 0 {
		return fail(errors.New("ceph: invalid connection mode"))
	}
	if len(options.Addresses) == 0 || len(options.Addresses) > 16 {
		return fail(errors.New("ceph: MGR listener needs 1 to 16 advertised addresses"))
	}
	for _, a := range options.Addresses {
		if a.Type != 2 || !a.Endpoint.IsValid() {
			return fail(errors.New("ceph: every advertised MGR address must be a msgr2 address with an endpoint"))
		}
	}
	options.Addresses = slices.Clone(options.Addresses)
	defaults := []struct {
		value      *int
		def, limit int
		name       string
	}{
		{&options.MaxHandshakes, 16, 1024, "MaxHandshakes"},
		{&options.MaxPeers, 256, 65536, "MaxPeers"},
		{&options.MaxBufferedMessages, 64, 4096, "MaxBufferedMessages"},
	}
	for _, field := range defaults {
		if *field.value == 0 {
			*field.value = field.def
		}
		if *field.value < 1 || *field.value > field.limit {
			return fail(fmt.Errorf("ceph: %s must be between 1 and %d", field.name, field.limit))
		}
	}
	if options.MaxPendingPeers == 0 {
		options.MaxPendingPeers = min(16, options.MaxPeers)
	}
	if options.MaxPendingPeers < 1 || options.MaxPendingPeers > options.MaxPeers {
		return fail(errors.New("ceph: MaxPendingPeers must be between 1 and MaxPeers"))
	}
	if options.HandshakeTimeout == 0 {
		options.HandshakeTimeout = 10 * time.Second
	}
	if options.KeepaliveInterval == 0 {
		options.KeepaliveInterval = d.options.KeepaliveInterval
	}
	if options.KeepaliveTimeout == 0 {
		options.KeepaliveTimeout = d.options.KeepaliveTimeout
	}
	if options.MaxFrameSize == 0 {
		options.MaxFrameSize = d.options.MaxFrameSize
	}
	if options.HandshakeTimeout < 0 || options.KeepaliveInterval <= 0 || options.KeepaliveTimeout <= options.KeepaliveInterval {
		return fail(errors.New("ceph: invalid MGR listener timeouts"))
	}
	if options.MaxFrameSize < 1024 || options.MaxFrameSize > 1<<30 {
		return fail(errors.New("ceph: frame limit must be between 1 KiB and 1 GiB"))
	}
	if options.MaxBufferedBytes == 0 {
		options.MaxBufferedBytes = int(options.MaxFrameSize)
	}
	if options.MaxBufferedBytes < 1024 || options.MaxBufferedBytes > 1<<30 {
		return fail(errors.New("ceph: MaxBufferedBytes must be between 1 KiB and 1 GiB"))
	}
	ctx, cancel := context.WithCancel(context.Background())
	l := &MgrListener{daemon: d, options: options, listener: options.Listener, ctx: ctx, cancel: cancel, changed: make(chan struct{}), peers: make(map[*MgrPeer]struct{})}
	gid := d.Identity().GlobalID
	l.policy = session.AcceptPolicy{
		Self: uint8(cephx.ServiceMgr), ServerID: gid, Addresses: wireAddresses(options.Addresses),
		Peers: map[uint8]uint64{
			uint8(cephx.ServiceOSD): session.MgrOSDPeerRequiredFeatures,
			entityTypeClient:        session.MgrClientPeerRequiredFeatures,
		},
		Mode: mode, Supported: session.MgrOSDPeerFeatures,
		Service: cephx.ServiceMgr, Keys: d.rotatingKey,
		HandshakeLimit: min(64<<10, options.MaxFrameSize), MessageLimit: options.MaxFrameSize,
		GlobalSeq: d.nextGlobalSeq,
	}
	if !d.publishListener(l) {
		cancel()
		return fail(ErrClosed)
	}
	if d.hooks != nil && d.hooks.afterListenerPublished != nil {
		d.hooks.afterListenerPublished(l)
	}
	return l, nil
}

func (l *MgrListener) signal() { close(l.changed); l.changed = make(chan struct{}) } // mu held

// errMgrPeerLimit refuses a peer beyond MaxPeers or MaxPendingPeers
// before SERVER_IDENT, so a refused peer never believes it was admitted.
var errMgrPeerLimit = errors.New("ceph: MGR peer or pending-session limit reached")

// authorize reserves a publication slot, then asks the consumer's hook. A
// reservation held by a handshake is released when it ends.
func (l *MgrListener) authorize(ctx context.Context, peer session.AcceptedPeer, reserved *bool) error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return ErrClosed
	}
	if len(l.peers)+l.reserved >= l.options.MaxPeers || len(l.pending)+l.reserved >= l.options.MaxPendingPeers {
		l.mu.Unlock()
		return errMgrPeerLimit
	}
	l.reserved++
	*reserved = true
	l.mu.Unlock()
	if l.options.Authorize == nil {
		return ErrMgrPeerDenied
	}
	identity := l.identity(peer, netip.AddrPort{})
	identity.Generation = 0
	if err := l.options.Authorize(ctx, identity); err != nil {
		return fmt.Errorf("%w: %w", ErrMgrPeerDenied, err)
	}
	return nil
}

func entityName(kind uint32, id string) string {
	names := map[uint32]string{cephx.ServiceMon: "mon", cephx.ServiceMDS: "mds", cephx.ServiceOSD: "osd", entityTypeClient: "client", cephx.ServiceMgr: "mgr", cephx.ServiceAuth: "auth"}
	name, ok := names[kind]
	if !ok {
		name = "type" + strconv.FormatUint(uint64(kind), 10)
	}
	return name + "." + id
}

func (l *MgrListener) identity(peer session.AcceptedPeer, remote netip.AddrPort) MgrPeerIdentity {
	t := peer.Ticket
	target := managerAddresses([]msgr.Address{peer.Target})
	return MgrPeerIdentity{
		Generation: peer.ServerGlobalSeq, Entity: entityName(t.EntityType, t.EntityID), GlobalID: t.GlobalID,
		SecretID: t.SecretID, TicketExpires: t.Expires, Caps: MgrPeerCaps{AllowAll: t.AllowAll, Text: t.Caps},
		MgrGlobalID: l.policy.ServerID, Addresses: managerAddresses(peer.Addresses), Target: target[0],
		PeerID: peer.PeerID, PeerGlobalSeq: peer.PeerGlobalSeq, PeerSupported: peer.PeerSupported,
		PeerRequired: peer.PeerRequired, PeerFlags: peer.PeerFlags, PeerCookie: peer.PeerCookie,
		Features: peer.Features, ConnectionMode: l.options.ConnectionMode, Remote: remote,
	}
}

func remoteOf(conn net.Conn) netip.AddrPort {
	if tcp, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		endpoint := tcp.AddrPort()
		return netip.AddrPortFrom(endpoint.Addr().Unmap(), endpoint.Port())
	}
	return netip.AddrPort{}
}

func (l *MgrListener) record(r MgrListenerRefusal) { // mu held
	if r.Refused {
		l.refused++
	} else {
		l.failed++
	}
	r.Sequence = l.refused + l.failed
	l.recent = append(l.recent, r)
	if len(l.recent) > maxRecentRefusals {
		l.recent = slices.Delete(l.recent, 0, len(l.recent)-maxRecentRefusals)
	}
	l.signal()
}

func (l *MgrListener) serve() {
	defer l.wg.Done()
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			l.mu.Lock()
			if !l.closed && l.failure == nil {
				l.failure = err
			}
			l.signal()
			l.mu.Unlock()
			l.cancel()
			return
		}
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			conn.Close()
			return
		}
		if l.handshakes >= l.options.MaxHandshakes {
			l.record(MgrListenerRefusal{Time: time.Now(), Remote: remoteOf(conn), Stage: "admission", Refused: true, Reason: "too many concurrent handshakes"})
			l.mu.Unlock()
			conn.Close()
			continue
		}
		l.handshakes++
		l.wg.Add(1)
		l.mu.Unlock()
		go l.handshake(conn)
	}
}

func (l *MgrListener) handshake(conn net.Conn) {
	defer l.wg.Done()
	remote := remoteOf(conn)
	policy := l.policy
	reserved := false
	policy.Authorize = func(ctx context.Context, peer session.AcceptedPeer) error { return l.authorize(ctx, peer, &reserved) }
	transport, peer, err := session.Accept(l.ctx, conn, policy, l.options.HandshakeTimeout)
	l.mu.Lock()
	l.handshakes--
	if reserved {
		l.reserved--
	}
	if err == nil && l.closed {
		transport.Conn.Close()
		err = &session.AcceptError{Stage: "admission", Err: ErrClosed}
	}
	if err != nil {
		refusal := MgrListenerRefusal{Time: time.Now(), Remote: remote, Reason: err.Error(), Err: err}
		var accept *session.AcceptError
		if errors.As(err, &accept) {
			refusal.Stage, refusal.Refused, refusal.Reason = string(accept.Stage), accept.Refused, accept.Err.Error()
		}
		if peer.Ticket.EntityID != "" {
			refusal.Entity = entityName(peer.Ticket.EntityType, peer.Ticket.EntityID)
		}
		l.record(refusal)
		l.mu.Unlock()
		return
	}
	p := &MgrPeer{listener: l, identity: l.identity(peer, remote), changed: make(chan struct{})}
	config := session.Config{WriteTimeout: l.options.HandshakeTimeout, KeepaliveInterval: l.options.KeepaliveInterval, KeepaliveTimeout: l.options.KeepaliveTimeout, Inbound: true}
	p.session = session.New(transport, config, p.enqueue, p.closed)
	l.peers[p] = struct{}{}
	l.pending = append(l.pending, p)
	l.accepted++
	l.last = p.identity
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		<-p.session.Done()
		p.session.Wait()
		l.mu.Lock()
		delete(l.peers, p)
		l.signal()
		l.mu.Unlock()
	}()
	p.session.Start()
	l.signal()
	l.mu.Unlock()
}

// Accept returns the next published peer session in publication order.
// ctx bounds only this wait; a canceled wait consumes nothing, also with a
// peer queued. Cancellation is decided under the listener lock immediately
// before the dequeue, so a peer published after ctx ends, even one
// published while this call waits for the lock, stays queued. After Close
// or a listener failure it returns the terminal cause once no published
// peer is waiting.
func (l *MgrListener) Accept(ctx context.Context) (*MgrPeer, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		l.mu.Lock()
		if err := ctx.Err(); err != nil {
			l.mu.Unlock()
			return nil, err
		}
		if len(l.pending) > 0 {
			p := l.pending[0]
			l.pending = l.pending[1:]
			l.signal()
			l.mu.Unlock()
			return p, nil
		}
		err, changed := l.terminalLocked(), l.changed
		l.mu.Unlock()
		if err != nil {
			return nil, err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (l *MgrListener) terminalLocked() error {
	if l.failure != nil {
		return l.failure
	}
	if l.closed {
		return ErrClosed
	}
	return nil
}

// WaitRefusal returns the oldest kept refusal or failure record whose
// Sequence exceeds after, waiting for one under ctx, which bounds only this
// wait. Only the latest 32 records are kept.
func (l *MgrListener) WaitRefusal(ctx context.Context, after uint64) (MgrListenerRefusal, error) {
	for {
		l.mu.Lock()
		for _, r := range l.recent {
			if r.Sequence > after {
				l.mu.Unlock()
				return r, nil
			}
		}
		err, changed := l.terminalLocked(), l.changed
		l.mu.Unlock()
		if err != nil {
			return MgrListenerRefusal{}, err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return MgrListenerRefusal{}, ctx.Err()
		}
	}
}

// State returns a local snapshot.
func (l *MgrListener) State() MgrListenerState {
	l.mu.Lock()
	defer l.mu.Unlock()
	return MgrListenerState{
		Closed: l.closed, Err: l.failure, Bound: l.listener.Addr(), Advertised: slices.Clone(l.options.Addresses),
		Handshakes: l.handshakes, Peers: len(l.peers), Pending: len(l.pending),
		Accepted: l.accepted, Refused: l.refused, Failed: l.failed, Recent: slices.Clone(l.recent), LastAccepted: l.last.clone(),
	}
}

// shutdown stops accepting and ends every handshake and session without
// waiting for them.
func (l *MgrListener) shutdown(cause error) {
	l.mu.Lock()
	if !l.closed {
		l.closed = true
		if l.failure == nil && cause != nil && !errors.Is(cause, ErrClosed) {
			l.failure = cause
		}
	}
	peers := make([]*MgrPeer, 0, len(l.peers))
	for p := range l.peers {
		peers = append(peers, p)
	}
	l.signal()
	l.mu.Unlock()
	l.cancel()
	l.listener.Close()
	for _, p := range peers {
		p.session.Fail(ErrClosed)
	}
}

// Close is idempotent: it closes the listener, every handshake and every
// published session, accepted or not, and waits for their workers and any
// Authorize call in progress.
func (l *MgrListener) Close() error {
	l.shutdown(ErrClosed)
	l.wg.Wait()
	l.daemon.removeListener(l)
	return nil
}

// MgrPeer is one published OSD session. Its methods are safe for
// concurrent use.
type MgrPeer struct {
	listener *MgrListener
	identity MgrPeerIdentity
	session  *session.Session

	mu      sync.Mutex
	changed chan struct{}
	queue   []MgrInboundMessage
	sizes   []int
	bytes   int
	err     error

	// schema is the connection's counter declarations; only the session's
	// reader (enqueue) uses it.
	schema msgr.MgrCounterSchema
}

func (p *MgrPeer) signal() { close(p.changed); p.changed = make(chan struct{}) } // mu held

func (id MgrPeerIdentity) clone() MgrPeerIdentity {
	id.Addresses = slices.Clone(id.Addresses)
	return id
}

// Identity returns the connection's identity; its slices are copies.
func (p *MgrPeer) Identity() MgrPeerIdentity { return p.identity.clone() }

// enqueue runs on the session's reader, one message at a time and in
// order. A decoded message counts twice toward MaxBufferedBytes: its raw
// segments and its decoded copy.
func (p *MgrPeer) enqueue(m msgr.MessageData) error {
	in := MgrInboundMessage{
		Sequence: m.Sequence, Transaction: m.Transaction, Type: m.Type, Priority: m.Priority,
		Version: m.Version, CompatVersion: m.CompatVersion,
		Front: slices.Clone(m.Front), Middle: slices.Clone(m.Middle), Data: slices.Clone(m.Data),
	}
	size := len(m.Front) + len(m.Middle) + len(m.Data) + 64
	decoded, err := p.decode(m, &in)
	if err != nil {
		return err
	}
	if decoded {
		size *= 2
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		// The session's close callback ran while its reader still held
		// this message: Next may already have returned the terminal cause,
		// so nothing is queued after it.
		return p.err
	}
	if len(p.queue) >= p.listener.options.MaxBufferedMessages || p.bytes+size > p.listener.options.MaxBufferedBytes {
		return fmt.Errorf("%w: %w", ErrMgrPeerOverflow, wire.ErrLimit)
	}
	p.queue = append(p.queue, in)
	p.sizes = append(p.sizes, size)
	p.bytes += size
	p.signal()
	return nil
}

func (p *MgrPeer) closed(err error) {
	p.mu.Lock()
	if p.err == nil {
		p.err = err
	}
	p.signal()
	p.mu.Unlock()
}

// Next returns the oldest undelivered inbound message. Accepted messages
// drain before the terminal cause, and none is accepted after the close
// callback recorded that cause. ctx bounds only this wait; a canceled wait
// consumes nothing, also with a message queued. As in Accept, cancellation
// is decided under the peer's queue lock immediately before the dequeue.
func (p *MgrPeer) Next(ctx context.Context) (MgrInboundMessage, error) {
	for {
		if err := ctx.Err(); err != nil {
			return MgrInboundMessage{}, err
		}
		p.mu.Lock()
		if err := ctx.Err(); err != nil {
			p.mu.Unlock()
			return MgrInboundMessage{}, err
		}
		if len(p.queue) > 0 {
			m := p.queue[0]
			p.queue[0] = MgrInboundMessage{}
			p.queue = p.queue[1:]
			p.bytes -= p.sizes[0]
			p.sizes = p.sizes[1:]
			p.mu.Unlock()
			return m, nil
		}
		err, changed := p.err, p.changed
		p.mu.Unlock()
		if err != nil {
			return MgrInboundMessage{}, err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return MgrInboundMessage{}, ctx.Err()
		}
	}
}

// Done is closed when the session has ended; Err is then its terminal
// cause (ErrClosed after Close).
func (p *MgrPeer) Done() <-chan struct{} { return p.session.Done() }
func (p *MgrPeer) Err() error            { return p.session.Err() }

// Close ends the session and waits for its workers. The stock peer sees a
// connection reset and reconnects as a new session.
func (p *MgrPeer) Close() error {
	p.session.Fail(ErrClosed)
	p.session.Wait()
	return nil
}
