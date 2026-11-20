package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/maps"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// MGR daemon-role MON control channel (protocol stage 3, SPEC.md "Protocol
// stages").
//
// A MgrDaemon is the MON side of a manager implementation above this
// library: it authenticates a named mgr.<id> principal to stock MON as the
// MGR entity type, keeps the MON-assigned global ID, subscribes to mgrmap,
// carries the consumer's beacons, delivers MgrMaps with this daemon's role
// in them, and holds the principal's rotating MGR service secrets. The
// consumer decides availability, what the beacon advertises and what work
// an active or standby daemon does; stock MON stays the authority that
// selects the active daemon, publishes the map and issues keys. Listen
// (mgr_listener.go) accepts stock OSD sessions with those secrets; the
// addresses a beacon advertises are caller input either way, and the
// bound listener address is reported by the listener, not here.
//
// Native reference (ceph/ceph/blob/v20.2.4/): src/mgr/MgrStandby.cc (want
// keys MON|OSD|MDS|MGR, mgrmap subscription, messenger name MGR(global ID),
// beacon content and activation by comparing MgrMap.active_gid with the
// MON-issued global ID), src/mon/MonClient.cc (in-session ticket renewal and
// rotating-secret refresh), src/mon/MgrMonitor.cc (beacon handling, the
// direct null map) and src/mon/MgrMap.h.

// ErrMgrDaemonIdentityChanged ends a MgrDaemon whose MON session would carry
// a different global ID than the one it was admitted with: a MON assigned a
// new ID on reconnection or in a renewal reply. The consumer starts a new
// lifecycle; work fenced by the old global ID stays fenced.
var ErrMgrDaemonIdentityChanged = errors.New("ceph: MGR daemon global ID changed")

// ErrMgrMapOverflow ends a MgrDaemon whose undelivered MgrMap events
// exceeded MaxBufferedMaps. Dropping an event could hide an active/standby
// transition, so the daemon fails instead. Accepted events still drain.
var ErrMgrMapOverflow = errors.New("ceph: MGR daemon map buffer exceeded")

// mgrDaemonTerminal marks a MON session cause that ends the daemon because
// every other MON session would meet it again: a MonMap of another cluster,
// or a committed MonMap or MgrMap this library cannot decode.
type mgrDaemonTerminal struct{ err error }

func (e *mgrDaemonTerminal) Error() string { return e.err.Error() }
func (e *mgrDaemonTerminal) Unwrap() error { return e.err }

// mgrDaemonFatal reports whether a MON session cause ends the daemon instead
// of being retried as MON loss: an explicit refusal, a changed global ID, a
// minimum release earlier than Tentacle, map overflow or a terminal cause.
// Transport loss, a missing auth reply and a reply that does not answer the
// oldest request retire only that session; the next one starts with no
// outstanding requests.
func mgrDaemonFatal(err error) bool {
	var rejection *AuthenticationError
	var terminal *mgrDaemonTerminal
	return errors.As(err, &rejection) || errors.As(err, &terminal) || errors.Is(err, ErrMgrDaemonIdentityChanged) ||
		errors.Is(err, maps.ErrRelease) || errors.Is(err, ErrMgrMapOverflow)
}

// errMgrAuthReplyTimeout retires a MON session that left an MAuth
// unanswered past its deadline (mgrAuthRequest.deadline).
var errMgrAuthReplyTimeout = errors.New("ceph: MON did not answer an MGR daemon auth request")

// MgrDaemonOptions configures DialMgrDaemon. Fields mean what they mean on
// Options unless stated otherwise.
type MgrDaemonOptions struct {
	Monitors []string
	// Identity is the programmatic mgr.<id> principal; Key is its secret.
	// Text keyrings and configuration files stay with the consumer.
	Identity       string
	Key            Key
	ConnectionMode ConnectionMode
	ExpectedFSID   string
	// Hostname is sent with subscriptions, as on Options.
	Hostname string
	// SubscribeDigest adds the mgrdigest subscription an active native MGR
	// holds; NextDigest then returns the latest MMgrDigest.
	SubscribeDigest bool
	// ServiceTicketTTL is the cluster's auth_service_ticket_ttl, which the
	// native rotating-secret refresh rule reads from its own configuration.
	// Zero selects the native default of one hour. It must be the value the
	// MONs run with, which a ceph.conf setting can make differ from the
	// central configuration database. A larger value delays each refresh up
	// to 30 s after the held current key expired, so the MON can rotate past
	// the held set and issue service tickets under a secret ID it lacks.
	ServiceTicketTTL time.Duration
	// MaxBufferedMaps bounds undelivered MgrMap events (default 64, at most
	// 4096); see ErrMgrMapOverflow.
	MaxBufferedMaps   int
	ConnectTimeout    time.Duration
	KeepaliveInterval time.Duration
	KeepaliveTimeout  time.Duration
	MaxFrameSize      uint32
	DialContext       func(context.Context, string, string) (net.Conn, error)
}

// MgrDaemonIdentity is the identity a MgrDaemon was admitted with. The global
// ID is the MON's assignment, separate from the configured name, and fixed
// for the daemon's lifetime.
type MgrDaemonIdentity struct {
	Entity   string // The principal, mgr.<id>.
	Name     string // The bare <id> a beacon and the MgrMap carry.
	GlobalID uint64 // MON-assigned; the daemon's messenger name is MGR(GlobalID).
	FSID     string // From the authenticated MonMap.
}

// ManagerAddress is one entity_addr_t of a MgrMap or beacon address vector:
// type (1 msgr1, 2 msgr2), nonce, endpoint and IPv6 flow/scope, in wire form.
// An invalid Endpoint is an address without a socket address.
type ManagerAddress struct {
	Type, Nonce       uint32
	Endpoint          netip.AddrPort
	FlowInfo, ScopeID uint32
}

// MgrCommandDescriptor is one MonCommand a beacon advertises. The strings
// are raw descriptors; the library neither parses nor executes them.
type MgrCommandDescriptor struct {
	Command, Help, Module, Permissions string
	Flags                              uint64
}

// MgrBeaconClient is a RADOS client a beacon reports for blocklisting.
type MgrBeaconClient struct {
	Name      string
	Addresses []ManagerAddress
}

// MgrBeacon is the consumer's beacon content. Native standby sends
// Available false with no Addresses; an active daemon sends the addresses
// stock OSDs then select from the published map. Commands are what native
// MGR includes in its first available beacon. Every field is carried
// unmodified except that Ceph's set and map encodings sort and deduplicate
// (see maps.EncodeMgrBeacon).
type MgrBeacon struct {
	Available bool
	Addresses []ManagerAddress // Advertised server addresses, in this order.
	Modules   []ManagerModule
	Commands  []MgrCommandDescriptor
	Metadata  map[string]string
	Services  map[string]string
	Features  uint64 // mgr_features
	Clients   []MgrBeaconClient
}

// MgrRole is this daemon's place in one MgrMap, decided by stock MON.
type MgrRole uint8

const (
	// MgrRoleAbsent: the map names this global ID neither active nor standby.
	MgrRoleAbsent MgrRole = iota
	// MgrRoleStandby: the global ID is a standby of the map.
	MgrRoleStandby
	// MgrRoleActive: the map's active_gid is this global ID. Available is
	// separate and follows a later available beacon.
	MgrRoleActive
)

// MgrMapStandby is one StandbyInfo of a MgrMap.
type MgrMapStandby struct {
	Name     string
	GlobalID uint64
	Modules  []ManagerModule
	Features uint64
}

// ManagerMap is a complete decoded modern MgrMap. ActiveAddresses is the
// exact active_addrs vector, every type and order included.
type ManagerMap struct {
	Epoch                uint32
	ActiveGlobalID       uint64
	ActiveName           string
	Available            bool
	ActiveAddresses      []ManagerAddress
	Standbys             []MgrMapStandby
	EnabledModules       []string
	Services             map[string]string
	AvailableModules     []ManagerModule
	ActiveChange         [2]uint32 // utime_t seconds and nanoseconds
	AlwaysOnModules      map[uint32][]string
	ActiveFeatures       uint64
	LastFailureOSDEpoch  uint32
	Clients              []MgrBeaconClient
	Flags                uint64
	ForceDisabledModules []string
}

// MgrMapEvent is one accepted MgrMap with this daemon's role in it.
//
// Ordinary maps are delivered in strictly increasing epoch order: a map
// whose epoch does not exceed the last delivered ordinary epoch, such as
// the current map a new MON session sends again, is not delivered and never
// rolls the daemon back. Null is the MON's direct null MgrMap
// (MgrMap::create_null_mgrmap, epoch 2^32-1) answering a beacon that claimed
// availability without this global ID being the pending active daemon. It
// is delivered with Role MgrRoleAbsent so the consumer stops active work;
// it does not raise the delivered epoch, so later ordinary maps are still
// delivered. MonitorGeneration identifies the MON session that delivered it.
type MgrMapEvent struct {
	Map               ManagerMap
	Null              bool
	Role              MgrRole
	MonitorGeneration uint64
}

// MgrRotatingKey identifies one rotating MGR service secret without its
// key material.
type MgrRotatingKey struct {
	ID      uint64
	Expires time.Time
}

// MgrRotatingKeys describes the held rotating MGR service secrets. Keys are
// in ascending ID order (native previous, current, next). Generation counts
// complete sets published locally (zero: none yet); Invalidations counts
// auth-epoch invalidations, which discard the held set. MonitorGeneration
// is the MON session whose reply the set came from.
type MgrRotatingKeys struct {
	Keys              []MgrRotatingKey
	MaxVersion        uint64
	Generation        uint64
	Invalidations     uint64
	MonitorGeneration uint64
	Received          time.Time
}

// MgrDaemonTicket is the local schedule and numeric secret ID of one held
// service ticket, without key or ticket material. Service is "auth",
// "mon", "osd", "mds" or "mgr".
type MgrDaemonTicket struct {
	Service             string
	SecretID            uint64
	Expires, RenewAfter time.Time
}

// MgrDaemonState is a local point-in-time view. It performs no I/O and holds
// no secrets.
type MgrDaemonState struct {
	Closed   bool
	Err      error // Terminal cause, once the daemon has failed or closed.
	Identity MgrDaemonIdentity
	Monitor  struct {
		Ready      bool
		Endpoint   string // Peer of the held MON connection.
		Generation uint64 // Local count of admitted MON sessions.
		MapEpoch   uint32
		AuthEpoch  uint32
	}
	// MgrMapEpoch is the last delivered ordinary epoch. StaleMgrMaps and
	// NullMgrMaps count maps not delivered as ordinary maps.
	MgrMapEpoch  uint32
	Role         MgrRole
	StaleMgrMaps uint64
	NullMgrMaps  uint64
	// Advertised is the address vector of the last beacon queued; a
	// listener's bound address is its own state (MgrListener.State).
	Advertised   []ManagerAddress
	Tickets      []MgrDaemonTicket
	Renewals     uint64 // Accepted in-session ticket renewals.
	RotatingKeys MgrRotatingKeys
	// StaleRotatingReplies counts verified rotating-secret replies that were
	// not published because their request predates the current MON
	// session, auth epoch or invalidation.
	StaleRotatingReplies uint64
}

// mgrAuthRequest is one MAuth outstanding on a MON session. The MON answers
// them in order without a transaction ID, so a reply is matched to the
// oldest entry. An entry still unanswered at its deadline (KeepaliveTimeout
// after it was queued, the silence bound of the session itself) retires the
// session: its correlation cannot be trusted, and the next session renews
// and refreshes with an empty queue.
type mgrAuthRequest struct {
	rotating      bool
	deadline      time.Time
	generation    uint64
	authEpoch     uint32
	invalidations uint64
}

// mgrDaemonHooks are private test seams, nil by default.
type mgrDaemonHooks struct {
	// beforeRotatingPublish runs after a rotating reply verified and before
	// its publication check, outside the daemon lock.
	beforeRotatingPublish func(generation uint64)
	// beforeSessionClosed runs in a MON session's close callback, after the
	// session made Done observable and before sessionClosed.
	beforeSessionClosed func(generation uint64)
	// afterListenerPublished runs in Listen once the daemon published the
	// listener, before Listen returns.
	afterListenerPublished func(*MgrListener)
}

type mgrDaemonHooksKey struct{}

// MgrDaemon is one MGR daemon identity's MON control channel. Its methods are
// safe for concurrent use. Close releases its connection and worker.
type MgrDaemon struct {
	options MgrDaemonOptions
	ctx     context.Context
	cancel  context.CancelFunc
	hooks   *mgrDaemonHooks
	wg      sync.WaitGroup
	wake    chan struct{}

	mu       sync.Mutex
	changed  chan struct{}
	closed   bool
	failure  error
	auth     *cephx.Client // Published snapshots are immutable.
	identity MgrDaemonIdentity
	fsid     [16]byte
	monMap   maps.Mon

	mon           *session.Session
	monReady      bool
	monEndpoint   string
	monGeneration uint64
	sessions      map[*session.Session]struct{}
	pending       []mgrAuthRequest
	lastRenewal   time.Time
	lastRotating  time.Time
	renewals      uint64

	ring           cephx.RotatingSecrets
	ringGeneration uint64
	invalidations  uint64
	ringSession    uint64
	ringReceived   time.Time
	staleRotating  uint64

	mgrEpoch   uint32
	role       MgrRole
	staleMaps  uint64
	nullMaps   uint64
	events     []MgrMapEvent
	digest     *ClusterDigest
	advertised []ManagerAddress

	listeners map[*MgrListener]struct{}
	globalSeq atomic.Uint64
}

// DialMgrDaemon authenticates options.Identity to a MON as the MGR entity
// type, requesting the native MgrStandby tickets (MON, OSD, MDS, MGR and
// AUTH), and returns once a MonMap with a Tentacle minimum release and the
// expected FSID has arrived on that session. It subscribes to monmap and
// mgrmap (and mgrdigest when selected) and requests the rotating MGR
// secrets without waiting for them (WaitRotatingKeys). ctx bounds setup only.
//
// Afterwards a worker renews tickets in session (MAuth with the handshake's
// server challenge) when any ticket passes its renewal time, refreshes the
// rotating secrets by the native rule, and on MON loss reconnects to a seed
// or MonMap MON presenting the same global ID and AUTH ticket. An MAuth the
// MON leaves unanswered for KeepaliveTimeout counts as MON loss. These causes
// end the daemon instead, whether they arrive while a session is admitted or
// while it is established, and State reports them: an explicit MON
// rejection (AuthenticationError), a different global ID
// (ErrMgrDaemonIdentityChanged), a MonMap whose minimum release is earlier
// than Tentacle or whose FSID differs from the expected or first
// authenticated one, a MonMap or MgrMap that does not decode, and
// ErrMgrMapOverflow. A cause of a session that was already replaced is
// ignored. No beacon is sent on the caller's behalf, also not after
// reconnection.
func DialMgrDaemon(ctx context.Context, options MgrDaemonOptions) (*MgrDaemon, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if options.ConnectionMode.sessionMode() == 0 {
		return nil, errors.New("ceph: invalid connection mode")
	}
	if len(options.Monitors) == 0 {
		return nil, errors.New("ceph: at least one monitor is required")
	}
	options.Monitors = append([]string(nil), options.Monitors...)
	for _, seed := range options.Monitors {
		if _, _, err := seedAddress(seed); err != nil {
			return nil, err
		}
	}
	if options.ConnectTimeout == 0 {
		options.ConnectTimeout = 10 * time.Second
	}
	if options.KeepaliveInterval == 0 {
		options.KeepaliveInterval = 15 * time.Second
	}
	if options.KeepaliveTimeout == 0 {
		options.KeepaliveTimeout = 45 * time.Second
	}
	if options.ConnectTimeout < 0 || options.KeepaliveInterval < 0 || options.KeepaliveTimeout <= options.KeepaliveInterval {
		return nil, errors.New("ceph: invalid MGR daemon timeouts")
	}
	if options.MaxFrameSize == 0 {
		options.MaxFrameSize = wire.DefaultLimit
	}
	if options.MaxFrameSize < 1024 || options.MaxFrameSize > 1<<30 {
		return nil, errors.New("ceph: frame limit must be between 1 KiB and 1 GiB")
	}
	if options.ServiceTicketTTL == 0 {
		options.ServiceTicketTTL = time.Hour
	}
	if options.ServiceTicketTTL < time.Second {
		return nil, errors.New("ceph: ServiceTicketTTL must be at least one second")
	}
	if options.MaxBufferedMaps == 0 {
		options.MaxBufferedMaps = 64
	}
	if options.MaxBufferedMaps < 1 || options.MaxBufferedMaps > 4096 {
		return nil, errors.New("ceph: MaxBufferedMaps must be between 1 and 4096")
	}
	if uint64(len(options.Hostname))+109 > uint64(options.MaxFrameSize) {
		return nil, fmt.Errorf("ceph: hostname requests exceed frame limit: %w", wire.ErrLimit)
	}
	if options.DialContext == nil {
		dialer := &net.Dialer{Timeout: options.ConnectTimeout}
		options.DialContext = dialer.DialContext
	}
	auth, err := cephx.NewMgrDaemon(options.Identity, options.Key.value)
	if err != nil {
		return nil, err
	}
	base, cancel := context.WithCancel(context.Background())
	d := &MgrDaemon{options: options, ctx: base, cancel: cancel, wake: make(chan struct{}, 1), changed: make(chan struct{}), auth: auth, sessions: make(map[*session.Session]struct{})}
	d.hooks, _ = ctx.Value(mgrDaemonHooksKey{}).(*mgrDaemonHooks)
	d.identity = MgrDaemonIdentity{Entity: auth.Name, Name: auth.ID()}
	if options.ExpectedFSID != "" {
		if d.fsid, err = parseFSID(options.ExpectedFSID); err != nil {
			cancel()
			return nil, err
		}
	}
	if err := d.connect(ctx); err != nil {
		d.Close()
		return nil, err
	}
	d.wg.Add(1)
	go d.supervise()
	return d, nil
}

func (d *MgrDaemon) signal() { close(d.changed); d.changed = make(chan struct{}) } // mu held

func (d *MgrDaemon) wakeWorker() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// fail records the first terminal cause and stops the daemon's work. It
// does not wait for workers; Close does.
func (d *MgrDaemon) fail(err error) {
	d.mu.Lock()
	all := d.failLocked(err)
	d.mu.Unlock()
	d.stop(all, err)
}

// failLocked records the first terminal cause and returns the sessions the
// caller passes to stop once it has released daemon.mu: failing a session
// runs its close callback, which takes the lock. daemon.mu held.
func (d *MgrDaemon) failLocked(err error) []*session.Session {
	if d.failure == nil {
		d.failure = err
	}
	all := make([]*session.Session, 0, len(d.sessions))
	for s := range d.sessions {
		all = append(all, s)
	}
	d.signal()
	return all
}

// stop ends the work of a daemon whose failure is recorded. A listener
// published before the failure is in d.listeners by then, and publishing a
// later one is refused (publishListener), so reading the set here, after
// the record, misses none.
func (d *MgrDaemon) stop(all []*session.Session, err error) {
	d.mu.Lock()
	listeners := d.listenersLocked()
	d.mu.Unlock()
	d.cancel()
	for _, s := range all {
		s.Fail(err)
	}
	for _, l := range listeners {
		l.shutdown(err)
	}
}

func (d *MgrDaemon) listenersLocked() []*MgrListener {
	out := make([]*MgrListener, 0, len(d.listeners))
	for l := range d.listeners {
		out = append(out, l)
	}
	return out
}

// publishListener registers l and starts its accept worker under daemon.mu,
// the lock Close and fail take to find the daemon's listeners: one that
// finds l also waits for that worker (MgrListener.Close), and a daemon
// already closed or failed refuses l.
func (d *MgrDaemon) publishListener(l *MgrListener) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.failure != nil {
		return false
	}
	if d.listeners == nil {
		d.listeners = make(map[*MgrListener]struct{})
	}
	d.listeners[l] = struct{}{}
	l.wg.Add(1)
	go l.serve()
	return true
}

func (d *MgrDaemon) removeListener(l *MgrListener) {
	d.mu.Lock()
	delete(d.listeners, l)
	d.mu.Unlock()
}

// nextGlobalSeq allocates the global sequence of one accepted connection,
// increasing over every listener of the daemon, as a native messenger's
// get_global_seq does.
func (d *MgrDaemon) nextGlobalSeq() uint64 { return d.globalSeq.Add(1) }

func (d *MgrDaemon) targets() []monTarget {
	d.mu.Lock()
	learned := append([]msgr.Address(nil), d.monMap.Addresses...)
	previous := d.monEndpoint
	d.mu.Unlock()
	targets, _ := monTargets(d.options.Monitors, learned, monTarget{}, false)
	// Try the MON that admitted the previous session first: its rotating
	// AUTH keys certainly verify the presented reclaim ticket.
	slices.SortStableFunc(targets, func(a, b monTarget) int {
		switch {
		case a.endpoint == previous && b.endpoint != previous:
			return -1
		case b.endpoint == previous && a.endpoint != previous:
			return 1
		}
		return 0
	})
	seen := make(map[monTarget]bool, len(targets))
	unique := targets[:0]
	for _, target := range targets {
		if !seen[target] {
			seen[target] = true
			unique = append(unique, target)
		}
	}
	return unique
}

// connect admits one MON session, trying every target once.
func (d *MgrDaemon) connect(ctx context.Context) error {
	var failures []error
	for _, target := range d.targets() {
		if err := d.ctx.Err(); err != nil {
			return ErrClosed
		}
		err := d.attempt(ctx, target)
		if err == nil {
			return nil
		}
		failures = append(failures, err)
		if mgrDaemonFatal(err) || ctx.Err() != nil {
			return errors.Join(failures...)
		}
	}
	if len(failures) == 0 {
		return errors.New("ceph: no monitor target")
	}
	return errors.Join(failures...)
}

func (d *MgrDaemon) attempt(ctx context.Context, target monTarget) error {
	linked, release := context.WithTimeout(ctx, d.options.ConnectTimeout)
	stop := context.AfterFunc(d.ctx, release)
	defer func() { stop(); release() }()
	d.mu.Lock()
	auth := copyAuth(d.auth)
	gid, invalidations := d.identity.GlobalID, d.invalidations
	d.mu.Unlock()
	conn, err := d.options.DialContext(linked, "tcp", target.endpoint)
	if err != nil {
		return err
	}
	address := target.address
	if !address.Endpoint.IsValid() {
		if tcp, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
			endpoint := tcp.AddrPort()
			address.Endpoint = netip.AddrPortFrom(endpoint.Addr().Unmap(), endpoint.Port())
		}
		if !address.Endpoint.IsValid() {
			conn.Close()
			return errors.New("ceph: dialer returned no peer IP address")
		}
	}
	transport, err := session.Handshake(linked, conn, address, 1, 0, session.MonAuth{Client: auth}, d.options.ConnectionMode.sessionMode(), d.options.MaxFrameSize, d.options.ConnectTimeout)
	if err != nil {
		return err
	}
	if gid != 0 && transport.GlobalID != gid {
		transport.Conn.Close()
		return ErrMgrDaemonIdentityChanged
	}
	admitted := make(chan struct{})
	var s *session.Session
	var generation uint64
	handler := &mgrDaemonSession{daemon: d, admitted: admitted}
	config := session.Config{WriteTimeout: d.options.ConnectTimeout, KeepaliveInterval: d.options.KeepaliveInterval, KeepaliveTimeout: d.options.KeepaliveTimeout}
	s = session.New(transport, config, func(m msgr.MessageData) error { return handler.handle(m) }, func(err error) {
		if d.hooks != nil && d.hooks.beforeSessionClosed != nil {
			d.hooks.beforeSessionClosed(handler.generation)
		}
		d.sessionClosed(s, err)
	})
	d.mu.Lock()
	if d.closed || d.failure != nil {
		d.mu.Unlock()
		transport.Conn.Close()
		return ErrClosed
	}
	d.monGeneration++
	generation = d.monGeneration
	handler.session, handler.generation = s, generation
	old := d.mon
	d.mon, d.monReady, d.monEndpoint, d.pending = s, false, target.endpoint, nil
	d.sessions[s] = struct{}{}
	s.Start()
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		<-s.Done()
		s.Wait()
		d.mu.Lock()
		delete(d.sessions, s)
		d.mu.Unlock()
	}()
	d.signal()
	d.mu.Unlock()
	if old != nil {
		old.Fail(errors.New("ceph: MGR daemon MON session replaced"))
	}
	subscribe := msgr.Subscribe(0, 0, d.options.Hostname)
	if d.options.SubscribeDigest {
		subscribe = msgr.DigestSubscribe(d.options.Hostname)
	}
	err = s.SendControl(linked, subscribe)
	if err == nil {
		select {
		case <-admitted:
			err = s.Err()
		case <-s.Done():
			err = s.Err()
		case <-linked.Done():
			s.Fail(linked.Err())
			err = s.Err()
		}
	}
	if err != nil {
		s.Fail(err)
		return err
	}
	d.mu.Lock()
	if d.mon != s || s.Err() != nil {
		d.mu.Unlock()
		if err := s.Err(); err != nil {
			// The session's own cause, which sessionClosed classified.
			return err
		}
		return errors.New("ceph: MGR daemon MON session lost during admission")
	}
	if d.identity.GlobalID == 0 {
		d.identity.GlobalID = transport.GlobalID
	}
	if d.invalidations != invalidations {
		// An auth epoch increased while this session authenticated; its
		// handshake tickets may predate the wipe. Renew them in session.
		auth = invalidateAuthTickets(auth)
	}
	d.auth = auth
	d.monReady = true
	handler.release()
	d.signal()
	d.mu.Unlock()
	d.wakeWorker()
	return nil
}

// sessionClosed runs once per MON session with its cause, after the session
// made Done observable and closed its transport.
func (d *MgrDaemon) sessionClosed(s *session.Session, cause error) {
	d.mu.Lock()
	all, fatal := d.settleLocked(s, cause)
	d.mu.Unlock()
	if fatal {
		d.stop(all, cause)
	}
	d.wakeWorker()
}

// settleLocked accounts for the end of MON session s with its cause. Only
// the current session's cause can end the daemon (mgrDaemonFatal); a
// replaced session's cause belongs to state the daemon already left. A
// session makes Done observable before its close callback runs, so the
// supervisor settles the current session itself before it reconnects:
// whichever of the two runs first decides under the same lock that d.mon
// still names s, and the replacing admission, which also takes that lock,
// can only follow. A fatal cause is recorded here, and the caller passes the
// returned sessions to stop after releasing daemon.mu. daemon.mu held.
func (d *MgrDaemon) settleLocked(s *session.Session, cause error) ([]*session.Session, bool) {
	if d.mon != s || cause == nil {
		return nil, false
	}
	d.monReady = false
	d.pending = nil
	d.signal()
	if d.failure != nil || d.closed || !mgrDaemonFatal(cause) {
		return nil, false
	}
	return d.failLocked(cause), true
}

// ownsLocked reports whether s is the current MON session and has not
// ended. Only such a session publishes maps, tickets or rotating secrets or
// queues an auth request: between a session's end and its settlement, d.mon
// still names it. daemon.mu held.
func (d *MgrDaemon) ownsLocked(s *session.Session) bool {
	return d.mon == s && s.Err() == nil
}

// supervise reconnects after MON loss and keeps tickets and rotating secrets
// current, on a one-second tick like native MonClient.
func (d *MgrDaemon) supervise() {
	defer d.wg.Done()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		d.mu.Lock()
		s := d.mon
		var all []*session.Session
		var fatal bool
		var cause error
		if s != nil {
			// The session's error is set before Done closes, so a session
			// that woke this loop is settled here even if its close
			// callback has not run yet.
			cause = s.Err()
			all, fatal = d.settleLocked(s, cause)
		}
		ready, failed := d.monReady, d.closed || d.failure != nil
		d.mu.Unlock()
		if fatal {
			d.stop(all, cause)
		}
		if failed || d.ctx.Err() != nil {
			return
		}
		if s == nil || !ready {
			err := d.connect(d.ctx)
			if err != nil && mgrDaemonFatal(err) {
				d.fail(err)
				return
			}
			if err != nil {
				select {
				case <-d.ctx.Done():
					return
				case <-time.After(500 * time.Millisecond):
				}
			}
			continue
		}
		d.maintain(s)
		select {
		case <-d.ctx.Done():
			return
		case <-d.wake:
		case <-tick.C:
		case <-s.Done():
		}
	}
}

// maintain retires a session whose oldest MAuth passed its deadline, and
// otherwise sends at most one ticket renewal and one rotating request while
// none of that kind is outstanding, each at most once a second.
func (d *MgrDaemon) maintain(s *session.Session) {
	now := time.Now()
	var messages []msgr.MessageData
	d.mu.Lock()
	if !d.ownsLocked(s) || !d.monReady {
		d.mu.Unlock()
		return
	}
	if len(d.pending) > 0 && !now.Before(d.pending[0].deadline) {
		d.mu.Unlock()
		s.Fail(errMgrAuthReplyTimeout)
		return
	}
	deadline := now.Add(d.options.KeepaliveTimeout)
	outstanding := func(rotating bool) bool {
		return slices.ContainsFunc(d.pending, func(r mgrAuthRequest) bool { return r.rotating == rotating })
	}
	if !outstanding(false) && now.Sub(d.lastRenewal) >= time.Second && ticketsNeedRenewal(d.auth, now) {
		if payload, err := d.auth.RenewalRequest(); err == nil {
			d.pending = append(d.pending, mgrAuthRequest{deadline: deadline, generation: d.monGeneration, authEpoch: d.monMap.AuthEpoch, invalidations: d.invalidations})
			d.lastRenewal = now
			messages = append(messages, msgr.AuthMessageRequest(payload, d.monMap.Epoch))
		}
	}
	if !outstanding(true) && now.Sub(d.lastRotating) >= time.Second && d.ring.NeedsRefresh(now, d.options.ServiceTicketTTL) {
		d.pending = append(d.pending, mgrAuthRequest{rotating: true, deadline: deadline, generation: d.monGeneration, authEpoch: d.monMap.AuthEpoch, invalidations: d.invalidations})
		d.lastRotating = now
		messages = append(messages, msgr.AuthMessageRequest(cephx.RotatingRequest(), d.monMap.Epoch))
	}
	d.mu.Unlock()
	for _, m := range messages {
		// Admission alone is bounded by the daemon: the recorded FIFO entry
		// must match a message that is actually written.
		if err := s.SendControl(d.ctx, m); err != nil {
			s.Fail(err)
			return
		}
	}
}

// ticketsNeedRenewal is CephXTicketManager's need: a requested ticket that is
// missing or past its renewal time.
func ticketsNeedRenewal(auth *cephx.Client, now time.Time) bool {
	for service := uint32(1); service <= cephx.ServiceAuth; service <<= 1 {
		if auth.Services()&service == 0 {
			continue
		}
		ticket, ok := auth.Tickets[service]
		if !ok || !now.Before(ticket.RenewAfter) {
			return true
		}
	}
	return false
}

// mgrDaemonSession handles one MON session's messages on its reader.
type mgrDaemonSession struct {
	daemon     *MgrDaemon
	session    *session.Session
	generation uint64
	admitted   chan struct{}
	ready      bool // A MonMap arrived; guarded by daemon.mu.
	released   bool // The session was published; guarded by daemon.mu.
	buffered   *maps.Mgr
}

// release delivers a MgrMap that arrived before admission. daemon.mu held.
func (h *mgrDaemonSession) release() {
	h.released = true
	if h.buffered != nil {
		m := *h.buffered
		h.buffered = nil
		h.daemon.acceptMgrMapLocked(m, h.generation)
	}
}

func (h *mgrDaemonSession) handle(m msgr.MessageData) error {
	d := h.daemon
	switch m.Type {
	case msgr.MonMapMessage:
		if err := validateMapMessageHeader(m); err != nil {
			return err
		}
		mon, err := maps.DecodeMon(m.Front)
		if err != nil {
			if errors.Is(err, maps.ErrRelease) {
				return err
			}
			return &mgrDaemonTerminal{fmt.Errorf("%w: invalid MON map: %w", msgr.ErrFrame, err)}
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		if !d.ownsLocked(h.session) {
			return nil
		}
		if d.fsid != [16]byte{} && d.fsid != mon.FSID {
			return &mgrDaemonTerminal{errors.New("ceph: monitor FSID mismatch")}
		}
		d.fsid = mon.FSID
		d.identity.FSID = fmt.Sprintf("%x-%x-%x-%x-%x", mon.FSID[:4], mon.FSID[4:6], mon.FSID[6:8], mon.FSID[8:10], mon.FSID[10:])
		if mon.Epoch >= d.monMap.Epoch {
			if d.monMap.Epoch != 0 && mon.AuthEpoch > d.monMap.AuthEpoch {
				// A service-key wipe committed a new auth epoch. Discard the
				// held set and service tickets; requests sent before this
				// point can no longer publish (mgrAuthRequest.authEpoch).
				d.ring = cephx.RotatingSecrets{}
				d.invalidations++
				d.auth = invalidateAuthTickets(d.auth)
				d.wakeWorker()
			}
			d.monMap = mon
		}
		if !h.ready {
			h.ready = true
			close(h.admitted)
		}
		d.signal()
	case msgr.MgrMapMessage:
		if err := validateMapMessageHeader(m); err != nil {
			return err
		}
		mgr, err := maps.DecodeMgr(m.Front)
		if err != nil {
			return &mgrDaemonTerminal{fmt.Errorf("%w: invalid MGR map: %w", msgr.ErrFrame, err)}
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		if !d.ownsLocked(h.session) {
			return nil
		}
		if !h.released {
			if h.buffered == nil || mgr.Epoch == maps.NullMgrEpoch || mgr.Epoch > h.buffered.Epoch {
				h.buffered = &mgr
			}
			return nil
		}
		return d.acceptMgrMapLocked(mgr, h.generation)
	case msgr.MgrDigestMessage:
		digest, err := msgr.DecodeDigest(m, d.options.MaxFrameSize)
		if err != nil {
			return err
		}
		d.mu.Lock()
		if d.ownsLocked(h.session) {
			d.digest = &ClusterDigest{MonStatus: digest.MonStatus, Health: digest.Health}
			d.signal()
		}
		d.mu.Unlock()
	case msgr.AuthReplyMessage:
		return h.authReply(m)
	}
	return nil
}

// acceptMgrMapLocked applies the delivery rules of MgrMapEvent. daemon.mu held.
func (d *MgrDaemon) acceptMgrMapLocked(m maps.Mgr, generation uint64) error {
	null := m.Epoch == maps.NullMgrEpoch
	if !null && m.Epoch <= d.mgrEpoch {
		d.staleMaps++
		return nil
	}
	if len(d.events) >= d.options.MaxBufferedMaps {
		if d.failure == nil {
			d.failure = ErrMgrMapOverflow
		}
		d.signal()
		go d.fail(ErrMgrMapOverflow)
		return ErrMgrMapOverflow
	}
	event := MgrMapEvent{Map: managerMap(m), Null: null, MonitorGeneration: generation}
	gid := d.identity.GlobalID
	if !null {
		d.mgrEpoch = m.Epoch
		switch {
		case gid != 0 && m.GlobalID == gid:
			event.Role = MgrRoleActive
		case slices.ContainsFunc(m.Standbys, func(s maps.Standby) bool { return s.GlobalID == gid }):
			event.Role = MgrRoleStandby
		}
	} else {
		d.nullMaps++
	}
	d.role = event.Role
	d.events = append(d.events, event)
	d.signal()
	return nil
}

func (h *mgrDaemonSession) authReply(m msgr.MessageData) error {
	d := h.daemon
	reply, err := msgr.DecodeAuthReply(m, d.options.MaxFrameSize)
	if err != nil {
		return err
	}
	d.mu.Lock()
	if !d.ownsLocked(h.session) {
		d.mu.Unlock()
		return nil
	}
	if len(d.pending) == 0 {
		d.mu.Unlock()
		return fmt.Errorf("%w: unsolicited auth reply", msgr.ErrFrame)
	}
	request := d.pending[0]
	d.pending = d.pending[1:]
	auth := copyAuth(d.auth)
	gid := d.identity.GlobalID
	d.mu.Unlock()
	if reply.Result != 0 {
		// An explicit refusal of a renewal or rotating request: the
		// principal's key or caps changed. The daemon cannot keep serving;
		// sessionClosed ends it with this cause.
		return &AuthenticationError{Method: 2, Code: reply.Result}
	}
	if reply.GlobalID != 0 && reply.GlobalID != gid {
		return ErrMgrDaemonIdentityChanged
	}
	kind, err := cephx.ResponseType(reply.ResultBL)
	if err != nil || (kind == 0x0400) != request.rotating {
		return fmt.Errorf("%w: auth reply does not answer the oldest request", msgr.ErrFrame)
	}
	if !request.rotating {
		if err := auth.FinishRenewal(reply.ResultBL); err != nil {
			return err
		}
		d.mu.Lock()
		if d.ownsLocked(h.session) && request.authEpoch == d.monMap.AuthEpoch && request.invalidations == d.invalidations {
			d.auth = auth
			d.renewals++
			d.signal()
		}
		d.mu.Unlock()
		return nil
	}
	ring, err := cephx.DecodeRotatingReply(reply.ResultBL, auth.Key)
	if err != nil {
		return err
	}
	if d.hooks != nil && d.hooks.beforeRotatingPublish != nil {
		d.hooks.beforeRotatingPublish(request.generation)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.ownsLocked(h.session) || request.generation != d.monGeneration || request.authEpoch != d.monMap.AuthEpoch || request.invalidations != d.invalidations {
		d.staleRotating++
		d.wakeWorker()
		return nil
	}
	d.ring = ring
	d.ringGeneration++
	d.ringSession = request.generation
	d.ringReceived = time.Now()
	d.signal()
	return nil
}

// rotatingKey returns the held rotating MGR secret of exactly id, the lookup
// an acceptor performs before the rest of authorizer verification.
func (d *MgrDaemon) rotatingKey(id uint64) (cephx.Key, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ring.Lookup(id)
}

// Identity returns the admitted identity.
func (d *MgrDaemon) Identity() MgrDaemonIdentity {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.identity
}

// State returns a local snapshot without I/O. Its slices are copies.
func (d *MgrDaemon) State() MgrDaemonState {
	d.mu.Lock()
	defer d.mu.Unlock()
	var state MgrDaemonState
	state.Closed, state.Err, state.Identity = d.closed, d.failure, d.identity
	state.Monitor.Ready = d.monReady && d.mon != nil && d.mon.Err() == nil
	if d.mon != nil {
		if remote := d.mon.RemoteAddr(); remote != nil {
			state.Monitor.Endpoint = remote.String()
		}
	}
	state.Monitor.Generation, state.Monitor.MapEpoch, state.Monitor.AuthEpoch = d.monGeneration, d.monMap.Epoch, d.monMap.AuthEpoch
	state.MgrMapEpoch, state.Role, state.StaleMgrMaps, state.NullMgrMaps = d.mgrEpoch, d.role, d.staleMaps, d.nullMaps
	state.Advertised = slices.Clone(d.advertised)
	names := map[uint32]string{cephx.ServiceAuth: "auth", cephx.ServiceMon: "mon", cephx.ServiceOSD: "osd", cephx.ServiceMDS: "mds", cephx.ServiceMgr: "mgr"}
	for _, service := range []uint32{cephx.ServiceAuth, cephx.ServiceMon, cephx.ServiceOSD, cephx.ServiceMDS, cephx.ServiceMgr} {
		if ticket, ok := d.auth.Tickets[service]; ok {
			state.Tickets = append(state.Tickets, MgrDaemonTicket{Service: names[service], SecretID: ticket.SecretID, Expires: ticket.Expires, RenewAfter: ticket.RenewAfter})
		}
	}
	state.Renewals = d.renewals
	state.RotatingKeys = d.rotatingKeysLocked()
	state.StaleRotatingReplies = d.staleRotating
	return state
}

func (d *MgrDaemon) rotatingKeysLocked() MgrRotatingKeys {
	keys := MgrRotatingKeys{MaxVersion: d.ring.MaxVersion, Generation: d.ringGeneration, Invalidations: d.invalidations, MonitorGeneration: d.ringSession, Received: d.ringReceived}
	for _, key := range d.ring.Keys {
		keys.Keys = append(keys.Keys, MgrRotatingKey{ID: key.ID, Expires: key.Expires})
	}
	return keys
}

// terminalLocked is the cause that ends waits. daemon.mu held.
func (d *MgrDaemon) terminalLocked() error {
	if d.failure != nil {
		return d.failure
	}
	if d.closed {
		return ErrClosed
	}
	return nil
}

// WaitRotatingKeys waits for a published rotating set whose Generation
// exceeds after. ctx bounds only this wait.
func (d *MgrDaemon) WaitRotatingKeys(ctx context.Context, after uint64) (MgrRotatingKeys, error) {
	for {
		d.mu.Lock()
		if d.ringGeneration > after && len(d.ring.Keys) > 0 {
			keys := d.rotatingKeysLocked()
			d.mu.Unlock()
			return keys, nil
		}
		err, changed := d.terminalLocked(), d.changed
		d.mu.Unlock()
		if err != nil {
			return MgrRotatingKeys{}, err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return MgrRotatingKeys{}, ctx.Err()
		}
	}
}

// NextMgrMap returns the oldest undelivered MgrMap event. Accepted events
// drain before the terminal cause. ctx bounds only this wait; a canceled
// wait consumes nothing.
func (d *MgrDaemon) NextMgrMap(ctx context.Context) (MgrMapEvent, error) {
	for {
		// A canceled wait consumes nothing, also with an event queued.
		if err := ctx.Err(); err != nil {
			return MgrMapEvent{}, err
		}
		d.mu.Lock()
		if len(d.events) > 0 {
			event := d.events[0]
			d.events = d.events[1:]
			d.mu.Unlock()
			return event, nil
		}
		err, changed := d.terminalLocked(), d.changed
		d.mu.Unlock()
		if err != nil {
			return MgrMapEvent{}, err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return MgrMapEvent{}, ctx.Err()
		}
	}
}

// NextDigest returns the latest unread MMgrDigest of a SubscribeDigest
// daemon; a newer digest replaces an unread one. ctx bounds only this wait;
// a canceled wait consumes nothing.
func (d *MgrDaemon) NextDigest(ctx context.Context) (ClusterDigest, error) {
	if !d.options.SubscribeDigest {
		return ClusterDigest{}, errors.New("ceph: MGR daemon has no digest subscription")
	}
	for {
		if err := ctx.Err(); err != nil {
			return ClusterDigest{}, err
		}
		d.mu.Lock()
		if d.digest != nil {
			digest := *d.digest
			d.digest = nil
			d.mu.Unlock()
			return digest, nil
		}
		err, changed := d.terminalLocked(), d.changed
		d.mu.Unlock()
		if err != nil {
			return ClusterDigest{}, err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ClusterDigest{}, ctx.Err()
		}
	}
}

// SendBeacon queues one MMgrBeacon with this daemon's global ID, name and
// FSID on the current MON session, waiting under ctx for one to be ready.
// ctx bounds only that wait and queue admission: once SendBeacon returns
// nil, canceling ctx no longer discards the beacon, which the session writer
// sends in order unless the session fails first. Success is not MON
// acceptance: MON never acknowledges a beacon, and an unchanged beacon need
// not change the map. A frame-size violation is ErrLimitExceeded.
func (d *MgrDaemon) SendBeacon(ctx context.Context, beacon MgrBeacon) error {
	content := maps.MgrBeacon{
		Available: beacon.Available, ServerAddresses: wireAddresses(beacon.Addresses), Features: beacon.Features,
		Metadata: beacon.Metadata, Services: beacon.Services,
	}
	for _, module := range beacon.Modules {
		content.Modules = append(content.Modules, wireModule(module))
	}
	for _, command := range beacon.Commands {
		content.Commands = append(content.Commands, maps.MgrCommand{Command: command.Command, Help: command.Help, Module: command.Module, Permissions: command.Permissions, Flags: command.Flags})
	}
	for _, client := range beacon.Clients {
		content.Clients = append(content.Clients, maps.MgrClient{Name: client.Name, Addresses: wireAddresses(client.Addresses)})
	}
	for {
		d.mu.Lock()
		err, changed := d.terminalLocked(), d.changed
		s, ready := d.mon, d.monReady
		content.GlobalID, content.Name, content.FSID = d.identity.GlobalID, d.identity.Name, d.fsid
		d.mu.Unlock()
		if err != nil {
			return err
		}
		if s != nil && ready && s.Err() == nil {
			message := maps.EncodeMgrBeacon(content)
			if uint64(len(message.Front))+41 > uint64(d.options.MaxFrameSize) {
				return fmt.Errorf("ceph: MGR beacon exceeds frame limit: %w", wire.ErrLimit)
			}
			// A beacon's caller commonly cancels its context on return; an
			// ordinary Send would let the writer discard it as abandoned.
			if err := s.SendControl(ctx, message); err != nil {
				return err
			}
			d.mu.Lock()
			d.advertised = slices.Clone(beacon.Addresses)
			d.mu.Unlock()
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Close is idempotent: it ends the MON session, every listener with its
// sessions (MgrListener.Close) and the worker, and waits for them. It sends
// no unregistering message; stock MON drops the daemon after its beacon
// grace. A daemon that fails closes its listeners' sockets too.
func (d *MgrDaemon) Close() error {
	d.mu.Lock()
	if !d.closed {
		d.closed = true
		d.signal()
	}
	all := make([]*session.Session, 0, len(d.sessions))
	for s := range d.sessions {
		all = append(all, s)
	}
	listeners := d.listenersLocked()
	d.mu.Unlock()
	d.cancel()
	for _, s := range all {
		s.Fail(ErrClosed)
	}
	for _, l := range listeners {
		l.Close()
	}
	d.wg.Wait()
	return nil
}

func wireAddresses(addresses []ManagerAddress) []msgr.Address {
	if addresses == nil {
		return nil
	}
	out := make([]msgr.Address, len(addresses))
	for i, a := range addresses {
		out[i] = msgr.Address{Type: a.Type, Nonce: a.Nonce, Endpoint: a.Endpoint, FlowInfo: a.FlowInfo, ScopeID: a.ScopeID}
	}
	return out
}

func managerAddresses(addresses []msgr.Address) []ManagerAddress {
	if len(addresses) == 0 {
		return nil
	}
	out := make([]ManagerAddress, len(addresses))
	for i, a := range addresses {
		out[i] = ManagerAddress{Type: a.Type, Nonce: a.Nonce, Endpoint: a.Endpoint, FlowInfo: a.FlowInfo, ScopeID: a.ScopeID}
	}
	return out
}

func wireModule(module ManagerModule) maps.ModuleInfo {
	info := maps.ModuleInfo{Name: module.Name, CanRun: module.CanRun, ErrorString: module.ErrorString}
	if len(module.Options) != 0 {
		info.Options = make(map[string]maps.ModuleOption, len(module.Options))
		for key, o := range module.Options {
			info.Options[key] = maps.ModuleOption{Name: o.Name, Type: o.Type, Level: o.Level, Flags: o.Flags, DefaultValue: o.DefaultValue, Min: o.Min, Max: o.Max, EnumAllowed: slices.Clone(o.EnumAllowed), Description: o.Description, LongDescription: o.LongDescription, Tags: slices.Clone(o.Tags), SeeAlso: slices.Clone(o.SeeAlso)}
		}
	}
	return info
}

func managerModules(modules []maps.ModuleInfo) []ManagerModule {
	var out []ManagerModule
	for _, module := range modules {
		reported := ManagerModule{Name: module.Name, CanRun: module.CanRun, ErrorString: module.ErrorString}
		if len(module.Options) != 0 {
			reported.Options = make(map[string]ManagerModuleOption, len(module.Options))
			for key, o := range module.Options {
				reported.Options[key] = ManagerModuleOption{Name: o.Name, Type: o.Type, Level: o.Level, Flags: o.Flags, DefaultValue: o.DefaultValue, Min: o.Min, Max: o.Max, EnumAllowed: slices.Clone(o.EnumAllowed), Description: o.Description, LongDescription: o.LongDescription, Tags: slices.Clone(o.Tags), SeeAlso: slices.Clone(o.SeeAlso)}
			}
		}
		out = append(out, reported)
	}
	return out
}

func managerMap(m maps.Mgr) ManagerMap {
	out := ManagerMap{
		Epoch: m.Epoch, ActiveGlobalID: m.GlobalID, ActiveName: m.Name, Available: m.Available,
		ActiveAddresses: managerAddresses(m.ActiveAddresses), EnabledModules: slices.Clone(m.EnabledModules),
		AvailableModules: managerModules(m.AvailableModules), ActiveChange: m.ActiveChange,
		ActiveFeatures: m.ActiveFeatures, LastFailureOSDEpoch: m.LastFailureOSDEpoch, Flags: m.Flags,
		ForceDisabledModules: slices.Clone(m.ForceDisabledModules),
	}
	if len(m.Services) != 0 {
		out.Services = make(map[string]string, len(m.Services))
		for k, v := range m.Services {
			out.Services[k] = v
		}
	}
	if len(m.AlwaysOnModules) != 0 {
		out.AlwaysOnModules = make(map[uint32][]string, len(m.AlwaysOnModules))
		for release, modules := range m.AlwaysOnModules {
			out.AlwaysOnModules[release] = slices.Clone(modules)
		}
	}
	for _, s := range m.Standbys {
		out.Standbys = append(out.Standbys, MgrMapStandby{Name: s.Name, GlobalID: s.GlobalID, Modules: managerModules(s.Modules), Features: s.Features})
	}
	for _, c := range m.Clients {
		out.Clients = append(out.Clients, MgrBeaconClient{Name: c.Name, Addresses: managerAddresses(c.Addresses)})
	}
	return out
}
