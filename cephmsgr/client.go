package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

type Client struct {
	options     Options
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	closed      bool
	changed     chan struct{}
	auth        *cephx.Client // Published snapshots are immutable.
	fsid        [16]byte
	monMap      maps.Mon
	mgrMap      maps.Mgr
	mon, mgr    *session.Session
	monReady    bool
	authErr     error // Explicit MON reauthentication rejection, until recovery.
	sessions    map[*session.Session]struct{}
	mgrGate     chan struct{}
	wake        chan struct{}
	calls       chan struct{}
	wg          sync.WaitGroup
	logWatch    *LogStream
	configWatch *ConfigStream
	digestWatch *DigestStream
}

// Dial establishes and authenticates a MON connection and verifies a MonMap
// whose configured minimum monitor release is Tentacle or later. ctx governs
// setup only; Close owns the returned client's lifetime.
func Dial(ctx context.Context, options Options) (*Client, error) {
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
	if options.ConnectTimeout < 0 {
		return nil, errors.New("ceph: negative connect timeout")
	}
	if options.KeepaliveInterval == 0 {
		options.KeepaliveInterval = 15 * time.Second
	}
	if options.KeepaliveTimeout == 0 {
		options.KeepaliveTimeout = 45 * time.Second
	}
	if options.KeepaliveInterval < 0 || options.KeepaliveTimeout <= options.KeepaliveInterval {
		return nil, errors.New("ceph: keepalive timeout must exceed a positive interval")
	}
	if options.MaxFrameSize == 0 {
		options.MaxFrameSize = wire.DefaultLimit
	}
	if options.MaxFrameSize < 1024 || options.MaxFrameSize > 1<<30 {
		return nil, errors.New("ceph: frame limit must be between 1 KiB and 1 GiB")
	}
	if options.MaxInFlight == 0 {
		options.MaxInFlight = 64
	}
	if options.MaxInFlight < 1 || options.MaxInFlight > 1024 {
		return nil, errors.New("ceph: MaxInFlight must be between 1 and 1024")
	}
	if options.DialContext == nil {
		dialer := &net.Dialer{Timeout: options.ConnectTimeout}
		options.DialContext = dialer.DialContext
	}
	auth, err := cephx.NewClient(options.Identity, options.Key.value)
	if err != nil {
		return nil, err
	}
	// Account for the largest subscription and the identity-bearing MGetConfig
	// before copying hostname into a frame. Both include the Messenger header.
	requestBytes := max(uint64(109), 57+uint64(len(strings.TrimPrefix(auth.Name, "client."))))
	if uint64(len(options.Hostname))+requestBytes > uint64(options.MaxFrameSize) {
		return nil, fmt.Errorf("ceph: hostname requests exceed frame limit: %w", wire.ErrLimit)
	}
	base, cancel := context.WithCancel(context.Background())
	c := &Client{options: options, ctx: base, cancel: cancel, changed: make(chan struct{}), auth: auth, sessions: make(map[*session.Session]struct{}), mgrGate: make(chan struct{}, 1), wake: make(chan struct{}, 1), calls: make(chan struct{}, options.MaxInFlight)}
	if options.ExpectedFSID != "" {
		c.fsid, err = parseFSID(options.ExpectedFSID)
		if err != nil {
			cancel()
			return nil, err
		}
	}
	if err = c.bootstrap(ctx); err != nil {
		c.Close()
		return nil, err
	}
	c.wg.Add(1)
	go c.supervise()
	return c, nil
}

func seedAddress(seed string) (string, msgr.Address, error) {
	var a msgr.Address
	a.Type = 2
	if strings.HasPrefix(seed, "v1:") {
		return "", a, errors.New("ceph: msgr1 monitor address unsupported")
	}
	seed = strings.TrimPrefix(seed, "v2:")
	if i := strings.LastIndex(seed, "/"); i >= 0 {
		n, err := strconv.ParseUint(seed[i+1:], 10, 32)
		if err != nil {
			return "", a, errors.New("ceph: invalid address nonce")
		}
		a.Nonce = uint32(n)
		seed = seed[:i]
	}
	host, port, err := net.SplitHostPort(seed)
	if err != nil || host == "" {
		return "", a, fmt.Errorf("ceph: monitor must be host:port: %q", seed)
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n == 0 {
		return "", a, errors.New("ceph: invalid monitor port")
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if ip.Zone() != "" {
			return "", a, errors.New("ceph: IPv6 monitor seed zones are unsupported")
		}
		a.Endpoint = netip.AddrPortFrom(ip, uint16(n))
	}
	return seed, a, nil
}
func dialAddress(a msgr.Address) string {
	ip := a.Endpoint.Addr()
	if a.ScopeID != 0 && ip.Is6() {
		ip = ip.WithZone(strconv.FormatUint(uint64(a.ScopeID), 10))
	}
	return netip.AddrPortFrom(ip, a.Endpoint.Port()).String()
}
func (c *Client) signal() { close(c.changed); c.changed = make(chan struct{}) } // mu held
func (c *Client) wakeMonitor() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}
func (c *Client) snapshotAuth() *cephx.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	return copyAuth(c.auth)
}
func copyAuth(auth *cephx.Client) *cephx.Client {
	copyAuth := *auth
	copyAuth.Tickets = make(map[uint32]cephx.Ticket, len(auth.Tickets))
	for id, t := range auth.Tickets {
		copyAuth.Tickets[id] = t
	}
	return &copyAuth
}
func invalidateAuthTickets(auth *cephx.Client) *cephx.Client {
	invalidated := copyAuth(auth)
	for id, ticket := range invalidated.Tickets {
		// Tentacle keeps AUTH rotating keys during a service-key wipe. Retain
		// the old proof and session key for secure global-ID reclamation.
		ticket.Expires, ticket.RenewAfter = time.Time{}, time.Time{}
		invalidated.Tickets[id] = ticket
	}
	return invalidated
}
func (c *Client) linkedContext(ctx context.Context) (context.Context, func()) {
	linked, cancel := context.WithTimeout(ctx, c.options.ConnectTimeout)
	stop := context.AfterFunc(c.ctx, cancel)
	return linked, func() { stop(); cancel() }
}
func (c *Client) open(ctx context.Context, endpoint string, address msgr.Address, role uint8, id uint64, auth session.Authenticator) (*session.Transport, error) {
	conn, err := c.options.DialContext(ctx, "tcp", endpoint)
	if err != nil {
		return nil, err
	}
	if !address.Endpoint.IsValid() {
		remote := conn.RemoteAddr()
		if tcp, ok := remote.(*net.TCPAddr); ok {
			// TCPAddr uses AF_INET for IP.To4(), including a 16-byte ParseIP
			// value. Only inferred socket addresses follow that convention;
			// literal seeds and learned wire addresses retain their family.
			endpoint := tcp.AddrPort()
			address.Endpoint = netip.AddrPortFrom(endpoint.Addr().Unmap(), endpoint.Port())
		} else if remote != nil {
			address.Endpoint, _ = netip.ParseAddrPort(remote.String())
		}
		if !address.Endpoint.IsValid() {
			conn.Close()
			return nil, errors.New("ceph: dialer returned no peer IP address")
		}
	}
	return session.Handshake(ctx, conn, address, role, id, auth, c.options.ConnectionMode.sessionMode(), c.options.MaxFrameSize, c.options.ConnectTimeout)
}

func (c *Client) attach(s *session.Session) bool {
	// Called with mu held so no worker is added after Close begins waiting.
	if c.closed {
		return false
	}
	c.sessions[s] = struct{}{}
	s.Start()
	c.wg.Add(1)
	go func() {
		<-s.Done()
		s.Wait()
		c.mu.Lock()
		delete(c.sessions, s)
		c.mu.Unlock()
		c.wg.Done()
	}()
	return true
}

func (c *Client) sessionConfig() session.Config {
	return session.Config{WriteTimeout: c.options.ConnectTimeout, KeepaliveInterval: c.options.KeepaliveInterval, KeepaliveTimeout: c.options.KeepaliveTimeout}
}

func (c *Client) connectMonitor(ctx context.Context) error {
	type target struct {
		endpoint string
		address  msgr.Address
	}
	c.mu.Lock()
	seeds := append([]string(nil), c.options.Monitors...)
	learned := append([]msgr.Address(nil), c.monMap.Addresses...)
	old, wasReady := c.mon, c.monReady
	c.mu.Unlock()
	var failures []error
	targets := make([]target, 0, len(seeds)+len(learned))
	for _, seed := range seeds {
		endpoint, address, err := seedAddress(seed)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		targets = append(targets, target{endpoint: endpoint, address: address})
	}
	for _, address := range learned {
		// A dial string cannot retain all authenticated address fields.
		// SERVER_IDENT compares the full target, including IPv6 scope/flow.
		targets = append(targets, target{endpoint: dialAddress(address), address: address})
	}
	seen := make(map[target]bool)
	for _, candidateTarget := range targets {
		if seen[candidateTarget] {
			continue
		}
		seen[candidateTarget] = true
		linked, release := c.linkedContext(ctx)
		c.mu.Lock()
		candidate := copyAuth(c.auth)
		attemptEpoch, knownEpoch := c.monMap.AuthEpoch, c.monMap.Epoch != 0
		c.mu.Unlock()
		transport, err := c.open(linked, candidateTarget.endpoint, candidateTarget.address, 1, 0, session.MonAuth{Client: candidate})
		if err != nil {
			release()
			failures = append(failures, err)
			if ctx.Err() != nil || c.ctx.Err() != nil {
				return errors.Join(failures...)
			}
			continue
		}
		ready := make(chan struct{})
		var readyOnce sync.Once
		var verifiedMon bool
		var bufferedMgr *msgr.MessageData
		var bufferedEpoch uint32
		var candidateEpoch uint32
		var s *session.Session
		s = session.New(transport, c.sessionConfig(), func(m msgr.MessageData) error {
			if m.Type == msgr.MgrDigestMessage {
				if !verifiedMon {
					return nil
				}
				return c.handleDigest(s, m)
			}
			if m.Type == msgr.ConfigMessage {
				if !verifiedMon {
					return nil
				}
				return c.handleConfig(s, m)
			}
			if m.Type == msgr.LogMessage {
				if !verifiedMon {
					return nil
				}
				return c.handleLog(s, m)
			}
			// MON can send mgrmap before monmap. Keep at most one frame until
			// this candidate's MonMap establishes cluster and release identity.
			if m.Type == msgr.MgrMapMessage && !verifiedMon {
				c.mu.Lock()
				current := c.mon == s
				c.mu.Unlock()
				if !current {
					return nil
				}
				if err := validateMapMessageHeader(m); err != nil {
					bufferedMgr = nil
					return err
				}
				mgr, err := maps.DecodeMgr(m.Front)
				if err != nil {
					bufferedMgr = nil
					return fmt.Errorf("%w: invalid MGR map: %w", msgr.ErrFrame, err)
				}
				if bufferedMgr == nil || mgr.Epoch >= bufferedEpoch {
					bufferedMgr = &msgr.MessageData{Type: m.Type, Front: m.Front}
					bufferedEpoch = mgr.Epoch
				}
				return nil
			}
			accepted, err := c.handleMap(s, m)
			if err != nil {
				bufferedMgr = nil
				return err
			}
			if accepted {
				if !verifiedMon {
					mon, _ := maps.DecodeMon(m.Front) // Already validated by handleMap.
					candidateEpoch = mon.AuthEpoch
				}
				verifiedMon = true
				if bufferedMgr != nil {
					_, err = c.handleMap(s, *bufferedMgr)
					bufferedMgr = nil
					if err != nil {
						return err
					}
				}
				readyOnce.Do(func() { close(ready) })
			}
			return nil
		}, func(err error) {
			c.stopLogForFailedSession(s, err)
			c.stopConfigForFailedSession(s, err)
			c.stopDigestForFailedSession(s, err)
			c.mu.Lock()
			if c.mon == s {
				wasReady := c.monReady
				c.monReady = false
				if c.logWatch != nil {
					c.logWatch.source = nil
				}
				if c.configWatch != nil {
					c.configWatch.source = nil
				}
				if c.digestWatch != nil {
					c.digestWatch.source = nil
				}
				c.signal()
				if wasReady {
					c.wakeMonitor()
				}
			}
			c.mu.Unlock()
		})
		if old != nil {
			// The old session may already be terminal while its custom Conn.Close
			// still delays onClose. Record its registered watch's cause before
			// making that callback stale by adopting this candidate.
			c.stopLogForFailedSession(old, old.Err())
			c.stopConfigForFailedSession(old, old.Err())
			c.stopDigestForFailedSession(old, old.Err())
		}
		c.mu.Lock()
		c.mon, c.monReady = s, false
		if c.logWatch != nil {
			// The log worker may miss a fast candidate failure and restoration
			// of the same old session. Force a cursor-based registration even
			// when it observes only the final restored MON.
			c.logWatch.source = nil
		}
		if c.configWatch != nil {
			// Request a full map even if the worker misses a failed candidate
			// and observes only restoration of the same old MON session.
			c.configWatch.source = nil
		}
		if c.digestWatch != nil {
			// Resubscribe even if the worker misses a failed candidate and
			// observes only restoration of the same admitted MON session.
			c.digestWatch.source = nil
		}
		attached := c.attach(s)
		c.signal()
		c.mu.Unlock()
		if !attached {
			release()
			s.Fail(ErrClosed)
			return ErrClosed
		}
		err = s.Send(c.ctx, msgr.Subscribe(0, 0, c.options.Hostname))
		if err == nil {
			err = waitMonitorAdmission(linked, s, ready)
		}
		release()
		var oldManager *session.Session
		if err == nil {
			c.mu.Lock()
			if c.closed {
				err = ErrClosed
			} else {
				if (knownEpoch && c.monMap.AuthEpoch > attemptEpoch) || candidateEpoch < c.monMap.AuthEpoch {
					// Authentication may precede a key wipe learned from MonMap.
					// Never overwrite its invalidation with an older candidate.
					candidate = invalidateAuthTickets(candidate)
				}
				if c.auth.GlobalID != candidate.GlobalID {
					oldManager, c.mgr = c.mgr, nil
				}
				c.auth = candidate
				c.authErr = nil
				c.monReady = true
				c.signal()
			}
			c.mu.Unlock()
		}
		if oldManager != nil {
			oldManager.Fail(errors.New("ceph: authenticated client identity changed"))
		}
		if err == nil {
			if old != nil && old != s {
				c.retireMonitor(old)
			}
			return nil
		}
		s.Fail(err)
		c.mu.Lock()
		if c.mon == s {
			c.mon, c.monReady = old, wasReady && old != nil && old.Err() == nil
			c.signal()
		}
		c.mu.Unlock()
		failures = append(failures, err)
		if ctx.Err() != nil || c.ctx.Err() != nil {
			return errors.Join(failures...)
		}
	}
	return errors.Join(failures...)
}

func waitMonitorAdmission(ctx context.Context, s *session.Session, ready <-chan struct{}) error {
	select {
	case <-ready:
		return s.Err()
	case <-s.Done():
		return s.Err()
	case <-ctx.Done():
		// A protocol failure can already be terminal when this goroutine
		// resumes after the setup deadline. Fail preserves the first cause
		// atomically, including when a reader is finishing concurrently.
		s.Fail(ctx.Err())
		return s.Err()
	}
}

func validateMapMessageHeader(m msgr.MessageData) error {
	// MMonMap and MMgrMap support message header version 1. Their nested map
	// envelopes have independent versions; a newer compatible header is valid.
	if (m.Type == msgr.MonMapMessage || m.Type == msgr.MgrMapMessage) && m.CompatVersion > 1 {
		return fmt.Errorf("%w: map message compatibility version %d: %w", msgr.ErrFrame, m.CompatVersion, wire.ErrVersion)
	}
	return nil
}

func (c *Client) handleMap(source *session.Session, m msgr.MessageData) (bool, error) {
	c.mu.Lock()
	current := c.mon == source
	c.mu.Unlock()
	if !current {
		return false, nil
	}
	if err := validateMapMessageHeader(m); err != nil {
		return false, err
	}
	switch m.Type {
	case msgr.MonMapMessage:
		mon, err := maps.DecodeMon(m.Front)
		if err != nil {
			if errors.Is(err, maps.ErrRelease) {
				return false, err
			}
			return false, fmt.Errorf("%w: invalid MON map: %w", msgr.ErrFrame, err)
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.mon != source {
			return false, nil
		}
		if c.fsid != [16]byte{} && c.fsid != mon.FSID {
			return false, errors.New("ceph: monitor FSID mismatch")
		}
		c.fsid = mon.FSID
		if mon.Epoch >= c.monMap.Epoch {
			if mon.AuthEpoch > c.monMap.AuthEpoch {
				c.auth = invalidateAuthTickets(c.auth)
				c.wakeMonitor()
			}
			c.monMap = mon
		}
		c.signal()
		return true, nil
	case msgr.MgrMapMessage:
		mgr, err := maps.DecodeMgr(m.Front)
		if err != nil {
			return false, fmt.Errorf("%w: invalid MGR map: %w", msgr.ErrFrame, err)
		}
		c.mu.Lock()
		if c.mon != source || mgr.Epoch < c.mgrMap.Epoch {
			c.mu.Unlock()
			return false, nil
		}
		changed := mgr.GlobalID != c.mgrMap.GlobalID || mgr.Available != c.mgrMap.Available || !reflect.DeepEqual(mgr.Addresses, c.mgrMap.Addresses)
		old := c.mgr
		if changed {
			c.mgr = nil
		}
		c.mgrMap = mgr
		c.signal()
		c.mu.Unlock()
		if changed && old != nil {
			old.Fail(ErrManagerChanged)
		}
	}
	return false, nil
}

func (c *Client) monitor(ctx context.Context) (*session.Session, error) {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, ErrClosed
		}
		s, ready, changed, authErr := c.mon, c.monReady, c.changed, c.authErr
		c.mu.Unlock()
		if authErr != nil {
			return nil, authErr
		}
		if s != nil && ready {
			if err := s.Err(); err == nil {
				return s, nil
			}
		}
		if s != nil {
			if err := s.Err(); err != nil {
				c.wakeMonitor()
			}
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
func (c *Client) manager(ctx context.Context) (*session.Session, error) {
	select {
	case c.mgrGate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, ErrClosed
	}
	// A MGR connection belongs to the client before its session is attached.
	// Close must also wait for a canceled handshake to finish cleaning it up.
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		<-c.mgrGate
		return nil, ErrClosed
	}
	c.wg.Add(1)
	c.mu.Unlock()
	defer func() {
		<-c.mgrGate
		c.wg.Done()
	}()
retryManager:
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, err := c.monitor(ctx); err != nil {
			return nil, err
		}
		c.mu.Lock()
		if c.mgr != nil && c.mgr.Err() == nil {
			s := c.mgr
			c.mu.Unlock()
			return s, nil
		}
		mapSnapshot, auth, changed := c.mgrMap, c.auth, c.changed
		c.mu.Unlock()
		if !mapSnapshot.Available {
			select {
			case <-changed:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-c.ctx.Done():
				return nil, ErrClosed
			}
		}
		authorizer, err := auth.Authorizer(cephx.ServiceMgr)
		if errors.Is(err, cephx.ErrTicket) {
			// No command or MGR handshake has started. Let the coordinator
			// obtain a current ticket and keep waiting under this context.
			c.wakeMonitor()
			select {
			case <-changed:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-c.ctx.Done():
				return nil, ErrClosed
			}
		}
		if err != nil {
			return nil, err
		}
		var failures []error
		for _, address := range mapSnapshot.Addresses {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			linked, release := c.linkedContext(ctx)
			transport, err := c.open(linked, dialAddress(address), address, 16, mapSnapshot.GlobalID, session.MgrAuth{Authorizer: authorizer})
			release()
			if err != nil {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				c.mu.Lock()
				stale := c.closed || c.authErr != nil || c.auth != auth || c.mgrMap.GlobalID != mapSnapshot.GlobalID || !c.mgrMap.Available || !reflect.DeepEqual(c.mgrMap.Addresses, mapSnapshot.Addresses)
				c.mu.Unlock()
				if stale {
					// Setup has not submitted an application command. Resolve
					// the current identity/map before returning an obsolete
					// endpoint or authorizer failure.
					continue retryManager
				}
				failures = append(failures, err)
				continue
			}
			var s *session.Session
			s = session.New(transport, c.sessionConfig(), nil, func(error) {
				c.mu.Lock()
				if c.mgr == s {
					c.mgr = nil
					c.signal()
				}
				c.mu.Unlock()
			})
			c.mu.Lock()
			stale := c.closed || c.authErr != nil || c.auth.GlobalID != auth.GlobalID || c.mgrMap.GlobalID != mapSnapshot.GlobalID || !c.mgrMap.Available || !reflect.DeepEqual(c.mgrMap.Addresses, mapSnapshot.Addresses)
			if !stale {
				c.mgr = s
				stale = !c.attach(s)
			}
			c.mu.Unlock()
			if stale {
				s.Fail(ErrManagerChanged)
				continue retryManager
			}
			return s, nil
		}
		if len(failures) > 0 {
			return nil, errors.Join(failures...)
		}
	}
}

// Close is idempotent and waits for all connection workers to exit.
func (c *Client) Close() error {
	// A session can already have a fatal cause while custom connection cleanup
	// delays onClose and a watch worker has not observed Done. Preserve that
	// registered source's cause before publishing ordinary client shutdown.
	// Error inspection and watch cancellation may invoke custom code.
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		c.wg.Wait()
		return nil
	}
	source := c.mon
	c.mu.Unlock()
	if source != nil {
		err := source.Err()
		c.stopLogForFailedSession(source, err)
		c.stopConfigForFailedSession(source, err)
		c.stopDigestForFailedSession(source, err)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		c.wg.Wait()
		return nil
	}
	c.closed = true
	configWatch, logWatch := c.configWatch, c.logWatch
	digestWatch := c.digestWatch
	if configWatch != nil {
		configWatch.stopLocked(ErrClosed)
	}
	if logWatch != nil {
		logWatch.stopLocked(ErrClosed)
	}
	if digestWatch != nil {
		digestWatch.stopLocked(ErrClosed)
	}
	c.signal()
	all := make([]*session.Session, 0, len(c.sessions))
	for s := range c.sessions {
		all = append(all, s)
	}
	c.mu.Unlock()
	c.cancel()
	if configWatch != nil {
		configWatch.cancel()
	}
	if logWatch != nil {
		logWatch.cancel()
	}
	if digestWatch != nil {
		digestWatch.cancel()
	}
	for _, s := range all {
		s.Fail(ErrClosed)
	}
	c.wg.Wait()
	return nil
}
