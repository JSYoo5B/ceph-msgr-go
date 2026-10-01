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
	options  Options
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	closed   bool
	changed  chan struct{}
	auth     *cephx.Client // Published snapshots are immutable.
	fsid     [16]byte
	monMap   maps.Mon
	mgrMap   maps.Mgr
	mon, mgr *session.Session
	monReady bool
	authErr  error // Explicit MON reauthentication rejection, until recovery.
	sessions map[*session.Session]struct{}
	mgrGate  chan struct{}
	wake     chan struct{}
	calls    chan struct{}
	wg       sync.WaitGroup
}

// Dial establishes and authenticates a MON connection and verifies a MonMap
// whose configured minimum monitor release is Tentacle or later. ctx governs
// setup only; Close owns the returned client's lifetime.
func Dial(ctx context.Context, options Options) (*Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
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
		a.Endpoint = netip.AddrPortFrom(ip.Unmap(), uint16(n))
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
	copyAuth := *c.auth
	copyAuth.Tickets = make(map[uint32]cephx.Ticket, len(c.auth.Tickets))
	for id, t := range c.auth.Tickets {
		copyAuth.Tickets[id] = t
	}
	return &copyAuth
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
		if tcp, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
			address.Endpoint = tcp.AddrPort()
		} else {
			address.Endpoint, _ = netip.ParseAddrPort(endpoint)
		}
		if !address.Endpoint.IsValid() {
			conn.Close()
			return nil, errors.New("ceph: dialer returned no peer IP address")
		}
	}
	return session.Handshake(ctx, conn, address, role, id, auth, c.options.MaxFrameSize, c.options.ConnectTimeout)
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

func (c *Client) connectMonitor(ctx context.Context) error {
	c.mu.Lock()
	seeds := append([]string(nil), c.options.Monitors...)
	for _, a := range c.monMap.Addresses {
		seeds = append(seeds, dialAddress(a)+"/"+strconv.FormatUint(uint64(a.Nonce), 10))
	}
	old, wasReady := c.mon, c.monReady
	c.mu.Unlock()
	var failures []error
	seen := make(map[string]bool)
	for _, seed := range seeds {
		linked, release := c.linkedContext(ctx)
		endpoint, address, err := seedAddress(seed)
		if err != nil {
			release()
			failures = append(failures, err)
			continue
		}
		identity := endpoint + "/" + strconv.FormatUint(uint64(address.Nonce), 10)
		if seen[identity] {
			release()
			continue
		}
		seen[identity] = true
		candidate := c.snapshotAuth()
		transport, err := c.open(linked, endpoint, address, 1, 0, session.MonAuth{Client: candidate})
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
		var s *session.Session
		s = session.New(transport, c.options.ConnectTimeout, func(m msgr.MessageData) error {
			accepted, err := c.handleMap(s, m)
			if accepted {
				readyOnce.Do(func() { close(ready) })
			}
			return err
		}, func(error) {
			c.mu.Lock()
			if c.mon == s {
				wasReady := c.monReady
				c.monReady = false
				c.signal()
				if wasReady {
					c.wakeMonitor()
				}
			}
			c.mu.Unlock()
		})
		c.mu.Lock()
		c.mon, c.monReady = s, false
		attached := c.attach(s)
		c.signal()
		c.mu.Unlock()
		if !attached {
			release()
			s.Fail(ErrClosed)
			return ErrClosed
		}
		err = s.Send(c.ctx, msgr.Subscribe(0, 0))
		if err == nil {
			select {
			case <-ready:
				err = s.Err()
			case <-s.Done():
				err = s.Err()
			case <-linked.Done():
				err = linked.Err()
			}
		}
		release()
		var oldManager *session.Session
		if err == nil {
			c.mu.Lock()
			if c.closed {
				err = ErrClosed
			} else {
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

func (c *Client) handleMap(source *session.Session, m msgr.MessageData) (bool, error) {
	c.mu.Lock()
	current := c.mon == source
	c.mu.Unlock()
	if !current {
		return false, nil
	}
	switch m.Type {
	case msgr.MonMapMessage:
		mon, err := maps.DecodeMon(m.Front)
		if err != nil {
			return false, err
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
			c.monMap = mon
		}
		c.signal()
		return true, nil
	case msgr.MgrMapMessage:
		mgr, err := maps.DecodeMgr(m.Front)
		if err != nil {
			return false, err
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
		defer func() { <-c.mgrGate }()
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, ErrClosed
	}
	for {
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
			linked, release := c.linkedContext(ctx)
			transport, err := c.open(linked, dialAddress(address), address, 16, mapSnapshot.GlobalID, session.MgrAuth{Authorizer: authorizer})
			release()
			if err != nil {
				failures = append(failures, err)
				continue
			}
			var s *session.Session
			s = session.New(transport, c.options.ConnectTimeout, nil, func(error) {
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
				break
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
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		c.wg.Wait()
		return nil
	}
	c.closed = true
	c.cancel()
	c.signal()
	all := make([]*session.Session, 0, len(c.sessions))
	for s := range c.sessions {
		all = append(all, s)
	}
	c.mu.Unlock()
	for _, s := range all {
		s.Fail(ErrClosed)
	}
	c.wg.Wait()
	return nil
}
