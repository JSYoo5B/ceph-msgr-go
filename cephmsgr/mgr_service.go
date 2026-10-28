package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
)

// MaxMgrServices bounds the service registrations one client holds open.
const MaxMgrServices = 16

var (
	// ErrMgrServiceDisabled means Options.EnableMgrService was not set.
	ErrMgrServiceDisabled = errors.New("ceph: MGR service registration requires Options.EnableMgrService")
	// ErrMgrServiceExists means this client already holds a registration for the
	// same service and daemon name, or MaxMgrServices are open. Nothing was sent.
	ErrMgrServiceExists = errors.New("ceph: MGR service registration already open")
	// ErrMgrServiceClosed means the registration handle was closed or failed.
	ErrMgrServiceClosed = errors.New("ceph: MGR service registration closed")
	// ErrMgrServiceOutcomeUnknown wraps a failure after the registration was
	// queued: the MGR may or may not have registered the daemon. The library
	// never opens a registration again on its own.
	ErrMgrServiceOutcomeUnknown = errors.New("ceph: MGR service registration outcome unknown")
	// ErrMgrServiceCloseUnconfirmed means the MGR's close acknowledgement was not
	// received. The local connection is torn down, but the MGR may still list the
	// daemon until its own beacon grace expires.
	ErrMgrServiceCloseUnconfirmed = errors.New("ceph: MGR service close not acknowledged")
)

// MgrServiceOptions identifies one service daemon registration and carries its
// raw, caller-supplied metadata and status. The client never infers host
// identity, reads configuration or collects statistics.
type MgrServiceOptions struct {
	// Service and Daemon are the service name (for example "rgw") and daemon
	// name the MGR files the daemon under. Both are required: a CLIENT peer
	// without a service name is not a daemon to the MGR.
	Service, Daemon string
	// Metadata and Status are sent unchanged in the open message.
	Metadata, Status map[string]string
	// DisableBeacon stops the automatic empty report the MGR needs within
	// mgr_service_beacon_grace to keep the daemon registered. The caller then
	// owns liveness with Report.
	DisableBeacon bool
}

// MgrServiceConfigure is the latest statistics configuration the MGR pushed.
type MgrServiceConfigure struct {
	StatsPeriod    time.Duration // 0 means the MGR asks for no reports
	StatsThreshold uint32
	// Extension holds raw configuration this client does not interpret.
	Extension []byte
}

type mgrServiceKey struct{ service, daemon string }

// MgrService is one open service-daemon registration on a dedicated
// connection to the active MGR, bound to that daemon's identity and map
// address. If the connection fails or the active MGR changes, the handle
// fails; registering again is an explicit new OpenMgrService call.
type MgrService struct {
	c       *Client
	options MgrServiceOptions
	key     mgrServiceKey
	binding managerBinding
	s       *session.Session

	mu         sync.Mutex
	configure  MgrServiceConfigure
	configured chan struct{} // closed on the first configure push
	periodWake chan struct{}
	closeAck   chan struct{}
	closing    bool
	closeDone  chan struct{} // closed once the first Close has its terminal result
	closeErr   error         // that result; guarded by mu, valid after closeDone
	err        error
	done       chan struct{}
	doneOnce   sync.Once
}

// OpenMgrService registers a service daemon with the active MGR. It returns
// once the MGR's configure push shows that the open was dispatched; it does not
// observe ServiceMap, whose publication follows on the MGR's own schedule (use
// a ServiceMap stream for visibility). ctx governs setup and that wait. After
// the open is queued, cancellation or failure returns ErrMgrServiceOutcomeUnknown.
func (c *Client) OpenMgrService(ctx context.Context, options MgrServiceOptions) (*MgrService, error) {
	if !c.options.EnableMgrService {
		return nil, ErrMgrServiceDisabled
	}
	open, err := msgr.MgrOpen(options.Service, options.Daemon, options.Metadata, options.Status)
	if err != nil {
		return nil, err
	}
	options.Metadata, options.Status = cloneStrings(options.Metadata), cloneStrings(options.Status)
	svc := &MgrService{
		c: c, options: options, key: mgrServiceKey{options.Service, options.Daemon},
		configured: make(chan struct{}), periodWake: make(chan struct{}, 1),
		closeAck: make(chan struct{}), closeDone: make(chan struct{}), done: make(chan struct{}),
	}
	if err := c.reserveMgrService(svc); err != nil {
		return nil, err
	}
	if err := c.dialMgrService(ctx, svc); err != nil {
		c.releaseMgrService(svc)
		return nil, err
	}
	c.wg.Add(2)
	go svc.watchBinding()
	go svc.beaconLoop()
	if err := svc.s.SendControl(ctx, open); err != nil {
		svc.fail(err)
		return nil, err
	}
	select {
	case <-svc.configured:
		return svc, nil
	case <-svc.done:
		return nil, fmt.Errorf("%w: %w", ErrMgrServiceOutcomeUnknown, svc.Err())
	case <-ctx.Done():
		svc.fail(ctx.Err())
		return nil, fmt.Errorf("%w: %w", ErrMgrServiceOutcomeUnknown, ctx.Err())
	}
}

func cloneStrings(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func (c *Client) reserveMgrService(svc *MgrService) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if c.mgrServices == nil {
		c.mgrServices = make(map[mgrServiceKey]*MgrService)
	}
	if _, exists := c.mgrServices[svc.key]; exists || len(c.mgrServices) >= MaxMgrServices {
		return ErrMgrServiceExists
	}
	c.mgrServices[svc.key] = svc
	return nil
}

func (c *Client) releaseMgrService(svc *MgrService) {
	c.mu.Lock()
	if c.mgrServices[svc.key] == svc {
		delete(c.mgrServices, svc.key)
	}
	c.mu.Unlock()
}

// dialMgrService opens a connection to the current active MGR for svc, bound
// to that MGR's name, GID, addresses and the client's authenticated identity.
func (c *Client) dialMgrService(ctx context.Context, svc *MgrService) error {
	release, err := c.beginManagerSetup(ctx)
	if err != nil {
		return err
	}
	defer release()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := c.monitor(ctx); err != nil {
			return err
		}
		c.mu.Lock()
		mapSnapshot, auth, changed := c.mgrMap, c.auth, c.changed
		c.mu.Unlock()
		if !mapSnapshot.Available {
			select {
			case <-changed:
				continue
			case <-ctx.Done():
				return ctx.Err()
			case <-c.ctx.Done():
				return ErrClosed
			}
		}
		authorizer, err := auth.Authorizer(cephx.ServiceMgr)
		if errors.Is(err, cephx.ErrTicket) {
			c.wakeMonitor()
			select {
			case <-changed:
				continue
			case <-ctx.Done():
				return ctx.Err()
			case <-c.ctx.Done():
				return ErrClosed
			}
		}
		if err != nil {
			return err
		}
		var failures []error
		for _, address := range mapSnapshot.Addresses {
			linked, release := c.linkedContext(ctx)
			transport, err := c.open(linked, dialAddress(address), address, 16, mapSnapshot.GlobalID, session.ServiceAuth{Authorizer: authorizer})
			release()
			if err != nil {
				if err := ctx.Err(); err != nil {
					return err
				}
				failures = append(failures, err)
				continue
			}
			svc.binding = bindManager(mapSnapshot, auth.GlobalID)
			config := c.sessionConfig()
			config.BeforeWrite = func(msgr.MessageData) error { return c.mgrServiceBeforeWrite(svc.binding) }
			svc.s = session.New(transport, config, svc.onMessage, func(err error) { svc.fail(err) })
			c.mu.Lock()
			stale := c.closed || c.authErr != nil || c.auth.GlobalID != auth.GlobalID || !svc.binding.matches(c.mgrMap, auth.GlobalID)
			if !stale {
				stale = !c.attach(svc.s)
			}
			c.mu.Unlock()
			if stale {
				svc.s.Fail(ErrManagerChanged)
				return ErrManagerChanged
			}
			return nil
		}
		if len(failures) > 0 {
			return errors.Join(failures...)
		}
	}
}

func (c *Client) mgrServiceBeforeWrite(b managerBinding) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.closed:
		return ErrClosed
	case c.authErr != nil:
		return c.authErr
	case !b.matches(c.mgrMap, c.auth.GlobalID):
		return ErrManagerChanged
	}
	return nil
}

// watchBinding fails the handle as soon as the active MGR identity or the
// client's authenticated identity differs from the bound one.
func (s *MgrService) watchBinding() {
	defer s.c.wg.Done()
	for {
		s.c.mu.Lock()
		changed := s.c.changed
		ok := s.c.closed || s.binding.matches(s.c.mgrMap, s.c.auth.GlobalID)
		s.c.mu.Unlock()
		if !ok {
			s.fail(ErrManagerChanged)
			return
		}
		select {
		case <-changed:
		case <-s.done:
			return
		case <-s.c.ctx.Done():
			return
		}
	}
}

func (s *MgrService) onMessage(m msgr.MessageData) error {
	switch m.Type {
	case msgr.MgrConfigureMessage:
		cfg, err := msgr.DecodeMgrConfigure(m, s.c.options.MaxFrameSize)
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.configure = MgrServiceConfigure{StatsPeriod: time.Duration(cfg.StatsPeriod) * time.Second, StatsThreshold: cfg.StatsThreshold, Extension: cfg.Extension}
		select {
		case <-s.configured:
		default:
			close(s.configured)
		}
		s.mu.Unlock()
		select {
		case s.periodWake <- struct{}{}:
		default:
		}
	case msgr.MgrCloseMessage:
		service, daemon, err := msgr.DecodeMgrClose(m, s.c.options.MaxFrameSize)
		if err != nil {
			return err
		}
		if service == s.options.Service && daemon == s.options.Daemon {
			s.mu.Lock()
			select {
			case <-s.closeAck:
			default:
				close(s.closeAck)
			}
			s.mu.Unlock()
		}
	}
	return nil
}

// beaconLoop sends the empty service report at the period the MGR configured,
// as the native client does. A period of zero means no reports.
func (s *MgrService) beaconLoop() {
	defer s.c.wg.Done()
	if s.options.DisableBeacon {
		return
	}
	var timer *time.Timer
	var tick <-chan time.Time
	stop := func() {
		if timer != nil {
			timer.Stop()
			timer, tick = nil, nil
		}
	}
	defer stop()
	for {
		select {
		case <-s.periodWake:
			stop()
			if period := s.Configure().StatsPeriod; period > 0 {
				timer = time.NewTimer(period)
				tick = timer.C
			}
		case <-tick:
			stop()
			report, err := msgr.MgrReport(s.options.Service, s.options.Daemon, nil, nil)
			if err == nil {
				err = s.queue(s.c.ctx, report)
			}
			if err != nil {
				s.fail(err)
				return
			}
			if period := s.Configure().StatsPeriod; period > 0 {
				timer = time.NewTimer(period)
				tick = timer.C
			}
		case <-s.done:
			return
		case <-s.c.ctx.Done():
			return
		}
	}
}

// queue enqueues one protocol message in order, unless the handle is closing
// or failed. ctx governs queue admission only.
func (s *MgrService) queue(ctx context.Context, m msgr.MessageData) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	closing, failure := s.closing, s.err
	s.mu.Unlock()
	if failure != nil {
		return fmt.Errorf("%w: %w", ErrMgrServiceClosed, failure)
	}
	if closing {
		return ErrMgrServiceClosed
	}
	return s.s.SendControl(ctx, m)
}

// Configure returns the latest statistics configuration pushed by the MGR.
func (s *MgrService) Configure() MgrServiceConfigure {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := s.configure
	cfg.Extension = append([]byte(nil), cfg.Extension...)
	return cfg
}

// UpdateMetadata queues replacement daemon metadata (MMgrUpdate). The MGR sends
// no reply: success means the message was queued, not that the MGR applied it,
// and a later session failure leaves that unknown.
func (s *MgrService) UpdateMetadata(ctx context.Context, metadata map[string]string) error {
	m, err := msgr.MgrUpdate(s.options.Service, s.options.Daemon, metadata)
	if err != nil {
		return err
	}
	return s.queue(ctx, m)
}

// Report queues one service report (MMgrReport) with optional daemon status and
// task status. A nil map keeps the MGR's previous value; a non-nil map replaces
// it. It is also a liveness beacon. As with UpdateMetadata, success only means
// the message was queued.
func (s *MgrService) Report(ctx context.Context, status, task map[string]string) error {
	m, err := msgr.MgrReport(s.options.Service, s.options.Daemon, status, task)
	if err != nil {
		return err
	}
	return s.queue(ctx, m)
}

// Done is closed when the handle is closed or has failed.
func (s *MgrService) Done() <-chan struct{} { return s.done }

// Err returns why the handle failed, or nil while open or after a clean Close.
func (s *MgrService) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *MgrService) fail(err error) {
	s.mu.Lock()
	if s.err == nil && !s.closing {
		s.err = err
	}
	s.mu.Unlock()
	s.finish(err)
}

func (s *MgrService) finish(err error) {
	s.doneOnce.Do(func() {
		close(s.done)
		s.c.releaseMgrService(s)
	})
	// Session.Fail is idempotent and calls back into fail synchronously, so it
	// must run outside the Once.
	if s.s != nil {
		s.s.Fail(err)
	}
}

// Close sends MMgrClose and waits for the MGR's echoed acknowledgement, then
// closes the connection. ctx bounds the wait. Without an acknowledgement it
// still tears the connection down and returns ErrMgrServiceCloseUnconfirmed:
// the MGR may keep the daemon until its beacon grace.
//
// Close may be called concurrently and repeatedly. Only the first call sends
// MMgrClose. Every other call waits for that call's terminal result under its
// own context and then returns the same result, so an unconfirmed close is
// never reported as success; a caller whose context ends first gets
// ErrMgrServiceCloseUnconfirmed wrapping its own context error.
func (s *MgrService) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		select {
		case <-s.closeDone:
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.closeErr
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ErrMgrServiceCloseUnconfirmed, ctx.Err())
		}
	}
	s.closing = true
	failed := s.err
	s.mu.Unlock()
	result := s.closeOnce(ctx, failed)
	s.mu.Lock()
	s.closeErr = result
	close(s.closeDone)
	s.mu.Unlock()
	return result
}

func (s *MgrService) closeOnce(ctx context.Context, failed error) error {
	if failed != nil {
		s.finish(ErrMgrServiceClosed)
		return failed
	}
	closeMessage, err := msgr.MgrClose(s.options.Service, s.options.Daemon)
	if err == nil {
		err = s.s.SendControl(ctx, closeMessage)
	}
	if err == nil {
		select {
		case <-s.closeAck:
		case <-s.done:
			err = errors.New("connection ended before the acknowledgement")
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	s.finish(ErrMgrServiceClosed)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrMgrServiceCloseUnconfirmed, err)
	}
	return nil
}
