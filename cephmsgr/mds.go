package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

var (
	ErrMDSDisabled        = errors.New("ceph: MDS communication requires EnableMDS")
	ErrMDSConnectionLimit = errors.New("ceph: MDS connection limit reached")
	ErrMDSTargetChanged   = errors.New("ceph: MDS filesystem/rank already has a different connection target")
	ErrMDSIdentityChanged = errors.New("ceph: MDS connection belongs to an earlier client identity")
	// ErrMDSAddressInUse refuses a second CephFS-profile connection to one
	// MDS address under another filesystem label.
	ErrMDSAddressInUse = errors.New("ceph: CephFS profile allows one connection per MDS address")
)

// MDSTarget is supplied by the caller's routing layer. FilesystemID is local
// routing metadata only: neither Messenger identification nor the MDS service
// authorizer authenticates an FSCID. Rank is the active nonnegative MDS rank,
// including rank zero, not a daemon GID. Address is a numeric v2 IP:port with an
// explicit /nonce suffix (including /0); an optional v2: prefix is accepted.
// IPv6 is bracketed and address zones are unsupported. No discovery occurs.
type MDSTarget struct {
	FilesystemID int32
	Rank         int32
	Address      string
}

type mdsTargetKey struct{ filesystemID, rank int32 }

// MDSConnection owns one fresh, explicitly targeted Messenger connection.
// It opens no CephFS session. A command may make the server mark this transport
// disposable/lossy; immediate remote closure is not promised. A failed handle
// stays terminal and is never reconnected. Close releases its cached slot and
// waits for setup/transport cleanup. Client.Close also owns its lifetime.
type MDSConnection struct {
	client    *Client
	target    MDSTarget
	address   msgr.Address
	ctx       context.Context
	cancel    context.CancelFunc
	setupDone chan struct{}
	closeOnce sync.Once
	// client.mu guards setupErr, session publication, cephfs and closed.
	setupErr                       error
	session                        *session.Session
	cephfs                         *MDSSession
	resumes                        atomic.Uint64
	closed                         bool
	features, globalID, generation uint64
}

// OpenMDS requires EnableMDS selected before Dial. ctx governs only setup and
// waiting for a concurrent setup. The returned handle lives until Close,
// client shutdown or terminal transport failure. One handle is shared for each
// (FilesystemID,Rank) and exact decoded address, preserving address family and
// nonce. A different address requires closing the old handle first.
func (c *Client) OpenMDS(ctx context.Context, target MDSTarget) (*MDSConnection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.ctx.Err() != nil {
		return nil, ErrClosed
	}
	if !c.options.EnableMDS {
		return nil, ErrMDSDisabled
	}
	_, address, err := seedAddress(target.Address)
	if err != nil || target.FilesystemID < 0 || target.Rank < 0 || !strings.Contains(target.Address, "/") || !address.Endpoint.IsValid() || address.Endpoint.Addr().IsUnspecified() {
		return nil, errors.New("ceph: MDS target requires nonnegative filesystem/rank and a numeric v2 address with explicit nonce")
	}
	key := mdsTargetKey{target.FilesystemID, target.Rank}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	if c.mdsUnresolved[address] {
		c.mu.Unlock()
		return nil, ErrMDSSessionActive
	}
	for other, held := range c.mdss {
		// A new connection with this GlobalID would take over the MDS's
		// remote session, which only the session's own handle may use.
		if other != key && held.address == address && held.cephfs != nil {
			c.mu.Unlock()
			return nil, ErrMDSSessionActive
		}
		// CephFS connections share one client address instance; the MDS
		// registers stateful connections by it, so a second one would
		// replace (and reset) the first.
		if other != key && held.address == address && c.options.EnableCephFS {
			c.mu.Unlock()
			return nil, ErrMDSAddressInUse
		}
	}
	if held := c.mdss[key]; held != nil {
		if held.address != address {
			c.mu.Unlock()
			return nil, ErrMDSTargetChanged
		}
		c.mu.Unlock()
		select {
		case <-held.setupDone:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.ctx.Done():
			return nil, ErrClosed
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if held.setupErr != nil {
			return nil, held.setupErr
		}
		if c.closed || held.closed {
			return nil, ErrClosed
		}
		if err := held.session.Err(); err != nil {
			return nil, err
		}
		return held, nil
	}
	if len(c.mdss) >= c.options.MaxMDSConnections {
		c.mu.Unlock()
		return nil, ErrMDSConnectionLimit
	}
	if c.mdsGeneration == ^uint64(0) {
		c.mu.Unlock()
		return nil, errors.New("ceph: MDS connection generations exhausted")
	}
	c.mdsGeneration++
	base, cancel := context.WithCancel(c.ctx)
	m := &MDSConnection{client: c, target: target, address: address, ctx: base, cancel: cancel, setupDone: make(chan struct{}), generation: c.mdsGeneration}
	if c.mdss == nil {
		c.mdss = make(map[mdsTargetKey]*MDSConnection)
	}
	c.mdss[key] = m
	c.wg.Add(1)
	c.mu.Unlock()
	defer c.wg.Done()
	operation, stopOperation := context.WithCancel(ctx)
	stop := context.AfterFunc(base, stopOperation)
	defer func() { stop(); stopOperation() }()
	err = m.open(operation)
	c.mu.Lock()
	if err == nil && (c.closed || m.closed) {
		err = ErrClosed
	}
	m.setupErr = err
	s := m.session
	c.mu.Unlock()
	if err != nil {
		cancel()
		if s != nil {
			s.Fail(err)
			s.Wait()
		}
		// Keep the slot reserved until cleanup completes; concurrent callers must
		// not bypass the configured setup/connection bound during a failed close.
		c.mu.Lock()
		if c.mdss[key] == m {
			delete(c.mdss, key)
		}
		close(m.setupDone)
		c.mu.Unlock()
		return nil, c.namedMonitorError(err)
	}
	c.mu.Lock()
	close(m.setupDone)
	c.mu.Unlock()
	return m, nil
}

func (m *MDSConnection) open(ctx context.Context) error {
	c := m.client
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := c.monitor(ctx); err != nil {
			return err
		}
		c.mu.Lock()
		auth, changed := c.auth, c.changed
		c.mu.Unlock()
		authorizer, err := auth.Authorizer(cephx.ServiceMDS)
		if errors.Is(err, cephx.ErrTicket) {
			c.wakeMonitor()
			select {
			case <-changed:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err != nil {
			return err
		}
		linked, release := c.linkedContext(ctx)
		tr, err := c.openFreshMDS(linked, m.address, m.target.Rank, session.ServiceAuth{Authorizer: authorizer})
		release()
		if err != nil {
			return err
		}
		if tr.GlobalID != auth.GlobalID {
			tr.Conn.Close()
			return fmt.Errorf("%w: MDS authenticated a different client identity", msgr.ErrAuthentication)
		}
		config := c.sessionConfig()
		config.NextTransaction = c.nextMDSTransaction
		if c.options.EnableCephFS {
			// The CephFS profile is a lossless client, as native clients use
			// for MDS: transport faults resume the Messenger session.
			config.Resume = m.resumeTransport
			config.OnResume = func() { m.resumes.Add(1) }
		}
		s := session.New(tr, config, m.dispatchMDSMessage, m.mdsTransportEnded)
		c.mu.Lock()
		stale := c.closed || m.closed || c.authErr != nil || c.auth.GlobalID != auth.GlobalID
		if !stale {
			m.session, m.features, m.globalID = s, tr.Features, auth.GlobalID
			stale = !c.attach(s)
		}
		c.mu.Unlock()
		if stale {
			s.Fail(ErrClosed)
			s.Wait()
			return ErrClosed
		}
		return nil
	}
}

func (c *Client) nextMDSTransaction() (uint64, error) {
	for {
		previous := c.mdsNext.Load()
		if previous == ^uint64(0) {
			return 0, errors.New("ceph: MDS transaction IDs exhausted")
		}
		if c.mdsNext.CompareAndSwap(previous, previous+1) {
			return previous + 1, nil
		}
	}
}

// Done closes when this handle's transport becomes terminal. A successful
// OpenMDS publishes exactly one transport; there is no replacement or recovery.
func (m *MDSConnection) Done() <-chan struct{} { return m.session.Done() }

func (m *MDSConnection) Close() error {
	m.closeOnce.Do(func() {
		c := m.client
		c.mu.Lock()
		m.closed = true
		c.mu.Unlock()
		m.cancel()
		<-m.setupDone
		c.mu.Lock()
		s := m.session
		c.mu.Unlock()
		if s != nil {
			s.Fail(ErrClosed)
			s.Wait()
		}
		c.mu.Lock()
		key := mdsTargetKey{m.target.FilesystemID, m.target.Rank}
		if c.mdss[key] == m {
			delete(c.mdss, key)
		}
		c.mu.Unlock()
	})
	return nil
}

func (m *MDSConnection) operationError(err error) error {
	if m.ctx.Err() == nil || !errors.Is(err, context.Canceled) || errors.Is(err, ErrClosed) {
		return err
	}
	var unknown *OutcomeUnknownError
	if errors.As(err, &unknown) {
		return &OutcomeUnknownError{Cause: errors.Join(unknown.Cause, ErrClosed)}
	}
	return errors.Join(err, ErrClosed)
}

// resumeTransport authenticates a new connection to the same MDS address and
// resumes the stateful Messenger session. The client GlobalID must be
// unchanged; an expired ticket waits for renewal within ctx.
func (m *MDSConnection) resumeTransport(ctx context.Context, state session.ResumeState) (*session.Transport, error) {
	c := m.client
	for {
		c.mu.Lock()
		auth, changed, closed := c.auth, c.changed, c.closed || m.closed
		c.mu.Unlock()
		if closed {
			return nil, ErrClosed
		}
		if auth.GlobalID != state.GlobalID {
			return nil, fmt.Errorf("%w: %w", msgr.ErrAuthentication, ErrMDSIdentityChanged)
		}
		authorizer, err := auth.Authorizer(cephx.ServiceMDS)
		if errors.Is(err, cephx.ErrTicket) {
			c.wakeMonitor()
			select {
			case <-changed:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %w", msgr.ErrAuthentication, err)
		}
		linked, release := c.linkedContext(ctx)
		t, err := c.resumeCephFS(linked, m.address, m.target.Rank, session.ServiceAuth{Authorizer: authorizer}, state)
		release()
		return t, err
	}
}

// Resumes counts Messenger session resumes after transport faults (CephFS
// profile only). A resume preserves message order and pending requests; it
// is neither a CephFS reconnect nor an application replay.
func (m *MDSConnection) Resumes() uint64 { return m.resumes.Load() }
