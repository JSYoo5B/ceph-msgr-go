package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
)

var ErrOSDDisabled = errors.New("ceph: OSD communication requires EnableOSD")
var ErrOSDConnectionLimit = errors.New("ceph: OSD connection limit reached")
var ErrOSDTargetChanged = errors.New("ceph: OSD ID already has a different connection target")
var ErrUnsupportedFeatures = msgr.ErrFeatures

// OSDTarget is chosen by the caller's routing layer. ID is the nonnegative OSD
// number, including osd.0. Address is a numeric Messenger v2 IP:port with an
// optional v2: prefix and /nonce suffix. IPv6 is bracketed; zones are unsupported.
// The caller is responsible for obtaining this target from its cluster map.
type OSDTarget struct {
	ID      int32
	Address string
}

// OSDConnection is a client-owned, explicitly targeted lossy session. OpenOSD
// shares one handle for the same OSD ID and wire address. Close releases its
// connection slot and waits for cleanup; Client.Close also owns its lifetime.
// A failed handle is not automatically reconnected and requests are not replayed.
// Compression reports the negotiated msgr2.1 onwire method on this OSD
// connection: "" (none), "snappy" or "zstd".
func (o *OSDConnection) Compression() string {
	o.client.mu.Lock()
	defer o.client.mu.Unlock()
	return compressionName(o.compression)
}

type OSDConnection struct {
	client       *Client
	target       OSDTarget
	address      msgr.Address
	ctx          context.Context
	cancel       context.CancelFunc
	setupDone    chan struct{}
	closeOnce    sync.Once
	closed       bool // guarded by client.mu, as are session and setupErr
	session      *session.Session
	setupErr     error
	features     uint64
	compression  msgr.CompressionAlgorithm // negotiated onwire compression
	globalID     uint64
	watchState   *osdWatchState   // guarded by client.mu
	backoffState *osdBackoffState // guarded by client.mu; inactive until writer gate is installed
	generation   uint64
	mapQueue     []queuedOSDMap // guarded by client.mu
	mapBytes     uint64
	mapTerminal  error
	mapChanged   chan struct{}
}

// OpenOSD authenticates an OSD service connection using this client's MON-issued
// ticket. ctx governs setup and waiting for a concurrent setup, not the returned
// handle's lifetime. It does not discover an address, calculate placement, or
// advertise the MON-only CRUSH admission exception. Unsupported OSD feature
// requirements fail with ErrUnsupportedFeatures before object transmission.
func (c *Client) OpenOSD(ctx context.Context, target OSDTarget) (*OSDConnection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.ctx.Err() != nil {
		return nil, ErrClosed
	}
	if !c.options.EnableOSD {
		return nil, ErrOSDDisabled
	}
	if !c.options.OSDProfile.valid() {
		return nil, fmt.Errorf("%w: unknown OSD profile", ErrUnsupportedFeatures)
	}
	_, address, err := seedAddress(target.Address)
	if err != nil || target.ID < 0 || !address.Endpoint.IsValid() {
		return nil, errors.New("ceph: OSD target requires a nonnegative ID and numeric v2 address")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	if held := c.osds[target.ID]; held != nil {
		if held.address != address {
			c.mu.Unlock()
			return nil, ErrOSDTargetChanged
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
	if len(c.osds) >= c.options.MaxOSDConnections {
		c.mu.Unlock()
		return nil, ErrOSDConnectionLimit
	}
	if c.osdGeneration == ^uint64(0) {
		c.mu.Unlock()
		return nil, errors.New("ceph: OSD connection generations exhausted")
	}
	c.osdGeneration++
	base, cancel := context.WithCancel(c.ctx)
	o := &OSDConnection{client: c, target: target, address: address, ctx: base, cancel: cancel, setupDone: make(chan struct{}), mapChanged: make(chan struct{}), generation: c.osdGeneration}
	c.osds[target.ID] = o
	c.wg.Add(1)
	c.mu.Unlock()
	defer c.wg.Done()
	operation, stopOperation := context.WithCancel(ctx)
	stop := context.AfterFunc(base, stopOperation)
	defer func() { stop(); stopOperation() }()
	err = o.open(operation)
	c.mu.Lock()
	if err == nil && (c.closed || o.closed) {
		err = ErrClosed
	}
	o.setupErr = err
	if err != nil {
		if c.osds[target.ID] == o {
			delete(c.osds, target.ID)
		}
	}
	close(o.setupDone)
	s := o.session
	c.mu.Unlock()
	if err != nil {
		cancel()
		if s != nil {
			s.Fail(err)
			s.Wait()
		}
		return nil, c.namedMonitorError(err)
	}
	return o, nil
}

func (o *OSDConnection) open(ctx context.Context) error {
	c := o.client
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
		authorizer, err := auth.Authorizer(cephx.ServiceOSD)
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
		tr, err := c.open(linked, dialAddress(o.address), o.address, 4, uint64(o.target.ID), session.ServiceAuth{Authorizer: authorizer, OSDCachePools: c.options.EnableOSDCachePools, ReplicatedPGV8: o.client.options.OSDProfile.replicatedPG(), ReplicatedPGReplyV8: o.client.options.OSDProfile.replyV8(), ReplicatedPGRequestV9: o.client.options.OSDProfile.requestV9()})
		release()
		if err != nil {
			return err
		}
		if tr.GlobalID != auth.GlobalID {
			tr.Conn.Close()
			return fmt.Errorf("%w: OSD authenticated a different client identity", msgr.ErrAuthentication)
		}
		config := c.sessionConfig()
		config.NextTransaction = c.nextOSDTransaction
		if o.client.options.OSDProfile.replicatedPG() {
			config.BeforeWrite = o.beforeWriteBackoff
		}
		var s *session.Session
		s = session.New(tr, config, func(m msgr.MessageData) error {
			if m.Type == msgr.OSDMapMessage {
				return o.handleMap(s, m)
			}
			if m.Type == osd.WatchNotifyMessage {
				return o.handleWatch(s, m)
			}
			if m.Type == osd.BackoffMessage && o.client.options.OSDProfile.replicatedPG() {
				return o.handleBackoff(s, m)
			}
			// Other routing and backoff instructions remain unsupported.
			return fmt.Errorf("%w: unsupported OSD control message %d", msgr.ErrFeatures, m.Type)
		}, func(err error) { o.stopMaps(err); o.stopWatch(err); o.stopBackoffs(err) })
		c.mu.Lock()
		stale := c.closed || o.closed || c.authErr != nil || c.auth.GlobalID != auth.GlobalID
		if !stale {
			o.session, o.features, o.globalID, o.compression = s, tr.Features, auth.GlobalID, tr.Compression
			if o.client.options.OSDProfile.replicatedPG() {
				b := o.backoffsLocked()
				b.mu.Lock()
				b.gateInstalled = true
				b.mu.Unlock()
			}
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

func (c *Client) nextOSDTransaction() (uint64, error) {
	for {
		previous := c.osdNext.Load()
		if previous == ^uint64(0) {
			return 0, errors.New("ceph: OSD transaction IDs exhausted")
		}
		if c.osdNext.CompareAndSwap(previous, previous+1) {
			return previous + 1, nil
		}
	}
}

func (o *OSDConnection) Close() error {
	o.closeOnce.Do(func() {
		c := o.client
		c.mu.Lock()
		o.closed = true
		c.mu.Unlock()
		o.cancel()
		<-o.setupDone
		c.mu.Lock()
		s := o.session
		c.mu.Unlock()
		if s != nil {
			s.Fail(ErrClosed)
			s.Wait()
		}
		c.mu.Lock()
		if c.osds[o.target.ID] == o {
			delete(c.osds, o.target.ID)
		}
		c.mu.Unlock()
	})
	return nil
}
