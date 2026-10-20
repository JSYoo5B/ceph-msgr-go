package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
)

// Backoff controls are available on the explicit replicated PG profile.
// The default object-hash profile does not negotiate the combined feature.
// Pinned provenance: Ceph v20.2.4 Objecter.cc:3835-3930, PG.cc:582-648,
// messages/MOSDBackoff.h (LGPL-2.1). No upstream implementation copied.
const (
	OSDBackoffBlock   uint8 = osd.BackoffBlock
	OSDBackoffUnblock uint8 = osd.BackoffUnblock
)

var (
	ErrOSDBackoff             = errors.New("ceph: OSD object admission blocked by backoff")
	ErrOSDBackoffOverflow     = errors.New("ceph: OSD backoff queue or active set exceeded")
	ErrOSDBackoffIdentity     = errors.New("ceph: OSD backoff does not belong to this live connection")
	ErrOSDBackoffAcknowledged = errors.New("ceph: OSD backoff acknowledgement already queued")
)

// OSDBackoffEvent preserves the daemon's actual PG, range and original map
// epoch. A range with equal endpoints blocks one object; other ranges are
// half open. The source and generation identify one authenticated connection.
// Matched reports that UNBLOCK removed a locally observed BLOCK. Receiving or
// dequeuing an event never sends ACK, retries an operation, or applies a map.
type OSDBackoffEvent struct {
	Op                          uint8
	Pool                        uint64
	Seed                        uint32
	Shard                       int8
	MapEpoch                    uint32
	ID                          uint64
	Begin, End                  OSDPGCursor
	Source                      OSDTarget
	GlobalID, SessionGeneration uint64
	Matched                     bool
	RawFront                    []byte
	receipt                     *osdBackoffReceipt
}

type osdBackoffReceipt struct {
	connection           *OSDConnection
	block                osd.Backoff
	generation, globalID uint64
}

type osdBackoffEntry struct {
	receipt       *osdBackoffReceipt
	bytes         uint64
	acking, acked bool
	ackStarted    bool
	// UNBLOCK may precede the explicit ACK. Keep the original receipt and
	// gate for its range until that ACK reaches the single writer.
	unblocked bool
}

type queuedOSDBackoff struct {
	event OSDBackoffEvent
	bytes uint64
}

type osdBackoffState struct {
	mu sync.Mutex
	// active also retains released-but-unacknowledged tombstones. Together
	// they share the same 64-entry/limit byte budget.
	active             map[uint64]*osdBackoffEntry
	queue              []queuedOSDBackoff
	bytes, activeBytes uint64
	limit              uint64
	terminal           error
	changed            chan struct{}
	// Set only after a verified single-writer admission hook is installed.
	// Default false deliberately prevents an ACK promise we cannot enforce.
	gateInstalled bool
}

func newOSDBackoffState(limit uint32) *osdBackoffState {
	return &osdBackoffState{active: make(map[uint64]*osdBackoffEntry), limit: uint64(limit), changed: make(chan struct{})}
}
func (b *osdBackoffState) signalLocked() { close(b.changed); b.changed = make(chan struct{}) }
func (b *osdBackoffState) stop(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.terminal == nil {
		b.terminal = err
		b.signalLocked()
	}
}

func backoffCursor(c osd.PGCursor) OSDPGCursor {
	return OSDPGCursor{Key: c.Key, Object: c.Object, Namespace: c.Namespace, Snapshot: c.Snap, Hash: c.Hash, Max: c.Max, Pool: c.Pool, Raw: bytes.Clone(c.Raw)}
}
func backoffSameIdentity(a, b osd.Backoff) bool {
	return a.PG == b.PG && a.Shard == b.Shard && a.ID == b.ID && bytes.Equal(a.Begin.Raw, b.Begin.Raw) && bytes.Equal(a.End.Raw, b.End.Raw)
}

func (b *osdBackoffState) accept(o *OSDConnection, decoded osd.Backoff, generation, globalID uint64) error {
	if decoded.Op != osd.BackoffBlock && decoded.Op != osd.BackoffUnblock {
		return fmt.Errorf("%w: daemon sent a backoff ACK", msgr.ErrFrame)
	}
	size := uint64(512 + len(decoded.RawFront) + len(decoded.Begin.Raw) + len(decoded.End.Raw))
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.terminal != nil {
		return nil
	}
	if len(b.queue) >= 64 || size > b.limit-b.bytes {
		return ErrOSDBackoffOverflow
	}
	previous := b.active[decoded.ID]
	if previous != nil && !backoffSameIdentity(previous.receipt.block, decoded) {
		return fmt.Errorf("%w: backoff ID reused with different PG or range", msgr.ErrFrame)
	}
	if decoded.Op == osd.BackoffBlock && previous != nil && previous.unblocked {
		return fmt.Errorf("%w: backoff ID reused before its released receipt was acknowledged", msgr.ErrFrame)
	}
	if decoded.Op == osd.BackoffBlock && previous != nil && previous.receipt.block.MapEpoch != decoded.MapEpoch {
		return fmt.Errorf("%w: backoff ID reused at a different epoch", msgr.ErrFrame)
	}
	matched := false
	var receipt *osdBackoffReceipt
	if decoded.Op == osd.BackoffBlock {
		if previous == nil {
			if len(b.active) >= 64 || size > b.limit-b.activeBytes {
				return ErrOSDBackoffOverflow
			}
			receipt = &osdBackoffReceipt{connection: o, block: decoded, generation: generation, globalID: globalID}
			b.active[decoded.ID] = &osdBackoffEntry{receipt: receipt, bytes: size}
			b.activeBytes += size
		} else {
			receipt = previous.receipt
		}
	} else if previous != nil {
		matched = true
		previous.unblocked = true
		// Ceph keeps a NEW->DELETING server entry until a delayed ACK_BLOCK
		// arrives (PG.cc:645-655, Session.cc:67-70). Releasing the local gate
		// before the writer reaches that ACK could let fresh requests be lost.
		if previous.ackStarted {
			delete(b.active, decoded.ID)
			b.activeBytes -= previous.bytes
		}
	}
	event := OSDBackoffEvent{Op: decoded.Op, Pool: decoded.PG.Pool, Seed: decoded.PG.Seed, Shard: decoded.Shard, MapEpoch: decoded.MapEpoch, ID: decoded.ID, Begin: backoffCursor(decoded.Begin), End: backoffCursor(decoded.End), Source: o.target, GlobalID: globalID, SessionGeneration: generation, Matched: matched, RawFront: bytes.Clone(decoded.RawFront), receipt: receipt}
	b.queue = append(b.queue, queuedOSDBackoff{event: event, bytes: size})
	b.bytes += size
	b.signalLocked()
	return nil
}

func (o *OSDConnection) backoffsLocked() *osdBackoffState {
	if o.backoffState == nil {
		o.backoffState = newOSDBackoffState(o.client.options.MaxFrameSize)
	}
	return o.backoffState
}

func (o *OSDConnection) stopBackoffs(err error) {
	o.client.mu.Lock()
	b := o.backoffsLocked()
	o.client.mu.Unlock()
	b.stop(err)
}

func (o *OSDConnection) handleBackoff(source *session.Session, m msgr.MessageData) error {
	c := o.client
	c.mu.Lock()
	accept := !c.closed && !o.closed && c.osds[o.target.ID] == o && o.session == source && c.auth.GlobalID == o.globalID
	c.mu.Unlock()
	if !accept {
		return nil
	}
	decoded, err := osd.DecodeBackoff(m, c.options.MaxFrameSize)
	if err != nil {
		return fmt.Errorf("%w: invalid OSD backoff: %w", msgr.ErrFrame, err)
	}
	return o.publishBackoff(source, decoded)
}

// Keep source admission and queue publication under the same client lock.
// Decoding can race an identity change, but an older source must not publish
// into the live connection's queue or install an active restriction afterward.
func (o *OSDConnection) publishBackoff(source *session.Session, decoded osd.Backoff) error {
	c := o.client
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || o.closed || c.osds[o.target.ID] != o || o.session != source || c.auth.GlobalID != o.globalID {
		return nil
	}
	return o.backoffsLocked().accept(o, decoded, o.generation, o.globalID)
}

// NextBackoff returns one queued control. Accepted events drain before the
// connection's stable terminal error. Cancellation consumes no event when
// already canceled. Queue and active set each retain at most 64 controls and
// a conservative MaxFrameSize byte budget; overflow fails the connection.
// Active restrictions and released pending-ACK receipts share one retained-set
// budget. A received UNBLOCK keeps its range gated until the single writer
// reaches its ACK, after refusing earlier queued matching object requests.
// The default object-hash profile returns ErrUnsupportedFeatures immediately.
func (o *OSDConnection) NextBackoff(ctx context.Context) (OSDBackoffEvent, error) {
	if err := ctx.Err(); err != nil {
		return OSDBackoffEvent{}, err
	}
	if !o.client.options.OSDProfile.replicatedPG() {
		return OSDBackoffEvent{}, ErrUnsupportedFeatures
	}
	for {
		if err := ctx.Err(); err != nil {
			return OSDBackoffEvent{}, err
		}
		c := o.client
		c.mu.Lock()
		b, s := o.backoffsLocked(), o.session
		c.mu.Unlock()
		b.mu.Lock()
		if len(b.queue) > 0 {
			next := b.queue[0]
			b.queue[0] = queuedOSDBackoff{}
			b.queue = b.queue[1:]
			b.bytes -= next.bytes
			b.mu.Unlock()
			return next.event, nil
		}
		terminal, changed := b.terminal, b.changed
		b.mu.Unlock()
		if terminal != nil {
			return OSDBackoffEvent{}, terminal
		}
		if s != nil {
			if err := s.Err(); err != nil {
				b.stop(err)
				continue
			}
		} else if o.ctx.Err() != nil {
			b.stop(ErrClosed)
			continue
		}
		var done <-chan struct{}
		baseDone := o.ctx.Done()
		if s != nil {
			done = s.Done()
			baseDone = nil
		}
		select {
		case <-changed:
		case <-done:
		case <-baseDone:
		case <-ctx.Done():
			return OSDBackoffEvent{}, ctx.Err()
		}
	}
}

// AckBackoff queues one explicit ACK_BLOCK using the retained original PG,
// epoch, ID and range, at the same priority as object requests. It does not
// acknowledge daemon receipt. After UNBLOCK, the original BLOCK event remains
// acknowledgeable; the single writer releases that range's local gate when it
// reaches the late ACK, after rejecting earlier queued matching objects.
// ctx controls queue admission only; cancellation after success cannot discard
// the queued ACK. Duplicate queued ACKs are
// refused. ACK transmission requires the replicated PG profile and its
// installed single-writer admission gate. The default profile rejects it.
func (o *OSDConnection) AckBackoff(ctx context.Context, event OSDBackoffEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	receipt := event.receipt
	if event.Op != OSDBackoffBlock || receipt == nil || receipt.connection != o {
		return ErrOSDBackoffIdentity
	}
	c := o.client
	c.mu.Lock()
	s, b := o.session, o.backoffsLocked()
	valid := !c.closed && !o.closed && c.osds[o.target.ID] == o && s != nil && o.generation == receipt.generation && o.globalID == receipt.globalID && c.auth.GlobalID == o.globalID
	c.mu.Unlock()
	if !valid {
		return ErrOSDBackoffIdentity
	}
	b.mu.Lock()
	if b.terminal != nil {
		err := b.terminal
		b.mu.Unlock()
		return err
	}
	current := b.active[receipt.block.ID]
	if current == nil || current.receipt != receipt {
		b.mu.Unlock()
		return ErrOSDBackoffIdentity
	}
	if !b.gateInstalled {
		b.mu.Unlock()
		return ErrUnsupportedFeatures
	}
	if current.acked || current.acking {
		b.mu.Unlock()
		return ErrOSDBackoffAcknowledged
	}
	current.acking = true
	b.mu.Unlock()
	message, err := osd.EncodeBackoffAck(receipt.block, c.options.MaxFrameSize)
	if err == nil {
		err = s.SendControl(ctx, message)
	}
	b.mu.Lock()
	current.acking = false
	if err == nil {
		current.acked = true
	}
	b.mu.Unlock()
	return err
}

// beforeWriteBackoff gates the matching actual PG/shard and head-object range. Only a
// single-writer pre-start hook can use this safely: an object already started
// before BLOCK must be written before a subsequent queued ACK. Refusal is a
// local unsent outcome; it never executes or replays a request after UNBLOCK.
func (o *OSDConnection) beforeWriteBackoff(message msgr.MessageData) error {
	if message.Type != osd.RequestMessage && message.Type != osd.BackoffMessage {
		return nil
	}
	var ack osd.Backoff
	if message.Type == osd.BackoffMessage {
		var err error
		ack, err = osd.DecodeBackoff(message, o.client.options.MaxFrameSize)
		if err != nil || ack.Op != osd.BackoffAckBlock || message.Transaction != 0 {
			return fmt.Errorf("%w: invalid queued backoff ACK", msgr.ErrFrame)
		}
	}
	o.client.mu.Lock()
	b := o.backoffState
	o.client.mu.Unlock()
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.gateInstalled {
		return nil
	}
	if b.terminal != nil {
		return b.terminal
	}
	if message.Type == osd.BackoffMessage {
		current := b.active[ack.ID]
		if current == nil || (!current.acking && !current.acked) || !backoffSameIdentity(current.receipt.block, ack) || current.receipt.block.MapEpoch != ack.MapEpoch {
			return ErrOSDBackoffIdentity
		}
		if current.ackStarted {
			return ErrOSDBackoffAcknowledged
		}
		// This hook runs only on the single ordered writer. Earlier matching
		// object messages have been refused; unrelated objects may pass. ACK is
		// next to start, so later matching objects cannot overtake its release.
		current.ackStarted = true
		if current.unblocked {
			delete(b.active, ack.ID)
			b.activeBytes -= current.bytes
		}
		return nil
	}
	if len(b.active) != 0 {
		target, err := o.decodeRequestAdmissionTarget(message)
		if err != nil {
			return fmt.Errorf("%w: invalid outgoing OSD admission target: %w", msgr.ErrFrame, err)
		}
		for _, entry := range b.active {
			block := entry.receipt.block
			// A BLOCK names the OSD's actual shard, while an erasure-coded
			// request may carry it in the pre-Tentacle primary-first
			// numbering (OSDPrimaryFirst), which these bytes cannot undo. So
			// any shard of a blocked erasure-coded PG matches: refusal is a
			// known unsent outcome and never lets a blocked object through.
			sameShard := block.Shard == target.Shard || block.Shard >= 0 && target.Shard >= 0
			if block.PG == target.PG && sameShard && osd.BackoffContains(block, target.Object) {
				return ErrOSDBackoff
			}
		}
	}
	return nil
}
