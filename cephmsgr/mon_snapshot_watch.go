package cephmsgr

import (
	"context"

	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
)

// monSnapshotWatch owns the lifecycle shared by the MON full-snapshot watches
// (ConfigStream, DigestStream and ServiceMapStream): the first terminal cause,
// the registered and observed MON sessions, release of the client slot that
// still names this watch, the waiter signal and the registration worker that
// follows MON recovery. Each stream keeps its typed unread value, registration
// payload, decoding, publication and errors. The FIFO log and OSDMap streams
// have different queue and cursor semantics and do not use this type.
//
// Error inspection, cancellation and Send may invoke caller code, so none of
// them runs under client.mu.
type monSnapshotWatch[S any] struct {
	client *Client
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	limit  uint64
	// slot is the client field that names the active watch of this kind and
	// self is the stream that owns this state. Only a watch still named by
	// its slot releases it, so a stale callback cannot stop a replacement.
	slot **S
	self *S
	// register sends the stream's subscription payload on an admitted MON.
	// identity is the authenticated client name captured with the decision.
	register func(ctx context.Context, mon *session.Session, identity string) error

	// All fields below are protected by client.mu.
	source   *session.Session // admitted MON this watch registered on; nil requests registration
	observed *session.Session // last registered MON, retained until terminal for failure attribution
	terminal error
	changed  chan struct{}
}

func newMonSnapshotWatch[S any](c *Client, ctx context.Context, cancel context.CancelFunc, limit uint64, slot **S, self *S, register func(context.Context, *session.Session, string) error) monSnapshotWatch[S] {
	return monSnapshotWatch[S]{client: c, ctx: ctx, cancel: cancel, done: make(chan struct{}), limit: limit, slot: slot, self: self, register: register, changed: make(chan struct{})}
}

// monSnapshotLifecycle lets the client coordinate every active snapshot watch
// without knowing its typed unread value.
type monSnapshotLifecycle interface {
	stopLocked(error) bool
	failSourceLocked(*session.Session, error) bool
	resetSourceLocked()
	cancelWatch()
}

// monSnapshotWatchesLocked returns the active snapshot watches. mu held.
func (c *Client) monSnapshotWatchesLocked() []monSnapshotLifecycle {
	watches := make([]monSnapshotLifecycle, 0, 3)
	if c.configWatch != nil {
		watches = append(watches, &c.configWatch.watch)
	}
	if c.digestWatch != nil {
		watches = append(watches, &c.digestWatch.watch)
	}
	if c.serviceMapWatch != nil {
		watches = append(watches, &c.serviceMapWatch.watch)
	}
	return watches
}

// stopMonSnapshotsLocked records err as the terminal cause of every active
// snapshot watch that has none and returns the watches to cancel after mu is
// released. mu held.
func (c *Client) stopMonSnapshotsLocked(err error) []monSnapshotLifecycle {
	var stopped []monSnapshotLifecycle
	for _, watch := range c.monSnapshotWatchesLocked() {
		if watch.stopLocked(err) {
			stopped = append(stopped, watch)
		}
	}
	return stopped
}

// resetMonSnapshotSourcesLocked requests a fresh full-snapshot registration
// from every active watch, including when the worker observes only the
// restoration of the same MON session after a failed candidate. mu held.
func (c *Client) resetMonSnapshotSourcesLocked() {
	for _, watch := range c.monSnapshotWatchesLocked() {
		watch.resetSourceLocked()
	}
}

// stopMonSnapshotsForFailedSession records a registered source's fatal cause
// before a replacement can make its delayed onClose callback stale.
func (c *Client) stopMonSnapshotsForFailedSession(source *session.Session, err error) {
	if !fatalLogSessionError(err) {
		return
	}
	var stopped []monSnapshotLifecycle
	c.mu.Lock()
	for _, watch := range c.monSnapshotWatchesLocked() {
		if watch.failSourceLocked(source, err) {
			stopped = append(stopped, watch)
		}
	}
	c.mu.Unlock()
	for _, watch := range stopped {
		watch.cancelWatch()
	}
}

func (w *monSnapshotWatch[S]) signalLocked() {
	close(w.changed)
	w.changed = make(chan struct{})
}

func (w *monSnapshotWatch[S]) ownsSlotLocked() bool { return *w.slot == w.self }

// stopLocked records the first terminal cause, clears both sessions, releases
// the slot only while it still names this watch and wakes every waiter.
// It reports whether this call recorded the cause. mu held.
func (w *monSnapshotWatch[S]) stopLocked(err error) bool {
	if w.terminal != nil {
		return false
	}
	w.terminal, w.source, w.observed = err, nil, nil
	if w.ownsSlotLocked() {
		*w.slot = nil
	}
	w.signalLocked()
	return true
}

func (w *monSnapshotWatch[S]) stop(err error) {
	w.client.mu.Lock()
	w.stopLocked(err)
	w.client.mu.Unlock()
	w.cancel()
}

func (w *monSnapshotWatch[S]) cancelWatch() { w.cancel() }

func (w *monSnapshotWatch[S]) resetSourceLocked() { w.source = nil }

// failSourceLocked stops the watch registered in the slot on the current MON
// source with that source's fatal cause. mu held.
func (w *monSnapshotWatch[S]) failSourceLocked(source *session.Session, err error) bool {
	return w.ownsSlotLocked() && w.client.mon == source && w.observed == source && w.stopLocked(err)
}

// failCurrent is failSourceLocked for the worker and Close: an ordinary client
// shutdown or explicit authentication rejection already owns the cause.
func (w *monSnapshotWatch[S]) failCurrent(source *session.Session, err error) bool {
	c := w.client
	c.mu.Lock()
	current := w.terminal == nil && !c.closed && c.authErr == nil && w.failSourceLocked(source, err)
	c.mu.Unlock()
	if current {
		w.cancel()
	}
	return current
}

// acceptsLocked reports whether a payload from source belongs to this watch's
// current registration on the admitted MON. mu held.
func (w *monSnapshotWatch[S]) acceptsLocked(source *session.Session) bool {
	c := w.client
	return w.ownsSlotLocked() && w.terminal == nil && w.source == source && c.mon == source && c.monReady && !c.closed && c.authErr == nil
}

// closeWatch preserves an observed source's fatal cause, then records closed
// only when no earlier cause exists, and joins the registration worker.
func (w *monSnapshotWatch[S]) closeWatch(closed error) {
	w.client.mu.Lock()
	source := w.observed
	w.client.mu.Unlock()
	if source != nil {
		err := source.Err()
		if fatalLogSessionError(err) {
			w.failCurrent(source, err)
		}
	}
	w.stop(closed)
	<-w.done
}

// run registers on each admitted MON session until the watch is terminal.
func (w *monSnapshotWatch[S]) run() {
	defer w.client.wg.Done()
	defer close(w.done)
	for {
		if err := w.ctx.Err(); err != nil {
			w.stop(err)
			return
		}
		c := w.client
		c.mu.Lock()
		terminal, closed, authErr := w.terminal, c.closed, c.authErr
		mon, ready, changed := c.mon, c.monReady, c.changed
		previous := w.observed
		if mon != previous || !ready {
			w.source = nil
		}
		c.mu.Unlock()
		if terminal != nil {
			return
		}
		if closed {
			w.stop(ErrClosed)
			return
		}
		if authErr != nil {
			w.stop(authErr)
			return
		}
		if previous != nil && previous == mon && fatalLogSessionError(previous.Err()) {
			if w.failCurrent(previous, previous.Err()) {
				return
			}
			continue
		}
		if mon != nil && ready && mon.Err() == nil {
			c.mu.Lock()
			register := w.terminal == nil && c.mon == mon && c.monReady && !c.closed && c.authErr == nil && w.source != mon
			identity := ""
			if register {
				w.source, w.observed = mon, mon
				identity = c.auth.Name
			}
			c.mu.Unlock()
			if register {
				if err := w.register(w.ctx, mon, identity); err != nil {
					if watchErr := w.ctx.Err(); watchErr != nil {
						w.stop(watchErr)
						return
					}
					if fatalLogSessionError(err) {
						if w.failCurrent(mon, err) {
							return
						}
						continue
					}
					c.wakeMonitor()
				}
			}
		}
		var monDone <-chan struct{}
		if mon != nil {
			monDone = mon.Done()
		}
		select {
		case <-changed:
		case <-monDone:
			if (ready || previous == mon) && fatalLogSessionError(mon.Err()) {
				if w.failCurrent(mon, mon.Err()) {
					return
				}
			}
			select {
			case <-changed:
			case <-w.ctx.Done():
			case <-c.ctx.Done():
			}
		case <-w.ctx.Done():
		case <-c.ctx.Done():
		}
	}
}
