package cephmsgr

import (
	"context"
	"errors"
	"fmt"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
)

// MGR daemon reports to MON (protocol stage 3, SPEC.md "Protocol stages").
//
// An active native MGR sends its MON an MMonMgrReport every report period
// (ceph/ceph/blob/v20.2.4/src/mgr/DaemonServer.cc, send_report): its health
// checks, an encoded ServiceMap when it changed, its progress events and its
// own GID in the front, and a PGMapDigest in the data segment. Here every
// part is the consumer's: the library encodes what the consumer supplies
// (internal/msgr mgr_report.go), adds this daemon's global ID and carries
// it on one MON session. It computes no statistic, aggregates no PGMap,
// keeps no report schedule and resends nothing. The digest is the
// consumer's pre-encoded PGMapDigest in the encoding a Tentacle MGR uses
// (structure 5/1 with CEPH_FEATURES_ALL); the library reads it completely
// before anything is sent and refuses what the MON could not decode, since
// the MON decodes it outside an exception handler.
//
// The MON replies nothing and accepts a report only from the MgrMap's
// active GID (ceph/ceph/blob/v20.2.4/src/mon/MgrStatMonitor.cc,
// preprocess_report): from a standby, or after this daemon lost the active
// role, it drops the report without a trace on the wire. A nil SendReport
// therefore means a local write on that MON session, never MON acceptance;
// what the MON committed is observed through MON reads (Client.Statfs
// answers from the committed digest, `health detail` lists the committed
// checks).
//
// Reports go through a MgrMonitorSession, a handle bound to one admitted MON
// session. When that session ends (MON loss and reconnection, a session
// failure, Close or a terminal daemon cause) the handle expires: SendReport
// through it is refused with ErrMgrMonitorExpired before anything is
// queued, and one already waiting to be written is withdrawn under the
// daemon lock that records the end, so nothing is written on that session
// after its end was recorded. The report is never moved to the replacement
// session; the consumer obtains a new handle (Monitor) and decides what to
// send there. Outcomes are those of the other daemon-role sends: nil once
// the frame was written to the socket; an error wrapping ErrNotSent when it
// never started; an OutcomeUnknownError when the write started and then
// failed or was abandoned.

// ErrMgrMonitorExpired refuses a send through a MgrMonitorSession whose MON
// session has ended. It wraps ErrNotSent and the session's cause.
var ErrMgrMonitorExpired = errors.New("ceph: MGR daemon MON session handle expired")

// ErrMgrReportContent refuses report content the MON could not decode or a
// native map could not hold: a duplicate health check code or progress
// event ID, an unknown severity, a missing or malformed digest or a
// malformed ServiceMap. Nothing was sent.
var ErrMgrReportContent = msgr.ErrMgrReportContent

// Health check severities (health_status_t) of MgrHealthCheck.Severity.
const (
	MgrHealthError = msgr.HealthError
	MgrHealthWarn  = msgr.HealthWarn
	MgrHealthOK    = msgr.HealthOK
)

// MgrHealthCheck is one health check of a report, keyed by Code, with its
// ordered Detail lines. The MON replaces the checks of its MGR stat service
// with the report's, so a check absent from the next accepted report is
// cleared. MgrProgressEvent is one progress event, keyed by ID
// (AddToStatus is ProgressEvent.add_to_ceph_s). Both are the encoder's own
// types, so a report is bounded before any of its lists is copied.
type (
	MgrHealthCheck   = msgr.HealthCheck
	MgrProgressEvent = msgr.ProgressEvent
)

// MgrMonReport is the consumer's content of one report. ServiceMap is an
// encoded ServiceMap or empty: the MON keeps its current ServiceMap for an
// empty one, and a nonempty one replaces it whatever its epoch. Digest is
// one encoded PGMapDigest and is required.
type MgrMonReport struct {
	HealthChecks   []MgrHealthCheck
	ServiceMap     []byte
	ProgressEvents []MgrProgressEvent
	Digest         []byte
}

// MgrMonitorSession is a handle bound to one admitted MON session of a
// MgrDaemon. Its identity is fixed when Monitor returns it.
type MgrMonitorSession struct {
	daemon     *MgrDaemon
	session    *session.Session
	generation uint64
	endpoint   string
}

// mgrMonitorSend is one SendReport between its admission and its outcome;
// cancel withdraws it with the recorded end of its session.
type mgrMonitorSend struct {
	session *session.Session
	cancel  context.CancelCauseFunc
}

// expireSendsLocked withdraws every send in flight on a session other than
// keep (nil: every send) with cause. A withdrawn send whose write has not
// started is never written (session.Deliver decides that under the
// session's lock). daemon.mu held.
func (d *MgrDaemon) expireSendsLocked(keep *session.Session, cause error) {
	for send := range d.sends {
		if send.session != keep {
			send.cancel(cause)
			delete(d.sends, send)
		}
	}
}

// Monitor waits under ctx for an admitted MON session and returns a handle
// bound to it. ctx bounds only this wait; a daemon that closed or failed
// returns its terminal cause.
func (d *MgrDaemon) Monitor(ctx context.Context) (*MgrMonitorSession, error) {
	done := ctx.Done()
	for {
		d.mu.Lock()
		if err := d.terminalLocked(); err != nil {
			d.mu.Unlock()
			return nil, err
		}
		if signaled(done) {
			d.mu.Unlock()
			return nil, ctx.Err()
		}
		if s := d.mon; s != nil && d.monReady && s.Err() == nil {
			m := &MgrMonitorSession{daemon: d, session: s, generation: d.monGeneration}
			if remote := s.RemoteAddr(); remote != nil {
				m.endpoint = remote.String()
			}
			d.mu.Unlock()
			return m, nil
		}
		changed := d.changed
		d.mu.Unlock()
		select {
		case <-changed:
		case <-done:
			return nil, ctx.Err()
		}
	}
}

// Generation is the MON session's local admission count, as
// MgrDaemonState.Monitor.Generation and MgrMapEvent.MonitorGeneration.
func (m *MgrMonitorSession) Generation() uint64 { return m.generation }

// Endpoint is the MON session's peer address.
func (m *MgrMonitorSession) Endpoint() string { return m.endpoint }

// Done is closed once the MON session has ended; the handle has expired by
// then.
func (m *MgrMonitorSession) Done() <-chan struct{} { return m.session.Done() }

func expired(cause error) error { return fmt.Errorf("%w: %w", ErrMgrMonitorExpired, notSent(cause)) }

// SendReport sends one MMonMgrReport with this daemon's global ID on the
// handle's MON session. The content is checked and encoded before anything
// else (ErrMgrReportContent, or ErrLimitExceeded when the frame exceeds
// MaxFrameSize, which is decided from the content's lengths before any of
// it is copied). Then, under the daemon lock, a handle whose session has
// ended is refused with ErrMgrMonitorExpired and a ctx that has ended with
// an error wrapping ErrNotSent. Otherwise the report waits for the session's
// ordered writer; see the package-level outcomes above. Nothing is retried,
// and a report is never sent on another session.
func (m *MgrMonitorSession) SendReport(ctx context.Context, report MgrMonReport) error {
	d := m.daemon
	// The caller's slices are shared, not copied: the encoder bounds the
	// whole frame first and sorts only its own copies.
	content := msgr.MonMgrReport{HealthChecks: report.HealthChecks, ServiceMap: report.ServiceMap, Progress: report.ProgressEvents, Digest: report.Digest}
	d.mu.Lock()
	content.GlobalID = d.identity.GlobalID
	d.mu.Unlock()
	message, err := msgr.EncodeMonMgrReport(content, d.options.MaxFrameSize)
	if err != nil {
		return err
	}
	done := ctx.Done()
	send, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	entry := &mgrMonitorSend{session: m.session, cancel: cancel}
	d.mu.Lock()
	switch {
	case d.terminalLocked() != nil:
		cause := d.terminalLocked()
		d.mu.Unlock()
		return expired(cause)
	case !d.ownsLocked(m.session) || !d.monReady:
		cause := m.session.Err()
		d.mu.Unlock()
		if cause == nil {
			cause = errors.New("ceph: MGR daemon MON session replaced")
		}
		return expired(cause)
	case signaled(done):
		d.mu.Unlock()
		return notSent(ctx.Err())
	}
	d.sends[entry] = struct{}{}
	d.mu.Unlock()
	err = m.session.Deliver(send, message)
	d.mu.Lock()
	delete(d.sends, entry)
	d.mu.Unlock()
	if err == nil {
		return nil
	}
	// A cancellation of send that did not come from ctx is the recorded end
	// of the session.
	var withdrawn error
	if ctx.Err() == nil && send.Err() != nil {
		withdrawn = context.Cause(send)
	}
	var unknown *OutcomeUnknownError
	switch {
	case errors.As(err, &unknown):
		if withdrawn != nil {
			return &OutcomeUnknownError{Cause: withdrawn}
		}
		return err
	case errors.Is(err, ErrNotSent) && withdrawn != nil:
		return expired(withdrawn)
	case errors.Is(err, ErrNotSent) && ctx.Err() == nil && m.session.Err() != nil:
		return expired(m.session.Err())
	}
	return err
}
