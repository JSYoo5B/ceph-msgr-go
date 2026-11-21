package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
)

// MGR daemon messages on a MgrPeer (protocol stage 3).
//
// Inbound, a peer's MMgrOpen, MMgrUpdate, MMgrReport, MPGStats, MMgrClose,
// MMgrCommand and MCommand are decoded in arrival order by internal/msgr
// (mgr_daemon_messages.go, pgstats.go) and delivered beside their raw
// segments; a malformed one ends the session. A report's packed counters
// are read against the declarations of the same connection
// (msgr.MgrCounterSchema): a new session, also of the same stock daemon
// after it reconnects, starts with no declarations, as the stock sender
// does. Those declarations are retained for the connection's life within
// msgr.MaxMgrCounterSchemaTypes paths and msgr.MaxMgrCounterSchemaPathBytes
// path bytes; a report that would pass either ends the session with an
// error wrapping ErrLimitExceeded, its declarations uncommitted (native
// keeps them without a bound). Nothing is interpreted beyond the wire: a
// non-service OSD's open, the metric payloads, the PG statistics, a close
// and a command mean what the consumer decides.
//
// Outbound, only three things are sent, each when the consumer asks:
// Configure, the acknowledgement of a received close
// (MgrPeerClose.Acknowledge, native handle_close's echo) and the reply to a
// received command (MgrPeerCommand.Reply, in the family it came in, with its
// TID). A close or command carries a reply handle bound to the connection
// that received it and to the message family and TID it carried, captured
// at receipt (editing the exported fields changes nothing). A handle answers at
// most once: a second answer, also one racing the first, is refused with
// ErrMgrReplyDuplicate, and once its connection has ended (a replacement
// connection of the same peer is another MgrPeer with its own handles) it
// is refused with ErrMgrReplyExpired. Both are decided under the peer's
// lock before anything is queued, so an answer after the connection's
// terminal cause never reaches the writer. Sending outcomes are distinct:
// nil means the frame was written to the socket (not that the peer
// processed it); an error wrapping ErrNotSent means nothing was sent, and
// the handle stays usable when the connection is still live (its ctx ended
// first); an OutcomeUnknownError means the write started and then failed,
// and the handle is spent. Nothing is ever resent on another connection: a
// stock client resends its own pending commands, with the same TID, on its
// new connection, which delivers them as new commands with new handles.

// ErrNotSent reports a send that is known not to have started on the wire.
var ErrNotSent = session.ErrNotSent

// ErrMgrReplyExpired refuses an answer whose connection has ended. It also
// wraps ErrNotSent.
var ErrMgrReplyExpired = errors.New("ceph: MGR reply handle expired with its connection")

// ErrMgrReplyDuplicate refuses a second answer through one handle.
var ErrMgrReplyDuplicate = errors.New("ceph: MGR reply already sent or in progress")

// MGR daemon message types, decoded by internal/msgr; see its contracts.
type (
	MgrDaemonOpen      = msgr.MgrDaemonOpen
	MgrDaemonUpdate    = msgr.MgrDaemonUpdate
	MgrDaemonReport    = msgr.MgrDaemonReport
	MgrPerfCounterType = msgr.MgrPerfCounterType
	MgrCounterValue    = msgr.MgrCounterValue
	MgrHealthMetric    = msgr.MgrHealthMetric
	MgrOpaqueEntry     = msgr.MgrOpaqueEntry
	MgrMetricPayload   = msgr.MgrMetricPayload
	MgrPGStats         = msgr.PGStats
	MgrPGStatsEntry    = msgr.PGStatEntry
	MgrConfigure       = msgr.MgrConfigureData
)

// Command families a MGR receives: MMgrCommand (a client's mgr command)
// and MCommand (a tell to this MGR). A reply goes back in the same family.
const (
	MgrCommandFamily  = msgr.MgrCommandMessage
	TellCommandFamily = msgr.TellCommandMessage
)

// MgrPeerReport is a received MMgrReport with its packed counters read
// against the connection's declarations, in packed (ascending path) order.
type MgrPeerReport struct {
	MgrDaemonReport
	Counters []MgrCounterValue
}

// MgrPeerClose is a received MMgrClose. Acknowledge sends native's echo.
type MgrPeerClose struct {
	DaemonName, ServiceName string
	Generation              uint64
	close                   msgr.MgrDaemonClose
	handle                  *mgrReply
}

// MgrPeerCommand is a received MMgrCommand or MCommand: its family, TID,
// FSID field, command strings and bulk input (the data segment), on the
// connection Generation. Executing it is the consumer's. The exported
// fields are a report of what arrived: Reply answers with the family and
// TID its handle captured on receipt, so editing them, on this value or on
// a copy sharing its handle, never changes the correlation.
type MgrPeerCommand struct {
	Family      uint16
	Transaction uint64
	FSID        [16]byte
	Command     []string
	Input       []byte
	Generation  uint64
	handle      *mgrReply
}

// mgrReply is one reply handle: its connection and, for a command, the
// family and TID received, fixed at receipt. state is guarded by its
// peer's mu.
type mgrReply struct {
	peer        *MgrPeer
	family      uint16
	transaction uint64
	state       uint8
}

const (
	replyOpen uint8 = iota
	replySending
	replyDone
)

// decode fills the decoded field of in for a MGR daemon message type.
func (p *MgrPeer) decode(m msgr.MessageData, in *MgrInboundMessage) (bool, error) {
	limit := p.listener.options.MaxFrameSize
	generation := p.identity.Generation
	var err error
	switch m.Type {
	case msgr.MgrOpenMessage:
		var o MgrDaemonOpen
		if o, err = msgr.DecodeMgrDaemonOpen(m, limit); err == nil {
			in.Open = &o
		}
	case msgr.MgrUpdateMessage:
		var u MgrDaemonUpdate
		if u, err = msgr.DecodeMgrDaemonUpdate(m, limit); err == nil {
			in.Update = &u
		}
	case msgr.MgrReportMessage:
		var r MgrDaemonReport
		if r, err = msgr.DecodeMgrDaemonReport(m, limit); err == nil {
			var counters []MgrCounterValue
			if counters, err = p.schema.Apply(r); err == nil {
				in.Report = &MgrPeerReport{MgrDaemonReport: r, Counters: counters}
			}
		}
	case msgr.PGStatsMessage:
		var s MgrPGStats
		if s, err = msgr.DecodePGStats(m, limit); err == nil {
			in.PGStats = &s
		}
	case msgr.MgrCloseMessage:
		var c msgr.MgrDaemonClose
		if c, err = msgr.DecodeMgrDaemonClose(m, limit); err == nil {
			in.Close = &MgrPeerClose{DaemonName: c.DaemonName, ServiceName: c.ServiceName, Generation: generation, close: c, handle: &mgrReply{peer: p}}
		}
	case msgr.MgrCommandMessage, msgr.TellCommandMessage:
		var c msgr.MgrCommandRequest
		if c, err = msgr.DecodeMgrCommandRequest(m, limit); err == nil {
			in.Command = &MgrPeerCommand{Family: m.Type, Transaction: m.Transaction, FSID: c.FSID, Command: c.Command,
				Input: slices.Clone(m.Data), Generation: generation, handle: &mgrReply{peer: p, family: m.Type, transaction: m.Transaction}}
		}
	default:
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("ceph: MGR peer %s message type %#x refused: %w", p.identity.Entity, m.Type, err)
	}
	return true, nil
}

func notSent(err error) error { return fmt.Errorf("%w: %w", ErrNotSent, err) }

// answer sends one reply through h, once.
func (h *mgrReply) answer(ctx context.Context, m msgr.MessageData) error {
	p := h.peer
	p.mu.Lock()
	switch {
	case h.state != replyOpen:
		p.mu.Unlock()
		return ErrMgrReplyDuplicate
	case p.err != nil:
		cause := p.err
		h.state = replyDone
		p.mu.Unlock()
		return fmt.Errorf("%w: %w", ErrMgrReplyExpired, notSent(cause))
	case ctx.Err() != nil:
		p.mu.Unlock()
		return notSent(ctx.Err())
	}
	h.state = replySending
	p.mu.Unlock()
	err := p.session.Deliver(ctx, m)
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case err == nil:
		h.state = replyDone
		return nil
	case errors.Is(err, ErrNotSent):
		if p.err != nil || p.session.Err() != nil {
			h.state = replyDone
			return fmt.Errorf("%w: %w", ErrMgrReplyExpired, err)
		}
		h.state = replyOpen
		return err
	default:
		h.state = replyDone
		return err
	}
}

// Acknowledge sends native handle_close's acknowledgement: the received
// MMgrClose, byte for byte, back on its connection. A stock MgrClient
// waits for it (up to mgr_client_service_daemon_unregister_timeout) before
// it shuts down. Removing the daemon from any state is the consumer's.
func (c *MgrPeerClose) Acknowledge(ctx context.Context) error {
	if c == nil || c.handle == nil {
		return errors.New("ceph: no received MGR close to acknowledge")
	}
	m, err := msgr.MgrCloseEcho(c.close)
	if err != nil {
		return err
	}
	return c.handle.answer(ctx, m)
}

// Reply answers the command in its family (MMgrCommandReply or
// MCommandReply) with its TID: the signed result, the status string and
// the bulk output as the data segment. The family and TID are the
// received ones, captured by the handle; the Family and Transaction fields
// are not read. Nothing is retried; see the outcomes above.
func (c *MgrPeerCommand) Reply(ctx context.Context, result int32, status string, output []byte) error {
	if c == nil || c.handle == nil {
		return errors.New("ceph: no received MGR command to reply to")
	}
	h := c.handle
	m, err := msgr.CommandReplyFor(h.family, h.transaction, result, status, output, h.peer.listener.options.MaxFrameSize)
	if err != nil {
		return err
	}
	return c.handle.answer(ctx, m)
}

// Configure sends MMgrConfigure 4/1 to the peer: the reporting period and
// threshold and any OSD perf-metric queries or metric configuration, all
// the consumer's (native DaemonServer sends one in reply to every open and
// again when its configuration changes). An MDS metric configuration goes
// as absent to a peer without SERVER_PACIFIC, as native should_encode
// decides; this acceptor never negotiates it. The outcomes are those of a
// reply: nil once written to the socket, ErrNotSent, or an
// OutcomeUnknownError.
func (p *MgrPeer) Configure(ctx context.Context, c MgrConfigure) error {
	m, err := msgr.EncodeMgrConfigure(c, p.identity.Features, p.listener.options.MaxFrameSize)
	if err != nil {
		return err
	}
	p.mu.Lock()
	cause := p.err
	p.mu.Unlock()
	if cause != nil {
		return notSent(cause)
	}
	return p.session.Deliver(ctx, m)
}
