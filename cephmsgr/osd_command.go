package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// OSDCommandPG identifies an actual placement group selected by the caller's
// routing layer. Seed is the already folded PG seed from the caller's OSDMap,
// not an object placement hash. Primary must equal the connection's OSD ID.
// MapEpoch is local submission metadata only; MCommand carries no epoch and
// the OSD resolves the pgid against its own current map.
type OSDCommandPG struct {
	Pool     uint64
	Seed     uint32
	Primary  int32
	MapEpoch uint32
}

// OSDCommandSubmission is local metadata retained when an admitted command
// ends with an error. It is not a server reply or proof of execution.
type OSDCommandSubmission struct {
	Transaction, GlobalID uint64
	OSD                   int32
	PG                    *OSDCommandPG
}

// OSDCommandResult preserves the raw MCommandReply output. Result.Code is the
// server's signed wire errno (Linux numbering), not a host errno constant.
type OSDCommandResult struct {
	Result
	Submission OSDCommandSubmission
}

// ErrOSDCommandTarget rejects a PG command whose JSON pgid or primary differs
// from the caller's explicit PG metadata, or a plain OSD command containing a
// pgid. It is returned before transmission.
var ErrOSDCommandTarget = errors.New("ceph: OSD command target differs from its explicit metadata")

// Command sends one raw daemon-local command (MCommand) to this explicitly
// targeted OSD on the same lossy session as object requests. The OSD requires
// the client's allow-all OSD capability; a refusal is a CommandError with code
// -13. A JSON object containing "pgid" must use PGCommand. ctx governs the
// local wait including MaxInFlight admission; cancellation after transmission
// starts returns OutcomeUnknownError. Commands are never retried or resent
// after reconnection, an EAGAIN reply or a map change.
func (o *OSDConnection) Command(ctx context.Context, command Command) (OSDCommandResult, error) {
	return o.command(ctx, command, nil)
}

// PGCommand sends one raw PG command to this OSD, which the caller selected as
// the PG's primary. The JSON must contain "pgid" equal to the canonical
// "<pool>.<hex seed>" of pg; the client validates and never injects or rewrites
// it. A non-primary OSD replies -11 (EAGAIN) and an OSD without the PG -2
// (ENOENT); neither executed the command and neither is retried or redirected.
func (o *OSDConnection) PGCommand(ctx context.Context, pg OSDCommandPG, command Command) (OSDCommandResult, error) {
	return o.command(ctx, command, &pg)
}

func (o *OSDConnection) command(ctx context.Context, command Command, pg *OSDCommandPG) (OSDCommandResult, error) {
	var result OSDCommandResult
	c := o.client
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if c.ctx.Err() != nil || o.ctx.Err() != nil {
		return result, ErrClosed
	}
	if uint64(len(command.JSON))+uint64(len(command.Input))+256 > uint64(c.options.MaxFrameSize) {
		return result, fmt.Errorf("ceph: OSD command exceeds frame limit: %w", ErrLimitExceeded)
	}
	select {
	case c.calls <- struct{}{}:
		defer func() { <-c.calls }()
	case <-ctx.Done():
		return result, ctx.Err()
	case <-o.ctx.Done():
		return result, ErrClosed
	}
	operation, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(o.ctx, cancel)
	defer func() { stop(); cancel() }()
	// A ready slot can race cancellation or Close. Check both lifetimes
	// again before validating JSON or copying caller-owned bulk input.
	if err := operation.Err(); err != nil {
		return result, o.operationError(err)
	}
	var object map[string]json.RawMessage
	var prefix string
	if err := json.Unmarshal(command.JSON, &object); err != nil || object == nil || json.Unmarshal(object["prefix"], &prefix) != nil || strings.TrimSpace(prefix) == "" {
		return result, errors.New("ceph: command JSON requires a string prefix")
	}
	if raw, ok := object["pgid"]; pg == nil && ok {
		return result, fmt.Errorf("%w: pgid requires PGCommand", ErrOSDCommandTarget)
	} else if pg != nil {
		var pgid string
		if !ok || json.Unmarshal(raw, &pgid) != nil || pgid != fmt.Sprintf("%d.%x", pg.Pool, pg.Seed) || pg.Primary != o.target.ID {
			return result, ErrOSDCommandTarget
		}
	}
	if _, err := c.monitor(operation); err != nil {
		return result, o.operationError(err)
	}
	c.mu.Lock()
	s, closed, globalID, fsid := o.session, c.closed || o.closed, c.auth.GlobalID, c.fsid
	c.mu.Unlock()
	if closed {
		return result, ErrClosed
	}
	if globalID != o.globalID {
		return result, errors.New("ceph: OSD connection belongs to an earlier client identity")
	}
	if fsid == [16]byte{} {
		return result, errors.New("ceph: OSD command requires an authenticated cluster FSID")
	}
	request := msgr.TellCommand(fsid, []string{string(command.JSON)}, append([]byte(nil), command.Input...))
	reply, err := s.Call(operation, request)
	result.Submission = OSDCommandSubmission{Transaction: reply.Transaction, GlobalID: o.globalID, OSD: o.target.ID}
	if pg != nil {
		copied := *pg
		result.Submission.PG = &copied
	}
	if err != nil {
		return result, o.operationError(err)
	}
	code, message, err := msgr.CommandReply(reply, c.options.MaxFrameSize)
	if err != nil {
		// A received but undecodable reply does not establish whether the
		// command ran. Keep raw output and retire the invalid OSD session.
		result.Data = append([]byte(nil), reply.Data...)
		cause := fmt.Errorf("%w: invalid OSD command reply: %w", msgr.ErrFrame, err)
		s.Fail(cause)
		return result, &OutcomeUnknownError{Cause: cause}
	}
	result.Result = Result{Data: append([]byte(nil), reply.Data...), Message: message, Code: code}
	if code < 0 {
		return result, &CommandError{Code: code, Message: message}
	}
	return result, nil
}
