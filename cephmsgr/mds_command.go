package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// MDSCommandSubmission preserves local attempt identity, including the original
// caller's target. Target.FilesystemID is a routing label, not an authenticated
// wire FSCID. This receipt does not prove execution or server permission.
type MDSCommandSubmission struct {
	Transaction, GlobalID, ConnectionGeneration uint64
	Target                                      MDSTarget
}

// MDSCommandResult preserves raw binary output, server text and the signed
// wire code (Linux numbering), including when Command returns CommandError.
type MDSCommandResult struct {
	Result
	Submission MDSCommandSubmission
}

// Command sends one raw MCommand to the explicitly targeted MDS. It sends no
// CephFS session OPEN, follows no redirect and never replays an uncertain send.
// ctx bounds slot admission and this local wait; cancellation after transmission
// starts preserves OutcomeUnknownError. A closed CephFS application session
// does not prevent native MDS command dispatch. Native allow_all command checks
// can ignore an fsname restriction when UID/path are unrestricted, so the local
// target and capability text do not establish filesystem permission isolation.
func (m *MDSConnection) Command(ctx context.Context, command Command) (MDSCommandResult, error) {
	var result MDSCommandResult
	c := m.client
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if c.ctx.Err() != nil || m.ctx.Err() != nil {
		return result, ErrClosed
	}
	if uint64(len(command.JSON))+uint64(len(command.Input))+256 > uint64(c.options.MaxFrameSize) {
		return result, fmt.Errorf("ceph: MDS command exceeds frame limit: %w", ErrLimitExceeded)
	}
	select {
	case c.calls <- struct{}{}:
		defer func() { <-c.calls }()
	case <-ctx.Done():
		return result, ctx.Err()
	case <-m.ctx.Done():
		return result, ErrClosed
	}
	operation, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(m.ctx, cancel)
	defer func() { stop(); cancel() }()
	if err := operation.Err(); err != nil {
		return result, m.operationError(err)
	}
	c.mu.Lock()
	s := m.session
	c.mu.Unlock()
	if err := s.Err(); err != nil {
		return result, err
	}
	var object struct {
		Prefix string `json:"prefix"`
	}
	if err := json.Unmarshal(command.JSON, &object); err != nil || strings.TrimSpace(object.Prefix) == "" {
		return result, errors.New("ceph: command JSON requires a string prefix")
	}
	if _, err := c.monitor(operation); err != nil {
		return result, m.operationError(err)
	}
	c.mu.Lock()
	closed, globalID, fsid := c.closed || m.closed, c.auth.GlobalID, c.fsid
	c.mu.Unlock()
	if closed {
		return result, ErrClosed
	}
	if globalID != m.globalID {
		return result, ErrMDSIdentityChanged
	}
	if fsid == [16]byte{} {
		return result, errors.New("ceph: MDS command requires an authenticated cluster FSID")
	}
	request := msgr.TellCommand(fsid, []string{string(command.JSON)}, append([]byte(nil), command.Input...))
	// Message::encode normalizes the native MCommand constructor's unset compat
	// value to its version on a fresh payload. Use that exact current wire header
	// here without changing other Tell users' independently selected contracts.
	request.CompatVersion = 1
	reply, err := s.Call(operation, request)
	result.Submission = MDSCommandSubmission{Transaction: reply.Transaction, GlobalID: m.globalID, ConnectionGeneration: m.generation, Target: m.target}
	if err != nil {
		return result, m.operationError(err)
	}
	var code int32
	var message string
	if reply.Type != msgr.TellCommandReplyMessage || reply.Version != 1 || reply.CompatVersion != 1 {
		err = wire.ErrVersion
	} else if len(reply.Middle) != 0 {
		err = msgr.ErrFrame
	} else if uint64(len(reply.Front))+uint64(len(reply.Middle))+uint64(len(reply.Data))+41 > uint64(c.options.MaxFrameSize) {
		err = wire.ErrLimit
	} else {
		code, message, err = msgr.CommandReply(reply, c.options.MaxFrameSize)
	}
	result.Data = append([]byte(nil), reply.Data...)
	if err != nil {
		cause := fmt.Errorf("%w: invalid MDS command reply: %w", msgr.ErrFrame, err)
		s.Fail(cause)
		return result, &OutcomeUnknownError{Cause: cause}
	}
	result.Code, result.Message = code, message
	if code < 0 {
		return result, &CommandError{Code: code, Message: message}
	}
	return result, nil
}
