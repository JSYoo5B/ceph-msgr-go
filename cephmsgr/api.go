// Package cephmsgr provides native Go communication with Ceph Tentacle MON/MGR
// and explicitly targeted OSD daemons over authenticated msgr2.1: raw commands,
// wire maps and subscriptions, and bounded read-only object requests.
// Callers own command construction, result interpretation and management policy.
// No Ceph installation is required on the client host. See README.md for the
// current verification status.
package cephmsgr

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

var ErrClosed = session.ErrClosed
var ErrManagerChanged = errors.New("ceph: active manager changed")

// ErrMalformedMessage identifies invalid Messenger framing or message encoding.
// A complete malformed payload may also wrap io.ErrUnexpectedEOF from decoding;
// check this marker before treating that cause as a transient connection loss.
// Started commands still have an OutcomeUnknownError and are never replayed.
var ErrMalformedMessage = msgr.ErrFrame

// ErrLimitExceeded identifies a command or MON control message rejected by its
// size preflight, or wire data rejected by a frame, byte-length,
// collection-count or authentication
// transcript bound. Some bounds are fixed rather than
// configurable through MaxFrameSize. Check ErrMalformedMessage independently:
// a valid frame can exceed a local limit, while an invalid encoded length or
// count can identify both errors. Started commands still have an
// OutcomeUnknownError and are never replayed. Watch buffer overflow, waiting
// for a MaxInFlight slot and invalid option values have separate errors.
var ErrLimitExceeded = wire.ErrLimit

// ErrKeepaliveTimeout means an established connection received no complete
// Messenger frame within KeepaliveTimeout. Started commands remain uncertain.
var ErrKeepaliveTimeout = session.ErrKeepaliveTimeout

type OutcomeUnknownError = session.OutcomeUnknownError

// AuthenticationError is an explicit server rejection of authentication or
// renewal. It preserves the method and server result code for errors.As.
// It may also be the cause of an OutcomeUnknownError for a started command.
type AuthenticationError = cephx.AuthenticationError

// Key is an opaque CephX credential. Formatting redacts its key material.
type Key struct{ value cephx.Key }

func ParseKey(base64Key string) (Key, error) {
	k, err := cephx.ParseKey(base64Key)
	return Key{value: k}, err
}
func (k Key) String() string   { return k.value.String() }
func (k Key) GoString() string { return k.String() }

// ConnectionMode selects the authenticated Messenger transport for every MON,
// MGR and OSD connection, including renewal, recovery and named MON tell sessions.
type ConnectionMode uint8

const (
	// SecureMode encrypts and authenticates frames with AES-GCM. It is the default.
	SecureMode ConnectionMode = iota
	// CRCMode uses unencrypted frames protected by CRC32C after CephX
	// authentication and transcript verification. CRC32C detects corruption;
	// it does not provide cryptographic integrity for subsequent traffic.
	CRCMode
)

func (mode ConnectionMode) sessionMode() session.ConnectionMode {
	switch mode {
	case SecureMode:
		return session.SecureMode
	case CRCMode:
		return session.CRCMode
	default:
		return 0
	}
}

type Options struct {
	// Monitors are host:port endpoints, optionally prefixed with v2: and
	// suffixed with /nonce. IPv6 addresses must be bracketed; %zone is unsupported.
	Monitors []string
	Identity string
	Key      Key
	// ConnectionMode defaults to SecureMode. Only this mode is offered to
	// every peer; an unsupported or different selection fails without fallback.
	ConnectionMode ConnectionMode
	// EnableOSD requests and renews OSD service tickets in addition to MON/MGR
	// credentials. OpenOSD requires this explicit opt-in. It does not subscribe
	// to OSDMap or enable placement, routing or automatic object retries.
	EnableOSD bool
	// MaxOSDConnections bounds OSD connections and concurrent setups held by
	// this client. Defaults to 16; valid values are 1 through 1024. Close an
	// OSDConnection to release its slot. It is independent of MaxInFlight.
	MaxOSDConnections int
	// MaxBufferedOSDMapBytes bounds each OSD connection's unread map queue.
	// Defaults to MaxFrameSize; valid values are 1 KiB through 1 GiB. The
	// queue also permits at most 64 batches. Overflow fails that OSD session
	// instead of dropping incremental maps needed by the caller's map layer.
	MaxBufferedOSDMapBytes uint32
	// Hostname is sent unchanged in every MON subscription and effective-config
	// request for this client's lifetime. Empty is the default. The client does
	// not look up, normalize or shorten an operating-system hostname.
	Hostname string
	// ExpectedFSID pins the cluster UUID. When empty, the first authenticated
	// MonMap establishes the FSID used to validate all later maps.
	ExpectedFSID string
	// ConnectTimeout bounds each endpoint's setup, additional bootstrap
	// retries after transient connection errors, and retired MON draining.
	ConnectTimeout time.Duration
	// KeepaliveInterval controls Messenger probes, independently of TCP
	// keepalive. Defaults to 15 seconds.
	KeepaliveInterval time.Duration
	// KeepaliveTimeout bounds silence on an established connection. Defaults
	// to 45 seconds and must exceed KeepaliveInterval. Any complete valid
	// frame counts as activity; request contexts do not reset this timeout.
	KeepaliveTimeout time.Duration
	// MaxFrameSize bounds the sum of logical segment lengths per Messenger
	// frame. Defaults to 16 MiB; accepted values range from 1 KiB to 1 GiB.
	// It also applies during setup and to unsolicited maps and streams, so a
	// bound below a server's bootstrap frame can prevent admission.
	MaxFrameSize uint32
	// MaxInFlight bounds concurrent command calls, including queued requests.
	// Defaults to 64, with a maximum of 1024. Waiting for a slot respects the
	// operation's context.
	MaxInFlight int
	// DialContext defaults to net.Dialer.DialContext. Custom implementations
	// must honor context cancellation; useful for proxies and in-process tests.
	// For hostname monitor seeds, RemoteAddr must identify the resolved Ceph
	// peer as a *net.TCPAddr or provide a numeric IP:port through String().
	DialContext func(context.Context, string, string) (net.Conn, error)
}

type Command struct {
	// JSON is one Ceph command object with a non-empty string prefix.
	JSON  json.RawMessage
	Input []byte
}
type Result struct {
	Data    []byte
	Message string
	Code    int32
}
type CommandError struct {
	Code    int32
	Message string
}

func (e *CommandError) Error() string {
	return fmt.Sprintf("ceph: command code %d: %s", e.Code, e.Message)
}

func parseFSID(s string) ([16]byte, error) {
	var id [16]byte
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return id, errors.New("ceph: invalid FSID")
	}
	p, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil || len(p) != 16 {
		return id, errors.New("ceph: invalid FSID")
	}
	copy(id[:], p)
	if id == [16]byte{} {
		return id, errors.New("ceph: zero FSID")
	}
	return id, nil
}

type commandOperation uint8

const (
	monitorCommand commandOperation = iota
	managerCommand
	monitorTell
	managerTell
)

func (c *Client) MonCommand(ctx context.Context, command Command) (Result, error) {
	return c.command(ctx, command, monitorCommand)
}
func (c *Client) MgrCommand(ctx context.Context, command Command) (Result, error) {
	return c.command(ctx, command, managerCommand)
}

// MonTell sends a daemon-local command to the currently admitted MON. Its
// target can change after recovery or ticket renewal. It does not select a
// monitor by name, send to a quorum, or replay an uncertain command.
// The server requires MON read, write and execute capabilities for tell.
func (c *Client) MonTell(ctx context.Context, command Command) (Result, error) {
	return c.command(ctx, command, monitorTell)
}

// MgrTell sends a daemon-local command to the current active MGR. These commands
// use the daemon's admin command schema, distinct from MGR module commands, and
// require the server's allow-all MGR capability. Context cancellation ends the
// local wait; an uncertain command is never replayed after recovery.
func (c *Client) MgrTell(ctx context.Context, command Command) (Result, error) {
	return c.command(ctx, command, managerTell)
}

func (c *Client) command(ctx context.Context, command Command, operation commandOperation) (Result, error) {
	mgr := operation == managerCommand || operation == managerTell
	tell := operation == monitorTell || operation == managerTell
	var result Result
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if c.ctx.Err() != nil {
		return result, ErrClosed
	}
	if uint64(len(command.JSON))+uint64(len(command.Input))+256 > uint64(c.options.MaxFrameSize) {
		return result, fmt.Errorf("ceph: command exceeds frame limit: %w", ErrLimitExceeded)
	}
	select {
	case c.calls <- struct{}{}:
		defer func() { <-c.calls }()
	case <-ctx.Done():
		return result, ctx.Err()
	case <-c.ctx.Done():
		return result, ErrClosed
	}
	// A ready slot can race cancellation or Close. Check both lifetimes
	// again before validating JSON or copying caller-owned bulk input.
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if c.ctx.Err() != nil {
		return result, ErrClosed
	}
	var object struct {
		Prefix string `json:"prefix"`
	}
	if err := json.Unmarshal(command.JSON, &object); err != nil || strings.TrimSpace(object.Prefix) == "" {
		return result, errors.New("ceph: command JSON requires a string prefix")
	}
	jsonCommand := string(command.JSON)
	input := append([]byte(nil), command.Input...)
	var s *session.Session
	var err error
	c.mu.Lock()
	fsid := c.fsid
	c.mu.Unlock()
	var request msgr.MessageData
	if tell {
		// A zero FSID makes MGR interpret MCommand as a legacy module command.
		// Never substitute that different route for daemon-local tell.
		if fsid == [16]byte{} {
			return result, errors.New("ceph: tell requires an authenticated cluster FSID")
		}
		request = msgr.TellCommand(fsid, []string{jsonCommand}, input)
	} else {
		request = msgr.CommandMessage(mgr, fsid, []string{jsonCommand}, input)
	}
	var reply msgr.MessageData
	for {
		if mgr {
			s, err = c.manager(ctx)
		} else {
			s, err = c.monitor(ctx)
		}
		if err != nil {
			return result, err
		}
		reply, err = s.Call(ctx, request)
		var unknown *OutcomeUnknownError
		if errors.Is(err, session.ErrRetired) && !errors.As(err, &unknown) {
			// Retirement rejected this call before admission. Resolve the
			// new session; never resend a call whose transmission started.
			continue
		}
		break
	}
	if err != nil {
		return result, err
	}
	code, message, err := msgr.CommandReply(reply, c.options.MaxFrameSize)
	if err != nil {
		// A received but undecodable reply does not establish whether the
		// command succeeded. Keep raw output and retire the invalid session.
		result.Data = append([]byte(nil), reply.Data...)
		cause := fmt.Errorf("%w: invalid command reply: %w", msgr.ErrFrame, err)
		s.Fail(cause)
		return result, &OutcomeUnknownError{Cause: cause}
	}
	result = Result{Data: append([]byte(nil), reply.Data...), Message: message, Code: code}
	if code < 0 {
		return result, &CommandError{Code: code, Message: message}
	}
	return result, nil
}
