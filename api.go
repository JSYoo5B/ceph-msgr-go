// Package cephmsgr provides native Go MON/MGR management commands over Ceph
// Tentacle's authenticated msgr2.1 transport. No Ceph installation is required
// on the client host. See README.md for the current verification status.
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
)

var ErrClosed = session.ErrClosed
var ErrManagerChanged = errors.New("ceph: active manager changed")

type OutcomeUnknownError = session.OutcomeUnknownError

// Key is an opaque CephX credential. Formatting redacts its key material.
type Key struct{ value cephx.Key }

func ParseKey(base64Key string) (Key, error) {
	k, err := cephx.ParseKey(base64Key)
	return Key{value: k}, err
}
func (k Key) String() string   { return k.value.String() }
func (k Key) GoString() string { return k.String() }

type Options struct {
	// Monitors are host:port endpoints, optionally prefixed with v2:.
	Monitors []string
	Identity string
	Key      Key
	// ExpectedFSID pins the cluster UUID. When empty, the first authenticated
	// MonMap establishes the FSID used to validate all later maps.
	ExpectedFSID   string
	ConnectTimeout time.Duration
	MaxFrameSize   uint32
	// MaxInFlight bounds concurrent command calls, including queued requests.
	// Defaults to 64. Waiting for a slot respects the operation's context.
	MaxInFlight int
	// DialContext defaults to net.Dialer.DialContext. Custom implementations
	// must honor context cancellation; useful for proxies and in-process tests.
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

func (c *Client) MonCommand(ctx context.Context, command Command) (Result, error) {
	return c.command(ctx, command, false)
}
func (c *Client) MgrCommand(ctx context.Context, command Command) (Result, error) {
	return c.command(ctx, command, true)
}
func (c *Client) command(ctx context.Context, command Command, mgr bool) (Result, error) {
	var result Result
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if uint64(len(command.JSON))+uint64(len(command.Input))+256 > uint64(c.options.MaxFrameSize) {
		return result, errors.New("ceph: command exceeds frame limit")
	}
	select {
	case c.calls <- struct{}{}:
		defer func() { <-c.calls }()
	case <-ctx.Done():
		return result, ctx.Err()
	case <-c.ctx.Done():
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
	if mgr {
		s, err = c.manager(ctx)
	} else {
		s, err = c.monitor(ctx)
	}
	if err != nil {
		return result, err
	}
	c.mu.Lock()
	fsid := c.fsid
	c.mu.Unlock()
	reply, err := s.Call(ctx, msgr.CommandMessage(mgr, fsid, []string{jsonCommand}, input))
	if err != nil {
		return result, err
	}
	code, message, err := msgr.CommandReply(reply)
	if err != nil {
		return result, err
	}
	result = Result{Data: append([]byte(nil), reply.Data...), Message: message, Code: code}
	if code < 0 {
		return result, &CommandError{Code: code, Message: message}
	}
	return result, nil
}
