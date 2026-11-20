package cephmsgr

import (
	"context"
	"errors"
	"fmt"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
)

// MapKind names a map whose committed version range MapVersion reports.
type MapKind string

const (
	OSDMapKind MapKind = "osdmap"
	MonMapKind MapKind = "monmap"
	// MDSMapKind and FSMapKind both name the MON's filesystem map service.
	MDSMapKind MapKind = "mdsmap"
	FSMapKind  MapKind = "fsmap"
)

// MapVersionResult is the MON's committed version range for one map: Newest
// is the latest committed epoch (the newest map's epoch), Oldest the oldest
// version the MON still keeps.
type MapVersionResult struct {
	Newest, Oldest uint64
}

// MapVersion asks the admitted MON for a map's committed version range with
// MMonGetVersion, as librados wait_for_latest_osdmap does before waiting for
// that epoch. It fetches no map. The MON answers once it is in quorum and the
// service is readable; ctx bounds the wait, which ends as an
// OutcomeUnknownError after transmission (the request is read-only).
//
// The MON copies the request's TID and handle into the reply and both are
// checked; a reply with another handle, or an oldest version above the newest,
// fails the session. A request pending when its connection is lost ends as an
// OutcomeUnknownError and is not retransmitted; the next call may use another
// admitted MON and takes the next client-wide handle, while TIDs are
// per-connection. Native evidence covers a lost reply under orderly MON stop
// and SIGKILL of a MON that answered or had parked the request out of quorum,
// with CRC framing only; a request delayed inside a MON that is in quorum is
// not verified. Source: ceph/ceph/blob/v20.2.4/src/messages/MMonGetVersion.h,
// ceph/ceph/blob/v20.2.4/src/messages/MMonGetVersionReply.h and
// ceph/ceph/blob/v20.2.4/src/mon/Monitor.cc (handle_get_version).
func (c *Client) MapVersion(ctx context.Context, kind MapKind) (MapVersionResult, error) {
	var result MapVersionResult
	switch kind {
	case OSDMapKind, MonMapKind, MDSMapKind, FSMapKind:
	default:
		// The MON silently ignores other names.
		return result, fmt.Errorf("ceph: unsupported map kind %q", kind)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if c.ctx.Err() != nil {
		return result, ErrClosed
	}
	select {
	case c.calls <- struct{}{}:
		defer func() { <-c.calls }()
	case <-ctx.Done():
		return result, ctx.Err()
	case <-c.ctx.Done():
		return result, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if c.ctx.Err() != nil {
		return result, ErrClosed
	}
	handle := c.versionHandle.Add(1)
	message := msgr.MonGetVersion(handle, string(kind))
	var s *session.Session
	var reply msgr.MessageData
	var err error
	for {
		if s, err = c.monitor(ctx); err != nil {
			return result, err
		}
		reply, err = s.Call(ctx, message)
		var unknown *OutcomeUnknownError
		if errors.Is(err, session.ErrRetired) && !errors.As(err, &unknown) {
			continue
		}
		break
	}
	if err != nil {
		return result, err
	}
	decoded, err := msgr.DecodeMonGetVersionReply(reply, c.options.MaxFrameSize)
	if err == nil && (decoded.Handle != handle || decoded.Oldest > decoded.Newest) {
		err = errors.New("reply handle or version range differs")
	}
	if err != nil {
		cause := fmt.Errorf("%w: invalid map version reply: %w", msgr.ErrFrame, err)
		s.Fail(cause)
		return result, &OutcomeUnknownError{Cause: cause}
	}
	return MapVersionResult{Newest: decoded.Newest, Oldest: decoded.Oldest}, nil
}
