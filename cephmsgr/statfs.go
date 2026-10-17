package cephmsgr

import (
	"context"
	"errors"
	"fmt"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

// StatfsResult is the MON's MStatfsReply. The three sizes are KiB; Version is
// the committed map version the answer was taken at. The figures come from the
// MGR stat service and lag the cluster by its reporting period.
type StatfsResult struct {
	Version                      uint64
	KB, KBUsed, KBAvail, Objects uint64
}

// Statfs asks the admitted MON for cluster-wide usage (dataPool nil, as
// librados cluster_stat) or for one pool's usage (as libcephfs statfs of a data
// pool). The MON has no error field: with insufficient "pg" read capability, an
// FSID mismatch or a removed pool it ignores the request and sends nothing, so
// the call then ends only when ctx does, as an OutcomeUnknownError (the request
// was sent but read-only, so nothing changed). Nothing is retried.
func (c *Client) Statfs(ctx context.Context, dataPool *int64) (StatfsResult, error) {
	var result StatfsResult
	if dataPool != nil && *dataPool < 0 {
		return result, errors.New("ceph: statfs pool id must not be negative")
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
	c.mu.Lock()
	fsid := c.fsid
	c.mu.Unlock()
	message := msgr.Statfs(fsid, dataPool)
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
	decoded, err := msgr.DecodeStatfsReply(reply, c.options.MaxFrameSize)
	if err != nil || decoded.FSID != fsid {
		if err == nil {
			err = errors.New("reply FSID differs from the cluster")
		}
		cause := fmt.Errorf("%w: invalid statfs reply: %w", msgr.ErrFrame, err)
		s.Fail(cause)
		return result, &OutcomeUnknownError{Cause: cause}
	}
	return StatfsResult{Version: decoded.Version, KB: decoded.KB, KBUsed: decoded.KBUsed, KBAvail: decoded.KBAvail, Objects: decoded.Objects}, nil
}
