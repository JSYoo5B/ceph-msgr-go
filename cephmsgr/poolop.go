package cephmsgr

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

// PoolOpKind selects an MPoolOp operation (include/ceph_fs.h POOL_OP_*). Pool
// creation and deletion through this message, and the removed AUID change,
// are not offered: raw MON commands cover them.
type PoolOpKind uint32

const (
	// PoolSnapshotCreate and PoolSnapshotRemove name a pool snapshot; they
	// fail with EINVAL on a pool in self-managed snapshot mode.
	PoolSnapshotCreate PoolOpKind = PoolOpKind(msgr.PoolOpCreateSnap)
	PoolSnapshotRemove PoolOpKind = PoolOpKind(msgr.PoolOpDeleteSnap)
	// SelfManagedSnapshotCreate allocates the next self-managed snapshot id
	// and returns it in the reply data; SelfManagedSnapshotRemove deletes
	// the id in PoolOpRequest.Snapshot. Both fail with EINVAL on a pool in
	// pool-snapshot mode.
	SelfManagedSnapshotCreate PoolOpKind = PoolOpKind(msgr.PoolOpCreateUnmanagedSnap)
	SelfManagedSnapshotRemove PoolOpKind = PoolOpKind(msgr.PoolOpDeleteUnmanagedSnap)
)

// MaxPoolSnapshotName bounds a pool snapshot name sent by PoolOp.
const MaxPoolSnapshotName = 1024

// PoolOpRequest is one MPoolOp. Pool is the numeric pool id. Name is the pool
// snapshot name (required for PoolSnapshot* operations, empty otherwise) and
// Snapshot the id for SelfManagedSnapshotRemove (zero otherwise).
type PoolOpRequest struct {
	Pool     uint32
	Op       PoolOpKind
	Name     string
	Snapshot uint64
}

// PoolOpResult preserves the MON's reply: the signed server code, the OSD map
// epoch it carried and the raw reply data. SelfManagedSnapshotCreate returns
// the new snapshot id as eight little-endian bytes; SnapshotID decodes it.
type PoolOpResult struct {
	Code  int32
	Epoch uint32
	Data  []byte
}

// SnapshotID decodes the id a SelfManagedSnapshotCreate reply carries.
func (r PoolOpResult) SnapshotID() (uint64, error) {
	if len(r.Data) != 8 {
		return 0, errors.New("ceph: pool op reply carries no snapshot id")
	}
	return binary.LittleEndian.Uint64(r.Data), nil
}

// PoolOpError reports a negative server code; the result is returned too.
type PoolOpError struct{ Code int32 }

func (e *PoolOpError) Error() string { return fmt.Sprintf("ceph: pool op result code %d", e.Code) }

// PoolOp sends one MPoolOp to the admitted MON and waits for its reply (the
// route Objecter::pool_op uses for snapshot operations that have no CLI
// command, notably self-managed snapshot id allocation for RBD). The paxos
// version is zero, so the MON does not wait for a newer map first. ctx bounds
// only the local wait: after transmission a cancelled wait or lost session
// returns an OutcomeUnknownError, because the MON may have allocated the id
// or changed the snapshot; the operation is never replayed. Allocating twice
// allocates two ids. The MON requires its pool-op capabilities for the
// identity (MON "osd" write, or the unmanaged-snapshot rule for self-managed
// operations) and answers EPERM otherwise.
func (c *Client) PoolOp(ctx context.Context, request PoolOpRequest) (PoolOpResult, error) {
	var result PoolOpResult
	switch request.Op {
	case PoolSnapshotCreate, PoolSnapshotRemove:
		if request.Name == "" || len(request.Name) > MaxPoolSnapshotName || strings.IndexByte(request.Name, 0) >= 0 || request.Snapshot != 0 {
			return result, osd.ErrRequest
		}
	case SelfManagedSnapshotCreate:
		if request.Name != "" || request.Snapshot != 0 {
			return result, osd.ErrRequest
		}
	case SelfManagedSnapshotRemove:
		if request.Name != "" || request.Snapshot == 0 || request.Snapshot > osd.MaxSnap {
			return result, osd.ErrRequest
		}
	default:
		return result, osd.ErrRequest
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if c.ctx.Err() != nil {
		return result, ErrClosed
	}
	if request.Pool > math.MaxInt32 {
		return result, osd.ErrRequest
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
	message := msgr.PoolOp(fsid, request.Pool, uint32(request.Op), request.Snapshot, request.Name)
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
	decoded, err := msgr.DecodePoolOpReply(reply, c.options.MaxFrameSize)
	if err != nil {
		cause := fmt.Errorf("%w: invalid pool op reply: %w", msgr.ErrFrame, err)
		s.Fail(cause)
		return result, &OutcomeUnknownError{Cause: cause}
	}
	result = PoolOpResult{Code: decoded.Code, Epoch: decoded.Epoch, Data: decoded.Data}
	if result.Code < 0 {
		return result, &PoolOpError{Code: result.Code}
	}
	return result, nil
}
