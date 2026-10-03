package cephmsgr

import (
	"context"
	"errors"
	"fmt"

	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

// ErrOSDMapOverflow fails an OSD session whose unread map queue exceeded its
// byte or batch limit. Accepted batches remain readable; none are coalesced.
var ErrOSDMapOverflow = errors.New("ceph: OSD map queue exceeded")

// OSDMapBlob preserves one encoded full map or incremental update. Data is
// opaque: the caller's map layer must decode and validate it before use.
type OSDMapBlob struct {
	Epoch uint32
	Data  []byte
}

// OSDMapBatch preserves a MOSDMap publication from an authenticated MON or OSD.
// OldestMap and NewestMap are the server's advertised trim/latest epochs,
// which can extend beyond the included blobs; both are absent (zero) in v1.
// They do not describe a map applied by this library. Each slice belongs to the
// caller; blob Data slices are views into RawFront's complete envelope.
// No map is applied here.
type OSDMapBatch struct {
	FSID                      string
	Version, CompatVersion    uint16
	OldestMap, NewestMap      uint32
	FullMaps, IncrementalMaps []OSDMapBlob
	RawFront                  []byte
}

type queuedOSDMap struct {
	batch OSDMapBatch
	bytes uint64
}

// NextMap receives the next queued map publication on this OSD connection.
// It does not subscribe on MON, request a map, calculate placement, change
// request epochs, or retry requests. OSDs may send maps when a request uses
// an older epoch. A caller needing a complete map chain must acquire and
// apply it in its own map layer; delivery is not a lossless subscription.
//
// Concurrent calls each receive a batch at most once. ctx cancels only this
// wait, and an already canceled context consumes nothing. Accepted batches
// drain before the stable terminal cause of a failed or closed connection.
func (o *OSDConnection) NextMap(ctx context.Context) (OSDMapBatch, error) {
	for {
		if err := ctx.Err(); err != nil {
			return OSDMapBatch{}, err
		}
		c := o.client
		c.mu.Lock()
		if len(o.mapQueue) != 0 {
			next := o.mapQueue[0]
			o.mapQueue[0] = queuedOSDMap{}
			o.mapQueue = o.mapQueue[1:]
			o.mapBytes -= next.bytes
			c.mu.Unlock()
			return next.batch, nil
		}
		terminal, changed, s := o.mapTerminal, o.mapChanged, o.session
		c.mu.Unlock()
		if terminal != nil {
			return OSDMapBatch{}, terminal
		}
		// Session.Err is inspected outside the client lock. Fail publishes
		// its cause before potentially delayed connection cleanup/onClose.
		if s != nil {
			if err := s.Err(); err != nil {
				o.stopMaps(err)
				continue
			}
		}
		if s == nil && o.ctx.Err() != nil {
			o.stopMaps(ErrClosed)
			continue
		}
		var done <-chan struct{}
		baseDone := o.ctx.Done()
		if s != nil {
			done = s.Done()
			// Close owns session failure. Wait for its arbitration rather
			// than fixing ErrClosed before a concurrent network failure.
			baseDone = nil
		}
		select {
		case <-changed:
		case <-done:
		case <-baseDone:
		case <-ctx.Done():
			return OSDMapBatch{}, ctx.Err()
		}
	}
}

func (o *OSDConnection) signalMapsLocked() {
	close(o.mapChanged)
	o.mapChanged = make(chan struct{})
}

func (o *OSDConnection) stopMapsLocked(err error) {
	if o.mapTerminal == nil {
		o.mapTerminal = err
		o.signalMapsLocked()
	}
}

func (o *OSDConnection) stopMaps(err error) {
	o.client.mu.Lock()
	o.stopMapsLocked(err)
	o.client.mu.Unlock()
}

func (o *OSDConnection) acceptsMapLocked(source *session.Session) bool {
	c := o.client
	return !c.closed && !o.closed && c.osds[o.target.ID] == o && o.session == source && o.mapTerminal == nil && c.auth.GlobalID == o.globalID
}

func (o *OSDConnection) handleMap(source *session.Session, m msgr.MessageData) error {
	c := o.client
	c.mu.Lock()
	accept, fsid := o.acceptsMapLocked(source), c.fsid
	c.mu.Unlock()
	if !accept {
		return nil
	}
	decoded, err := maps.DecodeOSDMessage(m, c.options.MaxFrameSize)
	if err == nil && decoded.FSID != fsid {
		err = errors.New("OSD map belongs to a different cluster")
	}
	if err != nil {
		err = fmt.Errorf("%w: invalid OSD map: %w", msgr.ErrFrame, err)
	}
	batch := publicOSDMapBatch(m, decoded)
	// RawFront owns all blob bytes; per-blob slices are views into that buffer.
	// Count envelope and slice metadata conservatively without double counting.
	size := osdMapBatchBytes(batch)
	c.mu.Lock()
	defer c.mu.Unlock()
	if !o.acceptsMapLocked(source) {
		return nil
	}
	if err != nil {
		// The session owns first-cause arbitration. A writer may already
		// have failed while this callback decoded outside the client lock.
		// NextMap and onClose observe the winning Session.Err after Fail.
		return err
	}
	if len(o.mapQueue) >= 64 || size > uint64(c.options.MaxBufferedOSDMapBytes)-o.mapBytes {
		return ErrOSDMapOverflow
	}
	o.mapQueue = append(o.mapQueue, queuedOSDMap{batch: batch, bytes: size})
	o.mapBytes += size
	o.signalMapsLocked()
	return nil
}

func publicOSDMapBatch(m msgr.MessageData, decoded maps.OSDMessage) OSDMapBatch {
	return OSDMapBatch{FSID: fmt.Sprintf("%x-%x-%x-%x-%x", decoded.FSID[:4], decoded.FSID[4:6], decoded.FSID[6:8], decoded.FSID[8:10], decoded.FSID[10:]), Version: m.Version, CompatVersion: m.CompatVersion, OldestMap: decoded.OldestMap, NewestMap: decoded.NewestMap, RawFront: decoded.RawFront, FullMaps: publicOSDMapBlobs(decoded.FullMaps), IncrementalMaps: publicOSDMapBlobs(decoded.IncrementalMaps)}
}

func osdMapBatchBytes(batch OSDMapBatch) uint64 {
	return 512 + uint64(len(batch.RawFront)) + 64*uint64(len(batch.FullMaps)+len(batch.IncrementalMaps))
}

func publicOSDMapBlobs(blobs []maps.OSDBlob) []OSDMapBlob {
	if len(blobs) == 0 {
		return nil
	}
	result := make([]OSDMapBlob, len(blobs))
	for i, blob := range blobs {
		result[i] = OSDMapBlob{Epoch: blob.Epoch, Data: blob.Data}
	}
	return result
}
