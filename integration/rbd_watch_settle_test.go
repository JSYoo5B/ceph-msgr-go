package integration_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"
)

// rbdWatchLedger is the receipt ledger of a Go watch responder that records
// a notification only after its acknowledgement returned. The OSD completes
// a notify once it has every watcher's acknowledgement, independently of
// when the acknowledging Go watcher receives the OSD's reply. A native
// notifier that returned has therefore had each acknowledgement it waited
// for submitted, and the Go handler that submitted it may still be waiting
// for that reply. A handler registers the notify ID when it receives the
// notification, before it acknowledges, so at the moment a native action
// returns every notification it waited for is either recorded or in flight
// here. A verdict on recorded notifications settles on those exact notify
// IDs first (settleRBDWatch).
//
// The ledger has no lock of its own: the responder that owns it calls every
// method with its own mutex held.
type rbdWatchLedger struct {
	received []uint64       // notify IDs in receipt order
	inFlight map[uint64]int // notify ID -> handlers not yet recorded
	recorded map[uint64]int // notify ID -> handlers recorded
}

// receive opens the record of one received notification.
func (l *rbdWatchLedger) receive(id uint64) {
	if l.inFlight == nil {
		l.inFlight, l.recorded = map[uint64]int{}, map[uint64]int{}
	}
	l.received = append(l.received, id)
	l.inFlight[id]++
}

// record closes the record of one received notification.
func (l *rbdWatchLedger) record(id uint64) {
	if l.inFlight == nil {
		l.inFlight, l.recorded = map[uint64]int{}, map[uint64]int{}
	}
	if l.inFlight[id]--; l.inFlight[id] <= 0 {
		delete(l.inFlight, id)
	}
	l.recorded[id]++
}

// mark is the number of notifications received so far.
func (l *rbdWatchLedger) mark() int { return len(l.received) }

// since returns the notify IDs received after mark, in receipt order.
func (l *rbdWatchLedger) since(mark int) []uint64 {
	return slices.Clone(l.received[min(mark, len(l.received)):])
}

// pending returns the notify IDs of ids that are not yet recorded and the
// number of received notifications still in flight.
func (l *rbdWatchLedger) pending(ids []uint64) (missing []uint64, inFlight int) {
	for _, id := range ids {
		if l.recorded[id] == 0 {
			missing = append(missing, id)
		}
	}
	for _, n := range l.inFlight {
		inFlight += n
	}
	return missing, inFlight
}

// rbdNotifySettleTimeout bounds the wait for a Go watcher to record the
// acknowledgements a returned native action waited for: each is one OSD
// round trip after the notify already completed.
const rbdNotifySettleTimeout = 30 * time.Second

// settleRBDWatch waits, bounded by ctx, until every notification in ids is
// recorded and no received notification is still unrecorded, and returns the
// recorded notifications. state reads the responder's records, its change
// channel (closed on every recording) and its first failure; it and the
// ledger are read with mu held.
func settleRBDWatch[T any](ctx context.Context, mu *sync.Mutex, l *rbdWatchLedger, state func() ([]T, <-chan struct{}, error), ids []uint64) ([]T, error) {
	for {
		mu.Lock()
		missing, inFlight := l.pending(ids)
		events, changed, failure := state()
		mu.Unlock()
		if failure != nil {
			return events, failure
		}
		if len(missing) == 0 && inFlight == 0 {
			return events, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return events, fmt.Errorf("watch notifications did not settle (unrecorded notify IDs %v, %d in flight): %w", missing, inFlight, ctx.Err())
		}
	}
}
