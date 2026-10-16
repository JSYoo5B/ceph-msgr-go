package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

func backoffUnitConnection() *OSDConnection {
	ctx := context.Background()
	c := &Client{ctx: ctx, options: Options{MaxFrameSize: 4096, OSDProfile: OSDProfileReplicatedPG}, auth: &cephx.Client{GlobalID: 7}, osds: make(map[int32]*OSDConnection)}
	o := &OSDConnection{client: c, ctx: ctx, target: OSDTarget{ID: 3, Address: "192.0.2.3:6800"}, globalID: 7, generation: 11, session: session.New(&session.Transport{}, session.Config{}, nil, nil)}
	c.osds[3] = o
	o.backoffState = newOSDBackoffState(4096)
	return o
}
func backoffUnitControl(t *testing.T, id uint64, op uint8) osd.Backoff {
	t.Helper()
	b := osd.Backoff{PG: osd.PG{Pool: 7, Seed: 3}, Shard: -1, MapEpoch: 15, ID: id, Op: op, Begin: osd.PGCursor{Object: "a", Pool: 7, Snap: osd.NoSnap}, End: osd.PGCursor{Object: "z", Pool: 7, Snap: osd.NoSnap}}
	m, err := osd.EncodeBackoff(b, 4096)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := osd.DecodeBackoff(m, 4096)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func TestOSDBackoffQueueOwnershipCancellationAndTerminal(t *testing.T) {
	o := backoffUnitConnection()
	b := o.backoffState
	first := backoffUnitControl(t, 1, osd.BackoffBlock)
	if err := b.accept(o, first, 11, 7); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := o.NextBackoff(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	b.stop(io.EOF)
	b.stop(ErrClosed)
	event, err := o.NextBackoff(context.Background())
	if err != nil || event.Op != OSDBackoffBlock || event.ID != 1 || event.MapEpoch != 15 || event.SessionGeneration != 11 || event.GlobalID != 7 || event.Source != o.target || !bytes.Equal(event.RawFront, first.RawFront) {
		t.Fatal(event, err)
	}
	event.RawFront[0] ^= 255
	event.Begin.Raw[0] ^= 255
	if bytes.Equal(event.RawFront, first.RawFront) || !bytes.Equal(event.receipt.block.RawFront, first.RawFront) || event.Begin.Raw[0] == event.receipt.block.Begin.Raw[0] {
		t.Fatal("returned event mutated retained ACK identity")
	}
	if _, err := o.NextBackoff(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatal("terminal first cause lost", err)
	}
}

func TestOSDBackoffIdentityAndNoImplicitAck(t *testing.T) {
	o := backoffUnitConnection()
	b := o.backoffState
	block := backoffUnitControl(t, 1, osd.BackoffBlock)
	if err := b.accept(o, block, 11, 7); err != nil {
		t.Fatal(err)
	}
	event, err := o.NextBackoff(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := o.AckBackoff(context.Background(), event); !errors.Is(err, ErrUnsupportedFeatures) {
		t.Fatal("ACK sent without verified writer gate", err)
	}
	if b.active[1].acked || b.active[1].acking {
		t.Fatal("receive/dequeue implicitly acknowledged")
	}
	foreign := backoffUnitConnection()
	if err := foreign.AckBackoff(context.Background(), event); !errors.Is(err, ErrOSDBackoffIdentity) {
		t.Fatal("cross-session ACK admitted", err)
	}
	o.generation++
	if err := o.AckBackoff(context.Background(), event); !errors.Is(err, ErrOSDBackoffIdentity) {
		t.Fatal("stale generation ACK admitted", err)
	}
	o.generation--
	conflicting := block
	conflicting.PG.Seed++
	if err := b.accept(o, conflicting, 11, 7); !errors.Is(err, msgr.ErrFrame) {
		t.Fatal("reused id with different PG admitted", err)
	}
	conflicting = block
	conflicting.MapEpoch++
	if err := b.accept(o, conflicting, 11, 7); !errors.Is(err, msgr.ErrFrame) {
		t.Fatal("reused id with changed block epoch admitted", err)
	}
	unblock := block
	unblock.Op = osd.BackoffUnblock
	unblock.MapEpoch++
	if err := b.accept(o, unblock, 11, 7); err != nil {
		t.Fatal("newer epoch UNBLOCK refused", err)
	}
	got, err := o.NextBackoff(context.Background())
	if err != nil || !got.Matched || len(b.active) != 1 || !b.active[1].unblocked {
		t.Fatal("unblock lost the pending ACK receipt", got, err)
	}
	b.mu.Lock()
	b.gateInstalled = true
	b.mu.Unlock()
	if err := o.beforeWriteBackoff(backoffUnitMatchingRequest(t)); !errors.Is(err, ErrOSDBackoff) {
		t.Fatal("released pending ACK did not retain conservative gate", err)
	}
	if err := o.AckBackoff(context.Background(), event); err != nil {
		t.Fatal("late ACK after unblock refused", err)
	}
	if len(b.active) != 1 || b.activeBytes == 0 {
		t.Fatal("ACK queue admission prematurely released gate")
	}
	if err := o.beforeWriteBackoff(backoffUnitMatchingRequest(t)); !errors.Is(err, ErrOSDBackoff) {
		t.Fatal("ahead queued object passed before ACK writer turn", err)
	}
	ackMessage, err := osd.EncodeBackoffAck(block, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.beforeWriteBackoff(ackMessage); err != nil {
		t.Fatal("writer rejected retained late ACK", err)
	}
	if len(b.active) != 0 || b.activeBytes != 0 {
		t.Fatal("ACK writer turn retained its released restriction")
	}

	if err := o.beforeWriteBackoff(backoffUnitMatchingRequest(t)); err != nil {
		t.Fatal("late ACK did not release local gate", err)
	}
	if err := b.accept(o, unblock, 11, 7); err != nil {
		t.Fatal(err)
	}
	got, err = o.NextBackoff(context.Background())
	if err != nil || got.Matched {
		t.Fatal("unknown unblock lost", got, err)
	}
	if err := b.accept(o, backoffUnitControl(t, 2, osd.BackoffAckBlock), 11, 7); !errors.Is(err, msgr.ErrFrame) {
		t.Fatal("server ACK direction accepted", err)
	}
}

func TestOSDBackoffBoundedActiveSetAndMatchingRangeGate(t *testing.T) {
	o := backoffUnitConnection()
	b := o.backoffState
	if err := b.accept(o, backoffUnitControl(t, 1, osd.BackoffBlock), 11, 7); err != nil {
		t.Fatal(err)
	}
	if err := o.beforeWriteBackoff(backoffUnitMatchingRequest(t)); err != nil {
		t.Fatal("unverified gate activated", err)
	}
	b.mu.Lock()
	b.gateInstalled = true
	b.mu.Unlock()
	if err := o.beforeWriteBackoff(backoffUnitMatchingRequest(t)); !errors.Is(err, ErrOSDBackoff) {
		t.Fatal("active block did not refuse unsent object", err)
	}
	// The gate admits only an actually queued ACK with its exact identity.
	entry := b.active[1]
	entry.acked = true
	ackMessage, err := osd.EncodeBackoffAck(entry.receipt.block, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.beforeWriteBackoff(ackMessage); err != nil {
		t.Fatal("ACK blocked by object gate", err)
	}
	for i := uint64(2); i < 100; i++ {
		err := b.accept(o, backoffUnitControl(t, i, osd.BackoffBlock), 11, 7)
		if errors.Is(err, ErrOSDBackoffOverflow) {
			if len(b.queue) > 64 || b.bytes > b.limit || len(b.active) > 64 || b.activeBytes > b.limit {
				t.Fatal("backoff budget exceeded")
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("unbounded backoff acceptance")
}

// Source admission before decode is insufficient: auth/session ownership may
// change while the bounded wire decoder runs. Publication rechecks atomically.
func TestOSDBackoffDecodedControlCannotPublishAfterIdentityChange(t *testing.T) {
	for _, kind := range []string{"authentication", "session", "owner", "closed"} {
		t.Run(kind, func(t *testing.T) {
			o := backoffUnitConnection()
			source := o.session
			decoded := backoffUnitControl(t, 1, osd.BackoffBlock)
			o.client.mu.Lock()
			switch kind {
			case "authentication":
				o.client.auth = &cephx.Client{GlobalID: 8}
			case "session":
				o.session = session.New(&session.Transport{}, session.Config{}, nil, nil)
			case "owner":
				delete(o.client.osds, o.target.ID)
			case "closed":
				o.closed = true
			}
			o.client.mu.Unlock()
			if err := o.publishBackoff(source, decoded); err != nil {
				t.Fatal(err)
			}
			b := o.backoffState
			b.mu.Lock()
			defer b.mu.Unlock()
			if len(b.active) != 0 || len(b.queue) != 0 {
				t.Fatal("stale decoded backoff changed live state")
			}
		})
	}
}

func TestOSDBackoffReleasedReceiptCannotAliasReusedID(t *testing.T) {
	o := backoffUnitConnection()
	b := o.backoffState
	b.gateInstalled = true
	block := backoffUnitControl(t, 1, osd.BackoffBlock)
	if err := b.accept(o, block, 11, 7); err != nil {
		t.Fatal(err)
	}
	original, err := o.NextBackoff(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	unblock := block
	unblock.Op = osd.BackoffUnblock
	if err := b.accept(o, unblock, 11, 7); err != nil {
		t.Fatal(err)
	}
	if err := b.accept(o, block, 11, 7); !errors.Is(err, msgr.ErrFrame) {
		t.Fatal("BLOCK reused a pending released receipt", err)
	}
	if err := o.AckBackoff(context.Background(), original); err != nil {
		t.Fatal(err)
	}
	ackMessage, err := osd.EncodeBackoffAck(block, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.beforeWriteBackoff(ackMessage); err != nil {
		t.Fatal(err)
	}
	if _, err := o.NextBackoff(context.Background()); err != nil {
		t.Fatal(err)
	} // UNBLOCK
	if err := b.accept(o, block, 11, 7); err != nil {
		t.Fatal(err)
	}
	replacement, err := o.NextBackoff(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if replacement.receipt == original.receipt {
		t.Fatal("reused ID aliased an old ACK receipt")
	}
	if err := o.AckBackoff(context.Background(), original); !errors.Is(err, ErrOSDBackoffIdentity) {
		t.Fatal("stale receipt acknowledged a replacement restriction", err)
	}
	if b.active[1].acked {
		t.Fatal("replacement restriction was implicitly acknowledged")
	}
}

func TestOSDBackoffReleasedPendingAcksShareActiveBudget(t *testing.T) {
	o := backoffUnitConnection()
	b := o.backoffState
	// Drain every public event. Only unacknowledged released receipts remain;
	// they must still exhaust the same retained-set budget as active blocks.
	for id := uint64(1); id < 100; id++ {
		block := backoffUnitControl(t, id, osd.BackoffBlock)
		err := b.accept(o, block, 11, 7)
		if errors.Is(err, ErrOSDBackoffOverflow) {
			if len(b.active) == 0 || len(b.active) > 64 || b.activeBytes > b.limit || b.bytes != 0 || len(b.queue) != 0 {
				t.Fatal("pending ACK tombstone budget is not bounded", len(b.active), b.activeBytes, b.bytes)
			}
			for _, entry := range b.active {
				if !entry.unblocked || entry.acked {
					t.Fatal("unexpected retained entry state")
				}
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err := o.NextBackoff(context.Background()); err != nil {
			t.Fatal(err)
		}
		unblock := block
		unblock.Op = osd.BackoffUnblock
		if err := b.accept(o, unblock, 11, 7); err != nil {
			t.Fatal(err)
		}
		if _, err := o.NextBackoff(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("released pending ACK receipts escaped the aggregate bound")
}
