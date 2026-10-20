package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func TestOSDEnforceSnapContextRefusesBeforeSharedSlotMonitorAndTID(t *testing.T) {
	cases := map[string]func(*OSDRequest){
		"read": func(r *OSDRequest) { r.Operations = []OSDOperation{{Code: OSDRead, Length: 1}} },
		"stat": func(r *OSDRequest) { r.Operations = []OSDOperation{{Code: OSDStat}} },
		"metadata-read": func(r *OSDRequest) {
			r.Operations = []OSDOperation{{Code: OSDGetXattrs, Xattr: OSDXattrInput{MaxOutputBytes: 8}}}
		},
		"read-call": func(r *OSDRequest) {
			r.Operations = []OSDOperation{{Code: OSDCall, Class: OSDClassInput{Mode: OSDCallRead}}}
		},
		"list-snapshots": func(r *OSDRequest) {
			r.Operations = []OSDOperation{{Code: OSDListSnapshots, ListSnapshots: OSDListSnapshotsInput{MaxOutputBytes: 128}}}
		},
		"list-watchers": func(r *OSDRequest) {
			r.Operations = []OSDOperation{{Code: OSDListWatchers, ListWatchers: OSDListWatchersInput{MaxOutputBytes: 128}}}
		},
		"watch": func(r *OSDRequest) {
			r.Operations = []OSDOperation{{Code: OSDWatchOperation, Watch: OSDWatchInput{Action: OSDWatchRegister}}}
		},
		"notify": func(r *OSDRequest) {
			r.Operations = []OSDOperation{{Code: OSDNotifyOperation, Watch: OSDWatchInput{ProtocolVersion: 1}}}
		},
		"notify-ack": func(r *OSDRequest) {
			r.Operations = []OSDOperation{{Code: OSDNotifyAckOperation, Watch: OSDWatchInput{NotifyID: 1}}}
		},
		"snapshot-write": func(r *OSDRequest) { r.ReadSnapshot = 3 },
		"ascending":      func(r *OSDRequest) { r.SnapContext = OSDSnapContext{Sequence: 7, Snapshots: []uint64{5, 7}} },
		"above-sequence": func(r *OSDRequest) { r.SnapContext = OSDSnapContext{Sequence: 5, Snapshots: []uint64{7}} },
		"ignore-cache":   func(r *OSDRequest) { r.IgnoreCache = true },
	}
	for _, profile := range []OSDProfile{OSDProfileObjectHash, OSDProfileReplicatedPG, OSDProfileReplicatedPGReplyV8, OSDProfileReplicatedPGRequestV9ReplyV8} {
		for name, edit := range cases {
			t.Run(profileNameForCacheTest(profile)+"/"+name, func(t *testing.T) {
				o := backoffUnitConnection()
				o.client.options.OSDProfile = profile
				o.client.options.EnableOSDCachePools = true
				o.features |= osd.FeatureCachePool
				o.client.calls = make(chan struct{}, 1)
				o.client.calls <- struct{}{}
				r := OSDRequest{EnforceSnapContext: true, MapEpoch: 17, Object: OSDObject{Pool: 7, Name: "object"}, Operations: []OSDOperation{{Code: OSDWriteFull, Length: 1, Data: []byte{1}}}}
				if profile.replicatedPG() {
					r.Placement = &OSDPlacement{PG: OSDPG{Pool: 7, Seed: 3}, Primary: o.target.ID}
				}
				edit(&r)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				got, err := o.Request(ctx, r)
				var unknown *OutcomeUnknownError
				if !errors.Is(err, osd.ErrRequest) || errors.As(err, &unknown) || got.Submission != (OSDSubmission{}) || o.client.osdNext.Load() != 0 || len(o.client.calls) != 1 {
					t.Fatal("invalid flag passed preflight or consumed foreign slot", got, err)
				}
			})
		}
	}
}

func enforceSnapProfileOpen(t *testing.T, profile OSDProfile) (*Client, *OSDConnection, *osdTestPeer, context.Context) {
	t.Helper()
	if profile == OSDProfileObjectHash {
		c, peers, _ := osdTestClient(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t.Cleanup(cancel)
		o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
		if err != nil {
			t.Fatal(err)
		}
		select {
		case peer := <-peers:
			return c, o, peer, ctx
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if profile == OSDProfileReplicatedPG {
		c, o, p, ctx, _ := backoffTransportOpen(t)
		return c, o, p, ctx
	}
	c, o, p, ctx, _ := replicatedReplyV8ProfileOpen(t, profile)
	return c, o, p, ctx
}

func TestOSDEnforceSnapContextWireUnknownAndExplicitResubmitAllProfiles(t *testing.T) {
	for _, profile := range []OSDProfile{OSDProfileObjectHash, OSDProfileReplicatedPG, OSDProfileReplicatedPGReplyV8, OSDProfileReplicatedPGRequestV9ReplyV8} {
		t.Run(profileNameForCacheTest(profile), func(t *testing.T) {
			c, o, peer, ctx := enforceSnapProfileOpen(t, profile)
			request := osdValidationRequest()
			request.ClientIncarnation = 31
			request.Operations = []OSDOperation{{Code: OSDWriteFull, Length: 4, Data: []byte{0, 255, 'A', '\n'}}}
			request.ModificationTime = OSDTime{Seconds: 1700000000, Nanoseconds: 5}
			request.SnapContext = OSDSnapContext{Sequence: 7, Snapshots: []uint64{7, 5}}
			request.OrderSnapshots = true
			if profile.replicatedPG() {
				request.Placement = &OSDPlacement{PG: OSDPG{Pool: uint64(request.Object.Pool), Seed: 3}, Primary: o.target.ID}
			}
			type result struct {
				reply OSDReply
				err   error
			}
			run := func(r OSDRequest) (msgr.MessageData, OSDReply) {
				local, cancel := context.WithCancel(ctx)
				defer cancel()
				done := make(chan result, 1)
				go func() { reply, err := o.Request(local, r); done <- result{reply, err} }()
				sent := peer.next(t)
				cancel()
				select {
				case got := <-done:
					var unknown *OutcomeUnknownError
					if !errors.As(got.err, &unknown) || !errors.Is(got.err, context.Canceled) || got.reply.Submission.Transaction != sent.Transaction || got.reply.Submission.GlobalID != o.globalID || got.reply.Submission.ClientIncarnation != 31 {
						t.Fatal("unknown submission identity lost", got.reply, got.err)
					}
					return sent, got.reply
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				return msgr.MessageData{}, OSDReply{}
			}
			plain, _ := run(request)
			request.EnforceSnapContext = true
			enforced, unknown := run(request)
			at := 8
			if profile.replicatedPG() {
				at = 32
			}
			flags := binary.LittleEndian.Uint32(enforced.Front[at:])
			before := binary.LittleEndian.Uint32(plain.Front[at:])
			copyFront := bytes.Clone(enforced.Front)
			binary.LittleEndian.PutUint32(copyFront[at:], flags&^0x100000)
			if flags != before|0x100000 || !bytes.Equal(copyFront, plain.Front) || !bytes.Equal(enforced.Data, plain.Data) || enforced.Version != plain.Version {
				t.Fatal("public flag changed another wire field")
			}
			// Returned messages own their encoded context/data; later caller
			// edits do not mutate the earlier selected frame.
			saved := bytes.Clone(enforced.Front)
			request.SnapContext.Snapshots[0] = 6
			if !bytes.Equal(saved, enforced.Front) {
				t.Fatal("caller snapshot slice was retained")
			}
			request.SnapContext.Snapshots = []uint64{7, 5}
			request.Resubmit = &unknown.Submission
			request.RetryAttempt = 1
			issued := c.osdNext.Load()
			again, _ := run(request)
			if again.Transaction != enforced.Transaction || c.osdNext.Load() != issued || binary.LittleEndian.Uint32(again.Front[at:])&uint32(osd.FlagEnforceSnapContext|osd.FlagRetry) != uint32(osd.FlagEnforceSnapContext|osd.FlagRetry) {
				t.Fatal("explicit resubmit lost original identity or flags")
			}
			normalizedRetry := bytes.Clone(again.Front)
			retryAt := len(normalizedRetry) - 12
			if !profile.replicatedPG() {
				retryAt -= 27
			}
			if binary.LittleEndian.Uint32(normalizedRetry[retryAt:]) != 1 {
				t.Fatal("resubmit attempt not encoded")
			}
			binary.LittleEndian.PutUint32(normalizedRetry[retryAt:], 0)
			binary.LittleEndian.PutUint32(normalizedRetry[at:], binary.LittleEndian.Uint32(normalizedRetry[at:])&^uint32(osd.FlagRetry))
			if !bytes.Equal(normalizedRetry, enforced.Front) || !bytes.Equal(again.Data, enforced.Data) {
				t.Fatal("resubmit changed original snapshot context or payload")
			}
			select {
			case extra := <-peer.requests:
				t.Fatal("mutation automatically resubmitted", extra.Transaction)
			default:
			}
		})
	}
}

func TestOSDEnforceSnapContextBoundAndFalsePreflight(t *testing.T) {
	o := backoffUnitConnection()
	r := OSDRequest{EnforceSnapContext: true, Operations: []OSDOperation{{Code: OSDWriteFull}}, SnapContext: OSDSnapContext{Snapshots: make([]uint64, 1024)}}
	if err := o.validateEnforceSnapContext(r); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("snapshot frame bound ignored", err)
	}
	r.EnforceSnapContext = false
	if err := o.validateEnforceSnapContext(r); err != nil {
		t.Fatal("false changed ordinary preflight", err)
	}
	for _, ops := range [][]OSDOperation{{{Code: OSDWriteFull}}, {{Code: OSDRollback, Snapshot: 4}}, {{Code: OSDCall, Class: OSDClassInput{Mode: OSDCallWrite}}}} {
		if err := o.validateEnforceSnapContext(OSDRequest{EnforceSnapContext: true, Operations: ops}); err != nil {
			t.Fatal("valid writing mode refused", ops, err)
		}
	}
}

func TestOSDEnforceSnapContextWatchPreflightPreservesCookieAndOwner(t *testing.T) {
	for _, action := range []string{"watch", "notify", "reconnect"} {
		t.Run(action, func(t *testing.T) {
			o := backoffUnitConnection()
			c := o.client
			o.features = 1<<8 | 1<<9 | 1<<21 | 1<<30 | 1<<56 | 1<<57
			c.calls = make(chan struct{}, 1)
			c.calls <- struct{}{} // No invalid operation may consume this slot.
			request := OSDRequest{Object: OSDObject{Pool: 7, Name: "watch-object"}, ClientIncarnation: 31, MapEpoch: 17, EnforceSnapContext: true, Placement: &OSDPlacement{PG: OSDPG{Pool: 7, Seed: 3}, Primary: o.target.ID}}
			options := OSDWatchOptions{MaxBufferedBytes: 128}
			older := &OSDConnection{client: c, generation: o.generation - 1, globalID: o.globalID}
			oldRequest := request
			oldRequest.EnforceSnapContext = false
			old := &OSDWatchStream{connection: older, cookie: 99, request: oldRequest, options: options, changed: make(chan struct{})}
			// The intentionally held control gate also proves refusal precedes
			// acquisition, without relying on a running writer or MON.
			registration := &osdWatchRegistration{client: c, globalID: o.globalID, owner: old, lastSessionGeneration: older.generation, generation: 5, gate: make(chan struct{}, 1)}
			old.registration = registration
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var reply OSDReply
			var err error
			switch action {
			case "watch":
				var handle *OSDWatch
				handle, reply, err = o.WatchObject(ctx, request, options)
				if handle != nil {
					t.Fatal("invalid WATCH allocated a delivery handle")
				}
			case "notify":
				var handle *OSDNotify
				handle, reply, err = o.NotifyObject(ctx, request, []byte{1}, options)
				if handle != nil {
					t.Fatal("invalid NOTIFY allocated a delivery handle")
				}
			case "reconnect":
				var handle *OSDWatch
				handle, reply, err = o.ReconnectWatch(ctx, &OSDWatch{old}, request)
				if handle != nil {
					t.Fatal("invalid RECONNECT allocated a replacement handle")
				}
			}
			if !errors.Is(err, osd.ErrRequest) || reply.Submission != (OSDSubmission{}) || c.osdNext.Load() != 0 || len(c.calls) != 1 || o.watchState != nil {
				t.Fatal("invalid snapshot flag consumed cookie, slot or request identity", reply, err)
			}
			if registration.owner != old || registration.generation != 5 || registration.lastSessionGeneration != older.generation || old.terminal != nil || len(registration.gate) != 0 {
				t.Fatal("invalid snapshot flag changed the confirmed owner's authority")
			}
			select {
			case <-old.changed:
				t.Fatal("invalid reconnect terminated old delivery")
			default:
			}
		})
	}
}

func TestOSDEnforceSnapContextRejectedReconnectRetainsOldPing(t *testing.T) {
	c, peers, dials := reconnectTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	old, oldPeer := reconnectOpen(t, c, peers, 0)
	next, nextPeer := reconnectOpen(t, c, peers, 1)
	watch := confirmedTestWatch(t, ctx, old, oldPeer, 5)
	request := watchTestRoute()
	request.EnforceSnapContext = true
	before := c.osdNext.Load()
	got, reply, err := next.ReconnectWatch(ctx, watch, request)
	if got != nil || !errors.Is(err, osd.ErrRequest) || reply.Submission != (OSDSubmission{}) || c.osdNext.Load() != before {
		t.Fatal("invalid flag reached reconnect submission", got, reply, err)
	}
	c.mu.Lock()
	unchanged := watch.registration.owner == watch.OSDWatchStream && watch.registration.generation == 5 && watch.registration.lastSessionGeneration == old.generation && watch.terminal == nil && old.watchState.streams[watch.cookie] == watch.OSDWatchStream && (next.watchState == nil || len(next.watchState.streams) == 0)
	c.mu.Unlock()
	if !unchanged {
		t.Fatal("rejected reconnect transferred authority")
	}
	pinged := make(chan error, 1)
	go func() { _, e := watch.Ping(ctx, 11); pinged <- e }()
	ping := oldPeer.next(t)
	op := OSDOperation{Code: OSDWatchOperation, Watch: OSDWatchInput{Cookie: watch.Cookie(), Action: OSDWatchPing, Generation: 5}}
	if err := osdMapsWrite(oldPeer, watchSyntheticReply(t, ping, op, nil)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-pinged:
		if err != nil {
			t.Fatal("invalid snapshot flag revoked confirmed old Ping", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case unexpected := <-nextPeer.requests:
		t.Fatal("invalid reconnect reached destination", unexpected.Transaction)
	default:
	}
	if dials.Load() != 2 {
		t.Fatal("invalid reconnect retried or opened another connection")
	}
}
