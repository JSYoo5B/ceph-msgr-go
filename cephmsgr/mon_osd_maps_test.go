package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

type monOSDMapsTestSubscription struct {
	next     uint64
	flags    byte
	hostname string
	all      map[string]uint64
}

type monOSDMapsTestPeer struct {
	conn          net.Conn
	sends         chan logTestSend
	subscriptions chan monOSDMapsTestSubscription
	commands      chan msgr.MessageData
	requests      chan msgr.MessageData
	done          chan struct{}
	errors        chan error
}

// The peer observes MMonSubscribe independently of OSDMapSubscribe and uses
// the MOSDMap reference envelope helper from osd_maps_test.go. Authentication
// and framing share the synthetic peer; real Ceph remains the wire oracle.
func (p *monOSDMapsTestPeer) serve(early bool) {
	defer close(p.done)
	defer p.conn.Close()
	r, w, err := mockAuthenticate(p.conn, peerConfig{role: 1, release: 20, fsid: [16]byte{1}})
	if err != nil {
		p.errors <- err
		return
	}
	type incoming struct {
		frame msgr.Frame
		err   error
	}
	// Keep reading ACKs while the sole writer publishes unsolicited admission
	// frames, rather than coupling both net.Pipe directions through the owner.
	frames, abort, drained := make(chan incoming, 16), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(drained)
		for {
			frame, err := r.Read()
			select {
			case frames <- incoming{frame, err}:
			case <-abort:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() { close(abort); p.conn.Close(); <-drained }()
	var sequence uint64
	send := func(m msgr.MessageData) error {
		sequence++
		m.Sequence, m.Priority = sequence, 127
		return w.Write(m.Frame())
	}
	if early {
		// A recovering candidate has not established cluster/release identity.
		if send(msgr.MessageData{Type: 41, Version: 1, CompatVersion: 1, Front: []byte{0}}) != nil {
			return
		}
	}
	// Admit only in response to the actual setup request. An unsolicited
	// MonMap can finish private admission before its queued GetMonMap writes;
	// setup-context release then legitimately drops that unnecessary request.
	admitted := false
	admit := func() error {
		for _, message := range []msgr.MessageData{
			{Type: msgr.MonMapMessage, Version: 1, Front: mockMonMap(20, [16]byte{1})},
			{Type: msgr.MgrMapMessage, Version: 1, Front: mockMgrMap(1, 99, 6800)},
		} {
			if err := send(message); err != nil {
				return err
			}
		}
		admitted = true
		return nil
	}
	for {
		select {
		case request := <-p.sends:
			err := send(request.message)
			request.result <- err
			if err != nil {
				return
			}
		case got := <-frames:
			if got.err != nil {
				return
			}
			if got.frame.Tag != msgr.Message {
				continue
			}
			m, err := msgr.DecodeMessage(got.frame)
			if err != nil {
				p.errors <- err
				return
			}
			p.requests <- m
			if m.Type == 5 { // Private operations explicitly admit this MON.
				if admit() != nil {
					return
				}
				continue
			}
			if m.Type == 6 { // MMonGetOSDMap: the test supplies a raw reply.
				continue
			}
			if m.Type == msgr.SubscribeMessage {
				d := wire.NewDecoder(m.Front)
				count := d.U32()
				if count > 8 || m.Version != 3 || m.CompatVersion != 1 || m.Transaction != 0 || len(m.Middle) != 0 || len(m.Data) != 0 {
					p.errors <- errors.New("unexpected MON subscription header")
					return
				}
				sub := monOSDMapsTestSubscription{all: make(map[string]uint64)}
				var present bool
				for range count {
					name, next, flags := d.String(), d.U64(), d.U8()
					sub.all[name] = next
					if name == "osdmap" {
						present, sub.next, sub.flags = true, next, flags
					}
				}
				sub.hostname = d.String()
				if err := d.Done(); err != nil {
					p.errors <- err
					return
				}
				if !admitted {
					if _, base := sub.all["monmap"]; !base {
						p.errors <- errors.New("OSD map subscription preceded MON admission request")
						return
					}
					if admit() != nil {
						return
					}
				}
				if present {
					p.subscriptions <- sub
				}
				continue
			}
			if m.Type != msgr.MonCommandMessage {
				p.errors <- errors.New("unexpected MON request outside subscription/command")
				return
			}
			p.commands <- m
			if strings.Contains(string(m.Front), "hold mutation") {
				continue
			}
			e := wire.Encoder{}
			msgr.Paxos(&e)
			e.U32(0)
			e.String("MON map-side status")
			e.U32(0)
			if send(msgr.MessageData{Type: msgr.MonCommandReplyMessage, Version: 1, Transaction: m.Transaction, Front: e.Data, Data: []byte{0, 255, 7}}) != nil {
				return
			}
		}
	}
}

func (p *monOSDMapsTestPeer) send(t *testing.T, ctx context.Context, m msgr.MessageData) {
	t.Helper()
	request := logTestSend{message: m, result: make(chan error, 1)}
	select {
	case p.sends <- request:
	case err := <-p.errors:
		t.Fatal("MON OSD map peer failed", err)
	case <-p.done:
		t.Fatal("MON OSD map peer stopped before send")
	case <-ctx.Done():
		t.Fatal("MON OSD map send did not start", ctx.Err())
	}
	select {
	case err := <-request.result:
		if err != nil {
			t.Fatal("MON OSD map send failed", err)
		}
	case <-ctx.Done():
		t.Fatal("MON OSD map send did not finish", ctx.Err())
	}
}

func (p *monOSDMapsTestPeer) subscription(t *testing.T, ctx context.Context, next uint64) {
	t.Helper()
	select {
	case sub := <-p.subscriptions:
		if sub.next != next || sub.flags != 0 || sub.hostname != "map-test-host" || len(sub.all) != 1 || sub.all["osdmap"] != next {
			t.Fatal("OSD subscription lost its cursor/flags/hostname or added unrelated keys", sub, next)
		}
	case err := <-p.errors:
		t.Fatal("MON OSD map peer failed", err)
	case <-ctx.Done():
		t.Fatal("OSD map subscription not observed", next, ctx.Err())
	}
}

func monOSDMapsTestFixture(t *testing.T) (*Client, context.Context, <-chan *monOSDMapsTestPeer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	options := mockOptions(t, 20, [16]byte{1})
	options.Hostname, options.MaxFrameSize, options.MaxInFlight = "map-test-host", 64<<10, 1
	dial := options.DialContext
	peers := make(chan *monOSDMapsTestPeer, 16)
	var mu sync.Mutex
	var all []*monOSDMapsTestPeer
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if !strings.HasSuffix(endpoint, ":3300") {
			return dial(ctx, network, endpoint)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		client, server := net.Pipe()
		p := &monOSDMapsTestPeer{conn: server, sends: make(chan logTestSend), subscriptions: make(chan monOSDMapsTestSubscription, 8), commands: make(chan msgr.MessageData, 128), requests: make(chan msgr.MessageData, 128), done: make(chan struct{}), errors: make(chan error, 8)}
		mu.Lock()
		early := len(all) != 0
		all = append(all, p)
		mu.Unlock()
		peers <- p
		go p.serve(early)
		return client, nil
	}
	c, err := Dial(ctx, options)
	if err != nil {
		cancel()
		t.Fatal("admit MON OSD map fixture", err)
	}
	t.Cleanup(func() {
		cancel()
		c.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, p := range all {
			p.conn.Close()
			select {
			case <-p.done:
			case <-time.After(time.Second):
				t.Error("MON OSD map peer did not terminate")
			}
		}
	})
	return c, ctx, peers
}

func monOSDMapsTestNextPeer(t *testing.T, ctx context.Context, peers <-chan *monOSDMapsTestPeer) *monOSDMapsTestPeer {
	t.Helper()
	select {
	case p := <-peers:
		return p
	case <-ctx.Done():
		t.Fatal("MON OSD map recovery did not dial a new peer", ctx.Err())
		return nil
	}
}

func monOSDMapsTestBarrier(t *testing.T, ctx context.Context, c *Client) {
	t.Helper()
	result, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)})
	if err != nil || result.Code != 0 || result.Message != "MON map-side status" || !bytes.Equal(result.Data, []byte{0, 255, 7}) {
		t.Fatal("MON OSD map handling blocked or changed command output", result, err)
	}
}

func monOSDMapsTestBatch(t *testing.T, ctx context.Context, stream *OSDMapStream, message msgr.MessageData) OSDMapBatch {
	t.Helper()
	batch, err := stream.Next(ctx)
	if err != nil || batch.FSID != "01000000-0000-0000-0000-000000000000" || batch.Version != message.Version || batch.CompatVersion != message.CompatVersion || !bytes.Equal(batch.RawFront, message.Front) {
		t.Fatal("MON OSD map lost its raw envelope or FIFO order", batch, err)
	}
	return batch
}

func monOSDMapsTestTerminal(t *testing.T, ctx context.Context, stream *OSDMapStream, want error) {
	t.Helper()
	for range 2 {
		batch, err := stream.Next(ctx)
		if !errors.Is(err, want) || len(batch.RawFront) != 0 {
			t.Fatal("MON OSD map terminal changed or returned rejected data", batch, err, want)
		}
	}
}

func TestMonOSDMapWatchRawFIFOOwnershipAndLocalCancellation(t *testing.T) {
	c, ctx, peers := monOSDMapsTestFixture(t)
	p := monOSDMapsTestNextPeer(t, ctx, peers)
	stream, err := c.WatchOSDMaps(ctx, OSDMapOptions{StartEpoch: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	p.subscription(t, ctx, 5)
	if c.options.EnableOSD {
		t.Fatal("MON-only map fixture unexpectedly requested OSD service tickets")
	}
	if _, err := c.WatchOSDMaps(ctx, OSDMapOptions{}); !errors.Is(err, ErrOSDMapWatchActive) {
		t.Fatal("a second MON map receiver was admitted", err)
	}
	first := osdMapsMessage(1, [16]byte{1}, []OSDMapBlob{{Epoch: 5, Data: []byte{0, 255, 7}}}, []OSDMapBlob{{Epoch: 6, Data: []byte{8}}})
	second := osdMapsMessage(4, [16]byte{1}, []OSDMapBlob{{Epoch: 7, Data: []byte{9}}}, []OSDMapBlob{{Epoch: 8, Data: []byte{10}}})
	p.send(t, ctx, first)
	p.send(t, ctx, second)
	monOSDMapsTestBarrier(t, ctx, c)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := stream.Next(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled Next ignored its local context", err)
	}
	batch := monOSDMapsTestBatch(t, ctx, stream, first)
	if len(batch.IncrementalMaps) != 1 || len(batch.FullMaps) != 1 || batch.IncrementalMaps[0].Epoch != 5 || batch.FullMaps[0].Epoch != 6 {
		t.Fatal("full and incremental containers were merged or lost", batch)
	}
	batch.RawFront[0], batch.IncrementalMaps[0].Data[0] = 77, 88
	if first.Front[0] != 1 {
		t.Fatal("returned data retained source ownership")
	}
	batch = monOSDMapsTestBatch(t, ctx, stream, second)
	if batch.OldestMap != 3 || batch.NewestMap != 99 || !bytes.Equal(batch.IncrementalMaps[0].Data, []byte{9}) {
		t.Fatal("later map changed through an earlier returned batch", batch)
	}
	if c.Snapshot().Manager.Ready {
		t.Fatal("MON map watch opened an unrelated MGR connection")
	}
	monOSDMapsTestBarrier(t, ctx, c)
}

func TestMonOSDMapWatchRecoveryUsesAcceptedBlobCursor(t *testing.T) {
	for _, profile := range []struct {
		start, epoch uint32
		next         uint64
	}{{0, 7, 8}, {11, 11, 12}, {math.MaxUint32, math.MaxUint32, uint64(math.MaxUint32) + 1}, {20, 3, 20}} {
		t.Run("epoch-"+strconv.FormatUint(uint64(profile.start), 10), func(t *testing.T) {
			c, ctx, peers := monOSDMapsTestFixture(t)
			first := monOSDMapsTestNextPeer(t, ctx, peers)
			stream, err := c.WatchOSDMaps(ctx, OSDMapOptions{StartEpoch: profile.start})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			first.subscription(t, ctx, uint64(profile.start))
			accepted := osdMapsMessage(1, [16]byte{1}, nil, []OSDMapBlob{{Epoch: profile.epoch, Data: []byte{255}}})
			empty := osdMapsMessage(4, [16]byte{1}, nil, nil) // NewestMap=99 alone is not a resume cursor.
			first.send(t, ctx, accepted)
			first.send(t, ctx, empty)
			monOSDMapsTestBarrier(t, ctx, c)
			c.mu.Lock()
			previous := c.mon
			c.mu.Unlock()
			first.conn.Close()
			second := monOSDMapsTestNextPeer(t, ctx, peers)
			second.subscription(t, ctx, profile.next) // queued, not consumed; never below StartEpoch.
			if err := c.handleMonOSDMap(previous, msgr.MessageData{Type: 41, Version: 1, CompatVersion: 1, Front: []byte{0}}); err != nil {
				t.Fatal("retired MON map source was decoded", err)
			}
			monOSDMapsTestBatch(t, ctx, stream, accepted)
			monOSDMapsTestBatch(t, ctx, stream, empty)
			// The stream preserves whole publications, including an overlapping
			// epoch from the replacement; it does not silently filter map blobs.
			second.send(t, ctx, accepted)
			monOSDMapsTestBarrier(t, ctx, c)
			monOSDMapsTestBatch(t, ctx, stream, accepted)
		})
	}
}

func TestMonOSDMapWatchEmptyPublicationDoesNotAdvanceInitialCursor(t *testing.T) {
	c, ctx, peers := monOSDMapsTestFixture(t)
	first := monOSDMapsTestNextPeer(t, ctx, peers)
	stream, err := c.WatchOSDMaps(ctx, OSDMapOptions{StartEpoch: 17})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	first.subscription(t, ctx, 17)
	empty := osdMapsMessage(4, [16]byte{1}, nil, nil)
	first.send(t, ctx, empty)
	monOSDMapsTestBarrier(t, ctx, c)
	first.conn.Close()
	second := monOSDMapsTestNextPeer(t, ctx, peers)
	second.subscription(t, ctx, 17)
	monOSDMapsTestBatch(t, ctx, stream, empty)
}

func TestMonOSDMapWatchOverflowDrainsFIFOAndPreservesSharedCommands(t *testing.T) {
	for _, profile := range []struct {
		name     string
		limit    uint32
		accepted uint32
		blobSize int
	}{{"bytes", 1024, 1, 32}, {"batch-count", 1 << 20, 64, 32}, {"oversized-batch", 1024, 0, 2048}} {
		t.Run(profile.name, func(t *testing.T) {
			c, ctx, peers := monOSDMapsTestFixture(t)
			p := monOSDMapsTestNextPeer(t, ctx, peers)
			stream, err := c.WatchOSDMaps(ctx, OSDMapOptions{MaxBufferedBytes: profile.limit})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			p.subscription(t, ctx, 0)
			for i := uint32(1); i <= profile.accepted+1; i++ {
				p.send(t, ctx, osdMapsMessage(1, [16]byte{1}, nil, []OSDMapBlob{{Epoch: i, Data: bytes.Repeat([]byte{byte(i)}, profile.blobSize)}}))
			}
			monOSDMapsTestBarrier(t, ctx, c)
			for i := uint32(1); i <= profile.accepted; i++ {
				monOSDMapsTestBatch(t, ctx, stream, osdMapsMessage(1, [16]byte{1}, nil, []OSDMapBlob{{Epoch: i, Data: bytes.Repeat([]byte{byte(i)}, profile.blobSize)}}))
			}
			monOSDMapsTestTerminal(t, ctx, stream, ErrOSDMapWatchOverflow)
			stream.Close()
			monOSDMapsTestTerminal(t, ctx, stream, ErrOSDMapWatchOverflow)
			replacement, err := c.WatchOSDMaps(ctx, OSDMapOptions{})
			if err != nil {
				t.Fatal("overflow did not release the map watch slot", err)
			}
			defer replacement.Close()
			p.subscription(t, ctx, 0)
			monOSDMapsTestBarrier(t, ctx, c)
		})
	}
}

func TestMonOSDMapWatchMalformedFailsStartedCommandWithoutReplay(t *testing.T) {
	for _, damage := range []string{"truncated", "foreign-fsid", "unexpected-data"} {
		t.Run(damage, func(t *testing.T) {
			c, ctx, peers := monOSDMapsTestFixture(t)
			p := monOSDMapsTestNextPeer(t, ctx, peers)
			stream, err := c.WatchOSDMaps(ctx, OSDMapOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			p.subscription(t, ctx, 0)
			accepted := osdMapsMessage(1, [16]byte{1}, nil, []OSDMapBlob{{Epoch: 5, Data: []byte{9}}})
			p.send(t, ctx, accepted)
			monOSDMapsTestBarrier(t, ctx, c)
			c.mu.Lock()
			source := c.mon
			c.mu.Unlock()
			result := make(chan error, 1)
			go func() {
				_, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"hold mutation"}`)})
				result <- err
			}()
			for {
				select {
				case m := <-p.commands:
					if strings.Contains(string(m.Front), "hold mutation") {
						goto started
					}
				case <-ctx.Done():
					t.Fatal("held command did not reach the MON peer", ctx.Err())
				}
			}
		started:
			bad := osdMapsMessage(4, [16]byte{1}, nil, []OSDMapBlob{{Epoch: 6, Data: []byte{8}}})
			switch damage {
			case "truncated":
				bad.Front = bad.Front[:len(bad.Front)-1]
			case "foreign-fsid":
				bad.Front[0] = 2
			case "unexpected-data":
				bad.Data = []byte{7}
			}
			p.send(t, ctx, bad)
			err = osdMapsAwaitResult(t, ctx, result)
			var unknown *OutcomeUnknownError
			if !errors.As(err, &unknown) || !errors.Is(err, ErrMalformedMessage) {
				t.Fatal("malformed MON map lost started-command uncertainty", err)
			}
			if err := c.WaitMonReady(ctx); err != nil {
				t.Fatal("shared MON did not recover after malformed map", err)
			}
			replacement := monOSDMapsTestNextPeer(t, ctx, peers)
			monOSDMapsTestBarrier(t, ctx, c)
			monOSDMapsTestBatch(t, ctx, stream, accepted)
			cause := source.Err()
			if !errors.Is(cause, ErrMalformedMessage) {
				t.Fatal("session did not arbitrate malformed-map failure", cause)
			}
			stream.Close()
			for range 2 {
				if _, err := stream.Next(ctx); err != cause {
					t.Fatal("watch close replaced the winning session error", err, cause)
				}
			}
			for {
				select {
				case m := <-replacement.commands:
					if strings.Contains(string(m.Front), "hold mutation") {
						t.Fatal("MON recovery replayed an uncertain command")
					}
				default:
					return
				}
			}
		})
	}
}

func TestMonOSDMapWatchConcurrentNextTransfersEachBatchOnce(t *testing.T) {
	c, ctx, peers := monOSDMapsTestFixture(t)
	p := monOSDMapsTestNextPeer(t, ctx, peers)
	stream, err := c.WatchOSDMaps(ctx, OSDMapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	p.subscription(t, ctx, 0)
	type result struct {
		batch OSDMapBatch
		err   error
	}
	results := make(chan result, 8)
	for range 8 {
		go func() { batch, err := stream.Next(ctx); results <- result{batch, err} }()
	}
	for epoch := uint32(1); epoch <= 8; epoch++ {
		p.send(t, ctx, osdMapsMessage(1, [16]byte{1}, nil, []OSDMapBlob{{Epoch: epoch, Data: []byte{byte(epoch)}}}))
	}
	seen := make(map[uint32]bool)
	for range 8 {
		select {
		case got := <-results:
			if got.err != nil || len(got.batch.FullMaps) != 1 {
				t.Fatal("concurrent Next did not receive one map", got.batch, got.err)
			}
			blob := got.batch.FullMaps[0]
			if seen[blob.Epoch] || blob.Epoch < 1 || blob.Epoch > 8 || !bytes.Equal(blob.Data, []byte{byte(blob.Epoch)}) {
				t.Fatal("concurrent Next duplicated or changed a batch", blob)
			}
			seen[blob.Epoch] = true
		case <-ctx.Done():
			t.Fatal("concurrent MON map Next remained blocked", ctx.Err())
		}
	}
}

func TestMonOSDMapWatchCloseAndLifetimeDrainAcceptedFIFO(t *testing.T) {
	for _, closer := range []string{"watch", "lifetime", "client"} {
		t.Run(closer, func(t *testing.T) {
			c, ctx, peers := monOSDMapsTestFixture(t)
			p := monOSDMapsTestNextPeer(t, ctx, peers)
			lifetime, cancel := context.WithCancel(ctx)
			defer cancel()
			stream, err := c.WatchOSDMaps(lifetime, OSDMapOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			p.subscription(t, ctx, 0)
			accepted := osdMapsMessage(1, [16]byte{1}, nil, []OSDMapBlob{{Epoch: 7, Data: []byte{5}}})
			p.send(t, ctx, accepted)
			monOSDMapsTestBarrier(t, ctx, c)
			want := ErrOSDMapStreamClosed
			switch closer {
			case "watch":
				stream.Close()
			case "lifetime":
				cancel()
				want = context.Canceled
			case "client":
				c.Close()
				want = ErrClosed
			}
			select {
			case <-stream.done:
			case <-ctx.Done():
				t.Fatal("MON map worker was not joined on termination", ctx.Err())
			}
			monOSDMapsTestBatch(t, ctx, stream, accepted)
			monOSDMapsTestTerminal(t, ctx, stream, want)
			if closer != "client" {
				monOSDMapsTestBarrier(t, ctx, c)
				replacement, err := c.WatchOSDMaps(ctx, OSDMapOptions{})
				if err != nil {
					t.Fatal("terminated watch did not release its slot", err)
				}
				defer replacement.Close()
				p.subscription(t, ctx, 0)
			}
		})
	}
}

func TestMonOSDMapWatchIgnoresUnadmittedAndUnregisteredSources(t *testing.T) {
	c, ctx, peers := monOSDMapsTestFixture(t)
	p := monOSDMapsTestNextPeer(t, ctx, peers)
	stream, err := c.WatchOSDMaps(ctx, OSDMapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	p.subscription(t, ctx, 0)
	c.mu.Lock()
	source := c.mon
	c.mu.Unlock()
	bad := msgr.MessageData{Type: 41, Version: 1, CompatVersion: 1, Front: []byte{0}}
	if err := c.handleMonOSDMap(new(session.Session), bad); err != nil {
		t.Fatal("unregistered source decoded an OSD map", err)
	}
	isolated := &Client{mon: source, options: c.options}
	isolated.osdMapWatch = &OSDMapStream{client: isolated, source: source, changed: make(chan struct{}), cancel: func() {}}
	if err := isolated.handleMonOSDMap(source, bad); err != nil {
		t.Fatal("unadmitted MON decoded an OSD map", err)
	}
	isolated.monReady = true
	isolated.osdMapWatch.source = new(session.Session)
	if err := isolated.handleMonOSDMap(source, bad); err != nil {
		t.Fatal("watch registered to another session decoded an OSD map", err)
	}
	monOSDMapsTestBarrier(t, ctx, c)
}

func (p *monOSDMapsTestPeer) nextRequest(t *testing.T, ctx context.Context, typ uint16) msgr.MessageData {
	t.Helper()
	var observed []uint16
	for {
		select {
		case message := <-p.requests:
			observed = append(observed, message.Type)
			if message.Type == typ {
				return message
			}
		case err := <-p.errors:
			t.Fatal("MON OSD map peer failed", err)
		case <-ctx.Done():
			t.Fatal("MON OSD map request was not observed", typ, "received types", observed, ctx.Err())
		}
	}
}

func TestMonOSDMapRequestPrivateIsolationPreservesPartialAndEmptyReplies(t *testing.T) {
	for _, profile := range []string{"latest", "partial-ranges", "empty-ranges"} {
		t.Run(profile, func(t *testing.T) {
			c, ctx, peers := monOSDMapsTestFixture(t)
			primary := monOSDMapsTestNextPeer(t, ctx, peers)
			stream, err := c.WatchOSDMaps(ctx, OSDMapOptions{StartEpoch: 11})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			primary.subscription(t, ctx, 11)
			before := c.Snapshot()
			c.mu.Lock()
			main, auth := c.mon, c.auth
			c.mu.Unlock()
			request := OSDMapRequest{}
			if profile != "latest" {
				request = OSDMapRequest{FullFirst: 7, FullLast: 10, IncrementalFirst: 11, IncrementalLast: 20}
			}
			type result struct {
				batch OSDMapBatch
				err   error
			}
			returned := make(chan result, 1)
			go func() { batch, err := c.RequestOSDMaps(ctx, request); returned <- result{batch, err} }()
			private := monOSDMapsTestNextPeer(t, ctx, peers)
			admission := private.nextRequest(t, ctx, 5)
			if admission.Transaction != 0 || admission.Version != 1 || admission.CompatVersion != 0 || len(admission.Front) != 0 {
				t.Fatal("private request did not independently admit its MON", admission)
			}
			if profile == "latest" {
				select {
				case sub := <-private.subscriptions:
					if sub.next != 0 || sub.flags != 1 || len(sub.all) != 1 || sub.hostname != "map-test-host" {
						t.Fatal("latest request was not an isolated ONETIME osdmap subscription", sub)
					}
				case <-ctx.Done():
					t.Fatal("private latest subscription not observed", ctx.Err())
				}
			} else {
				control := private.nextRequest(t, ctx, 6)
				// Pinned MMonGetOSDMap has an 18-byte Paxos prefix followed by
				// full-first/full-last/incremental-first/incremental-last U32s.
				want := make([]byte, 18)
				want[8], want[9] = 255, 255
				for _, bound := range []uint32{7, 10, 11, 20} {
					want = binary.LittleEndian.AppendUint32(want, bound)
				}
				if control.Transaction != 0 || control.Version != 1 || control.CompatVersion != 0 || !bytes.Equal(control.Front, want) || len(control.Middle) != 0 || len(control.Data) != 0 {
					t.Fatal("private range request changed native bounds or added TID correlation", control)
				}
			}
			reply := osdMapsMessage(4, [16]byte{1}, nil, []OSDMapBlob{{Epoch: 19, Data: []byte{0, 255, 9}}})
			if profile == "partial-ranges" {
				reply = osdMapsMessage(4, [16]byte{1}, []OSDMapBlob{{Epoch: 12, Data: []byte{8}}}, []OSDMapBlob{{Epoch: 8, Data: []byte{7}}})
			} else if profile == "empty-ranges" {
				reply = osdMapsMessage(4, [16]byte{1}, nil, nil)
			}
			private.send(t, ctx, reply)
			select {
			case got := <-returned:
				if got.err != nil || !bytes.Equal(got.batch.RawFront, reply.Front) || got.batch.NewestMap != 99 {
					t.Fatal("partial/empty native reply was rejected or treated as range completion", got.batch, got.err)
				}
			case <-ctx.Done():
				t.Fatal("private request waited for an entire requested range", ctx.Err())
			}
			select {
			case <-private.done:
			case <-ctx.Done():
				t.Fatal("private map request left its owned connection running", ctx.Err())
			}
			c.mu.Lock()
			preserved := c.mon == main && c.auth == auth
			c.mu.Unlock()
			if !preserved || !reflect.DeepEqual(before, c.Snapshot()) {
				t.Fatal("private map acquisition changed primary admission/identity", before, c.Snapshot())
			}
			shared := osdMapsMessage(1, [16]byte{1}, nil, []OSDMapBlob{{Epoch: 11, Data: []byte{6}}})
			primary.send(t, ctx, shared)
			monOSDMapsTestBarrier(t, ctx, c)
			monOSDMapsTestBatch(t, ctx, stream, shared) // A private reply must not enter this FIFO.
		})
	}
}

func TestMonOSDMapRequestCancellationAndClientCloseJoinPrivateConnection(t *testing.T) {
	for _, closer := range []string{"operation", "client"} {
		t.Run(closer, func(t *testing.T) {
			c, ctx, peers := monOSDMapsTestFixture(t)
			primary := monOSDMapsTestNextPeer(t, ctx, peers)
			operation, cancelOperation := context.WithCancel(ctx)
			defer cancelOperation()
			result := make(chan error, 1)
			go func() { _, err := c.RequestOSDMaps(operation, OSDMapRequest{}); result <- err }()
			private := monOSDMapsTestNextPeer(t, ctx, peers)
			private.nextRequest(t, ctx, 5)
			private.nextRequest(t, ctx, msgr.SubscribeMessage)
			// A subscription ACK without map data does not establish permission
			// or complete the request. The caller's context must still govern it.
			ack := binary.LittleEndian.AppendUint32(nil, 30)
			fsid := [16]byte{1}
			ack = append(ack, fsid[:]...)
			private.send(t, ctx, msgr.MessageData{Type: msgr.SubscribeAckMessage, Version: 1, Front: ack})
			if closer == "operation" {
				// The pending private request owns this client's sole MaxInFlight
				// slot. Watch registration and delivery must not require that slot.
				stream, err := c.WatchOSDMaps(ctx, OSDMapOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				primary.subscription(t, ctx, 0)
				message := osdMapsMessage(1, [16]byte{1}, nil, []OSDMapBlob{{Epoch: 7, Data: []byte{8}}})
				primary.send(t, ctx, message)
				monOSDMapsTestBatch(t, ctx, stream, message)
				select {
				case err := <-result:
					t.Fatal("ACK-only map request completed before caller cancellation", err)
				default:
				}
				cancelOperation()
			} else {
				c.Close()
			}
			want := context.Canceled
			if closer == "client" {
				want = ErrClosed
			}
			if err := osdMapsAwaitResult(t, ctx, result); !errors.Is(err, want) {
				t.Fatal("pending private map request lost its cancellation cause", err, want)
			}
			select {
			case <-private.done:
			case <-ctx.Done():
				t.Fatal("cancellation returned without joining private connection cleanup", ctx.Err())
			}
			if closer == "operation" {
				monOSDMapsTestBarrier(t, ctx, c)
				select {
				case p := <-peers:
					t.Fatal("canceled transmitted map request was replayed on another session", p)
				default:
				}
			}
		})
	}
}

func TestMonOSDMapWatchFirstFatalCauseSurvivesDelayedCleanup(t *testing.T) {
	for _, late := range []string{"malformed", "overflow", "watch-close", "client-close", "lifetime-cancel"} {
		t.Run(late, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			base, clientCancel := context.WithCancel(context.Background())
			conn, peer := net.Pipe()
			release := make(chan struct{})
			held := &managerCleanupConn{Conn: conn, closing: make(chan struct{}), cleaned: make(chan struct{}), release: release}
			var releaseOnce sync.Once
			finish := func() { releaseOnce.Do(func() { close(release) }) }
			c := &Client{ctx: base, cancel: clientCancel, changed: make(chan struct{}), fsid: [16]byte{1}, options: Options{MaxFrameSize: 64 << 10}, sessions: make(map[*session.Session]struct{})}
			watchBase, watchCancel := context.WithCancel(base)
			gate := &logTestLoopGate{Context: watchBase, entered: make(chan struct{}), release: make(chan struct{})}
			gate.armed.Store(true)
			stream := &OSDMapStream{client: c, ctx: gate, cancel: watchCancel, done: make(chan struct{}), limit: 1024, changed: make(chan struct{})}
			var source *session.Session
			source = session.New(&session.Transport{Conn: held}, session.Config{}, nil, func(err error) { c.stopOSDMapForFailedSession(source, err) })
			c.mon, c.monReady, c.osdMapWatch = source, true, stream
			stream.source, stream.observed = source, source
			c.sessions[source] = struct{}{}
			c.wg.Add(2)
			go func() { source.Wait(); c.wg.Done() }()
			go stream.run()
			t.Cleanup(func() { gate.unblock(); finish(); c.Close(); peer.Close(); watchCancel() })
			awaitCleanup(t, ctx, gate.entered)
			accepted := osdMapsMessage(1, [16]byte{1}, nil, []OSDMapBlob{{Epoch: 5, Data: []byte{9}}})
			if err := c.handleMonOSDMap(source, accepted); err != nil {
				t.Fatal(err)
			}
			cause := &net.OpError{Op: "read", Net: "tcp", Err: msgr.ErrCRC}
			go source.Fail(cause)
			awaitCleanup(t, ctx, held.closing)
			closed := make(chan error, 1)
			if late == "watch-close" {
				go func() { closed <- stream.Close() }()
			} else if late == "client-close" {
				go func() { closed <- c.Close() }()
			} else if late == "lifetime-cancel" {
				// The watch lifetime is independent of the live Next context.
				// Neither the paused worker nor onClose can publish the earlier
				// fatal cause before this cancellation reaches stop.
				watchCancel()
			} else {
				message := osdMapsMessage(4, [16]byte{1}, nil, []OSDMapBlob{{Epoch: 6, Data: []byte{8}}})
				if late == "malformed" {
					message.Front = message.Front[:len(message.Front)-1]
				}
				err := c.handleMonOSDMap(source, message)
				if late == "malformed" {
					if err != nil && !errors.Is(err, ErrMalformedMessage) {
						t.Fatal("late callback returned an unrelated error", err)
					}
					if err != nil {
						source.Fail(err) // Its already published fatal cause must win.
					}
				} else if err != nil {
					t.Fatal("watch-only overflow failed the shared session", err)
				}
			}
			gate.unblock()
			monOSDMapsTestBatch(t, ctx, stream, accepted)
			if _, err := stream.Next(ctx); err != cause {
				t.Fatal("late callback/close/cancellation replaced the session's first fatal cause", err, cause)
			}
			select {
			case <-held.cleaned:
				t.Fatal("fatal cause required connection cleanup to finish")
			default:
			}
			finish()
			source.Wait()
			if late == "watch-close" || late == "client-close" {
				select {
				case err := <-closed:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("close did not join released cleanup", ctx.Err())
				}
			}
			if _, err := stream.Next(ctx); err != cause {
				t.Fatal("onClose replaced the first fatal cause", err, cause)
			}
		})
	}
}
