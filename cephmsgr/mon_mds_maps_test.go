package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"github.com/jsyoo5b/ceph-msgr-go/internal/mds"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

type monMDSMapsTestSubscription struct {
	next     uint64
	flags    byte
	hostname string
	topic    string
	all      map[string]uint64
}

type monMDSMapsTestPeer struct {
	conn          net.Conn
	sends         chan logTestSend
	subscriptions chan monMDSMapsTestSubscription
	commands      chan msgr.MessageData
	requests      chan msgr.MessageData
	done          chan struct{}
	errors        chan error
}

// Synthetic MON exercises admission, registration and ordinary command sharing.
// Map payloads below derive from retained native fixtures; this peer itself is
// not evidence of live Ceph interoperability.
func (p *monMDSMapsTestPeer) serve(early bool) {
	defer close(p.done)
	defer p.conn.Close()
	r, w, err := mockAuthenticate(p.conn, peerConfig{role: 1, release: 20, fsid: [16]byte{1}, payload: func(tag msgr.Tag, p []byte) []byte { return backoffTransportFeatures(tag, p, 1<<29) }})
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
		if send(msgr.MessageData{Type: mds.MDSMapMessageType, Version: 2, CompatVersion: 1, Front: []byte{0}}) != nil {
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
				sub := monMDSMapsTestSubscription{all: make(map[string]uint64)}
				var present bool
				for range count {
					name, next, flags := d.String(), d.U64(), d.U8()
					sub.all[name] = next
					if name == "fsmap.user" || strings.HasPrefix(name, "mdsmap.") {
						sub.topic = name
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
						p.errors <- errors.New("MDS map subscription preceded MON admission request")
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

func (p *monMDSMapsTestPeer) send(t *testing.T, ctx context.Context, m msgr.MessageData) {
	t.Helper()
	request := logTestSend{message: m, result: make(chan error, 1)}
	select {
	case p.sends <- request:
	case err := <-p.errors:
		t.Fatal("MON MDS map peer failed", err)
	case <-p.done:
		t.Fatal("MON MDS map peer stopped before send")
	case <-ctx.Done():
		t.Fatal("MON MDS map send did not start", ctx.Err())
	}
	select {
	case err := <-request.result:
		if err != nil {
			t.Fatal("MON MDS map send failed", err)
		}
	case <-ctx.Done():
		t.Fatal("MON MDS map send did not finish", ctx.Err())
	}
}

func (p *monMDSMapsTestPeer) subscription(t *testing.T, ctx context.Context, topic string, next uint64) {
	t.Helper()
	select {
	case sub := <-p.subscriptions:
		if sub.next != next || sub.flags != 0 || sub.hostname != "map-test-host" || len(sub.all) != 1 || sub.all[topic] != next || sub.topic != topic {
			t.Fatal("MDS subscription lost its cursor/flags/hostname or added unrelated keys", sub, next)
		}
	case err := <-p.errors:
		t.Fatal("MON MDS map peer failed", err)
	case <-ctx.Done():
		t.Fatal("MDS map subscription not observed", next, ctx.Err())
	}
}

func monMDSMapsTestFixture(t *testing.T, wrap ...func(int, net.Conn) net.Conn) (*Client, context.Context, <-chan *monMDSMapsTestPeer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	options := mockOptions(t, 20, [16]byte{1})
	options.EnableMDSMaps = true
	options.Hostname, options.MaxFrameSize, options.MaxInFlight = "map-test-host", 64<<10, 1
	dial := options.DialContext
	peers := make(chan *monMDSMapsTestPeer, 16)
	var mu sync.Mutex
	var all []*monMDSMapsTestPeer
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if !strings.HasSuffix(endpoint, ":3300") {
			return dial(ctx, network, endpoint)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		client, server := net.Pipe()
		p := &monMDSMapsTestPeer{conn: server, sends: make(chan logTestSend), subscriptions: make(chan monMDSMapsTestSubscription, 8), commands: make(chan msgr.MessageData, 128), requests: make(chan msgr.MessageData, 128), done: make(chan struct{}), errors: make(chan error, 8)}
		mu.Lock()
		index := len(all)
		early := index != 0
		all = append(all, p)
		mu.Unlock()
		for _, wrapConn := range wrap {
			client = wrapConn(index, client)
		}
		peers <- p
		go p.serve(early)
		return client, nil
	}
	c, err := Dial(ctx, options)
	if err != nil {
		cancel()
		t.Fatal("admit MON MDS map fixture", err)
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
				t.Error("MON MDS map peer did not terminate")
			}
		}
	})
	return c, ctx, peers
}

func monMDSMapsTestNextPeer(t *testing.T, ctx context.Context, peers <-chan *monMDSMapsTestPeer) *monMDSMapsTestPeer {
	t.Helper()
	select {
	case p := <-peers:
		return p
	case <-ctx.Done():
		t.Fatal("MON MDS map recovery did not dial a new peer", ctx.Err())
		return nil
	}
}

func monMDSMapsTestBarrier(t *testing.T, ctx context.Context, c *Client) {
	t.Helper()
	result, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)})
	if err != nil || result.Code != 0 || result.Message != "MON map-side status" || !bytes.Equal(result.Data, []byte{0, 255, 7}) {
		t.Fatal("MON MDS map handling blocked or changed command output", result, err)
	}
}

// Native fixture data is copied before epoch/cluster edits for synthetic tests.
func mdsMapsTestMessage(t *testing.T, isMDS bool, epoch uint32) msgr.MessageData {
	t.Helper()
	name := "fsmap-multi"
	typ, version, compat := uint16(mds.FSMapUserMessage), uint16(1), uint16(1)
	if isMDS {
		name = "mdsmap-empty"
		typ, version, compat = mds.MDSMapMessageType, 2, 1
	}
	raw, err := os.ReadFile("../internal/mds/testdata/mds-map-native-" + name + ".front.hex")
	if err != nil {
		t.Fatal(err)
	}
	front, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	if isMDS {
		copy(front[:16], ([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}))
		binary.LittleEndian.PutUint32(front[16:20], epoch)
		binary.LittleEndian.PutUint32(front[30:34], epoch)
	} else {
		binary.LittleEndian.PutUint32(front[:4], epoch)
		binary.LittleEndian.PutUint32(front[10:14], epoch)
	}
	return msgr.MessageData{Type: typ, Version: version, CompatVersion: compat, Front: front}
}

func TestMonMDSMapsStreamsBindExplicitIDAndPreserveRaw(t *testing.T) {
	c, ctx, peers := monMDSMapsTestFixture(t)
	p := monMDSMapsTestNextPeer(t, ctx, peers)
	fs, err := c.WatchFSMapUser(ctx, FSMapUserOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	p.subscription(t, ctx, "fsmap.user", 0)
	stream, err := c.WatchMDSMaps(ctx, 7, MDSMapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	p.subscription(t, ctx, "mdsmap.7", 0)
	if _, err := c.WatchMDSMaps(ctx, 8, MDSMapOptions{}); !errors.Is(err, ErrMDSMapWatchActive) {
		t.Fatal(err)
	}
	fsMessage, mdsMessage := mdsMapsTestMessage(t, false, 0), mdsMapsTestMessage(t, true, 0)
	p.send(t, ctx, fsMessage)
	p.send(t, ctx, mdsMessage)
	monMDSMapsTestBarrier(t, ctx, c)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := fs.Next(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	directory, err := fs.Next(ctx)
	if err != nil || directory.Epoch != 0 || len(directory.Filesystems) != 2 || !bytes.Equal(directory.RawFront, fsMessage.Front) || directory.SourceGeneration == 0 {
		t.Fatal(directory, err)
	}
	update, err := stream.Next(ctx)
	if err != nil || update.Epoch != 0 || update.Map.Epoch != 0 || update.FilesystemID != 7 || update.FSID != "01000000-0000-0000-0000-000000000000" || !bytes.Equal(update.RawFront, mdsMessage.Front) || update.SourceGeneration != directory.SourceGeneration {
		t.Fatal(update, err)
	}
	monMDSMapsTestBarrier(t, ctx, c)
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int32{7, 8} {
		if _, err := c.WatchMDSMaps(ctx, id, MDSMapOptions{}); !errors.Is(err, ErrMDSMapSourceBound) {
			t.Fatalf("same MON FSCID %d escaped tombstone: %v", id, err)
		}
	}
	if _, err := stream.Next(ctx); !errors.Is(err, ErrMDSMapStreamClosed) {
		t.Fatal(err)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, err := c.WatchFSMapUser(ctx, FSMapUserOptions{StartEpoch: 17})
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	p.subscription(t, ctx, "fsmap.user", 17)
	// FSMapUser has no identity beyond its same cluster-wide topic. A late frame
	// from the old watch satisfying StartEpoch remains a raw observation.
	p.send(t, ctx, mdsMapsTestMessage(t, false, 18))
	got, err := replacement.Next(ctx)
	if err != nil || got.Epoch != 18 || got.SourceGeneration != directory.SourceGeneration {
		t.Fatal(got, err)
	}
}

func TestMonMDSMapsRecoveryUsesHighestAcceptedAndNewSource(t *testing.T) {
	c, ctx, peers := monMDSMapsTestFixture(t)
	p := monMDSMapsTestNextPeer(t, ctx, peers)
	fs, err := c.WatchFSMapUser(ctx, FSMapUserOptions{StartEpoch: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	p.subscription(t, ctx, "fsmap.user", 4)
	stream, err := c.WatchMDSMaps(ctx, 0, MDSMapOptions{StartEpoch: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	p.subscription(t, ctx, "mdsmap.0", 8)
	c.mu.Lock()
	old := c.mon
	c.mu.Unlock()
	p.send(t, ctx, mdsMapsTestMessage(t, false, 3))
	p.send(t, ctx, mdsMapsTestMessage(t, false, 6))
	p.send(t, ctx, mdsMapsTestMessage(t, false, 6))
	p.send(t, ctx, mdsMapsTestMessage(t, false, 5))
	p.send(t, ctx, mdsMapsTestMessage(t, true, 7))
	p.send(t, ctx, mdsMapsTestMessage(t, true, 10))
	p.send(t, ctx, mdsMapsTestMessage(t, true, 9))
	monMDSMapsTestBarrier(t, ctx, c)
	p.conn.Close()
	next := monMDSMapsTestNextPeer(t, ctx, peers)
	seen := map[string]uint64{}
	for range 2 {
		select {
		case sub := <-next.subscriptions:
			seen[sub.topic] = sub.next
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if seen["fsmap.user"] != 7 || seen["mdsmap.0"] != 11 {
		t.Fatal(seen)
	}
	stale := mdsMapsTestMessage(t, true, 100)
	stale.Front[0] = 255
	if err := c.handleMonMDSMap(old, stale); err != nil {
		t.Fatal("stale source parsed", err)
	}
	next.send(t, ctx, mdsMapsTestMessage(t, false, 7))
	next.send(t, ctx, mdsMapsTestMessage(t, true, 11))
	monMDSMapsTestBarrier(t, ctx, c)
	a, err := fs.Next(ctx)
	if err != nil || a.Epoch != 6 {
		t.Fatal(a, err)
	}
	b, err := fs.Next(ctx)
	if err != nil || b.Epoch != 7 || b.SourceGeneration == a.SourceGeneration {
		t.Fatal(b, err)
	}
	m, err := stream.Next(ctx)
	if err != nil || m.Epoch != 10 {
		t.Fatal(m, err)
	}
	n, err := stream.Next(ctx)
	if err != nil || n.Epoch != 11 || n.SourceGeneration == m.SourceGeneration || n.SourceGeneration != b.SourceGeneration {
		t.Fatal(n, err)
	}
	wait, release := context.WithCancel(ctx)
	release()
	if _, err := stream.Next(wait); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// No regressing/duplicate or stale-source frames remain queued.
	c.mu.Lock()
	fsCount, mdsCount := fs.watch.queueCount, stream.watch.queueCount
	c.mu.Unlock()
	if fsCount != 0 || mdsCount != 0 {
		t.Fatal(fsCount, mdsCount)
	}
}

func TestMonMDSMapsOverflowAndEpochExhaustionDrain(t *testing.T) {
	for _, isMDS := range []bool{false, true} {
		for _, mode := range []string{"count", "bytes", "epoch"} {
			t.Run(strconv.FormatBool(isMDS)+"/"+mode, func(t *testing.T) {
				c, ctx, peers := monMDSMapsTestFixture(t)
				p := monMDSMapsTestNextPeer(t, ctx, peers)
				var s *monMDSMapWatch
				limit := uint32(1 << 20)
				if mode == "bytes" {
					limit = 1024
				}
				if isMDS {
					w, err := c.WatchMDSMaps(ctx, 4, MDSMapOptions{MaxBufferedBytes: limit})
					if err != nil {
						t.Fatal(err)
					}
					defer w.Close()
					s = w.watch
					p.subscription(t, ctx, "mdsmap.4", 0)
				} else {
					w, err := c.WatchFSMapUser(ctx, FSMapUserOptions{MaxBufferedBytes: limit})
					if err != nil {
						t.Fatal(err)
					}
					defer w.Close()
					s = w.watch
					p.subscription(t, ctx, "fsmap.user", 0)
				}
				terminal := ErrMDSMapWatchOverflow
				count := 0
				switch mode {
				case "count":
					for i := range 65 {
						p.send(t, ctx, mdsMapsTestMessage(t, isMDS, uint32(i)))
					}
					count = 64
				case "bytes":
					p.send(t, ctx, mdsMapsTestMessage(t, isMDS, 1))
				case "epoch":
					p.send(t, ctx, mdsMapsTestMessage(t, isMDS, math.MaxUint32))
					count = 1
					terminal = ErrMDSMapEpochExhausted
				}
				monMDSMapsTestBarrier(t, ctx, c)
				for i := range count {
					q, err := s.next(ctx)
					epoch := q.fs.Epoch
					if isMDS {
						epoch = q.mds.Epoch
					}
					expected := uint32(i)
					if mode == "epoch" {
						expected = math.MaxUint32
					}
					if err != nil || epoch != expected {
						t.Fatal(epoch, err)
					}
				}
				for range 2 {
					if _, err := s.next(ctx); !errors.Is(err, terminal) {
						t.Fatal(err)
					}
				}
				monMDSMapsTestBarrier(t, ctx, c)
			})
		}
	}
}

func TestMonMDSMapsMalformedCurrentSourceFailsBothWatches(t *testing.T) {
	for _, kind := range []string{"fs-epoch", "fs-compat-zero", "mds-epoch", "fsid", "version"} {
		t.Run(kind, func(t *testing.T) {
			c, ctx, peers := monMDSMapsTestFixture(t)
			p := monMDSMapsTestNextPeer(t, ctx, peers)
			fs, err := c.WatchFSMapUser(ctx, FSMapUserOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer fs.Close()
			p.subscription(t, ctx, "fsmap.user", 0)
			stream, err := c.WatchMDSMaps(ctx, 9, MDSMapOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			p.subscription(t, ctx, "mdsmap.9", 0)
			p.send(t, ctx, mdsMapsTestMessage(t, false, 1))
			p.send(t, ctx, mdsMapsTestMessage(t, true, 2))
			monMDSMapsTestBarrier(t, ctx, c)
			invalid := mdsMapsTestMessage(t, !strings.HasPrefix(kind, "fs-"), 3)
			switch kind {
			case "fs-compat-zero":
				invalid.CompatVersion = 0
			case "fs-epoch":
				binary.LittleEndian.PutUint32(invalid.Front[10:14], 4)
			case "mds-epoch":
				binary.LittleEndian.PutUint32(invalid.Front[30:34], 4)
			case "fsid":
				invalid.Front[0] ^= 1
			case "version":
				invalid.Version++
			}
			p.send(t, ctx, invalid)
			if got, err := fs.Next(ctx); err != nil || got.Epoch != 1 {
				t.Fatal(got, err)
			}
			if got, err := stream.Next(ctx); err != nil || got.Epoch != 2 {
				t.Fatal(got, err)
			}
			_, a := fs.Next(ctx)
			_, b := stream.Next(ctx)
			if !errors.Is(a, msgr.ErrFrame) || !errors.Is(b, msgr.ErrFrame) {
				t.Fatal(a, b)
			}
		})
	}
}

func TestMonMDSMapsCancellationAndAdmissionBounds(t *testing.T) {
	c, ctx, peers := monMDSMapsTestFixture(t)
	p := monMDSMapsTestNextPeer(t, ctx, peers)
	if _, err := c.WatchMDSMaps(ctx, -1, MDSMapOptions{}); err == nil {
		t.Fatal("negative FSCID accepted")
	}
	for _, limit := range []uint32{1, 1023, 1<<30 + 1} {
		if _, err := c.WatchFSMapUser(ctx, FSMapUserOptions{MaxBufferedBytes: limit}); err == nil {
			t.Fatal(limit)
		}
	}
	lifetime, cancel := context.WithCancel(ctx)
	fs, err := c.WatchFSMapUser(lifetime, FSMapUserOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	p.subscription(t, ctx, "fsmap.user", 0)
	p.send(t, ctx, mdsMapsTestMessage(t, false, 1))
	monMDSMapsTestBarrier(t, ctx, c)
	cancel()
	if got, err := fs.Next(ctx); err != nil || got.Epoch != 1 {
		t.Fatal(got, err)
	}
	if _, err := fs.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := c.WatchFSMapUser(ctx, FSMapUserOptions{}); err != nil {
		t.Fatal(err)
	}
	monMDSMapsTestBarrier(t, ctx, c)
	disabled, disabledCtx, _ := monOSDMapsTestFixture(t)
	if _, err := disabled.WatchFSMapUser(disabledCtx, FSMapUserOptions{}); !errors.Is(err, ErrUnsupportedFeatures) {
		t.Fatal(err)
	}
	if _, err := disabled.WatchMDSMaps(disabledCtx, 0, MDSMapOptions{}); !errors.Is(err, ErrUnsupportedFeatures) {
		t.Fatal(err)
	}
}

func TestMonMDSMapsFirstFatalCauseSurvivesDelayedCleanup(t *testing.T) {
	for _, isMDS := range []bool{false, true} {
		for _, late := range []string{"malformed", "overflow", "watch-close", "client-close", "lifetime-cancel", "auth-rejection"} {
			t.Run(strconv.FormatBool(isMDS)+"/"+late, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				base, clientCancel := context.WithCancel(context.Background())
				conn, peer := net.Pipe()
				release := make(chan struct{})
				held := &managerCleanupConn{Conn: conn, closing: make(chan struct{}), cleaned: make(chan struct{}), release: release}
				var once sync.Once
				finish := func() { once.Do(func() { close(release) }) }
				c := &Client{ctx: base, cancel: clientCancel, changed: make(chan struct{}), fsid: [16]byte{1}, options: Options{MaxFrameSize: 64 << 10, EnableMDSMaps: true}, sessions: make(map[*session.Session]struct{})}
				watchBase, watchCancel := context.WithCancel(base)
				gate := &logTestLoopGate{Context: watchBase, entered: make(chan struct{}), release: make(chan struct{})}
				gate.armed.Store(true)
				s := &monMDSMapWatch{client: c, ctx: gate, cancel: watchCancel, done: make(chan struct{}), limit: 64 << 10, isMDS: isMDS, filesystemID: 7, id: 1, changed: make(chan struct{})}
				var source *session.Session
				source = session.New(&session.Transport{Conn: held}, session.Config{}, nil, func(err error) { c.stopMDSMapsForFailedSession(source, err) })
				c.mon, c.monReady = source, true
				if isMDS {
					c.mdsMapWatch = s
				} else {
					c.fsMapWatch = s
				}
				s.source, s.observed = source, source
				c.sessions[source] = struct{}{}
				c.wg.Add(2)
				go func() { source.Wait(); c.wg.Done() }()
				go s.run()
				t.Cleanup(func() { gate.unblock(); finish(); c.Close(); peer.Close(); watchCancel() })
				awaitCleanup(t, ctx, gate.entered)
				if err := c.handleMonMDSMap(source, mdsMapsTestMessage(t, isMDS, 5)); err != nil {
					t.Fatal(err)
				}
				cause := &net.OpError{Op: "read", Net: "tcp", Err: msgr.ErrCRC}
				go source.Fail(cause)
				awaitCleanup(t, ctx, held.closing)
				closed := make(chan error, 1)
				switch late {
				case "watch-close":
					go func() { closed <- s.close(ErrMDSMapStreamClosed) }()
				case "client-close":
					go func() { closed <- c.Close() }()
				case "lifetime-cancel":
					watchCancel()
				case "auth-rejection":
					c.rejectAuthentication(&AuthenticationError{Code: -13})
				default:
					message := mdsMapsTestMessage(t, isMDS, 6)
					if late == "malformed" {
						message.Front = message.Front[:len(message.Front)-1]
					} else {
						c.mu.Lock()
						s.limit = s.bufferedBytes
						c.mu.Unlock()
					}
					err := c.handleMonMDSMap(source, message)
					if err != nil {
						source.Fail(err)
					}
				}
				gate.unblock()
				q, err := s.next(ctx)
				epoch := q.fs.Epoch
				if isMDS {
					epoch = q.mds.Epoch
				}
				if err != nil || epoch != 5 {
					t.Fatal(epoch, err)
				}
				if _, err := s.next(ctx); err != cause {
					t.Fatal("later action replaced first fatal cause", err, cause)
				}
				select {
				case <-held.cleaned:
					t.Fatal("firstcause required cleanup completion")
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
						t.Fatal(ctx.Err())
					}
				}
				if _, err := s.next(ctx); err != cause {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestMonMDSMapsQueueFixedNodeReservation(t *testing.T) {
	if size := unsafe.Sizeof(queuedMONMDSMap{}); size > 768 {
		t.Fatalf("fixed queue node %d exceeds reserved 768 bytes", size)
	}
}

func TestMonMDSMapsLocalIdentityExhaustionAndClosedAdmission(t *testing.T) {
	for _, counter := range []string{"watch", "source"} {
		t.Run(counter, func(t *testing.T) {
			c, ctx, _ := monMDSMapsTestFixture(t)
			c.mu.Lock()
			if counter == "watch" {
				c.nextMDSMapWatchID = math.MaxUint64
			} else {
				c.mdsMapGeneration = math.MaxUint64
			}
			c.mu.Unlock()
			stream, err := c.WatchMDSMaps(ctx, 7, MDSMapOptions{})
			if counter == "watch" {
				if !errors.Is(err, ErrMDSMapEpochExhausted) {
					t.Fatal(err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				if _, err := stream.Next(ctx); !errors.Is(err, ErrMDSMapEpochExhausted) {
					t.Fatal(err)
				}
			}
			monMDSMapsTestBarrier(t, ctx, c)
			c.Close()
			if _, err := c.WatchFSMapUser(ctx, FSMapUserOptions{MaxBufferedBytes: 1}); !errors.Is(err, ErrClosed) {
				t.Fatal(err)
			}
			if _, err := c.WatchMDSMaps(ctx, -1, MDSMapOptions{MaxBufferedBytes: 1}); !errors.Is(err, ErrClosed) {
				t.Fatal(err)
			}
		})
	}
}
