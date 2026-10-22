package cephmsgr

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
)

func TestOSDCachePoolOptionRequiresOSD(t *testing.T) {
	if _, err := Dial(context.Background(), Options{EnableOSDCachePools: true}); !errors.Is(err, ErrOSDDisabled) {
		t.Fatal("cache-pool opt-in without OSD admitted", err)
	}
	for _, p := range []OSDProfile{OSDProfileObjectHash, OSDProfileReplicatedPG, OSDProfileReplicatedPGReplyV8, OSDProfileReplicatedPGRequestV9ReplyV8} {
		if err := validateOSDProfile(Options{EnableOSD: true, EnableOSDCachePools: true, OSDProfile: p}); err != nil {
			t.Fatal("existing profile excluded", p, err)
		}
	}
}

func TestOSDIgnoreCacheRefusesBeforeSlotMonitorAndIdentity(t *testing.T) {
	edits := map[string]func(*OSDConnection, *OSDRequest){
		"erasure": func(o *OSDConnection, r *OSDRequest) {
			if r.Placement == nil {
				r.Placement = &OSDPlacement{PG: OSDPG{Pool: 7, Seed: 3}, Primary: o.target.ID}
			}
			r.Placement.Erasure = true
		},
		"default-deny":    func(o *OSDConnection, r *OSDRequest) { o.client.options.EnableOSDCachePools = false },
		"missing-feature": func(o *OSDConnection, r *OSDRequest) { o.features &^= osd.FeatureCachePool },
		"compound": func(o *OSDConnection, r *OSDRequest) {
			r.Operations = append(r.Operations, OSDOperation{Code: OSDStat})
		},
		"opcode":        func(o *OSDConnection, r *OSDRequest) { r.Operations[0].Code = OSDAppend },
		"snapshot":      func(o *OSDConnection, r *OSDRequest) { r.ReadSnapshot = 1 },
		"snap-context":  func(o *OSDConnection, r *OSDRequest) { r.SnapContext.Snapshots = []uint64{1} },
		"snap-seq":      func(o *OSDConnection, r *OSDRequest) { r.SnapContext.Sequence = 1 },
		"order-snap":    func(o *OSDConnection, r *OSDRequest) { r.OrderSnapshots = true },
		"retry":         func(o *OSDConnection, r *OSDRequest) { r.RetryAttempt = 1 },
		"resubmit":      func(o *OSDConnection, r *OSDRequest) { r.Resubmit = &OSDSubmission{Transaction: 1} },
		"balance":       func(o *OSDConnection, r *OSDRequest) { r.BalanceReads = true },
		"localize":      func(o *OSDConnection, r *OSDRequest) { r.LocalizeReads = true },
		"skip-locks":    func(o *OSDConnection, r *OSDRequest) { r.SkipRWLocks = true },
		"redirect":      func(o *OSDConnection, r *OSDRequest) { r.IgnoreRedirect = true },
		"rw-order":      func(o *OSDConnection, r *OSDRequest) { r.OrderReadsWrites = true },
		"opflags":       func(o *OSDConnection, r *OSDRequest) { r.Operations[0].Flags = OSDFadviseNoCache },
		"truncate-size": func(o *OSDConnection, r *OSDRequest) { r.Operations[0].TruncateSize = 1 },
		"truncate-seq":  func(o *OSDConnection, r *OSDRequest) { r.Operations[0].TruncateSequence = 1 },
	}
	for _, profile := range []OSDProfile{OSDProfileObjectHash, OSDProfileReplicatedPG, OSDProfileReplicatedPGReplyV8, OSDProfileReplicatedPGRequestV9ReplyV8} {
		for name, edit := range edits {
			t.Run(profileNameForCacheTest(profile)+"/"+name, func(t *testing.T) {
				o := backoffUnitConnection()
				o.client.options.OSDProfile = profile
				o.client.options.EnableOSDCachePools = true
				o.features |= osd.FeatureCachePool
				// A foreign owner occupies the sole slot; MON and writer are unstarted.
				// A local capability refusal must never wait for or consume this token.
				o.client.calls = make(chan struct{}, 1)
				o.client.calls <- struct{}{}
				r := OSDRequest{IgnoreCache: true, MapEpoch: 17, Object: OSDObject{Pool: 7, Name: "object"}, Operations: []OSDOperation{{Code: OSDRead, Length: 2}}}
				if profile.replicatedPG() {
					r.Placement = &OSDPlacement{PG: OSDPG{Pool: 7, Seed: 3}, Primary: o.target.ID}
				}
				edit(o, &r)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				result, err := o.Request(ctx, r)
				if err == nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || result.Submission.Transaction != 0 || o.client.osdNext.Load() != 0 || len(o.client.calls) != 1 {
					t.Fatal("invalid bypass passed local preflight", err, result.Submission.Transaction, o.client.osdNext.Load(), len(o.client.calls))
				}
			})
		}
	}
}

func profileNameForCacheTest(p OSDProfile) string { return []string{"v6", "v8", "reply-v8", "v9"}[p] }

func TestOSDIgnoreCacheExplicitScopeAndOrdinaryRequests(t *testing.T) {
	o := backoffUnitConnection()
	o.client.options.EnableOSDCachePools = true
	o.features |= osd.FeatureCachePool
	for _, code := range []OSDOpcode{OSDStat, OSDRead, OSDWriteFull, OSDDelete} {
		r := OSDRequest{IgnoreCache: true, Operations: []OSDOperation{{Code: code}}, ReadSnapshot: OSDHeadSnapshot}
		if err := o.validateIgnoreCache(r); err != nil {
			t.Fatal("supported head operation refused", code, err)
		}
		r.Placement = &OSDPlacement{Erasure: true}
		if err := o.validateIgnoreCache(r); err == nil {
			t.Fatal("EC bypass admitted")
		}
		r.IgnoreCache = false
		if err := o.validateIgnoreCache(r); err != nil {
			t.Fatal("ordinary EC request policy changed", err)
		}
	}
	o.client.options.EnableOSDCachePools = false
	o.features = 0
	if err := o.validateIgnoreCache(OSDRequest{Operations: []OSDOperation{{Code: OSDAppend}}, RetryAttempt: 1}); err != nil {
		t.Fatal("ordinary request validation changed", err)
	}
}

// Real Dial/OpenOSD/private MON admission against authenticated synthetic peers
// checks public plumbing, strict reply selection and uncertain mutation policy.
func TestOSDCachePoolPublicTransportAndUnknownMutation(t *testing.T) {
	for _, profile := range []OSDProfile{OSDProfileObjectHash, OSDProfileReplicatedPG, OSDProfileReplicatedPGReplyV8, OSDProfileReplicatedPGRequestV9ReplyV8} {
		t.Run(profileNameForCacheTest(profile), func(t *testing.T) {
			options := mockOptions(t, 20, [16]byte{1})
			options.EnableOSD, options.EnableOSDCachePools, options.OSDProfile = true, true, profile
			var monDials, osdDials atomic.Int32
			peers := make(chan *osdTestPeer, 1)
			options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
				client, server := net.Pipe()
				if endpoint == "192.0.2.1:6800" {
					go mockDaemon(server, peerConfig{role: 16, id: 99})
					return client, nil
				}
				if endpoint != "192.0.2.1:6804" {
					services := uint32(cephx.ServiceAuth | cephx.ServiceMgr)
					primary := monDials.Add(1) == 1
					if primary {
						services |= cephx.ServiceOSD
					}
					cfg := peerConfig{role: 1, release: 20, fsid: [16]byte{1}, services: services, payload: func(tag msgr.Tag, p []byte) []byte {
						mask := osd.FeatureCachePool
						if profile.replicatedPG() {
							mask |= 1 << 30
						}
						return backoffTransportFeatures(tag, p, mask)
					}}
					if primary {
						go mockDaemon(server, cfg)
					} else {
						// A private MON explicitly requests its map. The general
						// command mock would misclassify MGetMap as a command.
						go func() {
							defer server.Close()
							r, w, err := mockAuthenticate(server, cfg)
							if err != nil {
								return
							}
							for {
								frame, err := r.Read()
								if err != nil {
									return
								}
								if frame.Tag != msgr.Message {
									continue
								}
								message, err := msgr.DecodeMessage(frame)
								if err != nil {
									return
								}
								if message.Type != msgr.GetMonMap().Type {
									continue
								}
								mapReply := msgr.MessageData{Type: msgr.MonMapMessage, Version: 1, Priority: 127, Sequence: 1, Front: mockMonMap(20, [16]byte{1})}
								if err := w.Write(mapReply.Frame()); err != nil {
									return
								}
							}
						}()
					}
					return client, nil
				}
				// Each OSD session identifies as the ID the test opened it with.
				osdID := osdDials.Add(1) - 1
				peer := &osdTestPeer{conn: server, requests: make(chan msgr.MessageData, 8), done: make(chan struct{})}
				peers <- peer
				go func() {
					defer close(peer.done)
					defer server.Close()
					r, w, err := mockAuthenticate(server, peerConfig{role: 4, id: uint64(osdID), payload: func(tag msgr.Tag, p []byte) []byte {
						mask := osd.FeatureCachePool
						if profile.replicatedPG() {
							mask |= 1<<8 | 1<<9 | 1<<21 | 1<<30 | 1<<56 | 1<<57
						}
						if profile.replyV8() {
							mask |= 1 << 58
						}
						if profile.requestV9() {
							mask |= 1 << 49
						}
						return backoffTransportFeatures(tag, p, mask)
					}})
					if err != nil {
						return
					}
					peer.writer = w
					for {
						frame, err := r.Read()
						if err != nil {
							return
						}
						if frame.Tag != msgr.Message {
							continue
						}
						m, err := msgr.DecodeMessage(frame)
						if err != nil {
							return
						}
						peer.requests <- m
					}
				}()
				return client, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c, err := Dial(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			// The same opted-in requirement applies to the independent named MON path.
			fsid, addresses, err := c.namedMonitorAddresses(ctx, "a")
			if err != nil || len(addresses) == 0 {
				t.Fatal(err)
			}
			private, err := c.openNamedMonitor(ctx, "a", fsid, addresses[0])
			if err != nil {
				t.Fatal("private MON lost cache-pool capability", err)
			}
			private.Fail(ErrClosed)
			private.Wait()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			request := OSDRequest{IgnoreCache: true, MapEpoch: 5, Object: OSDObject{Pool: 1, Name: "object"}, Operations: []OSDOperation{{Code: OSDRead, Length: 1}}}
			if profile.replicatedPG() {
				request.Placement = &OSDPlacement{PG: OSDPG{Pool: 1}, Primary: 0}
			}
			type result struct {
				reply OSDReply
				err   error
			}
			done := make(chan result, 1)
			go func() { r, err := o.Request(ctx, request); done <- result{r, err} }()
			sent := peer.next(t)
			offset := 8
			if profile.replicatedPG() {
				offset = 32
			}
			if binary.LittleEndian.Uint32(sent.Front[offset:])&uint32(osd.FlagIgnoreCache) == 0 {
				t.Fatal("public IgnoreCache flag was not transmitted")
			}
			if profile == OSDProfileObjectHash {
				peer.reply(t, sent)
			} else {
				backoffProfileReadReply(t, peer, profile, request, sent)
			}
			select {
			case got := <-done:
				if got.err != nil || got.reply.Code != 0 || len(got.reply.Operations) != 1 || string(got.reply.Operations[0].Data) != string([]byte{42}) {
					t.Fatal("raw reply changed", got.reply, got.err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			request.Operations = []OSDOperation{{Code: OSDWriteFull, Length: 2, Data: []byte{0, 255}}}
			go func() { r, err := o.Request(ctx, request); done <- result{r, err} }()
			mutation := peer.next(t)
			peer.conn.Close()
			select {
			case got := <-done:
				var unknown *OutcomeUnknownError
				if !errors.As(got.err, &unknown) || got.reply.Submission.Transaction != mutation.Transaction || mutation.Transaction == sent.Transaction || osdDials.Load() != 1 {
					t.Fatal("uncertain cache-bypass mutation lost identity or retried", got.reply.Submission, got.err, osdDials.Load())
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			// A sent cache flush whose connection is lost stays unknown with
			// its identity and is neither redialed nor resent.
			// A distinct OSD ID opens a fresh session; the lost one is not reused.
			o, err = c.OpenOSD(ctx, OSDTarget{ID: 1, Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer = <-peers
			flush := request
			flush.IgnoreOverlay, flush.Operations = true, []OSDOperation{{Code: OSDCacheFlush}}
			if profile.replicatedPG() {
				flush.Placement = &OSDPlacement{PG: OSDPG{Pool: 1}, Primary: 1}
			}
			go func() { r, err := o.Request(ctx, flush); done <- result{r, err} }()
			cached := peer.next(t)
			if flags := uint64(binary.LittleEndian.Uint32(cached.Front[offset:])); flags&(osd.FlagIgnoreCache|osd.FlagIgnoreOverlay|osd.FlagRead) != osd.FlagIgnoreCache|osd.FlagIgnoreOverlay|osd.FlagRead || flags&(osd.FlagWrite|osd.FlagSkipRWLocks) != 0 {
				t.Fatalf("cache flush request flags %#x", flags)
			}
			peer.conn.Close()
			select {
			case got := <-done:
				var unknown *OutcomeUnknownError
				if !errors.As(got.err, &unknown) || got.reply.Submission.Transaction != cached.Transaction || cached.Transaction == mutation.Transaction || osdDials.Load() != 2 {
					t.Fatal("uncertain cache flush lost identity or retried", got.reply.Submission, got.err, osdDials.Load())
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case extra := <-peer.requests:
				t.Fatal("cache flush was resent", extra.Transaction)
			default:
			}
		})
	}
}

func TestOSDCacheOperationPreflight(t *testing.T) {
	native := map[OSDOpcode]OSDRequest{
		OSDCacheFlush:    {IgnoreCache: true, IgnoreOverlay: true},
		OSDCacheTryFlush: {IgnoreCache: true, IgnoreOverlay: true, SkipRWLocks: true},
		OSDCacheEvict:    {IgnoreCache: true, IgnoreOverlay: true, SkipRWLocks: true},
		OSDCachePin:      {},
		OSDCacheUnpin:    {IgnoreCache: true},
	}
	for code, r := range native {
		o := backoffUnitConnection()
		o.client.options.EnableOSDCachePools = true
		o.features |= osd.FeatureCachePool
		r.Operations = []OSDOperation{{Code: code}}
		if err := o.validateIgnoreCache(r); err != nil {
			t.Fatal("IgnoreCache preflight refused a cache operation", code, err)
		}
		if err := o.validateCacheOperation(r); err != nil {
			t.Fatal("native cache flag set refused", code, err)
		}
		for name, edit := range map[string]func(*OSDConnection, *OSDRequest){
			"default-deny":    func(o *OSDConnection, r *OSDRequest) { o.client.options.EnableOSDCachePools = false },
			"missing-feature": func(o *OSDConnection, r *OSDRequest) { o.features &^= osd.FeatureCachePool },
			"resubmit":        func(o *OSDConnection, r *OSDRequest) { r.Resubmit = &OSDSubmission{Transaction: 1} },
			"erasure":         func(o *OSDConnection, r *OSDRequest) { r.Placement = &OSDPlacement{Erasure: true} },
			"compound": func(o *OSDConnection, r *OSDRequest) {
				r.Operations = append(r.Operations, OSDOperation{Code: OSDStat})
			},
			"wrong-locks": func(o *OSDConnection, r *OSDRequest) { r.SkipRWLocks = !r.SkipRWLocks },
		} {
			o, r := backoffUnitConnection(), r
			o.client.options.EnableOSDCachePools = true
			o.features |= osd.FeatureCachePool
			edit(o, &r)
			if err := o.validateCacheOperation(r); err == nil {
				t.Fatal("invalid cache request passed preflight", code, name)
			}
		}
	}
	o := backoffUnitConnection()
	if err := o.validateCacheOperation(OSDRequest{Operations: []OSDOperation{{Code: OSDRead}}}); err != nil {
		t.Fatal("ordinary request policy changed", err)
	}
	if err := o.validateCacheOperation(OSDRequest{Operations: []OSDOperation{{Code: OSDRead}}, IgnoreOverlay: true}); err == nil {
		t.Fatal("IgnoreOverlay admitted outside a cache operation")
	}
}

func TestOSDCacheOperationRefusesBeforeSlotMonitorAndIdentity(t *testing.T) {
	edits := map[string]func(*OSDConnection, *OSDRequest){
		"default-deny":    func(o *OSDConnection, r *OSDRequest) { o.client.options.EnableOSDCachePools = false },
		"missing-feature": func(o *OSDConnection, r *OSDRequest) { o.features &^= osd.FeatureCachePool },
		"no-overlay":      func(o *OSDConnection, r *OSDRequest) { r.IgnoreOverlay = false },
		"skip-locks":      func(o *OSDConnection, r *OSDRequest) { r.SkipRWLocks = true },
		"compound": func(o *OSDConnection, r *OSDRequest) {
			r.Operations = append(r.Operations, OSDOperation{Code: OSDStat})
		},
		"resubmit":        func(o *OSDConnection, r *OSDRequest) { r.Resubmit = &OSDSubmission{Transaction: 1} },
		"retry":           func(o *OSDConnection, r *OSDRequest) { r.RetryAttempt = 1 },
		"snapshot":        func(o *OSDConnection, r *OSDRequest) { r.ReadSnapshot = 1 },
		"opflags":         func(o *OSDConnection, r *OSDRequest) { r.Operations[0].Flags = OSDFailOK },
		"extent":          func(o *OSDConnection, r *OSDRequest) { r.Operations[0].Length = 1 },
		"input":           func(o *OSDConnection, r *OSDRequest) { r.Operations[0].Data = []byte{1} },
		"xattr":           func(o *OSDConnection, r *OSDRequest) { r.Operations[0].Xattr.Name = "k" },
		"overlay-on-read": func(o *OSDConnection, r *OSDRequest) { r.Operations[0] = OSDOperation{Code: OSDRead, Length: 1} },
	}
	for _, profile := range []OSDProfile{OSDProfileObjectHash, OSDProfileReplicatedPG, OSDProfileReplicatedPGReplyV8, OSDProfileReplicatedPGRequestV9ReplyV8} {
		for name, edit := range edits {
			t.Run(profileNameForCacheTest(profile)+"/"+name, func(t *testing.T) {
				o := backoffUnitConnection()
				o.client.options.OSDProfile = profile
				o.client.options.EnableOSDCachePools = true
				o.features |= osd.FeatureCachePool
				// A foreign owner occupies the sole slot; MON and writer are unstarted.
				o.client.calls = make(chan struct{}, 1)
				o.client.calls <- struct{}{}
				r := OSDRequest{IgnoreCache: true, IgnoreOverlay: true, MapEpoch: 17, Object: OSDObject{Pool: 7, Name: "object"}, Operations: []OSDOperation{{Code: OSDCacheFlush}}}
				if profile.replicatedPG() {
					r.Placement = &OSDPlacement{PG: OSDPG{Pool: 7, Seed: 3}, Primary: o.target.ID}
				}
				edit(o, &r)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				result, err := o.Request(ctx, r)
				if err == nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || result.Submission.Transaction != 0 || o.client.osdNext.Load() != 0 || len(o.client.calls) != 1 {
					t.Fatal("invalid cache operation passed local preflight", err, result.Submission.Transaction, o.client.osdNext.Load(), len(o.client.calls))
				}
			})
		}
	}
}
