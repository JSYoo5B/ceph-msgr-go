package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

type fanoutAdmissionContext struct {
	context.Context
	check func() error
}

func (c fanoutAdmissionContext) Err() error { return c.check() }

func TestMalformedReplyMarksAllAffectedCalls(t *testing.T) {
	for _, mgr := range []bool{false, true} {
		name := "MON"
		if mgr {
			name = "MGR"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			fsid := [16]byte{1}
			options := mockOptions(t, 20, fsid)
			blockedSent, malformedReady, queuedReady := make(chan struct{}), make(chan struct{}), make(chan struct{})
			allowReply, allowAdmission := make(chan struct{}), make(chan struct{})
			var replyOnce, admissionOnce sync.Once
			releaseReply := func() { replyOnce.Do(func() { close(allowReply) }) }
			releaseAdmission := func() { admissionOnce.Do(func() { close(allowAdmission) }) }
			// Release barriers before Close waits for the writer and mock peer.
			defer releaseReply()
			defer releaseAdmission()
			var commands atomic.Uint32
			options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
				client, peer := net.Pipe()
				role, id := uint8(1), uint64(0)
				if strings.HasSuffix(endpoint, ":6800") {
					role, id = 16, 99
				}
				go mockDaemon(peer, peerConfig{fsid: fsid, release: 20, role: role, id: id,
					command: func(m msgr.MessageData) {
						commands.Add(1)
						if strings.Contains(string(m.Front), "block") {
							close(blockedSent)
						}
					},
					reply: func(m *msgr.MessageData) {
						m.Front = m.Front[:len(m.Front)-1]
						close(malformedReady)
						<-allowReply
					},
				})
				return client, ctx.Err()
			}
			c, err := Dial(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { releaseReply(); releaseAdmission(); c.Close() }()
			call := c.MonCommand
			if mgr {
				call = c.MgrCommand
			}
			started := make(chan error, 1)
			go func() {
				_, err := call(ctx, Command{JSON: []byte(`{"prefix":"block"}`)})
				started <- err
			}()
			await := func(ch <-chan struct{}) {
				t.Helper()
				select {
				case <-ch:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			await(blockedSent)
			input := []byte{0, 255, 1, 0}
			type commandResult struct {
				result Result
				err    error
			}
			malformed := make(chan commandResult, 1)
			go func() {
				result, err := call(ctx, Command{JSON: []byte(`{"prefix":"mutation"}`), Input: input})
				malformed <- commandResult{result, err}
			}()
			await(malformedReady)
			c.mu.Lock()
			s := c.mon
			if mgr {
				s = c.mgr
			}
			c.mu.Unlock()
			// Session.Call checks its context at entry and again before the
			// writer starts the frame. Hold the second check so this registered
			// request has a known, unstarted outcome when the reply fails.
			var checks atomic.Uint32
			queuedContext := fanoutAdmissionContext{Context: ctx, check: func() error {
				if checks.Add(1) == 2 {
					close(queuedReady)
					<-allowAdmission
				}
				return ctx.Err()
			}}
			queued := make(chan error, 1)
			go func() {
				typ := msgr.MonCommandMessage
				if mgr {
					typ = msgr.MgrCommandMessage
				}
				_, err := s.Call(queuedContext, msgr.MessageData{Type: typ})
				queued <- err
			}()
			await(queuedReady)
			releaseReply()
			assertCause := func(err error, uncertain bool) {
				t.Helper()
				var unknown *OutcomeUnknownError
				if !errors.Is(err, ErrMalformedMessage) || !errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &unknown) != uncertain || testcluster.IsRecoveryError(err) {
					t.Fatalf("malformed reply lost cause or transmission outcome (uncertain=%v): %v", uncertain, err)
				}
			}
			trigger := <-malformed
			assertCause(trigger.err, true)
			if !bytes.Equal(trigger.result.Data, input) {
				t.Fatal("malformed reply lost raw output", trigger.result.Data)
			}
			assertCause(<-started, true)
			assertCause(<-queued, false)
			assertCause(s.Err(), false)
			releaseAdmission()
			s.Wait()
			if commands.Load() != 2 {
				t.Fatal("unstarted request was sent or uncertain command replayed", commands.Load())
			}
		})
	}
}
