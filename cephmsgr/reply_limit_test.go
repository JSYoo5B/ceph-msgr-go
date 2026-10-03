package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func publicLimitCorruptReply(reply *msgr.MessageData, mode string, limit uint32) {
	switch mode {
	case "frame":
		// The secure peer can write this valid frame, but its logical header,
		// front and data together exceed the client's configured frame bound.
		reply.Data = make([]byte, limit)
	case "bytes":
		offset := 4 // MMgrCommandReply and MCommandReply status string length.
		if reply.Type == msgr.MonCommandReplyMessage {
			offset += 18 // MMonCommandAck's PaxosServiceMessage prefix.
		}
		binary.LittleEndian.PutUint32(reply.Front[offset:], limit+1)
	case "count":
		// Independent MMonCommandAck front: Paxos18, result32, empty status,
		// then 1025 complete empty strings. It fits an 8192-byte frame and
		// contains enough bytes for every entry, but exceeds the 1024 cap.
		front := make([]byte, 18)
		front = binary.LittleEndian.AppendUint32(front, 0)
		front = binary.LittleEndian.AppendUint32(front, 0)
		front = binary.LittleEndian.AppendUint32(front, 1025)
		reply.Front = append(front, make([]byte, 1025*4)...)
	}
}

func publicLimitUnknown(t *testing.T, result Result, err error, mode string, input []byte) {
	t.Helper()
	var unknown *OutcomeUnknownError
	var server *CommandError
	if !errors.Is(err, ErrLimitExceeded) || !errors.Is(err, wire.ErrLimit) || !errors.As(err, &unknown) || !errors.Is(unknown.Cause, wire.ErrLimit) || errors.As(err, &server) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("received limit failure lost its public/raw cause or execution uncertainty", err)
	}
	if errors.Is(err, ErrMalformedMessage) != (mode != "frame") {
		t.Fatal("configured frame refusal and malformed decoder data were conflated", mode, err)
	}
	if mode == "frame" {
		if len(result.Data) != 0 {
			t.Fatal("an incomplete oversized frame published output", len(result.Data))
		}
	} else if !bytes.Equal(result.Data, input) {
		t.Fatal("a complete undecodable reply lost its raw output", len(result.Data))
	}
	if result.Code != 0 || result.Message != "" {
		t.Fatal("an undecodable reply claimed a known server result")
	}
}

func TestPublicReplyLimitsPreserveCauseRawOutputAndUnknownOutcome(t *testing.T) {
	const limit = 8192
	for index, routeName := range []string{"MonCommand", "MgrCommand", "MonTell", "MgrTell"} {
		for _, mode := range []string{"frame", "bytes", "count"} {
			if mode == "count" && index != 0 {
				continue // Only MMonCommandAck has an echoed command collection.
			}
			t.Run(routeName+"/"+mode, func(t *testing.T) {
				var corrupt atomic.Bool
				corrupt.Store(true)
				f := newTellFixture(t, [16]byte{1}, func(reply *msgr.MessageData) {
					if corrupt.Swap(false) {
						publicLimitCorruptReply(reply, mode, limit)
					}
				})
				f.options.MaxFrameSize = limit
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				c, err := Dial(ctx, f.options)
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				mgr := index == 1 || index == 3
				if mgr {
					if err := c.WaitMgrReady(ctx); err != nil {
						t.Fatal(err)
					}
				}
				c.mu.Lock()
				source := c.mon
				if mgr {
					source = c.mgr
				}
				c.mu.Unlock()
				call := publicLimitCalls(c)[index].call
				input := []byte{0, 255, 1, 0}
				result, err := call(ctx, Command{JSON: []byte(`{"prefix":"mutation"}`), Input: input})
				publicLimitUnknown(t, result, err, mode, input)
				if !errors.Is(source.Err(), ErrLimitExceeded) || !errors.Is(source.Err(), wire.ErrLimit) || errors.Is(source.Err(), ErrMalformedMessage) != (mode != "frame") {
					t.Fatal("failed secure source did not retain the reply's original classes", source.Err())
				}
				request := nextTellRequest(t, ctx, f)
				wantRole, wantType := uint8(1), msgr.MonCommandMessage
				if mgr {
					wantRole, wantType = 16, msgr.MgrCommandMessage
				}
				if index >= 2 {
					wantType = msgr.TellCommandMessage
				}
				if request.role != wantRole || request.data.Type != wantType || !bytes.Equal(request.data.Data, input) || f.calls.Load() != 1 {
					t.Fatal("limit failure occurred before the exact command reached its intended daemon")
				}
				result, err = call(ctx, Command{JSON: []byte(`{"prefix":"status"}`), Input: input})
				if err != nil || !bytes.Equal(result.Data, input) {
					t.Fatal("an explicit new command did not recover after the refused reply", err)
				}
				recovered := nextTellRequest(t, ctx, f)
				if f.calls.Load() != 2 || !bytes.Contains(recovered.data.Front, []byte(`"status"`)) || recovered.role != wantRole || recovered.data.Type != wantType {
					t.Fatal("recovery replayed the uncertain mutation or changed command routing", f.calls.Load())
				}
			})
		}
	}
}

func TestPublicNamedReplyLimitsPreservePrimaryAndDoNotReplay(t *testing.T) {
	const limit = 8192
	for _, mode := range []string{"frame", "bytes"} {
		t.Run(mode, func(t *testing.T) {
			var corrupt atomic.Bool
			corrupt.Store(true)
			f := namedTestFixtureFor(t, namedTestAddressB(), func(options *Options, _ []namedTestMember) {
				options.MaxFrameSize = limit
			}, func(peer *namedTestPeer) {
				peer.reply = func(reply *msgr.MessageData) {
					if corrupt.Swap(false) {
						publicLimitCorruptReply(reply, mode, limit)
					}
				}
			})
			input := []byte{0, 255, 1, 0}
			result, err := f.c.MonTellTo(f.ctx, "b", Command{JSON: []byte(`{"prefix":"mutation"}`), Input: input})
			publicLimitUnknown(t, result, err, mode, input)
			failed := namedTestTarget(t, f)
			failed.next(t, f.ctx, msgr.TellCommandMessage)
			if failed.tellCount.Load() != 1 || f.namedDials.Load() != 1 || f.monDials.Load() != 1 || f.mgrDials.Load() != 0 {
				t.Fatal("private reply refusal replayed the mutation, failed over, or affected another daemon")
			}
			namedTestPrimaryHealthy(t, f)
			result, err = f.c.MonTellTo(f.ctx, "b", Command{JSON: []byte(`{"prefix":"status"}`), Input: input})
			if err != nil || !bytes.Equal(result.Data, input) {
				t.Fatal("a new directed operation did not return its known raw result", err)
			}
			fresh := namedTestTarget(t, f)
			request := fresh.next(t, f.ctx, msgr.TellCommandMessage)
			if !bytes.Contains(request.Front, []byte(`"status"`)) || fresh.tellCount.Load() != 1 || failed.tellCount.Load() != 1 || f.namedDials.Load() != 2 || f.monDials.Load() != 1 || f.mgrDials.Load() != 0 {
				t.Fatal("explicit recovery changed the target or replayed the prior unknown operation")
			}
		})
	}
}
