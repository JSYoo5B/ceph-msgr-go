package cephmsgr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

type mdsCommandResponse struct {
	result MDSCommandResult
	err    error
}

func mdsCommandAsync(m *MDSConnection, ctx context.Context, command Command) <-chan mdsCommandResponse {
	done := make(chan mdsCommandResponse, 1)
	go func() {
		result, err := m.Command(ctx, command)
		done <- mdsCommandResponse{result, err}
	}()
	return done
}

func mdsCommandReceive(t *testing.T, ctx context.Context, done <-chan mdsCommandResponse) mdsCommandResponse {
	t.Helper()
	select {
	case response := <-done:
		return response
	case <-ctx.Done():
		t.Fatal("MDS command did not complete", ctx.Err())
	}
	return mdsCommandResponse{}
}

func mdsCommandWire(t *testing.T, m msgr.MessageData) string {
	t.Helper()
	if m.Type != 97 || m.Version != 1 || m.CompatVersion != 1 || m.Priority != 127 || m.Transaction == 0 || len(m.Middle) != 0 {
		t.Fatal("MDS command is not canonical MCommand 97/v1/compat1", m)
	}
	d := wire.NewDecoder(m.Front)
	if !bytes.Equal(d.Raw(16), []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}) || d.U32() != 1 {
		t.Fatal("MDS command changed authenticated FSID or command count")
	}
	command := d.String()
	if err := d.Done(); err != nil {
		t.Fatal("MDS command appended routing metadata to its raw front", err)
	}
	return command
}

func TestMDSCommandPublicRawWireAndSignedResults(t *testing.T) {
	for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
		t.Run(fmt.Sprintf("mode%d", mode), func(t *testing.T) {
			_, m, peer, ctx, dials := mdsTestOpenMode(t, mode)
			for _, code := range []int32{0, 7, -13, -22, -12345} {
				command := []byte(" {\"prefix\": \"raw command\", \"opaque\": \"α\\u0000\", \"value\": 42} ")
				input := []byte{0, 255, 1, 0, 'x'}
				done := mdsCommandAsync(m, ctx, Command{JSON: command, Input: input})
				request := peer.next(t)
				if got := mdsCommandWire(t, request); got != string(command) || !bytes.Equal(request.Data, input) {
					t.Fatal("MDS command altered raw JSON or binary input", got, request.Data)
				}
				text, data := "raw\x00status α", []byte{255, 0, 4, 0, 'r'}
				peer.reply(t, request, code, text, data)
				got := mdsCommandReceive(t, ctx, done)
				if got.result.Code != code || got.result.Message != text || !bytes.Equal(got.result.Data, data) || got.result.Submission.Transaction != request.Transaction || got.result.Submission.GlobalID != 42 || got.result.Submission.ConnectionGeneration == 0 || got.result.Submission.Target != (MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/17"}) {
					t.Fatal("MDS raw result or local submission identity lost", got.result, got.err)
				}
				var commandErr *CommandError
				var unknown *OutcomeUnknownError
				if code < 0 {
					if !errors.As(got.err, &commandErr) || commandErr.Code != code || commandErr.Message != text || errors.As(got.err, &unknown) {
						t.Fatal("signed MDS server refusal lost its known outcome", got.err)
					}
				} else if got.err != nil {
					t.Fatal("nonnegative MDS status was converted to an error", got.err)
				}
			}
			if dials.Load() != 1 {
				t.Fatal("known MDS results caused a reconnect", dials.Load())
			}
		})
	}
}

func TestMDSCommandCopiesQueuedJSONAndBinaryInput(t *testing.T) {
	c, m, peer, ctx, _ := mdsTestOpen(t)
	pause := peer.pauseNextClientWrite()
	var release sync.Once
	defer release.Do(func() { close(pause.release) })
	first := mdsCommandAsync(m, ctx, Command{JSON: []byte(`{"prefix":"first"}`)})
	select {
	case <-pause.started:
	case <-ctx.Done():
		t.Fatal("first MDS command did not block the single writer", ctx.Err())
	}
	before := c.mdsNext.Load()
	jsonInput, binaryInput := []byte(`{"prefix":"queued","n":7}`), []byte{0, 255, 0, 7}
	wantJSON, wantBinary := bytes.Clone(jsonInput), bytes.Clone(binaryInput)
	second := mdsCommandAsync(m, ctx, Command{JSON: jsonInput, Input: binaryInput})
	// Transaction allocation occurs after the public command has snapshotted
	// its inputs and before its frame can pass the deliberately blocked writer.
	for c.mdsNext.Load() == before {
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal("second MDS command was not admitted", ctx.Err())
		}
	}
	for i := range jsonInput {
		jsonInput[i] = 'x'
	}
	for i := range binaryInput {
		binaryInput[i] ^= 255
	}
	release.Do(func() { close(pause.release) })
	a, b := peer.next(t), peer.next(t)
	if mdsCommandWire(t, a) != `{"prefix":"first"}` || mdsCommandWire(t, b) != string(wantJSON) || !bytes.Equal(b.Data, wantBinary) || a.Transaction == b.Transaction {
		t.Fatal("queued MDS command retained caller-owned mutable input", a, b)
	}
	peer.reply(t, b, 0, "second", []byte{2})
	peer.reply(t, a, 0, "first", []byte{1})
	if got := mdsCommandReceive(t, ctx, first); got.err != nil || got.result.Message != "first" || got.result.Submission.Transaction != a.Transaction {
		t.Fatal("first command correlation lost", got)
	}
	if got := mdsCommandReceive(t, ctx, second); got.err != nil || got.result.Message != "second" || got.result.Submission.Transaction != b.Transaction {
		t.Fatal("second command correlation lost", got)
	}
}

func TestMDSCommandConcurrentRepliesCorrelateByTransaction(t *testing.T) {
	_, m, peer, ctx, _ := mdsTestOpen(t)
	const count = 16
	results := make([]<-chan mdsCommandResponse, count)
	for i := range results {
		results[i] = mdsCommandAsync(m, ctx, Command{JSON: []byte(fmt.Sprintf(`{"prefix":"raw","index":%d}`, i))})
	}
	requests := make([]msgr.MessageData, count)
	indices := make([]int, count)
	seen := make(map[uint64]bool)
	for i := range requests {
		requests[i] = peer.next(t)
		var command struct{ Index int }
		if err := json.Unmarshal([]byte(mdsCommandWire(t, requests[i])), &command); err != nil || command.Index < 0 || command.Index >= count || seen[requests[i].Transaction] {
			t.Fatal("concurrent command identity invalid", command, requests[i].Transaction, err)
		}
		seen[requests[i].Transaction], indices[i] = true, command.Index
	}
	for i := count - 1; i >= 0; i-- {
		peer.reply(t, requests[i], 0, fmt.Sprint(indices[i]), []byte{byte(indices[i])})
	}
	for i, done := range results {
		got := mdsCommandReceive(t, ctx, done)
		if got.err != nil || got.result.Message != fmt.Sprint(i) || !bytes.Equal(got.result.Data, []byte{byte(i)}) || !seen[got.result.Submission.Transaction] {
			t.Fatal("out-of-order MDS reply completed another command", i, got)
		}
	}
}

func TestMDSCommandMalformedRepliesPreserveUnknownOutcomeWithoutReplay(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*msgr.MessageData)
		raw    bool
	}{
		{"wrong type", func(m *msgr.MessageData) { m.Type = msgr.MonCommandReplyMessage }, false},
		{"zero version", func(m *msgr.MessageData) { m.Version = 0 }, false},
		{"new version", func(m *msgr.MessageData) { m.Version = 2 }, true},
		{"zero compatibility", func(m *msgr.MessageData) { m.CompatVersion = 0 }, true},
		{"new compatibility", func(m *msgr.MessageData) { m.CompatVersion = 2 }, false},
		{"middle", func(m *msgr.MessageData) { m.Middle = []byte{7} }, true},
		{"truncated text", func(m *msgr.MessageData) { m.Front = m.Front[:7] }, true},
		{"trailing front", func(m *msgr.MessageData) { m.Front = append(m.Front, 7) }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, m, peer, ctx, dials := mdsTestOpen(t)
			done := mdsCommandAsync(m, ctx, Command{JSON: []byte(`{"prefix":"mutation"}`)})
			request := peer.next(t)
			e := wire.Encoder{}
			e.U32(0)
			e.String("invalid reply")
			reply := msgr.MessageData{Type: 98, Version: 1, CompatVersion: 1, Transaction: request.Transaction, Front: e.Data, Data: []byte{0, 255, 8}}
			tc.change(&reply)
			peer.send(t, reply)
			got := mdsCommandReceive(t, ctx, done)
			var unknown *OutcomeUnknownError
			var server *CommandError
			if !errors.As(got.err, &unknown) || !errors.Is(got.err, msgr.ErrFrame) || errors.As(got.err, &server) || got.result.Submission.Transaction != request.Transaction || got.result.Submission.GlobalID != 42 || got.result.Submission.ConnectionGeneration == 0 || tc.raw && !bytes.Equal(got.result.Data, reply.Data) {
				t.Fatal("invalid MDS response became a known result or lost raw output/identity", got)
			}
			select {
			case <-m.Done():
			case <-ctx.Done():
				t.Fatal("invalid MDS reply did not terminate the transport", ctx.Err())
			}
			if again, err := m.Command(ctx, Command{JSON: []byte(`{"prefix":"after"}`)}); err == nil || again.Submission != (MDSCommandSubmission{}) {
				t.Fatal("failed MDS handle accepted another command", again, err)
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			if dials.Load() != 1 {
				t.Fatal("malformed MDS reply caused automatic reconnection", dials.Load())
			}
			select {
			case replay := <-peer.requests:
				t.Fatal("MDS command was replayed", replay.Transaction)
			default:
			}
		})
	}
}

func TestMDSCommandSentDisconnectIsUnknownAndTerminal(t *testing.T) {
	_, m, peer, ctx, dials := mdsTestOpen(t)
	done := mdsCommandAsync(m, ctx, Command{JSON: []byte(`{"prefix":"mutation"}`)})
	request := peer.next(t)
	peer.conn.Close()
	got := mdsCommandReceive(t, ctx, done)
	var unknown *OutcomeUnknownError
	if !errors.As(got.err, &unknown) || got.result.Submission.Transaction != request.Transaction || got.result.Submission.GlobalID != 42 || got.result.Code != 0 || got.result.Data != nil {
		t.Fatal("sent MDS disconnect lost its uncertain outcome", got)
	}
	if again, err := m.Command(ctx, Command{JSON: []byte(`{"prefix":"after"}`)}); err == nil || again.Submission != (MDSCommandSubmission{}) {
		t.Fatal("MDS command silently reconnected a failed handle", again, err)
	}
	if dials.Load() != 1 {
		t.Fatal("uncertain MDS command was retried", dials.Load())
	}
}
