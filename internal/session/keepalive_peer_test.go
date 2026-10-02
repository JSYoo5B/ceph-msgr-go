package session

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestPeerKeepaliveEchoPreservesSecureCommandSequences(t *testing.T) {
	s, reader, writer := testSession(t)
	s.config.KeepaliveInterval = time.Hour
	s.config.KeepaliveTimeout = 2 * time.Hour
	key := make([]byte, 16)
	clientNonce, peerNonce := make([]byte, 12), make([]byte, 12)
	clientNonce[0], peerNonce[0] = 1, 2
	for _, setup := range []func() error{
		func() error { return s.transport.Writer.EnableSecure(key, clientNonce) },
		func() error { return reader.EnableSecure(key, clientNonce) },
		func() error { return writer.EnableSecure(key, peerNonce) },
		func() error { return s.transport.Reader.EnableSecure(key, peerNonce) },
	} {
		if err := setup(); err != nil {
			t.Fatal(err)
		}
	}
	s.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	exchange := func(sequence uint64) {
		t.Helper()
		finished := make(chan error, 1)
		go func() {
			response, err := s.Call(ctx, msgr.MessageData{Type: msgr.MonCommandMessage})
			if err == nil && !bytes.Equal(response.Front, []byte{0, 255, 7}) {
				err = msgr.ErrFrame
			}
			finished <- err
		}()
		request := readRequest(t, reader)
		if request.Sequence != sequence || request.AckSequence != sequence-1 {
			t.Fatal("keepalive changed command sequence or acknowledged a message", request.Sequence, request.AckSequence)
		}
		response := msgr.MessageData{Type: msgr.MonCommandReplyMessage, Transaction: request.Transaction, Sequence: sequence, AckSequence: sequence, Front: []byte{0, 255, 7}}
		if err := writer.Write(response.Frame()); err != nil {
			t.Fatal(err)
		}
		ack, err := reader.Read()
		if err != nil || ack.Tag != msgr.Ack || len(ack.Segments) != 1 || len(ack.Segments[0]) != 8 || binary.LittleEndian.Uint64(ack.Segments[0]) != sequence {
			t.Fatal("command response lost its transport acknowledgement", ack.Tag, err)
		}
		select {
		case err := <-finished:
			if err != nil {
				t.Fatal("secure command stream failed", err)
			}
		case <-ctx.Done():
			t.Fatal("command did not finish", ctx.Err())
		}
	}
	exchange(1)

	// Tentacle ProtocolV2.cc:1701-1715 echoes the peer's utime_t bytes while
	// recording local receive time independently. Past/future peer clocks
	// must not become a local deadline or alter Messenger message counters.
	for _, seconds := range []uint32{0, ^uint32(0)} {
		timestamp := make([]byte, 8)
		binary.LittleEndian.PutUint32(timestamp, seconds)
		binary.LittleEndian.PutUint32(timestamp[4:], 987654321)
		previous := time.Now().Add(-time.Minute)
		s.lastReceive.Store(&previous)
		beforeWrite := time.Now()
		if err := writer.Write(msgr.Frame{Tag: msgr.Keepalive, Segments: [][]byte{timestamp}}); err != nil {
			t.Fatal(err)
		}
		ack, err := reader.Read()
		if err != nil || ack.Tag != msgr.KeepaliveAck || len(ack.Segments) != 1 || !bytes.Equal(ack.Segments[0], timestamp) {
			t.Fatal("peer timestamp was not echoed exactly", ack.Tag, ack.Segments, err)
		}
		afterRead, receivedAt := time.Now(), *s.lastReceive.Load()
		if s.sent.Load() != 1 || s.received.Load() != 1 || receivedAt.Before(beforeWrite) || receivedAt.After(afterRead) || s.Err() != nil {
			t.Fatal("peer keepalive changed message counters or failed to refresh local activity", s.sent.Load(), s.received.Load(), s.Err())
		}
	}
	exchange(2)
}
