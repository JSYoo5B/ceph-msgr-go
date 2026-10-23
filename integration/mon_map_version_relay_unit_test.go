package integration_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

type fragmentedRelayWriter struct{ io.Writer }

func (w fragmentedRelayWriter) Write(p []byte) (int, error) {
	if len(p) > 3 {
		p = p[:3]
	}
	return w.Writer.Write(p)
}

func versionRelayReply(tid, handle uint64) msgr.MessageData {
	e := wire.Encoder{}
	e.U64(handle)
	e.U64(71)
	e.U64(65)
	return msgr.MessageData{Type: msgr.MonGetVersionReplyMessage, Transaction: tid, Sequence: 9, Version: 2, Front: e.Data}
}

func TestMonMapVersionRelayFragmentationCorrelationAndEOF(t *testing.T) {
	local, peer := net.Pipe()
	t.Cleanup(func() { local.Close(); peer.Close() })
	deadline := time.Now().Add(5 * time.Second)
	local.SetDeadline(deadline)
	peer.SetDeadline(deadline)
	relay := newMapVersionRelay()
	conn := relay.wrap(local, "synthetic-mon:3300")
	relay.arm()
	request := msgr.MonGetVersion(41, "osdmap")
	request.Transaction, request.Sequence = 37, 5
	banner := msgr.Banner{Supported: msgr.Revision1, Required: msgr.Revision1}
	keepalive := msgr.Frame{Tag: msgr.Keepalive, Segments: [][]byte{{1, 2, 3, 4}}}
	wrongTID, wrongHandle := versionRelayReply(38, 41), versionRelayReply(37, 42)
	server := make(chan error, 1)
	go func() {
		defer peer.Close()
		var received bytes.Buffer
		input := io.TeeReader(peer, &received)
		if _, err := msgr.ReadBanner(input, banner); err != nil {
			server <- err
			return
		}
		writer := fragmentedRelayWriter{peer}
		if err := msgr.WriteFull(writer, banner.Encode()); err != nil {
			server <- err
			return
		}
		frame, err := msgr.NewReader(input, mapVersionRelayLimit).Read()
		if err != nil {
			server <- err
			return
		}
		message, err := msgr.DecodeMessage(frame)
		if err != nil || message.Transaction != 37 || message.Sequence != 5 || !bytes.Equal(message.Front, request.Front) {
			server <- errors.New("fragmented outgoing request changed")
			return
		}
		var expected bytes.Buffer
		expected.Write(banner.Encode())
		if err := msgr.NewWriter(&expected, mapVersionRelayLimit).Write(request.Frame()); err != nil || !bytes.Equal(received.Bytes(), expected.Bytes()) {
			server <- errors.New("outgoing transcript bytes changed")
			return
		}
		frames := []msgr.Frame{wrongTID.Frame(), wrongHandle.Frame(), versionRelayReply(37, 41).Frame(), keepalive}
		for _, frame := range frames {
			if err := msgr.NewWriter(writer, mapVersionRelayLimit).Write(frame); err != nil {
				server <- err
				return
			}
		}
		server <- nil
	}()
	clientWriter := fragmentedRelayWriter{conn}
	if err := msgr.WriteFull(clientWriter, banner.Encode()); err != nil {
		t.Fatal(err)
	}
	if _, err := msgr.ReadBanner(conn, banner); err != nil {
		t.Fatal(err)
	}
	if err := msgr.NewWriter(clientWriter, mapVersionRelayLimit).Write(request.Frame()); err != nil {
		t.Fatal(err)
	}
	reader := msgr.NewReader(conn, mapVersionRelayLimit)
	for _, expected := range []msgr.Frame{wrongTID.Frame(), wrongHandle.Frame(), keepalive} {
		frame, err := reader.Read()
		if err != nil {
			t.Fatal(err)
		}
		var got, want bytes.Buffer
		msgr.NewWriter(&got, mapVersionRelayLimit).Write(frame)
		msgr.NewWriter(&want, mapVersionRelayLimit).Write(expected)
		if !bytes.Equal(got.Bytes(), want.Bytes()) {
			t.Fatal("unrelated receive frame changed or was withheld")
		}
	}
	if _, err := reader.Read(); !errors.Is(err, io.EOF) {
		t.Fatal("withheld reply masked real peer EOF", err)
	}
	if err := <-server; err != nil {
		t.Fatal(err)
	}
	select {
	case held := <-relay.held:
		if held.Request.TID != 37 || held.Request.Handle != 41 || held.Request.Sequence != 5 || !held.Held || held.Newest != 71 || held.Oldest != 65 {
			t.Fatal("reply barrier identity differs", held)
		}
	default:
		t.Fatal("matching reply never reached the deterministic barrier")
	}
	end := <-relay.ended
	if end.Connection != 1 || !errors.Is(end.Err, io.EOF) {
		t.Fatal("peer termination receipt lost EOF", end)
	}
	requests, replies := relay.snapshot()
	if len(requests) != 1 || len(replies) != 1 || !replies[0].Held {
		t.Fatal("unrelated reply was correlated", requests, replies)
	}
}

func TestMonMapVersionRelayFreshConnectionPassesNewIdentity(t *testing.T) {
	relay := newMapVersionRelay()
	for _, tc := range []struct {
		tid, handle uint64
		hold        bool
	}{{9, 11, true}, {12, 12, false}} {
		local, peer := net.Pipe()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		deadline, _ := ctx.Deadline()
		local.SetDeadline(deadline)
		peer.SetDeadline(deadline)
		conn := relay.wrap(local, "synthetic-mon:3300")
		if tc.hold {
			relay.arm()
		}
		request := msgr.MonGetVersion(tc.handle, "osdmap")
		request.Transaction, request.Sequence = tc.tid, 1
		banner := msgr.Banner{Supported: msgr.Revision1, Required: msgr.Revision1}
		server := make(chan error, 1)
		go func() {
			defer peer.Close()
			if _, err := msgr.ReadBanner(peer, banner); err != nil {
				server <- err
				return
			}
			if err := msgr.WriteFull(peer, banner.Encode()); err != nil {
				server <- err
				return
			}
			if _, err := msgr.NewReader(peer, mapVersionRelayLimit).Read(); err != nil {
				server <- err
				return
			}
			server <- msgr.NewWriter(peer, mapVersionRelayLimit).Write(versionRelayReply(tc.tid, tc.handle).Frame())
		}()
		if err := msgr.WriteFull(conn, banner.Encode()); err != nil {
			t.Fatal(err)
		}
		if _, err := msgr.ReadBanner(conn, banner); err != nil {
			t.Fatal(err)
		}
		if err := msgr.NewWriter(conn, mapVersionRelayLimit).Write(request.Frame()); err != nil {
			t.Fatal(err)
		}
		frame, err := msgr.NewReader(conn, mapVersionRelayLimit).Read()
		if tc.hold {
			if !errors.Is(err, io.EOF) {
				t.Fatal("original withheld reply escaped", err)
			}
		} else {
			message, decodeErr := msgr.DecodeMessage(frame)
			if err != nil || decodeErr != nil || message.Transaction != tc.tid {
				t.Fatal("fresh reply did not pass through", err, decodeErr, message)
			}
		}
		if err := <-server; err != nil {
			t.Fatal(err)
		}
		conn.Close()
		cancel()
	}
	requests, replies := relay.snapshot()
	if len(requests) != 2 || len(replies) != 2 || requests[0].Connection == requests[1].Connection || !replies[0].Held || replies[1].Held {
		t.Fatal("recovery reused the old relay identity or held the fresh reply", requests, replies)
	}
}
