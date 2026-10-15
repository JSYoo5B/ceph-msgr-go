package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func osdCommandReply(transaction uint64, code int32, message string, data []byte) msgr.MessageData {
	e := wire.Encoder{}
	e.U32(uint32(code))
	e.String(message)
	return msgr.MessageData{Type: msgr.TellCommandReplyMessage, Version: 1, Transaction: transaction, Front: e.Data, Data: data}
}

// decodeOSDTell independently reads the MCommand front written by the
// client: FSID followed by a string vector (pinned messages/MCommand.h).
func decodeOSDTell(t *testing.T, m msgr.MessageData) ([16]byte, []string) {
	t.Helper()
	d := wire.NewDecoderLimit(m.Front, 1<<20)
	var fsid [16]byte
	copy(fsid[:], d.Raw(16))
	n := d.Count(4, 16)
	commands := make([]string, n)
	for i := range commands {
		commands[i] = d.String()
	}
	if err := d.Done(); err != nil || m.Type != msgr.TellCommandMessage || m.Version != 1 {
		t.Fatal("client sent an invalid MCommand", m.Type, m.Version, err)
	}
	return fsid, commands
}

func TestOSDCommandInvalidInputsNeverAllocateTIDOrSend(t *testing.T) {
	c, peers, dials := osdTestClient(t)
	o, err := c.OpenOSD(context.Background(), OSDTarget{ID: 0, Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	pg := OSDCommandPG{Pool: 2, Seed: 0x1f, Primary: 0, MapEpoch: 9}
	for _, tc := range []struct {
		name string
		pg   *OSDCommandPG
		json string
		in   []byte
		want error
	}{
		{name: "missing prefix", json: `{"format":"json"}`},
		{name: "non-object JSON", json: `["version"]`},
		{name: "null JSON", json: `null`},
		{name: "numeric prefix", json: `{"prefix":7}`},
		{name: "blank prefix", json: `{"prefix":" "}`},
		{name: "plain command with pgid", json: `{"prefix":"query","pgid":"2.1f"}`, want: ErrOSDCommandTarget},
		{name: "PG command without pgid", pg: &pg, json: `{"prefix":"query"}`, want: ErrOSDCommandTarget},
		{name: "PG command decimal seed", pg: &pg, json: `{"prefix":"query","pgid":"2.31"}`, want: ErrOSDCommandTarget},
		{name: "PG command different pool", pg: &pg, json: `{"prefix":"query","pgid":"3.1f"}`, want: ErrOSDCommandTarget},
		{name: "PG command shard suffix", pg: &pg, json: `{"prefix":"query","pgid":"2.1fs0"}`, want: ErrOSDCommandTarget},
		{name: "PG command numeric pgid", pg: &pg, json: `{"prefix":"query","pgid":2}`, want: ErrOSDCommandTarget},
		{name: "PG command other primary", pg: &OSDCommandPG{Pool: 2, Seed: 0x1f, Primary: 4}, json: `{"prefix":"query","pgid":"2.1f"}`, want: ErrOSDCommandTarget},
		{name: "oversized input", json: `{"prefix":"version"}`, in: make([]byte, c.options.MaxFrameSize), want: ErrLimitExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			before := c.osdNext.Load()
			var result OSDCommandResult
			var err error
			if tc.pg != nil {
				result, err = o.PGCommand(ctx, *tc.pg, Command{JSON: []byte(tc.json), Input: tc.in})
			} else {
				result, err = o.Command(ctx, Command{JSON: []byte(tc.json), Input: tc.in})
			}
			if err == nil || tc.want != nil && !errors.Is(err, tc.want) || result.Submission != (OSDCommandSubmission{}) || c.osdNext.Load() != before {
				t.Fatalf("invalid OSD command reached submission: %+v %v", result, err)
			}
			select {
			case m := <-peer.requests:
				t.Fatalf("invalid OSD command was transmitted as %d", m.Type)
			default:
			}
		})
	}
	if o.session.Err() != nil || dials.Load() != 1 {
		t.Fatal("local OSD command rejection failed/reconnected the session", o.session.Err(), dials.Load())
	}
}

func TestOSDCommandRepliesShareTheObjectSessionAndPreserveRawResults(t *testing.T) {
	c, peers, dials := osdTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	o, err := c.OpenOSD(ctx, OSDTarget{ID: 0, Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	type response struct {
		result OSDCommandResult
		err    error
	}
	input := []byte{0, 0xff, 'b', 'u', 'l', 'k'}
	commandDone := make(chan response, 1)
	go func() {
		r, err := o.Command(ctx, Command{JSON: []byte(`{"prefix":"perf schema","format":"json"}`), Input: input})
		commandDone <- response{r, err}
	}()
	command := peer.next(t)
	fsid, commands := decodeOSDTell(t, command)
	if fsid != ([16]byte{1}) || len(commands) != 1 || commands[0] != `{"prefix":"perf schema","format":"json"}` || !bytes.Equal(command.Data, input) || command.Transaction == 0 {
		t.Fatal("OSD command FSID, raw JSON vector or bulk input changed", fsid, commands)
	}
	input[0] = 'X' // caller-owned input was copied before transmission
	objectDone := make(chan error, 1)
	go func() { _, err := o.Request(ctx, osdTestRequest()); objectDone <- err }()
	object := peer.next(t)
	if object.Type != msgr.OSDOpMessage || object.Transaction <= command.Transaction {
		t.Fatal("object request did not share the client-wide OSD TID sequence", object.Transaction, command.Transaction)
	}
	// The later object request stays pending while the earlier command's
	// binary reply arrives: replies are matched by TID, not by order.
	output := []byte{0, 0xff, '\n', 0x80}
	_ = osdMapsWrite(peer, osdCommandReply(command.Transaction, 0, "", output))
	got := <-commandDone
	if got.err != nil || got.result.Code != 0 || got.result.Message != "" || !bytes.Equal(got.result.Data, output) || got.result.Submission.Transaction != command.Transaction || got.result.Submission.GlobalID != 42 || got.result.Submission.OSD != 0 || got.result.Submission.PG != nil {
		t.Fatalf("raw OSD command result or submission changed: %+v %v", got.result, got.err)
	}
	if o.session.Err() != nil || dials.Load() != 1 {
		t.Fatal("OSD command failed the shared session", o.session.Err())
	}
	_ = o.Close()
	<-objectDone
}

func TestOSDCommandServerRefusalsAndInvalidReplies(t *testing.T) {
	pg := OSDCommandPG{Pool: 2, Seed: 0x1f, Primary: 0, MapEpoch: 9}
	for _, tc := range []struct {
		name    string
		pg      *OSDCommandPG
		reply   func(uint64) msgr.MessageData
		code    int32
		invalid bool
	}{
		{name: "allow-all capability refusal", reply: func(id uint64) msgr.MessageData { return osdCommandReply(id, -13, "", nil) }, code: -13},
		{name: "unknown prefix", reply: func(id uint64) msgr.MessageData {
			return osdCommandReply(id, -22, "unknown command prefix nosuch", nil)
		}, code: -22},
		{name: "PG not primary", pg: &pg, reply: func(id uint64) msgr.MessageData { return osdCommandReply(id, -11, "not primary for pgid 2.1f", nil) }, code: -11},
		{name: "PG absent", pg: &pg, reply: func(id uint64) msgr.MessageData { return osdCommandReply(id, -2, "i don't have pgid 2.1f", nil) }, code: -2},
		{name: "PG success", pg: &pg, reply: func(id uint64) msgr.MessageData {
			return osdCommandReply(id, 0, "", []byte(`{"state":"active+clean"}`))
		}},
		{name: "truncated reply front", reply: func(id uint64) msgr.MessageData {
			m := osdCommandReply(id, 0, "status", nil)
			m.Front = m.Front[:6]
			return m
		}, invalid: true},
		{name: "unsupported reply compatibility", reply: func(id uint64) msgr.MessageData {
			m := osdCommandReply(id, 0, "", nil)
			m.CompatVersion = 2
			return m
		}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peers, dials := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{ID: 0, Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			type response struct {
				result OSDCommandResult
				err    error
			}
			done := make(chan response, 1)
			go func() {
				var r OSDCommandResult
				var err error
				if tc.pg != nil {
					r, err = o.PGCommand(ctx, *tc.pg, Command{JSON: []byte(`{"prefix":"query","pgid":"2.1f"}`)})
				} else {
					r, err = o.Command(ctx, Command{JSON: []byte(`{"prefix":"nosuch"}`)})
				}
				done <- response{r, err}
			}()
			m := peer.next(t)
			_, commands := decodeOSDTell(t, m)
			if tc.pg != nil && commands[0] != `{"prefix":"query","pgid":"2.1f"}` {
				t.Fatal("client rewrote PG command JSON", commands)
			}
			_ = osdMapsWrite(peer, tc.reply(m.Transaction))
			got := <-done
			var command *CommandError
			var unknown *OutcomeUnknownError
			switch {
			case tc.invalid:
				if !errors.As(got.err, &unknown) || errors.As(got.err, &command) || got.result.Submission.Transaction != m.Transaction {
					t.Fatal("invalid OSD command reply was not an uncertain outcome", got.err)
				}
				if _, err := o.NextMap(ctx); err == nil {
					t.Fatal("invalid OSD command reply did not terminate the session")
				}
			case tc.code < 0:
				if !errors.As(got.err, &command) || command.Code != tc.code || got.result.Code != tc.code || errors.As(got.err, &unknown) {
					t.Fatal("known OSD command refusal changed class or code", got.err)
				}
				if o.session.Err() != nil {
					t.Fatal("known OSD command refusal failed the session")
				}
			default:
				if got.err != nil || got.result.Submission.PG == nil || *got.result.Submission.PG != pg || !strings.Contains(string(got.result.Data), "active+clean") {
					t.Fatal("PG command result or submission metadata changed", got.result, got.err)
				}
			}
			_ = o.Close()
			select {
			case <-peer.done:
			case <-ctx.Done():
				t.Fatal("OSD command peer cleanup stalled")
			}
			if dials.Load() != 1 {
				t.Fatal("OSD command automatically reconnected")
			}
			select {
			case replay := <-peer.requests:
				t.Fatal("OSD command was resent", replay.Type, replay.Transaction)
			default:
			}
		})
	}
}

func TestOSDCommandCancellationIsUncertainAndNeverResent(t *testing.T) {
	c, peers, _ := osdTestClient(t)
	o, err := c.OpenOSD(context.Background(), OSDTarget{ID: 0, Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	ctx, cancel := context.WithCancel(context.Background())
	type response struct {
		result OSDCommandResult
		err    error
	}
	done := make(chan response, 1)
	go func() {
		r, err := o.Command(ctx, Command{JSON: []byte(`{"prefix":"bench"}`)})
		done <- response{r, err}
	}()
	first := peer.next(t)
	cancel()
	got := <-done
	var unknown *OutcomeUnknownError
	if !errors.As(got.err, &unknown) || !errors.Is(got.err, context.Canceled) || got.result.Submission.Transaction != first.Transaction || got.result.Code != 0 || got.result.Data != nil {
		t.Fatalf("cancelled OSD command lost uncertainty or submission identity: %+v %v", got.result, got.err)
	}
	// A late reply for the cancelled TID is dropped; the next command still
	// matches its own reply on the same session.
	_ = osdMapsWrite(peer, osdCommandReply(first.Transaction, 0, "late", []byte("late")))
	next, nextCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer nextCancel()
	go func() {
		r, err := o.Command(next, Command{JSON: []byte(`{"prefix":"version"}`)})
		done <- response{r, err}
	}()
	second := peer.next(t)
	if second.Transaction == first.Transaction {
		t.Fatal("cancelled OSD command was resent")
	}
	_ = osdMapsWrite(peer, osdCommandReply(second.Transaction, 0, "", []byte("v20")))
	got = <-done
	if got.err != nil || string(got.result.Data) != "v20" || o.session.Err() != nil {
		t.Fatal("late reply corrupted the next OSD command", got.result, got.err)
	}
	_ = o.Close()
	select {
	case replay := <-peer.requests:
		t.Fatal("OSD command replayed after cancellation", replay.Transaction)
	default:
	}
}

func TestOSDCommandReplyTypeForObjectTIDFailsTheSession(t *testing.T) {
	c, peers, _ := osdTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	done := make(chan error, 1)
	go func() { _, err := o.Request(ctx, osdTestRequest()); done <- err }()
	m := peer.next(t)
	_ = osdMapsWrite(peer, osdCommandReply(m.Transaction, 0, "", nil))
	var unknown *OutcomeUnknownError
	if err := <-done; !errors.As(err, &unknown) || !errors.Is(err, msgr.ErrFrame) {
		t.Fatal("MCommandReply for an object TID completed or lost uncertainty", err)
	}
}
