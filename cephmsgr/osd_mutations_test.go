package cephmsgr

import (
	"context"
	"errors"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
)

func TestOSDMutationCompletionRequiresOnDisk(t *testing.T) {
	input := osd.Request{MapEpoch: 5, Locator: osd.Locator{Pool: 1}, Object: "object", Operations: []osd.Operation{{Code: osd.Append, Length: 1, Data: []byte{1}}}}
	reply := osd.Reply{Object: "object", PG: osd.PG{Pool: 1}, Operations: []osd.OperationResult{{Code: osd.Append, Length: 1}}}
	for _, flags := range []uint64{0, osd.FlagAck, osd.FlagOnNVRAM, osd.FlagAck | osd.FlagOnNVRAM} {
		reply.Flags = flags
		if err := validateOSDReply(input, reply, false); err == nil {
			t.Fatal("preliminary completion accepted", flags)
		}
	}
	reply.Flags = osd.FlagOnDisk | osd.FlagAck
	if err := validateOSDReply(input, reply, false); err != nil {
		t.Fatal(err)
	}
	reply.Flags, reply.Result = 0, -17
	if err := validateOSDReply(input, reply, false); err != nil {
		t.Fatal("known server refusal must not require durability", err)
	}
	input.Operations = []osd.Operation{{Code: osd.Zero, Offset: 2, Length: 9}}
	reply.Result, reply.Flags = 0, osd.FlagOnDisk
	reply.Operations = []osd.OperationResult{{Code: osd.Truncate, Offset: 2, Length: 9}}
	if err := validateOSDReply(input, reply, false); err != nil {
		t.Fatal("native zero-to-truncate normalization", err)
	}
	reply.Operations[0].Code = osd.Delete
	if err := validateOSDReply(input, reply, false); err == nil {
		t.Fatal("unrelated opcode accepted")
	}
}

func TestOSDMutationUnknownOutcomeRetainsSubmission(t *testing.T) {
	c, peers, _ := osdTestClient(t)
	o, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		reply OSDReply
		err   error
	}
	finished := make(chan result, 1)
	r := osdTestRequest()
	r.ClientIncarnation = 9
	r.Operations = []OSDOperation{{Code: OSDAppend, Length: 1, Data: []byte{255}}}
	go func() { reply, err := o.Request(ctx, r); finished <- result{reply, err} }()
	m := peer.next(t)
	cancel()
	got := <-finished
	var unknown *OutcomeUnknownError
	if !errors.As(got.err, &unknown) || !errors.Is(got.err, context.Canceled) || got.reply.Submission.Transaction != m.Transaction || got.reply.Submission.GlobalID != 42 || got.reply.Submission.ClientIncarnation != 9 || got.reply.Submission.MapEpoch != 5 || got.reply.Submission.Attempt != 0 || got.reply.Transaction != 0 || len(got.reply.RawFront) != 0 {
		t.Fatalf("submission identity/reply distinction lost: %+v %v", got.reply, got.err)
	}
	o.Close()
	select {
	case replay := <-peer.requests:
		t.Fatal("mutation replayed", replay.Transaction)
	default:
	}
}
