package cephmsgr

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
)

func resubmitReply(t *testing.T, input OSDRequest, attempt int32, tid uint64) msgr.MessageData {
	t.Helper()
	outputs := make([][]byte, len(input.Operations))
	m := osdXattrSyntheticReply(t, input.Operations, outputs, make([]int32, len(input.Operations)), 0, OSDReplyOnDisk)
	start := 4 + len(input.Object.Name) + 17 + 8 + 4 + 12 + 4 + 4
	binary.LittleEndian.PutUint32(m.Front[start+38*len(input.Operations):], uint32(attempt))
	m.Transaction = tid
	return m
}

func TestOSDResubmitRepeatsTheOriginalIdentityOnTheWire(t *testing.T) {
	c, peers, _ := osdTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	input := osdValidationRequest()
	input.ClientIncarnation = 31
	input.Operations = []OSDOperation{{Code: OSDAppend, Length: 2, Data: []byte{1, 2}}}
	type result struct {
		r OSDReply
		e error
	}
	run := func(request OSDRequest) (msgr.MessageData, chan result) {
		done := make(chan result, 1)
		go func() { r, e := o.Request(ctx, request); done <- result{r, e} }()
		return peer.next(t), done
	}
	first, firstDone := run(input)
	if binary.LittleEndian.Uint32(first.Front[8:])&uint32(osd.FlagRetry) != 0 {
		t.Fatal("first attempt carried the retry flag")
	}
	reply := resubmitReply(t, input, 0, first.Transaction)
	if err := osdMapsWrite(peer, reply); err != nil {
		t.Fatal(err)
	}
	got := <-firstDone
	if got.e != nil || got.r.Submission.Transaction != first.Transaction {
		t.Fatal(got.e, got.r.Submission)
	}

	again := input
	again.Resubmit, again.RetryAttempt = &got.r.Submission, 1
	second, secondDone := run(again)
	if second.Transaction != first.Transaction || binary.LittleEndian.Uint32(second.Front[8:])&uint32(osd.FlagRetry) == 0 {
		t.Fatal("resubmission changed the transaction or lacks the retry flag", second.Transaction, first.Transaction)
	}
	if err := osdMapsWrite(peer, resubmitReply(t, again, 1, second.Transaction)); err != nil {
		t.Fatal(err)
	}
	r := <-secondDone
	if r.e != nil || r.r.RetryAttempt != 1 || r.r.Submission.Transaction != first.Transaction {
		t.Fatal("resubmitted reply", r.e, r.r.RetryAttempt)
	}

	// A reply that does not echo the attempt is not this request's answer.
	third := input
	third.Resubmit, third.RetryAttempt = &got.r.Submission, 2
	third2, thirdDone := run(third)
	if err := osdMapsWrite(peer, resubmitReply(t, third, 0, third2.Transaction)); err != nil {
		t.Fatal(err)
	}
	var unknown *OutcomeUnknownError
	if e := (<-thirdDone).e; !errors.As(e, &unknown) {
		t.Fatal("mismatched retry echo was accepted", e)
	}
}

func TestOSDResubmitRefusedBeforeSubmission(t *testing.T) {
	c, peers, _ := osdTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	good := osdValidationRequest()
	good.ClientIncarnation = 5
	good.Operations = []OSDOperation{{Code: OSDAppend, Length: 1, Data: []byte{1}}}
	// Issue one real transaction so identity checks, not "never issued", decide.
	warm := make(chan error, 1)
	go func() { _, e := o.Request(ctx, good); warm <- e }()
	sent := peer.next(t)
	if err := osdMapsWrite(peer, resubmitReply(t, good, 0, sent.Transaction)); err != nil || <-warm != nil {
		t.Fatal("warm-up request", err)
	}
	submission := func(tid uint64) *OSDSubmission {
		return &OSDSubmission{Transaction: tid, GlobalID: o.globalID, ClientIncarnation: 5}
	}
	for name, edit := range map[string]func(*OSDRequest){
		"attempt without a submission": func(r *OSDRequest) { r.RetryAttempt = 1 },
		"submission without attempt":   func(r *OSDRequest) { r.Resubmit = submission(sent.Transaction) },
		"negative attempt":             func(r *OSDRequest) { r.Resubmit, r.RetryAttempt = submission(sent.Transaction), -1 },
		"attempt beyond bound":         func(r *OSDRequest) { r.Resubmit, r.RetryAttempt = submission(sent.Transaction), OSDMaxRetryAttempt+1 },
		"zero transaction":             func(r *OSDRequest) { r.Resubmit, r.RetryAttempt = submission(0), 1 },
		"transaction never issued":     func(r *OSDRequest) { r.Resubmit, r.RetryAttempt = submission(sent.Transaction+1000), 1 },
		"other client identity": func(r *OSDRequest) {
			s := submission(sent.Transaction)
			s.GlobalID++
			r.Resubmit, r.RetryAttempt = s, 1
		},
		"other incarnation": func(r *OSDRequest) {
			s := submission(sent.Transaction)
			s.ClientIncarnation++
			r.Resubmit, r.RetryAttempt = s, 1
		},
	} {
		r := good
		edit(&r)
		before := c.osdNext.Load()
		if reply, err := o.Request(ctx, r); err == nil || reply.Submission != (OSDSubmission{}) || c.osdNext.Load() != before {
			t.Fatal("invalid resubmission accepted", name, err)
		}
	}
	select {
	case m := <-peer.requests:
		t.Fatal("invalid resubmission was sent", m.Transaction)
	default:
	}
}
