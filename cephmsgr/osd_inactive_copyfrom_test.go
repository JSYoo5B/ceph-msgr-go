package cephmsgr

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
)

func TestOSDInactiveCopyFromNeverAllocatesTIDOrSends(t *testing.T) {
	c, peers, dials := osdTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	r := osdTestRequest()
	r.Operations = []OSDOperation{{Code: OSDMapExt, Length: 1, MapExt: OSDMapExtInput{MaxOutputBytes: 32}, CopyFrom: OSDCopyFromInput{SourceName: "unexpected"}}}
	before := c.osdNext.Load()
	reply, err := o.Request(ctx, r)
	if !errors.Is(err, osd.ErrRequest) || reply.Submission != (OSDSubmission{}) || c.osdNext.Load() != before {
		t.Fatalf("inactive source input reached submission: reply=%+v error=%v", reply, err)
	}
	select {
	case request := <-peer.requests:
		t.Fatalf("invalid input sent an object request with TID %d", request.Transaction)
	default:
	}
	// A local input error must leave the established connection usable. The
	// next ordinary read is the first actual object submission, not a retry.
	completed := make(chan error, 1)
	go func() {
		reply, err := o.Request(ctx, osdTestRequest())
		if err == nil && (reply.Transaction != before+1 || len(reply.RawData) != 1 || reply.RawData[0] != 42) {
			err = errors.New("valid read lost identity or output after local rejection")
		}
		completed <- err
	}()
	request := peer.next(t)
	if request.Transaction != before+1 {
		t.Fatal("rejected input consumed a transaction ID", request.Transaction)
	}
	peer.reply(t, request)
	if err := <-completed; err != nil || dials.Load() != 1 {
		t.Fatal("local rejection changed the connection", err, dials.Load())
	}
}
