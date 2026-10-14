package integration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestOSDMapWaitReportsRPCFailureWithoutWaitingForMapDeadline(t *testing.T) {
	read := make(chan error, 1)
	maps := make(chan osdMapWaitResult) // No map or map deadline has occurred.
	read <- context.DeadlineExceeded
	_, first, err := waitOSDMapAndRead(context.Background(), read, maps, false)
	if !errors.Is(first, context.DeadlineExceeded) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("first stale RPC failure was hidden by map waiting", first, err)
	}
}

func TestOSDMapWaitPreservesAncientRPCErrorWithMapFailure(t *testing.T) {
	read := make(chan error)
	maps := make(chan osdMapWaitResult)
	mapFailure := errors.New("map delivery failed")
	go func() {
		// Unbuffered sends establish the failure order independently of
		// goroutine scheduling, without sleeps or duplicated timeout logic.
		read <- context.DeadlineExceeded
		maps <- osdMapWaitResult{err: mapFailure}
	}()
	_, first, err := waitOSDMapAndRead(context.Background(), read, maps, true)
	if !errors.Is(first, context.DeadlineExceeded) || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, mapFailure) {
		t.Fatal("ancient RPC failure was lost when map waiting failed", first, err)
	}
}

func TestOSDMapWaitAllowsDeliveredMapAndAncientDroppedRead(t *testing.T) {
	read := make(chan error, 1)
	maps := make(chan osdMapWaitResult, 1)
	read <- context.DeadlineExceeded
	maps <- osdMapWaitResult{batch: cephmsgr.OSDMapBatch{FSID: "fixture", Version: 1}}
	batch, first, err := waitOSDMapAndRead(context.Background(), read, maps, true)
	if err != nil || !errors.Is(first, context.DeadlineExceeded) || batch.FSID != "fixture" {
		t.Fatal("native map receipt was equated with ancient read success", batch, first, err)
	}
}

func TestOSDMapWaitRetainsBothAlreadyBufferedFailures(t *testing.T) {
	for _, ancient := range []bool{false, true} {
		for i := 0; i < 100; i++ {
			read := make(chan error, 1)
			maps := make(chan osdMapWaitResult, 1)
			mapFailure := errors.New("independent map failure")
			read <- context.DeadlineExceeded
			maps <- osdMapWaitResult{err: mapFailure}
			_, first, err := waitOSDMapAndRead(context.Background(), read, maps, ancient)
			if !errors.Is(first, context.DeadlineExceeded) || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, mapFailure) {
				t.Fatal("select ordering discarded an already completed failure", ancient, first, err)
			}
		}
	}
}

func TestOSDMapWaitAfterMapFailureIsBoundedByContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	read := make(chan error) // The RPC has not produced a result.
	maps := make(chan osdMapWaitResult)
	mapFailure := errors.New("map failure before RPC completion")
	go func() {
		maps <- osdMapWaitResult{err: mapFailure}
		cancel() // The unbuffered send establishes map failure before cancel.
	}()
	_, first, err := waitOSDMapAndRead(ctx, read, maps, true)
	if first != nil || !errors.Is(err, context.Canceled) || !errors.Is(err, mapFailure) {
		t.Fatal("map failure wait lacked an explicit bound or fabricated an RPC result", first, err)
	}
}

func TestOSDMapWaitContextBoundaryRetainsCompletedFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	read := make(chan error, 1)
	maps := make(chan osdMapWaitResult, 1)
	mapFailure := errors.New("completed map failure")
	read <- context.DeadlineExceeded
	maps <- osdMapWaitResult{err: mapFailure}
	_, first, err := waitOSDMapAndRead(ctx, read, maps, true)
	if !errors.Is(first, context.DeadlineExceeded) || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, mapFailure) {
		t.Fatal("context boundary discarded a completed failure", first, err)
	}
}
