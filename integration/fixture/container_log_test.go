package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

type containerLogFake struct {
	io.Reader
	closed   int
	closeErr error
}

func (f *containerLogFake) Close() error {
	f.closed++
	return f.closeErr
}

func containerLogFrom(f *containerLogFake) containerLogOpener {
	return func(context.Context) (io.ReadCloser, error) { return f, nil }
}

func TestContainerLogIsCompleteOrRefused(t *testing.T) {
	f := &containerLogFake{Reader: strings.NewReader("12345")}
	data, err := readCompleteContainerLog(context.Background(), containerLogFrom(f), 5)
	if err != nil || string(data) != "12345" || f.closed != 1 {
		t.Fatal("log at its bound was not returned whole", string(data), err, f.closed)
	}
	f = &containerLogFake{Reader: strings.NewReader("123456")}
	if data, err := readCompleteContainerLog(context.Background(), containerLogFrom(f), 5); !errors.Is(err, errContainerLogExceedsBound) || data != nil || f.closed != 1 {
		t.Fatal("truncated log was accepted as complete", string(data), err)
	}
	cause := errors.New("close failed")
	f = &containerLogFake{Reader: strings.NewReader("1"), closeErr: cause}
	if _, err := readCompleteContainerLog(context.Background(), containerLogFrom(f), 5); !errors.Is(err, cause) {
		t.Fatal("close failure was dropped", err)
	}
	open := func(context.Context) (io.ReadCloser, error) { return nil, cause }
	if _, err := readCompleteContainerLog(context.Background(), open, 5); !errors.Is(err, cause) {
		t.Fatal("open failure was dropped", err)
	}
	empty := &containerLogFake{Reader: strings.NewReader("")}
	if _, err := readCompleteContainerLog(context.Background(), containerLogFrom(empty), 0); err == nil || empty.closed != 0 {
		t.Fatal("unbounded read accepted")
	}
}

// blockingLog blocks reads until it is closed.
type blockingLog struct{ done chan struct{} }

func (b *blockingLog) Read([]byte) (int, error) { <-b.done; return 0, io.ErrClosedPipe }
func (b *blockingLog) Close() error {
	select {
	case <-b.done:
	default:
		close(b.done)
	}
	return nil
}

func TestContainerLogClosesStreamWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stream := &blockingLog{done: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, err := readCompleteContainerLog(ctx, func(context.Context) (io.ReadCloser, error) { return stream, nil }, 5)
		result <- err
	}()
	cancel()
	if err := <-result; err == nil {
		t.Fatal("canceled collection returned a complete log")
	}
}

func TestManagerRejectionRefusesOversizedLog(t *testing.T) {
	start := "2026-10-18T00:00:00.000+0000 7f 0 ceph version 20.2.0\n"
	rejected := "2026-10-18T00:00:01.000+0000 7f -1 monclient(hunting): handle_auth_bad_method failed to auth with my available methods: (13) Permission denied\n"
	open := func(log string) containerLogOpener {
		return containerLogFrom(&containerLogFake{Reader: strings.NewReader(log)})
	}
	line, process, err := managerRejectionFromLog(context.Background(), open(start+rejected))
	if err != nil || line+"\n" != rejected || process+"\n" != start {
		t.Fatal("complete MGR log not interpreted", line, process, err)
	}
	// The refusal is in the oversized log's head: a bounded prefix must not
	// read as "no refusal".
	huge := start + rejected + string(bytes.Repeat([]byte("x"), managerRejectionLogLimit))
	if line, _, err := managerRejectionFromLog(context.Background(), open(huge)); err == nil || line != "" || !strings.Contains(err.Error(), "exceeds bound") {
		t.Fatal("oversized MGR log was interpreted", line, err)
	}
}
