package main

import (
	"context"
	"errors"
	"io"
)

// containerLogOpener opens the complete Docker log stream of one container,
// such as testcontainers.Container.Logs or a provider daemon handle's Logs.
type containerLogOpener func(context.Context) (io.ReadCloser, error)

// errContainerLogExceedsBound reports a log that did not fit its bound. A
// truncated log is never returned as complete evidence.
var errContainerLogExceedsBound = errors.New("container log exceeds its bound")

// withContainerLog is the only collection path for daemon logs that consumer
// attribution needs from process start. ceph-testcontainers-go v0.5.0 has no
// primitive that returns a daemon's complete log since its process started
// together with an explicit truncation signal: diagnostics.Collect keeps a
// bounded tail of at most LogTail lines, and Finalization.Verify evaluates only
// classified events (assertions, fatal signals, cluster log levels) after its
// own activation. The provider's review of these recipes
// (JSYoo5B/ceph-testcontainers-go#540) also reads the handles' container Logs
// and proposed no new API. So collection stays here: one full Docker log
// stream, closed when ctx ends, with interpretation and bounds owned by read.
func withContainerLog(ctx context.Context, open containerLogOpener, read func(io.Reader) error) (err error) {
	logs, err := open(ctx)
	if err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = logs.Close() })
	readErr := read(logs)
	stop()
	return errors.Join(readErr, logs.Close())
}

// readCompleteContainerLog returns the whole log stream, or
// errContainerLogExceedsBound when it is longer than limit bytes.
func readCompleteContainerLog(ctx context.Context, open containerLogOpener, limit int) ([]byte, error) {
	if limit <= 0 {
		return nil, errors.New("invalid container log bound")
	}
	var data []byte
	err := withContainerLog(ctx, open, func(r io.Reader) error {
		var readErr error
		data, readErr = io.ReadAll(io.LimitReader(r, int64(limit)+1))
		if readErr == nil && len(data) > limit {
			readErr = errContainerLogExceedsBound
		}
		return readErr
	})
	if err != nil {
		return nil, err
	}
	return data, nil
}
