package session

import (
	"context"
	"errors"
	"io"
	"net"
)

// RetryableSetupError applies the bootstrap retry policy before commands are
// admitted. Network errors, EOF, closed sockets and endpoint deadlines are
// retryable. Every joined cause must qualify; caller cancellation and other
// refusals are not retryable.
func RetryableSetupError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	return setupTransportError(err, false)
}

// Cleanup can also expose local pipe/short-write failures. Those remain outside
// bootstrap's retry policy, but retain context mapping when cleanup overlaps
// cancellation. Inspect every joined cause so a refusal cannot be hidden by I/O.
func setupTransportError(err error, allowLocalIO bool) bool {
	if err == nil {
		return false
	}
	switch e := err.(type) {
	case interface{ Unwrap() []error }:
		children := e.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !setupTransportError(child, allowLocalIO) {
				return false
			}
		}
		return true
	case net.Error:
		return true
	case interface{ Unwrap() error }:
		return setupTransportError(e.Unwrap(), allowLocalIO)
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) ||
		(allowLocalIO && (errors.Is(err, io.ErrClosedPipe) || errors.Is(err, io.ErrShortWrite)))
}
