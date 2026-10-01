package cephmsgr

import (
	"context"
	"errors"
	"io"
	"net"
	"time"
)

func retryableSetup(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	switch e := err.(type) {
	case interface{ Unwrap() []error }:
		children := e.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !retryableSetup(child) {
				return false
			}
		}
		return true
	case net.Error:
		return true
	case interface{ Unwrap() error }:
		return retryableSetup(e.Unwrap())
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed)
}

func (c *Client) bootstrap(ctx context.Context) error {
	err := c.connectMonitor(ctx)
	if !retryableSetup(err) {
		return err
	}
	// Tentacle can close a new authentication connection when temporarily
	// busy. No command has been admitted. Bound additional setup attempts by
	// ConnectTimeout and the caller's context, preserving both failure causes.
	retryCtx, cancel := context.WithTimeout(ctx, c.options.ConnectTimeout)
	defer cancel()
	backoff := 250 * time.Millisecond
	for {
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-retryCtx.Done():
			timer.Stop()
			return errors.Join(err, retryCtx.Err())
		}
		err = c.connectMonitor(retryCtx)
		if retryCtx.Err() != nil {
			return errors.Join(err, retryCtx.Err())
		}
		if !retryableSetup(err) {
			return err
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}
