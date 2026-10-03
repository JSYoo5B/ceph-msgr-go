package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

type publicLimitCall struct {
	name string
	call func(context.Context, Command) (Result, error)
}

func publicLimitCalls(c *Client) []publicLimitCall {
	return []publicLimitCall{
		{"MonCommand", c.MonCommand},
		{"MgrCommand", c.MgrCommand},
		{"MonTell", c.MonTell},
		{"MgrTell", c.MgrTell},
		{"MonTellTo", func(ctx context.Context, command Command) (Result, error) {
			return c.MonTellTo(ctx, "b", command)
		}},
	}
}

type publicLimitResult struct {
	result Result
	err    error
}

func publicLimitKnown(t *testing.T, result Result, err, want error) {
	t.Helper()
	var unknown *OutcomeUnknownError
	var server *CommandError
	if !errors.Is(err, want) || errors.As(err, &unknown) || errors.As(err, &server) || errors.Is(err, ErrMalformedMessage) || len(result.Data) != 0 || result.Code != 0 || result.Message != "" {
		t.Fatalf("local refusal lost its known outcome or error class: want=%v err=%v data-length=%d", want, err, len(result.Data))
	}
	if errors.Is(err, ErrLimitExceeded) != (want == ErrLimitExceeded) {
		t.Fatal("local limit classification overrode cancellation or shutdown", err)
	}
}

func TestPublicOutboundLimitPrecedesOccupiedSlotAndDaemonDial(t *testing.T) {
	const limit = 4096
	f := namedTestFixtureFor(t, namedTestAddressB(), func(options *Options, _ []namedTestMember) {
		options.MaxFrameSize, options.MaxInFlight = limit, 1
	}, nil)
	held, cancelHeld := context.WithCancel(f.ctx)
	defer cancelHeld()
	first := make(chan error, 1)
	go func() {
		_, err := f.c.MonCommand(held, Command{JSON: []byte(`{"prefix":"hold primary mutation"}`)})
		first <- err
	}()
	f.primary.next(t, f.ctx, msgr.MonCommandMessage)
	if len(f.c.calls) != 1 {
		t.Fatal("the received command did not occupy the sole admission slot")
	}
	query := []byte(`{"prefix":"status"}`)
	padded := append(append([]byte(nil), query...), bytes.Repeat([]byte{' '}, 2048-len(query))...)
	commands := []struct {
		name    string
		command Command
	}{
		// Invalid JSON also proves that size rejection precedes JSON parsing.
		{"JSON", Command{JSON: bytes.Repeat([]byte{'x'}, limit)}},
		{"Input", Command{JSON: query, Input: make([]byte, limit)}},
		{"combined", Command{JSON: padded, Input: make([]byte, 2000)}},
	}
	for _, route := range publicLimitCalls(f.c) {
		t.Run(route.name, func(t *testing.T) {
			for _, command := range commands {
				t.Run(command.name, func(t *testing.T) {
					operation, cancel := context.WithCancel(f.ctx)
					defer cancel()
					waiter := &tellWaitingContext{Context: operation, waiting: make(chan struct{})}
					finished := make(chan publicLimitResult, 1)
					go func() {
						result, err := route.call(waiter, command.command)
						finished <- publicLimitResult{result, err}
					}()
					select {
					case outcome := <-finished:
						publicLimitKnown(t, outcome.result, outcome.err, ErrLimitExceeded)
						if !errors.Is(outcome.err, wire.ErrLimit) {
							t.Fatal("outbound limit lost its original wire cause", outcome.err)
						}
					case <-waiter.waiting:
						cancel()
						t.Fatal("oversized command entered the occupied-slot select")
					case <-f.ctx.Done():
						t.Fatal("size refusal did not finish", f.ctx.Err())
					}
					select {
					case <-waiter.waiting:
						t.Fatal("size refusal evaluated the admission wait context")
					default:
					}
					if len(f.c.calls) != 1 || f.monDials.Load() != 1 || f.mgrDials.Load() != 0 || f.namedDials.Load() != 0 || len(f.primary.requests) != 0 {
						t.Fatal("rejected command was admitted, transmitted, or opened a daemon connection")
					}
				})
			}
		})
	}
	cancelHeld()
	var unknown *OutcomeUnknownError
	if err := admissionResult(t, f.ctx, first); !errors.As(err, &unknown) || !errors.Is(err, context.Canceled) || errors.Is(err, ErrLimitExceeded) {
		t.Fatal("an independently sent command lost its cancellation outcome", err)
	}
	if _, err := f.c.MonCommand(f.ctx, Command{JSON: query}); err != nil {
		t.Fatal("local size refusal damaged the retained MON session", err)
	}
	f.primary.next(t, f.ctx, msgr.MonCommandMessage)
	if f.monDials.Load() != 1 || f.mgrDials.Load() != 0 || f.namedDials.Load() != 0 {
		t.Fatal("local refusal replaced a healthy session or eagerly dialed another daemon")
	}
}

func TestPublicOutboundLimitPreservesCanceledAndClosedPrecedence(t *testing.T) {
	f := namedTestFixtureFor(t, namedTestAddressB(), func(options *Options, _ []namedTestMember) {
		options.MaxFrameSize = 4096
	}, nil)
	command := Command{JSON: []byte(`invalid JSON`), Input: make([]byte, 4096)}
	canceled, cancel := context.WithCancel(f.ctx)
	cancel()
	for _, route := range publicLimitCalls(f.c) {
		t.Run(route.name+"/canceled", func(t *testing.T) {
			result, err := route.call(canceled, command)
			publicLimitKnown(t, result, err, context.Canceled)
		})
	}
	if err := f.c.Close(); err != nil {
		t.Fatal(err)
	}
	for _, route := range publicLimitCalls(f.c) {
		t.Run(route.name+"/closed", func(t *testing.T) {
			result, err := route.call(context.Background(), command)
			publicLimitKnown(t, result, err, ErrClosed)
			result, err = route.call(canceled, command)
			publicLimitKnown(t, result, err, context.Canceled)
		})
	}
	if len(f.c.calls) != 0 || f.monDials.Load() != 1 || f.mgrDials.Load() != 0 || f.namedDials.Load() != 0 {
		t.Fatal("cancellation or shutdown admitted an oversized command or dialed a daemon")
	}
	for len(f.primary.requests) != 0 {
		if request := <-f.primary.requests; request.Type == msgr.MonCommandMessage || request.Type == msgr.TellCommandMessage {
			t.Fatal("canceled or closed operation transmitted a command")
		}
	}
}
