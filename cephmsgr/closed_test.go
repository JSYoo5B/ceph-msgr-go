package cephmsgr

import (
	"context"
	"errors"
	"testing"
)

func TestClosedClientRejectsCommandsBeforeValidation(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	options.MaxFrameSize = 4096
	c, err := Dial(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	for _, call := range []func(context.Context, Command) (Result, error){c.MonCommand, c.MgrCommand} {
		for _, command := range []Command{
			{JSON: []byte(`{"prefix":"status"}`)},
			{JSON: []byte(`not JSON`)},
			{JSON: []byte(`{"prefix":"status"}`), Input: make([]byte, c.options.MaxFrameSize)},
		} {
			// A ready admission slot and canceled client lifetime must not
			// randomly select validation errors instead of the close result.
			for i := 0; i < 16; i++ {
				_, err := call(context.Background(), command)
				var unknown *OutcomeUnknownError
				if !errors.Is(err, ErrClosed) || errors.As(err, &unknown) {
					t.Fatal("command on closed client lost its known shutdown result", err)
				}
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := call(ctx, Command{}); !errors.Is(err, context.Canceled) {
			t.Fatal("already canceled operation lost its context result", err)
		}
	}
}
