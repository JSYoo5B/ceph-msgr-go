package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// budgetedObservation runs one native observation inside a pause window and
// only then judges the window's budget. The observation runs under ctx, the
// test's own context, never under a context that ends with the budget: the
// owned-pool oracle takes its input from one shared file and runs requests
// one at a time (nativePoolResult), so a request abandoned at a budget
// deadline stays queued in the fixture and later reads the input of the next
// request, which then fails or answers the wrong question. ended reports
// whether the budget ran out (non-nil); it is called once observe returned.
func budgetedObservation(ctx context.Context, ended func() error, observe func(context.Context)) error {
	observe(ctx)
	return ended()
}

// pauseDeadline is the ended function of a budget that runs out at deadline.
func pauseDeadline(deadline time.Time) func() error {
	return func() error {
		if late := time.Since(deadline); late >= 0 {
			return fmt.Errorf("the observation returned %v past the pause budget", late.Round(time.Millisecond))
		}
		return nil
	}
}

func TestBudgetedObservationOutlivesItsBudget(t *testing.T) {
	ctx := context.Background()
	past := time.Now().Add(-time.Second)
	ran := false
	err := budgetedObservation(ctx, pauseDeadline(past), func(observe context.Context) {
		ran = true
		if observe.Err() != nil {
			t.Error("the observation ran under an ended context:", observe.Err())
		}
		if _, ok := observe.Deadline(); ok {
			t.Error("the observation ran under a deadline")
		}
	})
	if !ran || err == nil {
		t.Fatal("an observation past its budget was not run to completion and refused:", ran, err)
	}
	slow := time.Now().Add(20 * time.Millisecond)
	err = budgetedObservation(ctx, pauseDeadline(slow), func(observe context.Context) {
		time.Sleep(40 * time.Millisecond)
		if observe.Err() != nil {
			t.Error("the observation was cut off at the budget deadline")
		}
	})
	if err == nil {
		t.Error("an observation that returned after the budget was accepted")
	}
	if err := budgetedObservation(ctx, pauseDeadline(time.Now().Add(time.Minute)), func(context.Context) {}); err != nil {
		t.Error("an observation within its budget refused:", err)
	}
	held, stop := context.WithCancel(ctx)
	stop()
	ended := errors.New("held phase over")
	if err := budgetedObservation(ctx, func() error {
		if held.Err() != nil {
			return ended
		}
		return nil
	}, func(observe context.Context) {
		if observe.Err() != nil {
			t.Error("the observation inherited the held phase's end")
		}
	}); !errors.Is(err, ended) {
		t.Error("the held phase's end was not reported:", err)
	}
}
