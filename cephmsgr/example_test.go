package cephmsgr_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// mutationOutcome is the decision a caller makes after a mutating call
// returns. Only a received reply establishes the result. An uncertain outcome
// must be resolved by reading cluster state, never by sending the mutation
// again because a local wait ended.
func mutationOutcome(result cephmsgr.Result, err error) string {
	var unknown *cephmsgr.OutcomeUnknownError
	var rejected *cephmsgr.CommandError
	switch {
	case err == nil:
		return fmt.Sprintf("applied: code %d", result.Code)
	case errors.As(err, &unknown):
		return fmt.Sprintf("unknown (%v): read cluster state before deciding to retry", unknown.Cause)
	case errors.As(err, &rejected):
		return fmt.Sprintf("rejected by the daemon: code %d %q", rejected.Code, rejected.Message)
	default:
		return fmt.Sprintf("failed: %v", err)
	}
}

// A daemon command's context bounds only the local wait. A deadline that
// expires after transmission started reports an OutcomeUnknownError: the
// daemon may still have applied the mutation, so the caller verifies before
// acting again. (An MDS request wait instead returns the bare context error
// while its call stays pending; see the package overview.)
func ExampleOutcomeUnknownError() {
	timedOut := &cephmsgr.OutcomeUnknownError{Cause: context.DeadlineExceeded}
	fmt.Println(mutationOutcome(cephmsgr.Result{}, timedOut))
	fmt.Println(errors.Is(timedOut, context.DeadlineExceeded))

	closed := fmt.Errorf("create pool: %w", &cephmsgr.OutcomeUnknownError{Cause: cephmsgr.ErrClosed})
	fmt.Println(mutationOutcome(cephmsgr.Result{}, closed))

	rejected := &cephmsgr.CommandError{Code: -17, Message: "pool 'example' already exists"}
	fmt.Println(mutationOutcome(cephmsgr.Result{Code: -17}, rejected))
	fmt.Println(mutationOutcome(cephmsgr.Result{}, nil))
	// Output:
	// unknown (context deadline exceeded): read cluster state before deciding to retry
	// true
	// unknown (ceph: client closed): read cluster state before deciding to retry
	// rejected by the daemon: code -17 "pool 'example' already exists"
	// applied: code 0
}

// A dialed client lives until Close; the Dial context bounds only setup and
// each command context bounds only its own wait. This example needs a
// reachable cluster, so it is compiled but not run.
func Example() {
	key, err := cephmsgr.ParseKey(os.Getenv("CEPH_CLIENT_KEY"))
	if err != nil {
		log.Fatal(err)
	}
	setup, cancelSetup := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelSetup()
	client, err := cephmsgr.Dial(setup, cephmsgr.Options{
		Monitors: []string{"192.0.2.10:3300"},
		Identity: "client.admin",
		Key:      key,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	call, cancelCall := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelCall()
	result, err := client.MonCommand(call, cephmsgr.Command{JSON: []byte(`{"prefix":"osd pool create","pool":"example"}`)})
	fmt.Println(mutationOutcome(result, err))
	var unknown *cephmsgr.OutcomeUnknownError
	if errors.As(err, &unknown) {
		// Verify with a read instead of resending the creation.
		verify, cancelVerify := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelVerify()
		pools, err := client.MonCommand(verify, cephmsgr.Command{JSON: []byte(`{"prefix":"osd pool ls","format":"json"}`)})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("pools: %s\n", pools.Data)
	}
}
