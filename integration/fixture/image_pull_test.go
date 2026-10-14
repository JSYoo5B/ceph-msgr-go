package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
)

const retryTestImage = "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"

func TestImageFetchRegistryRecovery(t *testing.T) {
	// Moby retains the Docker daemon's Internal class, not the registry's
	// exact HTTP status. The narrow status message still permits fetch retry.
	first := fmt.Errorf("Error response from daemon: %w", errdefs.ErrInternal.WithMessage("received unexpected HTTP status: 502 Bad Gateway"))
	ctx := context.Background()
	var diagnostics bytes.Buffer
	attempts := 0
	var delays []time.Duration
	err := pullFixtureImage(ctx, retryTestImage, func(actual context.Context) error {
		if actual != ctx {
			t.Fatal("pull context replaced")
		}
		attempts++
		if attempts == 1 {
			return first
		}
		return nil
	}, func(actual context.Context, delay time.Duration) error {
		if actual != ctx {
			t.Fatal("wait context replaced")
		}
		delays = append(delays, delay)
		return nil
	}, &diagnostics)
	if err != nil || attempts != 2 || len(delays) != 1 || delays[0] != time.Second {
		t.Fatalf("recovery err=%v attempts=%d delays=%v", err, attempts, delays)
	}
	text := diagnostics.String()
	firstIndex := strings.Index(text, first.Error())
	if firstIndex < 0 || strings.Index(text, "recovered after 2") < firstIndex || strings.Count(text, retryTestImage) != 2 {
		t.Fatalf("first fetch failure/reference not preserved before recovery: %q", text)
	}
}

func TestImageFetchExhaustionPreservesEveryAttempt(t *testing.T) {
	first := errors.New("received unexpected HTTP status: 502 Bad Gateway")
	middle := errors.New("received unexpected HTTP status: 503 Service Unavailable")
	last := errors.New("received unexpected HTTP status: 504 Gateway Timeout")
	failures := []error{first, middle, last}
	attempts := 0
	var delays []time.Duration
	var diagnostics bytes.Buffer
	err := pullFixtureImage(context.Background(), retryTestImage, func(context.Context) error {
		failure := failures[attempts]
		attempts++
		return failure
	}, func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		return nil
	}, &diagnostics)
	if attempts != 3 || len(delays) != 2 || delays[0] != time.Second || delays[1] != 2*time.Second {
		t.Fatalf("unbounded fetch attempts/waits: %d %v", attempts, delays)
	}
	for _, failure := range failures {
		if !errors.Is(err, failure) {
			t.Fatalf("fetch failure dropped: %v; got %v", failure, err)
		}
	}
	if !strings.Contains(err.Error(), retryTestImage) || !strings.Contains(err.Error(), "after 3 attempts") ||
		strings.Count(diagnostics.String(), "attempt 1") != 1 || strings.Contains(diagnostics.String(), "recovered") {
		t.Fatalf("incorrect final fetch evidence: err=%v diagnostic=%q", err, diagnostics.String())
	}
}

func TestImageFetchPermanentErrorsNeverRetry(t *testing.T) {
	for _, failure := range []error{
		errors.New("received unexpected HTTP status: 403 Forbidden"),
		errors.New("received unexpected HTTP status: 404 Not Found"),
		errors.New("received unexpected HTTP status: 500 Internal Server Error"),
		errors.New("no matching manifest for linux/arm64"),
		errors.New("invalid checksum digest"),
		errors.New("error pulling image: connection reset"),
		errdefs.ErrInternal.WithMessage("daemon internal failure without registry status"),
		errors.New("received unexpected HTTP status: 5020 Bad Gateway"),
		errors.New("received unexpected HTTP status: 503x"),
		errdefs.ErrUnauthenticated.WithMessage("received unexpected HTTP status: 502 Bad Gateway"),
		errdefs.ErrPermissionDenied.WithMessage("received unexpected HTTP status: 503 Service Unavailable"),
		errdefs.ErrNotFound.WithMessage("received unexpected HTTP status: 504 Gateway Timeout"),
		errdefs.ErrInvalidArgument.WithMessage("received unexpected HTTP status: 502 Bad Gateway"),
		errdefs.ErrNotImplemented.WithMessage("received unexpected HTTP status: 503 Service Unavailable"),
		context.Canceled,
		context.DeadlineExceeded,
	} {
		t.Run(failure.Error(), func(t *testing.T) {
			attempts := 0
			var diagnostics bytes.Buffer
			err := pullFixtureImage(context.Background(), retryTestImage, func(context.Context) error {
				attempts++
				return failure
			}, func(context.Context, time.Duration) error {
				t.Fatal("permanent image failure reached backoff")
				return nil
			}, &diagnostics)
			if attempts != 1 || !errors.Is(err, failure) || diagnostics.Len() != 0 {
				t.Fatalf("permanent error retried/changed: attempts=%d err=%v diagnostic=%q", attempts, err, diagnostics.String())
			}
		})
	}
}

func TestImageFetchCanceledBeforePull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := pullFixtureImage(ctx, retryTestImage, func(context.Context) error {
		t.Fatal("canceled bootstrap reached pull")
		return nil
	}, func(context.Context, time.Duration) error {
		t.Fatal("canceled bootstrap reached backoff")
		return nil
	}, &bytes.Buffer{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("lost parent cancellation: %v", err)
	}
}

func TestImageFetchCanceledInBackoffHasNoAdditionalPull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := errors.New("received unexpected HTTP status: 502 Bad Gateway")
	attempts, waits := 0, 0
	err := pullFixtureImage(ctx, retryTestImage, func(context.Context) error {
		attempts++
		return first
	}, func(actual context.Context, delay time.Duration) error {
		waits++
		cancel()
		return actual.Err()
	}, &bytes.Buffer{})
	if attempts != 1 || waits != 1 || !errors.Is(err, context.Canceled) || !errors.Is(err, first) {
		t.Fatalf("canceled backoff lost first error or pulled again: attempts=%d waits=%d err=%v", attempts, waits, err)
	}
}

func TestImageFetchCancellationEvenIfWaitReturnsNil(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempts := 0
	err := pullFixtureImage(ctx, retryTestImage, func(context.Context) error {
		attempts++
		return errors.New("received unexpected HTTP status: 503 Service Unavailable")
	}, func(context.Context, time.Duration) error {
		cancel()
		return nil
	}, &bytes.Buffer{})
	if attempts != 1 || !errors.Is(err, context.Canceled) {
		t.Fatalf("wait callback cancellation allowed another pull: %d %v", attempts, err)
	}
}

func TestImageFetchDiagnosticIsBoundedAndQuoted(t *testing.T) {
	attempts := 0
	var diagnostics bytes.Buffer
	err := pullFixtureImage(context.Background(), retryTestImage, func(context.Context) error {
		attempts++
		if attempts == 1 {
			return fmt.Errorf("received unexpected HTTP status: 504 Gateway Timeout\n%s", strings.Repeat("x", 4096))
		}
		return nil
	}, func(context.Context, time.Duration) error { return nil }, &diagnostics)
	if err != nil || diagnostics.Len() > 1200 || strings.Count(diagnostics.String(), "\n") != 2 ||
		!strings.Contains(diagnostics.String(), `\n`) || !strings.Contains(diagnostics.String(), "...") {
		t.Fatalf("unbounded/multiline error diagnostic: len=%d text=%q err=%v", diagnostics.Len(), diagnostics.String(), err)
	}
}

func TestImageFetchWaitHonorsCanceledParent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitImageFetch(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("timer ignored cancellation: %v", err)
	}
	if err := waitImageFetch(context.Background(), 0); err != nil {
		t.Fatalf("zero timer failed: %v", err)
	}
}
