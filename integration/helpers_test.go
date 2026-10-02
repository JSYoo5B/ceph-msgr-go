package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

func integrationOptions(t *testing.T) cephmsgr.Options {
	t.Helper()
	monitors := os.Getenv("CEPH_MSGR_MONITORS")
	if monitors == "" {
		t.Skip("set CEPH_MSGR_MONITORS, CEPH_MSGR_KEY_FILE and CEPH_MSGR_IDENTITY for a real cluster")
	}
	encoded, err := os.ReadFile(os.Getenv("CEPH_MSGR_KEY_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	key, err := cephmsgr.ParseKey(strings.TrimSpace(string(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	return cephmsgr.Options{Monitors: strings.Split(monitors, ","), Identity: os.Getenv("CEPH_MSGR_IDENTITY"), Key: key, ExpectedFSID: os.Getenv("CEPH_MSGR_FSID"), DialContext: testcluster.Dialer()}
}

func fixtureClient(t *testing.T, timeout time.Duration) (*cephmsgr.Client, context.Context) {
	t.Helper()
	if os.Getenv("CEPH_MSGR_CONTROL_DIR") == "" {
		t.Skip("configuration and module changes require the disposable fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	c, err := cephmsgr.Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, ctx
}

func waitClientState(t *testing.T, c *cephmsgr.Client, ctx context.Context, matches func(cephmsgr.State) bool) cephmsgr.State {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		state := c.Snapshot()
		if matches(state) {
			return state
		}
		select {
		case <-ctx.Done():
			t.Fatal("client state did not settle", ctx.Err(), state)
		case <-ticker.C:
		}
	}
}

func fixtureRecoveryRead(ctx context.Context, c *cephmsgr.Client, mgr bool) error {
	call, prefix := c.MonCommand, "status"
	if mgr {
		call, prefix = c.MgrCommand, "pg stat"
	}
	command, _ := json.Marshal(map[string]string{"prefix": prefix, "format": "json"})
	for {
		result, err := call(ctx, cephmsgr.Command{JSON: command})
		if err == nil {
			if !json.Valid(result.Data) {
				return fmt.Errorf("invalid %s output after quorum recovery", prefix)
			}
			return nil
		}
		var unknown *cephmsgr.OutcomeUnknownError
		var network *net.OpError
		if !errors.As(err, &unknown) && !errors.As(err, &network) && !errors.Is(err, cephmsgr.ErrManagerChanged) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, net.ErrClosed) {
			return err
		}
		// This is a read-only oracle. Mutations are never retried by this
		// helper or by the product's session recovery.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
