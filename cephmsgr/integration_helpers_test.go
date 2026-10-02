package cephmsgr

import (
	"context"
	"os"
	"testing"
	"time"
)

func fixtureClient(t *testing.T, timeout time.Duration) (*Client, context.Context) {
	t.Helper()
	if os.Getenv("CEPH_MSGR_CONTROL_DIR") == "" {
		t.Skip("configuration and module changes require the disposable fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	c, err := Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, ctx
}
