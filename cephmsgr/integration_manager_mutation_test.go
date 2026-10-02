package cephmsgr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fixtureBalancerMode(t *testing.T, c *Client, ctx context.Context) string {
	t.Helper()
	result, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"balancer status"}`)})
	var status struct {
		Mode string `json:"mode"`
	}
	if err != nil || json.Unmarshal(result.Data, &status) != nil || status.Mode == "" {
		t.Fatal("read independent balancer mode", err)
	}
	return status.Mode
}

func TestCephAppliedManagerMutationIsNotReplayedIntegration(t *testing.T) {
	observer, ctx := fixtureClient(t, 45*time.Second)
	original := fixtureBalancerMode(t, observer, ctx)
	wanted := "upmap"
	if original == wanted {
		wanted = "none"
	}
	restore := func(ctx context.Context) error {
		encoded, _ := json.Marshal(map[string]string{"prefix": "balancer mode", "mode": original})
		_, err := observer.MgrCommand(ctx, Command{JSON: encoded})
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := restore(cleanup); err != nil {
			t.Error("restore original balancer mode", err)
		}
	}()
	options := integrationOptions(t)
	dial := options.DialContext
	var armed, wrapped atomic.Bool
	var writes atomic.Int32
	blocked := make(chan struct{})
	var blockOnce sync.Once
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		_, port, _ := net.SplitHostPort(endpoint)
		if port == "36800" || port == "36801" {
			if wrapped.CompareAndSwap(false, true) {
				conn = &testcluster.LostReplyConn{Conn: conn, Armed: &armed, Closed: make(chan struct{}), SignalBlocked: func() { blockOnce.Do(func() { close(blocked) }) }}
			}
			conn = &commandWriteProbe{Conn: conn, writes: &writes}
		}
		return conn, nil
	}
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := fixtureBalancerMode(t, c, ctx); got != original {
		t.Fatal("clients disagree before the mutation", got, original)
	}
	c.mu.Lock()
	name := c.mgrMap.Name
	c.mu.Unlock()
	mutation, _ := json.Marshal(map[string]string{"prefix": "balancer mode", "mode": wanted})
	mutation = append(mutation, bytes.Repeat([]byte{' '}, 40<<10)...)
	armed.Store(true)
	finished := make(chan error, 1)
	go func() {
		_, err := c.MgrCommand(ctx, Command{JSON: mutation})
		finished <- err
	}()
	for fixtureBalancerMode(t, observer, ctx) != wanted {
		select {
		case err := <-finished:
			t.Fatal("mutation ended before its independently observed execution", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	select {
	case <-blocked:
	case err := <-finished:
		t.Fatal("applied mutation finished without a withheld response", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if writes.Load() != 1 {
		t.Fatal("initial mutation was not written exactly once", writes.Load())
	}
	// Overwrite the applied mode independently before replacing the MGR.
	// Any replay would overwrite this restored value with the first mutation.
	if err := restore(ctx); err != nil {
		t.Fatal("independent mode overwrite", err)
	}
	result, err := observer.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"config get","who":"mgr","key":"mgr/balancer/mode"}`)})
	if err != nil || strings.TrimSpace(string(result.Data)) != original {
		t.Fatal("MON did not confirm the independently committed overwrite", err, string(result.Data))
	}
	encoded, _ := json.Marshal(map[string]string{"prefix": "mgr fail", "who": name})
	if _, err := observer.MonCommand(ctx, Command{JSON: encoded}); err != nil {
		t.Fatal("MGR replacement with an applied mutation still awaiting its reply", err)
	}
	select {
	case err := <-finished:
		var unknown *OutcomeUnknownError
		if !errors.As(err, &unknown) || !errors.Is(err, ErrManagerChanged) {
			t.Fatal("applied mutation did not retain its unknown outcome on MGR replacement", err)
		}
	case <-ctx.Done():
		t.Fatal("MGR replacement did not release the pending mutation", ctx.Err())
	}
	if got := fixtureBalancerMode(t, c, ctx); got != original || writes.Load() != 1 {
		t.Fatal("fresh MGR replayed the applied mutation", got, original, writes.Load())
	}
	if got := fixtureBalancerMode(t, observer, ctx); got != original {
		t.Fatal("independent observer found a replayed mode change", got)
	}
	t.Logf("MGR applied %s, independent client committed %s, pending reply became unknown on failover; mutation writes=%d, new MGR kept %s", wanted, original, writes.Load(), original)
}
