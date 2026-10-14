package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

type controlledDaemon struct {
	testcontainers.Container
	stops int
}

func (d *controlledDaemon) Stop(context.Context, *time.Duration) error { d.stops++; return nil }
func TestControllerStopsOwnedDaemonBeforeAcknowledging(t *testing.T) {
	out := t.TempDir()
	d := &controlledDaemon{}
	c := &fixtureCluster{cluster: &ceph.Container{Container: d}}
	if err := os.WriteFile(filepath.Join(out, "stop-mon-a"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := c.controls(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	if d.stops != 1 || !fileExists(filepath.Join(out, "mon-a-stopped")) {
		t.Fatal("daemon action was not acknowledged")
	}
	if err := c.controls(context.Background(), out); err != nil || d.stops != 1 {
		t.Fatal("completed mutation was replayed", err, d.stops)
	}
}
func TestControllerRejectsMalformedRequestBeforeDaemonAction(t *testing.T) {
	for _, request := range []string{"", "-1 a", "1 absent", "1 a extra"} {
		out := t.TempDir()
		d := &controlledDaemon{}
		c := &fixtureCluster{cluster: &ceph.Container{Container: d}}
		if err := os.WriteFile(filepath.Join(out, "stop-mon"), []byte(request), 0600); err != nil {
			t.Fatal(err)
		}
		if err := c.controls(context.Background(), out); err == nil || d.stops != 0 {
			t.Fatal("invalid control request executed", request, err, d.stops)
		}
	}
}
