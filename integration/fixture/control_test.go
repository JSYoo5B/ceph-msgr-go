package main

import (
	"context"
	"os"
	"path/filepath"
	"slices"
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

// A request's input lines name the request-scoped inputs the claim renames
// (request); a malformed or missing input fails the claim.
func TestRequestClaimsItsOwnInputs(t *testing.T) {
	out := t.TempDir()
	c := &fixtureCluster{}
	write := func(name, data string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(out, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("a-input.json.7", "seven")
	write("b-input.json.7", "b seven")
	write("a-input.json.8", "eight")
	write("verify-x", "7 mode\ninput a-input.json\ninput b-input.json\n")
	fields, err := c.request(out, "verify-x")
	if err != nil || !slices.Equal(fields, []string{"7", "mode"}) {
		t.Fatal(fields, err)
	}
	for name, want := range map[string]string{"a-input.json": "seven", "b-input.json": "b seven", "a-input.json.8": "eight"} {
		if data, err := os.ReadFile(filepath.Join(out, name)); err != nil || string(data) != want {
			t.Fatal("claimed input", name, string(data), err)
		}
	}
	if fileExists(filepath.Join(out, "a-input.json.7")) || fileExists(filepath.Join(out, "verify-x")) {
		t.Fatal("the claim left the request or its scoped input")
	}
	for _, content := range []string{
		"9 mode\ninput a-input.json\n",         // its input was never written
		"9 mode\ninput ../a-input.json\n",      // not a plain name
		"9 mode\ninput a-input.json extra\n",   // more than one name
		"9 mode\nother a-input.json\n",         // not an input line
		"9 mode\ninput x.json\ninput x.json\n", // named twice
	} {
		write("verify-x", content)
		if _, err := c.request(out, "verify-x"); err == nil {
			t.Fatalf("claimed %q", content)
		}
		os.Remove(filepath.Join(out, "verify-x"))
	}
}
