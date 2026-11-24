package main

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"
)

func TestRGWNativePausedUploadBody(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	held, release := make(chan struct{}), make(chan struct{})
	b := &rgwNativePausedBody{ReadCloser: io.NopCloser(strings.NewReader("prefix-tail")), ctx: ctx, remaining: 6, held: held, release: release}
	buf := make([]byte, 30)
	n, err := b.Read(buf)
	if err != nil || string(buf[:n]) != "prefix" {
		t.Fatalf("prefix=%q err=%v", buf[:n], err)
	}
	done := make(chan string, 1)
	go func() {
		tail, err := io.ReadAll(b)
		if err != nil {
			done <- err.Error()
		} else {
			done <- string(tail)
		}
	}()
	select {
	case <-held:
	case <-time.After(time.Second):
		t.Fatal("body was not held")
	}
	select {
	case got := <-done:
		t.Fatalf("released early: %q", got)
	default:
	}
	close(release)
	select {
	case got := <-done:
		if got != "-tail" {
			t.Fatalf("tail=%q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("body did not resume")
	}
}
func TestRGWNativePausedUploadCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	b := &rgwNativePausedBody{ReadCloser: io.NopCloser(strings.NewReader("tail")), ctx: ctx, held: make(chan struct{}), release: make(chan struct{})}
	cancel()
	if _, err := b.Read(make([]byte, 4)); err != context.Canceled {
		t.Fatalf("cancellation=%v", err)
	}
}
func TestRGWNativeManagerMapIdentity(t *testing.T) {
	good := `{"epoch":8,"available":true,"active_name":"b","active_gid":12,"active_addrs":{"addrvec":[{"addr":"192.0.2.2:6800","nonce":52}]},"standbys":[]}`
	m, err := rgwNativeManagerMap([]byte(good))
	if err != nil || m.Name != "b" || m.GID != 12 {
		t.Fatalf("map=%+v err=%v", m, err)
	}
	for _, raw := range []string{`{}`, strings.Replace(good, `"nonce":52`, `"nonce":0`, 1), strings.Replace(good, `"active_name":"b"`, `"active_name":""`, 1), strings.Replace(good, `"available":true`, `"available":false`, 1)} {
		if _, err := rgwNativeManagerMap([]byte(raw)); err == nil {
			t.Fatalf("accepted invalid identity: %s", raw)
		}
	}
}

func TestRGWNativeHistoryAcrossRestart(t *testing.T) {
	before := []byte(`{"ops":[{"description":"first process only","type_data":{"client_info":{"client":"client.1","tid":4}}},{"description":"initial receive","type_data":{"client_info":{"client":"client.1","tid":5}}}]}`)
	after := []byte(`{"ops":[{"description":"retry receive","type_data":{"client_info":{"client":"client.1","tid":5}}},{"description":"second process only","type_data":{"client_info":{"client":"client.2","tid":4}}}]}`)
	var joined struct {
		Ops []struct {
			Description string `json:"description"`
		} `json:"ops"`
	}
	if err := json.Unmarshal(rgwNativeJoinHistory(t, before, after), &joined); err != nil {
		t.Fatal(err)
	}
	if len(joined.Ops) != 3 || joined.Ops[0].Description != "first process only" || joined.Ops[1].Description != "retry receive" || joined.Ops[2].Description != "second process only" {
		t.Fatalf("lost or duplicated across restart: %+v", joined)
	}
}
