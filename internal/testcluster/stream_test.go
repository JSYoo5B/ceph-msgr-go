package testcluster

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func testStreamRelay(t *testing.T) (*StreamRelay, net.Conn) {
	t.Helper()
	remote, peer := net.Pipe()
	relay := NewStreamRelay(remote)
	deadline := time.Now().Add(3 * time.Second)
	relay.SetDeadline(deadline)
	peer.SetDeadline(deadline)
	t.Cleanup(func() { relay.Close(); peer.Close(); relay.Wait() })
	return relay, peer
}

func waitGate(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("relay did not reach its gate")
	}
}

func TestStreamRelayResumesFragmentedBytesWithoutLoss(t *testing.T) {
	relay, peer := testStreamRelay(t)
	input := bytes.Repeat([]byte{0, 255, 13, 10}, 4096)
	output := bytes.Repeat([]byte{255, 0, 1}, 4096)
	relay.ArmWritePause()
	sent := make(chan error, 1)
	go func() { _, err := relay.Write(input); sent <- err }()
	prefix := make([]byte, 4096)
	if _, err := io.ReadFull(peer, prefix); err != nil || !bytes.Equal(prefix, input[:4096]) {
		t.Fatal("partial bytes did not reach the peer", err)
	}
	waitGate(t, relay.WriteBlocked)
	select {
	case err := <-sent:
		t.Fatal("large write finished while the relay was paused", err)
	default:
	}
	relay.ResumeWrite()
	tail := make([]byte, len(input)-len(prefix))
	if _, err := io.ReadFull(peer, tail); err != nil || !bytes.Equal(tail, input[len(prefix):]) {
		t.Fatal("resumed write changed bytes", err)
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	relay.ArmReadPause()
	replied := make(chan error, 1)
	go func() { _, err := peer.Write(output); replied <- err }()
	waitGate(t, relay.ReadBlocked)
	relay.ResumeRead()
	got := make([]byte, len(output))
	if _, err := io.ReadFull(relay, got); err != nil || !bytes.Equal(got, output) {
		t.Fatal("resumed fragmented read changed bytes", err)
	}
	if err := <-replied; err != nil {
		t.Fatal(err)
	}
	if relay.ReadChunks.Load() <= 1 || relay.WriteChunks.Load() <= 1 {
		t.Fatal("relay did not fragment the stream")
	}
}

func TestStreamRelayPausePreservesWriteDeadline(t *testing.T) {
	relay, peer := testStreamRelay(t)
	relay.ArmWritePause()
	finished := make(chan error, 1)
	go func() { _, err := relay.Write(make([]byte, 8192)); finished <- err }()
	if _, err := io.ReadFull(peer, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	waitGate(t, relay.WriteBlocked)
	relay.SetWriteDeadline(time.Now().Add(20 * time.Millisecond))
	var timeout net.Error
	if err := <-finished; !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatal("paused development relay ignored the connection deadline", err)
	}
}

func TestStreamRelayCloseReleasesBothPausedWorkers(t *testing.T) {
	relay, peer := testStreamRelay(t)
	relay.ArmWritePause()
	relay.ArmReadPause()
	finished := make(chan error, 1)
	go func() { _, err := relay.Write(make([]byte, 8192)); finished <- err }()
	if _, err := io.ReadFull(peer, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	waitGate(t, relay.WriteBlocked)
	if _, err := peer.Write([]byte("held response")); err != nil {
		t.Fatal(err)
	}
	waitGate(t, relay.ReadBlocked)
	relay.Close()
	relay.Wait()
	if err := <-finished; err == nil {
		t.Fatal("Close did not interrupt the blocked writer")
	}
}
