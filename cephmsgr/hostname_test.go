package cephmsgr

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
)

func TestHostnameSubscriptionsStayConsistentAcrossSources(t *testing.T) {
	for _, test := range []struct{ name, hostname string }{
		{"empty default", ""},
		{"caller bytes", " Host.雪\x00\xff \n"},
		{"long caller value", strings.Repeat("Host.雪 ", 200)},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, ctx, options, peers := hostnameTestFixture(t, test.hostname)
			identity := options.Identity
			options.Hostname = "caller changed its Options after Dial"
			peer := hostnameTestNextPeer(t, ctx, peers)
			peer.registrations(t, ctx, test.hostname, identity, "maps")
			configs, err := client.WatchConfig(ctx, ConfigOptions{})
			if err != nil {
				t.Fatal(err)
			}
			peer.registrations(t, ctx, test.hostname, identity, "config", "getconfig")
			logs, err := client.WatchLogs(ctx, LogOptions{Level: LogDebug})
			if err != nil {
				t.Fatal(err)
			}
			peer.registrations(t, ctx, test.hostname, identity, "log-debug")
			digests, err := client.WatchDigest(ctx, DigestOptions{})
			if err != nil {
				t.Fatal(err)
			}
			peer.registrations(t, ctx, test.hostname, identity, "mgrdigest")

			// Host selection must not turn these raw streams into an effective
			// configuration loader or a JSON result interpreter.
			config := map[string]string{" unknown\x00\xff ": " not JSON\x00\xff\n"}
			if err := peer.send(configTestMessage(" unknown\x00\xff ", config[" unknown\x00\xff "])); err != nil {
				t.Fatal(err)
			}
			if got, err := configs.Next(ctx); err != nil || !reflect.DeepEqual(got, config) {
				t.Fatal("hostname changed raw configuration", err)
			}
			if err := peer.send(digestTestMessage(" not JSON\x00\xff ", "raw health\n\x00")); err != nil {
				t.Fatal(err)
			}
			digestTestNext(t, ctx, digests, " not JSON\x00\xff ", "raw health\n\x00")
			entry := logTestRawEntry("raw log\x00\xff\n")
			if err := peer.send(logTestMessage([16]byte{1}, 7, entry)); err != nil {
				t.Fatal(err)
			}
			logTestBatch(t, ctx, logs, 7, entry)

			// Rejected registrations and cancelled local waits must not send a
			// different host, another subscription, GetConfig or JSON command.
			if next, err := client.WatchConfig(ctx, ConfigOptions{}); next != nil || !errors.Is(err, ErrConfigWatchActive) {
				t.Fatal("second config watch admitted", err)
			}
			if next, err := client.WatchLogs(ctx, LogOptions{}); next != nil || !errors.Is(err, ErrLogWatchActive) {
				t.Fatal("second log watch admitted", err)
			}
			if next, err := client.WatchDigest(ctx, DigestOptions{}); next != nil || !errors.Is(err, ErrDigestWatchActive) {
				t.Fatal("second digest watch admitted", err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if next, err := client.WatchConfig(cancelled, ConfigOptions{}); next != nil || !errors.Is(err, context.Canceled) {
				t.Fatal("cancelled config registration", err)
			}
			if next, err := client.WatchLogs(cancelled, LogOptions{}); next != nil || !errors.Is(err, context.Canceled) {
				t.Fatal("cancelled log registration", err)
			}
			if next, err := client.WatchDigest(cancelled, DigestOptions{}); next != nil || !errors.Is(err, context.Canceled) {
				t.Fatal("cancelled digest registration", err)
			}
			if _, err := configs.Next(cancelled); !errors.Is(err, context.Canceled) {
				t.Fatal("cancelled config wait", err)
			}
			if _, err := logs.Next(cancelled); !errors.Is(err, context.Canceled) {
				t.Fatal("cancelled log wait", err)
			}
			if _, err := digests.Next(cancelled); !errors.Is(err, context.Canceled) {
				t.Fatal("cancelled digest wait", err)
			}
			logTestBarrier(t, ctx, client)
			select {
			case message := <-peer.requests:
				if message.Type != 50 || len(peer.requests) != 0 {
					t.Fatal("rejected watch or local cancellation transmitted a request", message.Type)
				}
			default:
				t.Fatal("explicit barrier was not observed")
			}

			configs.Close()
			configs, err = client.WatchConfig(ctx, ConfigOptions{})
			if err != nil {
				t.Fatal("same-session config reopen", err)
			}
			peer.registrations(t, ctx, test.hostname, identity, "config", "getconfig")
			initialID := client.Snapshot().GlobalID
			// Move only the fixture ticket's renewal time. The real coordinator
			// performs a fresh encrypted CephX exchange and watch registration.
			auth := client.snapshotAuth()
			ticket := auth.Tickets[cephx.ServiceAuth]
			ticket.RenewAfter = time.Now().Add(-time.Second)
			auth.Tickets[cephx.ServiceAuth] = ticket
			client.mu.Lock()
			client.auth = auth
			client.mu.Unlock()
			client.wakeMonitor()
			peer = hostnameTestNextPeer(t, ctx, peers)
			peer.registrations(t, ctx, test.hostname, identity, "maps", "config", "getconfig", "log-debug", "mgrdigest")
			if client.Snapshot().GlobalID != initialID {
				t.Fatal("renewal changed client identity")
			}
			peer.conn.Close()
			peer = hostnameTestNextPeer(t, ctx, peers)
			peer.registrations(t, ctx, test.hostname, identity, "maps", "config", "getconfig", "log-debug", "mgrdigest")
			if err := peer.send(configTestMessage("recovered", "raw value\x00\xff")); err != nil {
				t.Fatal(err)
			}
			if got, err := configs.Next(ctx); err != nil || got["recovered"] != "raw value\x00\xff" {
				t.Fatal("recovered hostname watch lost its raw map", err)
			}
			if state := client.Snapshot(); !state.Monitor.Ready || state.Manager.Ready {
				t.Fatal("hostname handling changed connection routing")
			}
		})
	}
}

func TestHostnameRequestLimitBeforeDial(t *testing.T) {
	const frameLimit = 1024
	for _, test := range []struct {
		name, identity string
		hostBytes      int
	}{
		{"subscription governs", "client.test", frameLimit - 109},
		{"getconfig governs", "client." + strings.Repeat("x", 900), frameLimit - (57 + 900)},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, extra := range []int{0, 1} {
				options := mockOptions(t, 20, [16]byte{1})
				options.Identity, options.Hostname = test.identity, strings.Repeat("h", test.hostBytes+extra)
				options.MaxFrameSize = frameLimit
				calls := 0
				dialCause := errors.New("fixture stops after preflight")
				options.DialContext = func(context.Context, string, string) (net.Conn, error) { calls++; return nil, dialCause }
				client, err := Dial(context.Background(), options)
				if client != nil {
					client.Close()
					t.Fatal("sentinel dial returned a client")
				}
				if extra == 0 {
					// This proves exact-bound validation passes, not that a 1 KiB
					// limit can bootstrap every possible CephX exchange.
					if calls != 1 || !errors.Is(err, dialCause) || errors.Is(err, ErrLimitExceeded) {
						t.Fatal("exact request limit failed before the fixture dial", calls, err)
					}
				} else {
					var unknown *OutcomeUnknownError
					if calls != 0 || !errors.Is(err, ErrLimitExceeded) || errors.Is(err, ErrMalformedMessage) || errors.As(err, &unknown) {
						t.Fatal("oversize hostname was dialed or misclassified", calls, err)
					}
				}
			}
		})
	}
}
