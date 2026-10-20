package cephmsgr

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestMappedIPv6MonitorSeedsPreserveWireFamily(t *testing.T) {
	for _, tc := range []struct{ seed, endpoint string }{
		{"v2:[::ffff:c000:201]:3300/7", "[::ffff:c000:201]:3300"},
		{"v2:[::ffff:192.0.2.1]:3300/7", "[::ffff:192.0.2.1]:3300"},
	} {
		t.Run(tc.seed, func(t *testing.T) {
			endpoint, address, err := seedAddress(tc.seed)
			want := netip.MustParseAddrPort("[::ffff:192.0.2.1]:3300")
			if err != nil || endpoint != tc.endpoint || address.Endpoint != want || address.Type != 2 || address.Nonce != 7 || address.FlowInfo != 0 || address.ScopeID != 0 {
				t.Fatal("mapped IPv6 seed lost its wire family", endpoint, address, err)
			}
		})
	}
}

func TestScopedMonitorSeedsRejectedBeforeDial(t *testing.T) {
	for _, tc := range []struct {
		name     string
		monitors []string
	}{
		{"numeric", []string{"[fe80::1%3]:3300"}},
		{"named", []string{"[fe80::1%en0]:3300"}},
		{"nonce", []string{"v2:[fe80::1%3]:3300/7"}},
		{"mapped IPv4", []string{"[::ffff:192.0.2.1%3]:3300"}},
		{"after valid seed", []string{"192.0.2.1:3300", "[fe80::1%en0]:3300"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := mockOptions(t, 20, [16]byte{1})
			options.Monitors = tc.monitors
			var calls atomic.Int32
			options.DialContext = func(context.Context, string, string) (net.Conn, error) {
				calls.Add(1)
				return nil, errors.New("unexpected dial")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := Dial(ctx, options)
			if c != nil {
				c.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "zone") || !strings.Contains(err.Error(), "unsupported") || calls.Load() != 0 {
				t.Fatal("scoped seed did not fail during input validation", err, calls.Load())
			}
		})
	}
}

func TestUnscopedIPv6AndDNSMonitorSeedsReachDialer(t *testing.T) {
	for _, tc := range []struct {
		seed, endpoint string
		nonce          uint32
		literal        bool
	}{
		{"[2001:db8::1]:3300", "[2001:db8::1]:3300", 0, true},
		{"v2:[2001:db8::1]:3300/7", "[2001:db8::1]:3300", 7, true},
		{"monitor.example:3300", "monitor.example:3300", 0, false},
		{"v2:monitor.example:3300/9", "monitor.example:3300", 9, false},
	} {
		t.Run(tc.seed, func(t *testing.T) {
			endpoint, address, err := seedAddress(tc.seed)
			if err != nil || endpoint != tc.endpoint || address.Type != 2 || address.Nonce != tc.nonce || address.ScopeID != 0 || address.FlowInfo != 0 || address.Endpoint.IsValid() != tc.literal {
				t.Fatal("unscoped seed identity changed", endpoint, address, err)
			}
			if tc.literal && (address.Endpoint.String() != tc.endpoint || address.Endpoint.Addr().Zone() != "") {
				t.Fatal("unscoped IPv6 endpoint changed", address)
			}
			options := mockOptions(t, 20, [16]byte{1})
			options.Monitors = []string{tc.seed}
			dialFailure := errors.New("test dial stopped before connection setup")
			var calls int
			options.DialContext = func(_ context.Context, network, got string) (net.Conn, error) {
				calls++
				if network != "tcp" || got != tc.endpoint {
					t.Fatal("unscoped seed routing changed", network, got)
				}
				return nil, dialFailure
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err = Dial(ctx, options)
			if !errors.Is(err, dialFailure) || calls != 1 {
				t.Fatal("unscoped seed did not reach the dialer", err, calls)
			}
		})
	}
}
