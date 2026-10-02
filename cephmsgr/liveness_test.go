package cephmsgr

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestInvalidKeepaliveConfigurationDoesNotDial(t *testing.T) {
	for _, tc := range []struct {
		name              string
		interval, timeout time.Duration
	}{
		{"negative interval", -time.Second, 0},
		{"negative timeout", 0, -time.Second},
		{"timeout equals interval", time.Second, time.Second},
		{"timeout below interval", 2 * time.Second, time.Second},
		{"interval exceeds default timeout", time.Minute, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := mockOptions(t, 20, [16]byte{1})
			options.KeepaliveInterval, options.KeepaliveTimeout = tc.interval, tc.timeout
			options.DialContext = func(context.Context, string, string) (net.Conn, error) {
				t.Fatal("invalid timing reached the network")
				return nil, nil
			}
			c, err := Dial(context.Background(), options)
			if c != nil || err == nil {
				if c != nil {
					c.Close()
				}
				t.Fatal("invalid keepalive configuration accepted", err)
			}
		})
	}
}
