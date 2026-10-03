package cephmsgr

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

func TestConnectionModeDefaultAndWireSelection(t *testing.T) {
	if (Options{}).ConnectionMode != SecureMode || SecureMode.sessionMode() != session.SecureMode || CRCMode.sessionMode() != session.CRCMode {
		t.Fatal("connection mode defaults or wire selections changed")
	}
}

func TestDialRejectsInvalidConnectionModeBeforeDialing(t *testing.T) {
	for _, mode := range []ConnectionMode{2, 255} {
		called := false
		options := Options{Monitors: []string{"192.0.2.1:3300"}, ConnectionMode: mode, DialContext: func(context.Context, string, string) (net.Conn, error) {
			called = true
			return nil, nil
		}}
		c, err := Dial(context.Background(), options)
		if c != nil || err == nil || !strings.Contains(err.Error(), "invalid connection mode") || called {
			t.Fatal("invalid mode reached the network", mode, c, err, called)
		}
	}
}
