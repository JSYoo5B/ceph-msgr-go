package cephmsgr

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestCephHostnameSeedIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_CONTROL_DIR") == "" || os.Getenv("CEPH_MSGR_TEST_PROXY") != "" {
		t.Skip("requires container-local disposable Ceph and the default Go DNS dialer")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	options := integrationOptions(t)
	options.Monitors = []string{"v2:localhost:33300/0"}
	options.DialContext = nil
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal("authenticate hostname seed with the resolved peer address", err)
	}
	defer c.Close()
	for _, target := range []struct {
		call   func(context.Context, Command) (Result, error)
		prefix string
	}{{c.MonCommand, "status"}, {c.MgrCommand, "pg stat"}} {
		encoded, _ := json.Marshal(map[string]string{"prefix": target.prefix, "format": "json"})
		result, err := target.call(ctx, Command{JSON: encoded})
		if err != nil || !json.Valid(result.Data) {
			t.Fatal("command after hostname bootstrap", target.prefix, err)
		}
	}
	c.mu.Lock()
	peer := c.mon.RemoteAddr()
	c.mu.Unlock()
	t.Logf("hostname seed resolved to %s; authenticated MON/MGR commands passed", peer)
}
