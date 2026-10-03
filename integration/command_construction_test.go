package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestCephCommandConstructionIntegration(t *testing.T) {
	ordinaryLogFixture(t)
	c, ctx := fixtureClient(t, 30*time.Second)
	config, err := cephmsgr.NewCommand("config get", map[string]any{"who": "client.test", "key": "debug_ms"})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := c.MonCommand(ctx, config); err != nil || len(result.Data) == 0 {
		t.Fatal("built MON command", err)
	}
	iostat, err := cephmsgr.NewCommand("iostat", map[string]any{"width": 80, "print_header": true})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := c.MgrCommand(ctx, iostat); err != nil || !bytes.Contains(result.Data, []byte("Read IOPS")) {
		t.Fatal("built MGR integer/boolean arguments", err)
	}
	pgs, err := cephmsgr.NewCommand("pg dump", map[string]any{"dumpcontents": []string{"pgs"}, "format": "json"})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := c.MgrCommand(ctx, pgs); err != nil || !json.Valid(result.Data) {
		t.Fatal("built MGR array arguments", err)
	}
	key := fmt.Sprintf("ceph-msgr-command-%d", time.Now().UnixNano())
	set, err := cephmsgr.NewCommand("config-key set", map[string]any{"key": key, "val": "Go 명령\n\x00unicode 雪"})
	if err != nil {
		t.Fatal(err)
	}
	// Fixture mutations are sent once. Store a Unicode/NUL string, then replace
	// it using independent binary bulk input; neither uses RADOS object I/O.
	if _, err := c.MonCommand(ctx, set); err != nil {
		t.Fatal("built Unicode config-key set", err)
	}
	get, err := cephmsgr.NewCommand("config-key get", map[string]any{"key": key})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := c.MonCommand(ctx, get); err != nil || !bytes.Equal(result.Data, []byte("Go 명령\n\x00unicode 雪")) {
		t.Fatal("built Unicode config-key value changed", err, result.Data)
	}
	set, err = cephmsgr.NewCommand("config-key set", map[string]any{"key": key})
	if err != nil {
		t.Fatal(err)
	}
	set.Input = []byte{'G', 'o', 0, 0xff, 0x80, '\n'}
	if _, err := c.MonCommand(ctx, set); err != nil {
		t.Fatal("built command with independent binary input", err)
	}
	if result, err := c.MonCommand(ctx, get); err != nil || !bytes.Equal(result.Data, set.Input) {
		t.Fatal("built binary config-key value changed", err, result.Data)
	}
	remove, err := cephmsgr.NewCommand("config-key rm", map[string]any{"key": key})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.MonCommand(ctx, remove); err != nil {
		t.Fatal("built config-key cleanup", err)
	}
	t.Log("Go named arguments executed on MON/MGR; Unicode/NUL and binary command input preserved")
}
