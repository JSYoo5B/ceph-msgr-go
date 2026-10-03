package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

const configFixtureOption = "client_mount_timeout"
const configFixtureRaw = "ceph_msgr_config_fixture_raw"

type configSlotContext struct {
	context.Context
	checks           atomic.Uint32
	entered, release chan struct{}
	once             sync.Once
}

func (ctx *configSlotContext) Err() error {
	// A warmed MON command reaches Session.Call's first Err check after the
	// public API owns its sole command slot, before anything is queued to TCP.
	if ctx.checks.Add(1) == 3 {
		close(ctx.entered)
		<-ctx.release
	}
	return ctx.Context.Err()
}

func (ctx *configSlotContext) unblock() { ctx.once.Do(func() { close(ctx.release) }) }

func configFixtureSubset(config map[string]string) map[string]string {
	selected := make(map[string]string)
	for _, key := range []string{configFixtureOption, configFixtureRaw} {
		if value, ok := config[key]; ok {
			selected[key] = value
		}
	}
	return selected
}

func waitEffectiveConfig(t *testing.T, ctx context.Context, stream *cephmsgr.ConfigStream, want map[string]string) map[string]string {
	t.Helper()
	for {
		config, err := stream.Next(ctx)
		if err != nil {
			t.Fatal("receive expected effective config", err)
		}
		if reflect.DeepEqual(configFixtureSubset(config), want) {
			return config
		}
	}
}

func drainConfigUpdates(t *testing.T, ctx context.Context, stream *cephmsgr.ConfigStream) {
	t.Helper()
	quiet, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	for {
		if _, err := stream.Next(quiet); errors.Is(err, context.DeadlineExceeded) {
			return
		} else if err != nil {
			t.Fatal("drain redundant registration snapshots", err)
		}
	}
}

func verifyNativeConfig(t *testing.T, ctx context.Context, control, identity, label string, got map[string]string) {
	t.Helper()
	probe, stop := context.WithTimeout(ctx, 15*time.Second)
	err := testcluster.ControlDaemon(probe, control, "verify", "config", identity+" "+label)
	stop()
	if err != nil {
		t.Fatal("fresh bounded native CLI config oracle", label, err)
	}
	data, err := os.ReadFile(filepath.Join(control, "config-oracle.json"))
	var oracle struct {
		RequestID  uint64            `json:"request_id"`
		Identity   string            `json:"identity"`
		Label      string            `json:"label"`
		Values     map[string]string `json:"values"`
		MapSHA256  string            `json:"map_sha256"`
		MapEntries int               `json:"map_entries"`
	}
	if err != nil || len(data) > 32<<10 || json.Unmarshal(data, &oracle) != nil || oracle.RequestID == 0 || oracle.Identity != identity || oracle.Label != label || oracle.MapEntries != len(got) || oracle.MapSHA256 != configMapFingerprint(got) || !reflect.DeepEqual(oracle.Values, configFixtureSubset(got)) {
		t.Fatal("full effective config differs from fresh native CLI", identity, label, err)
	}
}

func configMapFingerprint(config map[string]string) string {
	keys := make([]string, 0, len(config))
	for key := range config {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	hash := sha256.New()
	var length [8]byte
	for _, key := range keys {
		for _, raw := range []string{key, config[key]} {
			binary.BigEndian.PutUint64(length[:], uint64(len(raw)))
			hash.Write(length[:])
			hash.Write([]byte(raw))
		}
	}
	return hex.EncodeToString(hash.Sum(nil))
}

type configFixtureSetting struct {
	who, key, value string
	present         bool
}

func captureConfigFixtureSettings(t *testing.T, ctx context.Context, admin *cephmsgr.Client) []configFixtureSetting {
	t.Helper()
	result, err := admin.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"config dump","format":"json"}`)})
	var entries []struct{ Section, Name, Value, Mask string }
	if err != nil || result.Code != 0 || json.Unmarshal(result.Data, &entries) != nil {
		t.Fatal("capture original controlled config settings", result.Code, err)
	}
	saved := []configFixtureSetting{{who: "global", key: configFixtureOption}, {who: "client", key: configFixtureOption}, {who: "client.test", key: configFixtureOption}, {who: "client.test", key: configFixtureRaw}}
	for _, entry := range entries {
		for i := range saved {
			if entry.Section == saved[i].who && entry.Name == saved[i].key && entry.Mask == "" {
				if saved[i].present {
					t.Fatal("duplicate original controlled config setting")
				}
				saved[i].value, saved[i].present = entry.Value, true
			}
		}
	}
	return saved
}

func restoreConfigFixtureSettings(t *testing.T, saved []configFixtureSetting) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := cephmsgr.Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Error("authenticate fresh controlled-config cleanup", err)
		return
	}
	defer admin.Close()
	for _, entry := range saved {
		command := map[string]any{"prefix": "config rm", "who": entry.who, "name": entry.key}
		if entry.present {
			command["prefix"], command["value"], command["force"] = "config set", entry.value, true
		}
		encoded, _ := json.Marshal(command)
		result, err := admin.MonCommand(ctx, cephmsgr.Command{JSON: encoded})
		if err != nil || result.Code != 0 {
			t.Errorf("restore controlled %s/%s once: code=%d error=%v", entry.who, entry.key, result.Code, err)
		}
	}
}

func configMutationOnce(t *testing.T, ctx context.Context, admin *cephmsgr.Client, command map[string]any) {
	t.Helper()
	encoded, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	// Mutations are submitted once, including cleanup. An uncertain result
	// fails the test; no command is re-executed in a fresh session.
	result, err := admin.MonCommand(ctx, cephmsgr.Command{JSON: encoded})
	if err != nil || result.Code != 0 {
		t.Fatal("once-only controlled config mutation", result.Code, err)
	}
}
