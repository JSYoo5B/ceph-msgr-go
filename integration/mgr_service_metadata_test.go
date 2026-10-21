package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

type mgrServiceMetadataReceipt struct {
	ModuleOwned bool                         `json:"module_enabled_by_test"`
	Restored    bool                         `json:"restored"`
	Snapshots   map[string]map[string]string `json:"snapshots"`
}

func prepareMgrServiceMetadata(t *testing.T, ctx context.Context) *mgrServiceMetadataReceipt {
	t.Helper()
	r := &mgrServiceMetadataReceipt{Snapshots: make(map[string]map[string]string)}
	t.Cleanup(func() {
		data, err := json.MarshalIndent(r, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "mgr-service-metadata.json"), append(data, '\n'))
		}
		if err != nil {
			t.Error("preserve native metadata evidence", err)
		}
	})
	prepared := nativePool(t, ctx, "servicemetadata", map[string]string{"action": "prepare"})
	if !prepared.MetadataReady {
		t.Fatal("native MGR metadata oracle did not become ready")
	}
	r.ModuleOwned = prepared.MetadataModuleOwned
	t.Cleanup(func() {
		if !r.ModuleOwned {
			r.Restored = true
			return
		}
		// Parent cancellation does not cancel restoration. Mutations are
		// submitted once; only readiness/metadata reads may be polled.
		stop, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		r.Restored = nativePool(t, stop, "servicemetadata", map[string]string{"action": "restore"}).MetadataRestored
		if !r.Restored {
			t.Error("native metadata module was not restored")
		}
	})
	return r
}

func (r *mgrServiceMetadataReceipt) observe(t *testing.T, ctx context.Context, phase, service, daemon string, wanted map[string]string) {
	t.Helper()
	native := nativePool(t, ctx, "servicemetadata", struct {
		Action      string            `json:"action"`
		Service     string            `json:"service"`
		Daemon      string            `json:"daemon"`
		Metadata    map[string]string `json:"metadata"`
		WaitSeconds int               `json:"wait_seconds"`
	}{"observe", service, daemon, wanted, 30})
	if !native.ServiceMatched || native.DaemonMetadata == nil {
		t.Fatal("native daemon-state metadata did not match", phase)
	}
	// Upstream adds hostname separately, outside the caller metadata. Omit
	// it from the public receipt even if a fixture supplies one in the future.
	delete(native.DaemonMetadata, "hostname")
	if !reflect.DeepEqual(native.DaemonMetadata, wanted) {
		t.Fatal("native daemon-state metadata differs", phase)
	}
	r.Snapshots[phase] = native.DaemonMetadata
}
