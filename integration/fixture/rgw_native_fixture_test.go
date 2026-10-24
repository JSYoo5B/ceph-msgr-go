package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// rgwNativeStartFixture starts the dedicated owned RGW fixture shared by the
// native checkpoints and registers its independently verified cleanup. Each
// test gets a new disposable cluster; no cluster is reused across tests.
func rgwNativeStartFixture(t *testing.T, ctx context.Context) (*rgw.Container, *rgw.Gateway) {
	t.Helper()
	config := filepath.Join(t.TempDir(), "rgw-checkpoint.conf")
	if err := os.WriteFile(config, []byte("[global]\nosd_pool_default_pg_num = 1\n[osd]\nosd_op_history_size = 4096\nosd_op_history_duration = 600\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cluster, err := rgw.Run(ctx, rgwNativeImage,
		ceph.WithMonitorCount(1), ceph.WithManagerCount(1), ceph.WithOSDCount(1),
		ceph.WithPoolDefaults(1, 1), ceph.WithOSDObjectStore(ceph.OSDBlueStore),
		ceph.WithMessengerMode(ceph.MessengerV2Secure),
		ceph.WithAuth(ceph.AuthConfig{KeyType: ceph.CipherAES256K, ServiceCipher: ceph.CipherAES256K}),
		ceph.WithConfigFile(config), rgw.WithGateways(rgw.Config{SkipUserCreation: true}))
	if cluster != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
			defer stop()
			var owned []testcontainers.Container
			for _, monitor := range cluster.Monitors() {
				owned = append(owned, monitor.Container)
			}
			for _, manager := range cluster.Managers() {
				owned = append(owned, manager.Container)
			}
			for _, osd := range cluster.OSDs() {
				owned = append(owned, osd.Container)
			}
			owned = append(owned, cluster.ServiceContainers()...)
			volumes := map[string]bool{}
			for _, container := range owned {
				inspected, err := container.Inspect(cleanup)
				if err != nil {
					t.Errorf("inspect owned resources before cleanup: %v", err)
					continue
				}
				for _, mount := range inspected.Mounts {
					if mount.Type == "volume" && mount.Name != "" {
						volumes[mount.Name] = true
					}
				}
			}
			network := cluster.NetworkName()
			if err := cluster.Terminate(cleanup); err != nil {
				t.Errorf("terminate owned RGW fixture: %v", err)
				return
			}
			docker, err := testcontainers.NewDockerClientWithOpts(cleanup)
			if err != nil {
				t.Error("open independent cleanup inspector:", err)
				return
			}
			defer func() { _ = docker.Close() }()
			for _, container := range owned {
				if _, err := docker.ContainerInspect(cleanup, container.GetContainerID(), client.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
					t.Errorf("owned container removal not independently confirmed: %v", err)
				}
			}
			for volume := range volumes {
				if _, err := docker.VolumeInspect(cleanup, volume, client.VolumeInspectOptions{}); !errdefs.IsNotFound(err) {
					t.Errorf("owned volume removal not independently confirmed: %v", err)
				}
			}
			if network != "" {
				if _, err := docker.NetworkInspect(cleanup, network, client.NetworkInspectOptions{}); !errdefs.IsNotFound(err) {
					t.Errorf("owned network removal not independently confirmed: %v", err)
				}
			}
			if !t.Failed() {
				t.Logf("RGW_NATIVE_CLEANUP independent inspection confirms containers=%d volumes=%d network=removed", len(owned), len(volumes))
			}
		})
	}
	if err != nil {
		t.Fatal("start RGW fixture:", err)
	}
	gateways := cluster.Gateways()
	if len(gateways) != 1 || len(cluster.OSDs()) != 1 {
		t.Fatal("checkpoint requires exactly one gateway and OSD")
	}
	return cluster, gateways[0]
}
