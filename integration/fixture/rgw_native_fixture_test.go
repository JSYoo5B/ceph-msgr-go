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
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(1).WithManagerCount(1).WithOSDCount(1).WithPoolDefaults(1, 1)),
		ceph.WithOSDStorage(ceph.OSDStorage{}.WithObjectStore(ceph.OSDBlueStore)),
		ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithMessengerMode(ceph.MessengerV2Secure)),
		ceph.WithAuth(ceph.AuthConfig{KeyType: ceph.CipherAES256K, ServiceCipher: ceph.CipherAES256K}),
		ceph.WithConfigFile(config), rgw.WithGateways(rgw.Config{SkipUserCreation: true}))
	if cluster != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
			defer stop()
			var owned []testcontainers.Container
			monitors, err := cluster.Monitors(cleanup)
			if err != nil {
				t.Errorf("list owned monitors before cleanup: %v", err)
			}
			for _, monitor := range monitors {
				owned = append(owned, monitor.Container)
			}
			managers, err := cluster.Managers(cleanup)
			if err != nil {
				t.Errorf("list owned managers before cleanup: %v", err)
			}
			for _, manager := range managers {
				owned = append(owned, manager.Container)
			}
			osds, err := cluster.OSDs(cleanup)
			if err != nil {
				t.Errorf("list owned OSDs before cleanup: %v", err)
			}
			for _, osd := range osds {
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
			network := clusterNetworkName(cluster.Container)
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
	gateways, err := rgw.Gateways(ctx, cluster.Container)
	if err != nil {
		t.Fatal("list owned gateways:", err)
	}
	if len(gateways) != 1 || len(rgwNativeOSDs(t, ctx, cluster)) != 1 {
		t.Fatal("checkpoint requires exactly one gateway and OSD")
	}
	return cluster, gateways[0]
}

// rgwNativeOSDs lists the fixture's owned OSD descriptors.
func rgwNativeOSDs(t *testing.T, ctx context.Context, cluster *rgw.Container) []*ceph.OSDContainer {
	t.Helper()
	osds, err := cluster.OSDs(ctx)
	if err != nil {
		t.Fatal("list owned OSDs:", err)
	}
	if len(osds) == 0 {
		t.Fatal("RGW fixture has no owned OSD")
	}
	return osds
}

// rgwNativePoolStates reads the native state, including the pool ID, of every
// pool the cluster has. Pool discovery is MON metadata traffic.
func rgwNativePoolStates(ctx context.Context, cluster *rgw.Container) ([]ceph.PoolState, error) {
	pools, err := observe(cluster.Container).Pools(ctx)
	if err != nil {
		return nil, err
	}
	states := make([]ceph.PoolState, 0, len(pools))
	for _, pool := range pools {
		state, err := pool.Status(ctx)
		if err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	return states, nil
}
