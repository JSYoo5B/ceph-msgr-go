package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// The provider owns daemons and topology; the adapter owns only the native
// oracle/test client and the IPC directory. No daemon entrypoint is replaced.
type fixtureCluster struct {
	testcontainers.Container
	cluster     *ceph.Container
	profile     profile
	cancel      context.CancelFunc
	done        chan struct{}
	mu          sync.Mutex
	controlErr  error
	pauses      map[string]*ceph.ContainerPause
	xattrLimits *xattrLimitsScope
}

func (c *fixtureCluster) NetworkName() string { return c.cluster.NetworkName() }
func (c *fixtureCluster) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	if c.cancel != nil {
		c.cancel()
		<-c.done
	}
	var errs []error
	// Restore paused containers before requesting their orderly cleanup.
	for _, pause := range c.pauses {
		errs = append(errs, pause.Resume(ctx))
	}
	if c.xattrLimits != nil {
		errs = append(errs, c.xattrLimits.Restore(ctx))
	}
	if c.Container != nil {
		errs = append(errs, c.Container.Terminate(ctx, opts...))
	}
	if c.cluster != nil {
		errs = append(errs, c.cluster.Terminate(ctx))
	}
	return errors.Join(errs...)
}
func (c *fixtureCluster) checkControl() error { c.mu.Lock(); defer c.mu.Unlock(); return c.controlErr }

// fixtureBootstrapConfig is passed through WithConfigFile before ceph.Run.
// Pool defaults must reach MGR's automatic .mgr creation as well as explicit
// data pools; changing .mgr after bootstrap is too late for initial admission.
func fixtureBootstrapConfig(p profile) string {
	tick := 5
	if p.shortTickets == "1" {
		tick = 1
	}
	config := fmt.Sprintf("[global]\nmon_tick_interval = %d\nmon_subscribe_interval = 86400\n", tick)
	if p.idleSessions == "1" {
		config = "[global]\nmon_tick_interval = 1\nmon_subscribe_interval = 2\nmon_session_timeout = 3\nmon_client_ping_interval = 1\nmon_client_hunt_interval = 1\n"
	}
	if p.osd == "1" {
		config += "osd_pool_default_flag_hashpspool = false\nosd_crush_update_on_start = false\nosd_beacon_report_interval = 1\n[mon]\npaxos_service_trim_min = 1\nmon_min_osdmap_epochs = 5\nmon_osd_force_trim_to = 5\n"
	}
	return config
}

func startCluster(ctx context.Context, p profile, out string) (*fixtureCluster, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out, err := filepath.Abs(out)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(out)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New("fixture output must be a directory")
	}
	ttl := 12 * time.Second
	if p.idleSessions == "1" || p.authEpoch == "1" {
		ttl = 120 * time.Second
	}
	authTTL := ttl
	if p.shortTickets == "1" {
		authTTL = 1500 * time.Millisecond
	}
	cfgPath := filepath.Join(out, "bootstrap.conf")
	if err = os.WriteFile(cfgPath, []byte(fixtureBootstrapConfig(p)), 0600); err != nil {
		return nil, err
	}
	mode := ceph.MessengerSecure
	if p.connectionMode == "crc" {
		mode = ceph.MessengerCRC
	}
	monMode, mgrMode := mode, mode
	if p.modeRejection == "mon" {
		monMode = ceph.MessengerCRC
	}
	if p.modeRejection == "mgr" {
		mgrMode = ceph.MessengerCRC
	}
	policy := ceph.MessengerPolicy{Monitors: []ceph.MessengerConnectionMode{monMode}, Managers: []ceph.MessengerConnectionMode{mgrMode}, OSDs: []ceph.MessengerConnectionMode{mode}}
	opts := []testcontainers.ContainerCustomizer{ceph.WithMonitorCount(3), ceph.WithStartupTimeout(fixtureStartupTimeout), ceph.WithMessengerMode(ceph.MessengerV2Only), ceph.WithMessengerPolicy(policy), ceph.WithAuth(ceph.AuthConfig{KeyType: ceph.CephxCipher(p.keyType), ServiceCipher: ceph.CephxCipher(p.serviceCipher), MonTicketTTL: authTTL, ServiceTicketTTL: ttl}), ceph.WithConfigFile(cfgPath)}
	if p.mgrCount == "0" {
		opts = append(opts, ceph.WithNoInitialManagers())
	} else {
		opts = append(opts, ceph.WithManagerCount(2))
	}
	if p.mappedIPv6 == "1" {
		opts = append(opts, ceph.WithAddressFamily(ceph.AddressIPv4MappedIPv6))
	} else if p.ipFamily == "6" {
		opts = append(opts, ceph.WithAddressFamily(ceph.AddressIPv6))
	}
	if p.osd == "1" {
		count, _ := strconv.Atoi(p.osdReplicas)
		minimum := 1
		if count == 3 {
			minimum = 2
		}
		opts = append(opts, ceph.WithOSDCount(count), ceph.WithOSDObjectStore(ceph.OSDObjectStore(p.osdStore)), ceph.WithPoolDefaults(count, minimum))
	} else {
		opts = append(opts, ceph.WithNoInitialOSDs())
	}
	cluster, err := ceph.Run(ctx, p.image, opts...)
	if cluster == nil {
		return nil, err
	}
	c := &fixtureCluster{cluster: cluster, profile: p, pauses: make(map[string]*ceph.ContainerPause)}
	if err != nil {
		return c, err
	}
	if err = c.waitReady(ctx, p.mgrCount == "2"); err != nil {
		return c, err
	}
	addresses, err := cluster.DaemonAddresses(ctx)
	if err != nil {
		return c, err
	}
	status, err := cluster.Status(ctx)
	if err != nil {
		return c, err
	}
	p.fsid = status.FSID
	peers := make(map[string]string)
	var monitors []string
	for _, mon := range addresses.Monitors {
		for _, addr := range mon.Addrs {
			if addr.Type == "v2" {
				peers["mon."+mon.Name] = addr.Addr
				monitors = append(monitors, addr.Addr)
			}
		}
	}
	for _, mgr := range cluster.Managers() {
		ip, e := mgr.ContainerIP(ctx)
		if p.ipFamily == "6" && p.mappedIPv6 != "1" {
			inspected, inspectErr := mgr.Inspect(ctx)
			if inspectErr != nil {
				return c, inspectErr
			}
			if inspected.NetworkSettings == nil || inspected.NetworkSettings.Networks[cluster.NetworkName()] == nil {
				return c, errors.New("MGR has no fixture network endpoint")
			}
			ip = inspected.NetworkSettings.Networks[cluster.NetworkName()].GlobalIPv6Address.String()
		}
		if e != nil || ip == "" {
			return c, errors.New("MGR has no selected native IP address")
		}
		peers["mgr."+mgr.DaemonName] = ip
	}
	if addresses.ActiveManager != nil {
		for _, addr := range addresses.ActiveManager.Addrs {
			if addr.Type == "v2" {
				peers["mgr."+addresses.ActiveManager.Name] = addr.Addr
			}
		}
	}
	for _, osd := range addresses.OSDs {
		for _, addr := range osd.Public {
			if addr.Type == "v2" {
				peers[fmt.Sprintf("osd.%d", osd.ID)] = addr.Addr
			}
		}
	}
	encoded, _ := json.Marshal(peers)
	p.peers = string(encoded)
	p.monAddresses = strings.Join(monitors, ",")
	c.profile = p
	if len(monitors) != 3 {
		return c, errors.New("native map must advertise three v2 MONs")
	}
	if err = os.WriteFile(filepath.Join(out, "fsid"), []byte(p.fsid), 0600); err != nil {
		return c, err
	}
	if err = os.WriteFile(filepath.Join(out, "peers.json"), encoded, 0600); err != nil {
		return c, err
	}
	nativeMonitors, err := cluster.MonitorBootstrapAddresses(ctx)
	if err != nil {
		return c, err
	}
	// Consumer identities and native text-keyring oracle inputs stay private.
	all := []struct {
		name, file string
		caps       ceph.ClientCaps
	}{
		{"test", "admin", ceph.ClientCaps{Mon: "allow *", MGR: "allow *", OSD: "allow *"}},
		{"readonly", "readonly", ceph.ClientCaps{Mon: "allow r", MGR: "allow r", OSD: "allow r"}},
		{"revocable", "revocable", ceph.ClientCaps{Mon: "allow *", MGR: "allow *"}},
		{"configwatch", "configwatch", ceph.ClientCaps{Mon: `allow command "fsid"`, MGR: "allow r"}},
	}
	var clients []byte
	for _, entry := range all {
		client, e := cluster.CreateClient(ctx, entry.name, entry.caps)
		if e != nil {
			return c, e
		}
		_, keyring, e := client.ConnectionConfig()
		if e != nil {
			return c, e
		}
		if e = os.WriteFile(filepath.Join(out, entry.file+".keyring"), keyring, 0600); e != nil {
			return c, e
		}
		for _, line := range strings.Split(string(keyring), "\n") {
			k, v, ok := strings.Cut(line, "=")
			if ok && strings.TrimSpace(k) == "key" {
				name := entry.file + ".key"
				if entry.name == "test" {
					name = "key"
				}
				if e = os.WriteFile(filepath.Join(out, name), []byte(strings.TrimSpace(v)+"\n"), 0600); e != nil {
					return c, e
				}
			}
		}
		if entry.name == "test" || entry.name == "readonly" {
			clients = append(clients, keyring...)
		}
	}
	if err = os.WriteFile(filepath.Join(out, "clients.keyring"), clients, 0600); err != nil {
		return c, err
	}
	env := p.containerEnvironment()
	env["CEPH_MSGR_NATIVE_MONITORS"] = nativeMonitors
	clientOpts := []testcontainers.ContainerCustomizer{cluster.WithClient(ceph.WithClientMessengerPolicy(ceph.ClientMessengerPolicy{MonitorsAndManagers: []ceph.MessengerConnectionMode{ceph.MessengerSecure, ceph.MessengerCRC}, OtherDaemons: []ceph.MessengerConnectionMode{ceph.MessengerSecure, ceph.MessengerCRC}})), ceph.WithIdleEntrypoint(), testcontainers.WithEnv(env), testcontainers.WithLabels(map[string]string{"ceph-msgr-go.integration": "true"}), testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
		monHost, _, _ := net.SplitHostPort(peers["mon.a"])
		ip, _ := netip.ParseAddr(monHost)
		host.ExtraHosts = append(host.ExtraHosts, "fixture-mon-a:"+ip.Unmap().String())
		host.Mounts = append(host.Mounts, mount.Mount{Type: mount.TypeBind, Source: out, Target: "/out"})
		host.PortBindings = network.PortMap{network.MustParsePort("40000/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: "0"}}}
	})}
	if p.runtime == "host" {
		clientOpts = append(clientOpts, testcontainers.WithExposedPorts("40000/tcp"))
	}
	c.Container, err = testcontainers.Run(ctx, p.image, clientOpts...)
	if err != nil {
		return c, err
	}
	if err = cluster.RefreshClientMonitorConfig(ctx, c.Container); err != nil {
		return c, err
	}
	if p.osd == "1" {
		if err = c.prepareOSD(ctx, out); err != nil {
			return c, err
		}
	}
	if err = c.launch(ctx, "sh", "-c", "sh /out/cluster.sh > /out/native-oracles.log 2>&1 &"); err != nil {
		return c, err
	}
	if err = wait.ForExec([]string{"test", "-f", "/out/ready"}).WithStartupTimeout(fixtureStartupTimeout).WaitUntilReady(ctx, c.Container); err != nil {
		return c, fmt.Errorf("native oracle startup: %w", err)
	}
	if p.runtime == "host" {
		if err = c.launch(ctx, "sh", "-c", "/out/relay -peers /out/peers.json -ready /out/relay.ready > /out/relay.log 2>&1 &"); err != nil {
			return c, err
		}
		if err = wait.ForExec([]string{"test", "-f", "/out/relay.ready"}).WaitUntilReady(ctx, c.Container); err != nil {
			return c, err
		}
	}
	controlCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.done = make(chan struct{})
	go c.controlLoop(controlCtx, out)
	return c, nil
}
func (c *fixtureCluster) exec(ctx context.Context, args ...string) error {
	result, err := c.cluster.RunNativeCommand(ctx, c.Container, ceph.NativeCommand{Argv: args, Timeout: fixtureStartupTimeout, MaxStdout: 16 << 20, MaxStderr: 64 << 10, Label: "fixture native recipe"})
	if err != nil {
		return err
	}
	if !result.Complete() || result.ExitCode != 0 {
		return fmt.Errorf("fixture native recipe exit=%d complete=%t", result.ExitCode, result.Complete())
	}
	return nil
}
func (c *fixtureCluster) waitReady(ctx context.Context, managers bool) error {
	deadline, cancel := context.WithTimeout(ctx, fixtureStartupTimeout)
	defer cancel()
	for {
		q, err := c.cluster.QuorumStatus(deadline)
		ready := err == nil && len(q.QuorumNames) == 3
		if managers {
			m, e := c.cluster.ManagerStatus(deadline)
			ready = ready && e == nil && m.Available && len(m.Standbys) == 1
		}
		if ready {
			return nil
		}
		select {
		case <-deadline.Done():
			return deadline.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// Persistent test services belong to the owned client container. A supervised
// RunNativeCommand deliberately kills descendants at completion, so it cannot
// launch the oracle worker or relay. Container termination bounds their life.
func (c *fixtureCluster) launch(ctx context.Context, args ...string) error {
	code, output, err := c.Container.Exec(ctx, args)
	if output != nil {
		_, readErr := io.Copy(io.Discard, output)
		err = errors.Join(err, readErr)
	}
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("fixture worker launch exit=%d", code)
	}
	return nil
}
