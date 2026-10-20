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
	mdsFresh    *mdsFreshScope
	mdsCommands *mdsCommandsScope
	mdsSessions *mdsSessionsScope
	mdsCephFS   *mdsCephFSScope
	// mdsCephFSRanks is the multi-rank CephFS acceptance scope.
	mdsCephFSRanks *mdsCephFSRanksScope
	// mdsCephFSCompletion is the CephFS request-completion acceptance scope.
	mdsCephFSCompletion *mdsCephFSCompletionScope
	// mdsCephFSData is the CephFS file data acceptance scope.
	mdsCephFSData *mdsCephFSDataScope
	// rbdBase is the RBD base acceptance scope (owned pool and image).
	rbdBase    *rbdBaseScope
	cachePools *osdCachePoolScope
}

func (c *fixtureCluster) NetworkName() string { return c.cluster.NetworkName() }
func (c *fixtureCluster) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	if c.cancel != nil {
		c.cancel()
		<-c.done
	}
	var errs []error
	errs = append(errs, c.finishMDSFresh(ctx), c.finishRBDBase(ctx), c.finishOSDCachePools(ctx))
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
	if p.mdsFresh == "1" {
		// Only this closed disposable fixture permits removal of its owned
		// CephFS pools. Other profiles retain their existing deletion policy.
		config += "mon_allow_pool_delete = true\n"
	}
	if p.idleSessions == "1" {
		config = "[global]\nmon_tick_interval = 1\nmon_subscribe_interval = 2\nmon_session_timeout = 3\nmon_client_ping_interval = 1\nmon_client_hunt_interval = 1\n"
	}
	if p.osd == "1" {
		hashPool := "false"
		if p.osdPGV8() {
			hashPool = "true"
		}
		config += "osd_pool_default_flag_hashpspool = " + hashPool + "\nosd_crush_update_on_start = false\nosd_beacon_report_interval = 1\n[mon]\npaxos_service_trim_min = 1\nmon_min_osdmap_epochs = 5\nmon_osd_force_trim_to = 5\n"
		if p.osdPGV8() {
			// Keep short ACK controls available for independent native
			// history observation alongside the fixture's preparation work.
			config += "[osd]\nosd_op_history_size = 1024\n"
		}
	}
	return config
}

// shortTicketAuthTTL is the fractional AUTH ticket lifetime of the
// short-ticket profile. Native MonClients renew at expiry minus a quarter of
// the validity and only on their one-second tick (pinned CephxProtocol.cc
// 218-223, MonClient.cc 1025-1107). Under the former 1.5 s TTL with one-second
// MON key rotation they often presented an AUTH ticket whose key the MON had
// already discarded and were refused for good, so the native MGR stopped
// refreshing its rotating keys and refused Go authorizers with -13. 6.5 s keeps
// native MonClients alive while staying fractional for the Go renewal test.
const shortTicketAuthTTL = 6500 * time.Millisecond

// fixtureTicketTTLs returns the AUTH and service ticket lifetimes of a profile.
func fixtureTicketTTLs(p profile) (auth, service time.Duration) {
	service = 12 * time.Second
	if p.idleSessions == "1" || p.authEpoch == "1" {
		service = 120 * time.Second
	}
	auth = service
	if p.monFreeze == "1" {
		// The opt-in freeze can outlast the ordinary 12 s AUTH proof.
		// A native OSD whose MON hunt then fails authentication may stop
		// reconnecting altogether. Keep its reclaim proof valid across the
		// bounded pause; service tickets still expire on the ordinary clock.
		// Dedicated short-ticket authentication below retains its own TTL.
		auth = 120 * time.Second
	}
	if p.osd == "1" && p.osdProfile == "replicated-pg-v8" && p.osdReplicas == "3" &&
		p.osdStore == "bluestore" && p.mdsFresh != "1" && p.shortTickets != "1" && p.testRun == "^TestCephOSDUpmapIntegration$" {
		// This placement-only recipe can spend tens of seconds in native
		// controls. A 12 s ticket leaves only 3 s at renewal, less than one
		// 10 s MON setup attempt. Give both reclaim and OSD service proofs
		// the same window already used by other long-control profiles.
		// Dedicated short/fractional authentication profiles stay unchanged.
		auth = 120 * time.Second
		service = 120 * time.Second
	}
	if p.shortTickets == "1" {
		auth = shortTicketAuthTTL
	}
	return auth, service
}

// fixtureOSDBlockSize returns an explicit sparse-file capacity only for the
// closed soak selection. Both four-minute workloads share the same devices:
// bounded object bytes do not bound BlueStore/RocksDB metadata allocation.
// Zero leaves the producer's default untouched for every other profile.
func fixtureOSDBlockSize(p profile) int64 {
	if p.osd == "1" && p.osdStore == "bluestore" && p.osdReplicas == "3" &&
		p.osdSoak != "" && p.testRun == "^TestCephOSDSoak(Failover)?Integration$" {
		return 4 << 30
	}
	return 0
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
	authTTL, ttl := fixtureTicketTTLs(p)
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
		if size := fixtureOSDBlockSize(p); size != 0 {
			opts = append(opts, ceph.WithOSDBlockSize(size))
		}
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
		host.PortBindings = network.PortMap{network.MustParsePort(relayContainerPort): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: "0"}}}
	})}
	if p.runtime == "host" {
		clientOpts = append(clientOpts, testcontainers.WithExposedPorts(relayContainerPort))
	}
	c.Container, err = testcontainers.Run(ctx, p.image, clientOpts...)
	if err != nil {
		return c, err
	}
	if err = cluster.RefreshClientMonitorConfig(ctx, c.Container); err != nil {
		return c, err
	}
	if p.monPing == "1" {
		if err = c.prepareMonPing(ctx, out); err != nil {
			return c, err
		}
	}
	if p.osd == "1" {
		if err = c.prepareOSD(ctx, out); err != nil {
			return c, err
		}
	}
	if p.mdsFresh == "1" {
		// prepareOSD installs the fixture CRUSH hierarchy first. CephFS then
		// adds its owned pool rules; a later CRUSH overwrite would lose them.
		if err = c.prepareMDSFresh(ctx, out); err != nil {
			return c, err
		}
	}
	if p.mdsCommands == "1" {
		if err = c.prepareMDSCommands(ctx, out); err != nil {
			return c, err
		}
	}
	if p.mdsSessions == "1" {
		if err = c.prepareMDSSessions(ctx, out); err != nil {
			return c, err
		}
	}
	if p.mdsCephFS == "1" {
		if err = c.prepareMDSCephFS(ctx, out); err != nil {
			return c, err
		}
	}
	if p.mdsCephFSRanks == "1" {
		if err = c.prepareMDSCephFSRanks(ctx, out); err != nil {
			return c, err
		}
	}
	if p.mdsCephFSCompletion == "1" {
		if err = c.prepareMDSCephFSCompletion(ctx, out); err != nil {
			return c, err
		}
	}
	if p.mdsCephFSData == "1" {
		if err = c.prepareMDSCephFSData(ctx, out); err != nil {
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
		if err = c.launch(ctx, "sh", "-c", "/out/relay -listen :"+relayListenPort+" -peers /out/peers.json -ready /out/relay.ready > /out/relay.log 2>&1 &"); err != nil {
			return c, err
		}
		if err = waitRelay(ctx, out, relayStartupTimeout); err != nil {
			return c, err
		}
	}
	if p.rbdBase == "1" {
		// After the oracle worker: the base OSD fixture is settled and the
		// native driver's configuration copy exists.
		if err = c.prepareRBDBase(ctx, out); err != nil {
			return c, err
		}
	}
	if p.osdCachePools == "1" {
		if err = c.prepareOSDCachePools(ctx, out); err != nil {
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
