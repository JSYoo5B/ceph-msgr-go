package main

import "testing"

func TestRelayOnlyReachesOwnedNativePeers(t *testing.T) {
	peers := map[string]string{"mon.a": "172.22.0.2:3300", "mgr.a": "172.22.0.3:6800", "mgr.b": "fd00::4", "osd.0": "172.22.0.5:6800", "mds.fs-0": "172.22.0.6:6800"}
	for _, endpoint := range []string{"172.22.0.2:3300", "172.22.0.3:6801", "[fd00::4]:6800", "172.22.0.5:6800", "172.22.0.6:6800", "172.22.0.6:6802"} {
		if !allowed(endpoint, peers) {
			t.Fatal("owned native endpoint refused", endpoint)
		}
	}
	for _, endpoint := range []string{"172.22.0.8:3300", "172.22.0.2:6789", "172.22.0.5:6801", "127.0.0.1:22", "172.22.0.3:0", "hostname:3300", "172.22.0.7:6800", "172.22.0.6:0"} {
		if allowed(endpoint, peers) {
			t.Fatal("unowned/invalid endpoint admitted", endpoint)
		}
	}
}
