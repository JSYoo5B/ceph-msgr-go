package testcluster

import (
	"encoding/json"
	"net"
	"net/netip"
	"os"
	"strings"
)

// SplitPeer identifies test fault targets from the provider's native topology.
// MGR standbys publish no address: their container IP identifies a later
// active MGR regardless of its dynamically allocated listener port.
func SplitPeer(endpoint string) (string, string, error) {
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		return host, "", err
	}
	var peers map[string]string
	if json.Unmarshal([]byte(os.Getenv("CEPH_MSGR_TEST_PEERS")), &peers) == nil {
		ip, _ := netip.ParseAddr(host)
		for name, address := range peers {
			peerHost, peerPort, e := net.SplitHostPort(address)
			if e != nil {
				peerHost = address
			}
			peerIP, _ := netip.ParseAddr(strings.Trim(peerHost, "[]"))
			if ip.IsValid() && peerIP.IsValid() && ip.Unmap() == peerIP.Unmap() && (strings.HasPrefix(name, "mgr.") || port == peerPort) {
				return host, name, nil
			}
		}
		return host, "", nil
	}
	return host, "", nil

}
func PeerEndpoint(name string) string {
	var peers map[string]string
	_ = json.Unmarshal([]byte(os.Getenv("CEPH_MSGR_TEST_PEERS")), &peers)
	return peers[name]
}
