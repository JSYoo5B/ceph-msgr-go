// Development-only relay for host tests; CephX remains end to end.
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"net/netip"
	"os"
	"time"
)

func allowed(endpoint string, peers map[string]string) bool {
	target, err := netip.ParseAddrPort(endpoint)
	if err != nil || target.Port() == 0 {
		return false
	}
	for name, peer := range peers {
		address, e := netip.ParseAddrPort(peer)
		if e == nil && address == target {
			return true
		}
		if len(name) >= 4 && name[:4] == "mgr." {
			ip, e := netip.ParseAddr(peer)
			if e != nil && address.IsValid() {
				ip = address.Addr()
				e = nil
			}
			if e == nil && ip.Unmap() == target.Addr().Unmap() {
				return true
			}
		}
	}
	return false
}
func forward(conn net.Conn, peers map[string]string) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	var header [2]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return
	}
	size := binary.BigEndian.Uint16(header[:])
	if size == 0 || size > 256 {
		return
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(conn, data); err != nil {
		return
	}
	endpoint := string(data)
	if !allowed(endpoint, peers) {
		return
	}
	remote, err := net.DialTimeout("tcp", endpoint, 5*time.Second)
	if err != nil {
		return
	}
	defer remote.Close()
	if _, err = conn.Write([]byte{0}); err != nil {
		return
	}
	conn.SetDeadline(time.Time{})
	done := make(chan struct{}, 2)
	go func() { io.Copy(remote, conn); done <- struct{}{} }()
	go func() { io.Copy(conn, remote); done <- struct{}{} }()
	<-done
	conn.Close()
	remote.Close()
	<-done
}
func main() {
	listen := flag.String("listen", ":40000", "fixture listener")
	peersFile := flag.String("peers", "", "native daemon endpoint allowlist")
	ready := flag.String("ready", "", "readiness file")
	flag.Parse()
	data, err := os.ReadFile(*peersFile)
	if err != nil {
		log.Fatal(err)
	}
	var peers map[string]string
	if err = json.Unmarshal(data, &peers); err != nil || len(peers) == 0 {
		log.Fatal("invalid native daemon allowlist")
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	if *ready != "" {
		if err = os.WriteFile(*ready, nil, 0600); err != nil {
			log.Fatal(err)
		}
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go forward(conn, peers)
	}
}
