// The relay is a development fixture, never a product dependency. It forwards
// opaque TCP bytes to fixture loopback ports; CephX stays end to end.
package main

import (
	"encoding/binary"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

func forward(conn net.Conn, upstream string, osd bool) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	var header [2]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return
	}
	port := binary.BigEndian.Uint16(header[:])
	switch port {
	case 33300, 33301, 33302, 36800, 36801:
	case 36900:
		if !osd {
			return
		}
	default:
		return
	}
	remote, err := net.DialTimeout("tcp", net.JoinHostPort(upstream, strconv.Itoa(int(port))), 5*time.Second)
	if err != nil {
		return
	}
	defer remote.Close()
	if _, err := conn.Write([]byte{0}); err != nil {
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
	upstream := flag.String("upstream", "127.0.0.1", "Ceph loopback address")
	ready := flag.String("ready", "", "optional readiness file")
	osd := flag.Bool("osd", false, "allow the opt-in OSD fixture CLIENT port")
	flag.Parse()
	if ip := net.ParseIP(*upstream); ip == nil || !ip.IsLoopback() {
		log.Fatal("upstream must be loopback")
	}
	l, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	if *ready != "" {
		if err := os.WriteFile(*ready, nil, 0600); err != nil {
			log.Fatal(err)
		}
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go forward(conn, *upstream, *osd)
	}
}
