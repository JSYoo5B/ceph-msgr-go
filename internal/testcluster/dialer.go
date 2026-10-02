// Package testcluster contains development-only helpers for the disposable Ceph fixture.
package testcluster

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"time"
)

// Logical peer addresses keep MON selection and handshake checks meaningful
// when a host-native test connects through the development TCP relay.
type fixtureProxyConn struct {
	net.Conn
	peer *net.TCPAddr
}

func (c *fixtureProxyConn) RemoteAddr() net.Addr { return c.peer }

// Dialer connects directly or through the host fixture relay.
func Dialer() func(context.Context, string, string) (net.Conn, error) {
	dialer := &net.Dialer{}
	proxy := os.Getenv("CEPH_MSGR_TEST_PROXY")
	return func(ctx context.Context, network, endpoint string) (_ net.Conn, err error) {
		if proxy == "" {
			return dialer.DialContext(ctx, network, endpoint)
		}
		host, port, err := net.SplitHostPort(endpoint)
		ip := net.ParseIP(host)
		if err != nil || ip == nil || !ip.IsLoopback() {
			return nil, errors.New("fixture proxy requires a loopback endpoint")
		}
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			return nil, err
		}
		conn, err := dialer.DialContext(ctx, "tcp", proxy)
		if err != nil {
			return nil, err
		}
		stop := context.AfterFunc(ctx, func() { conn.Close() })
		defer func() {
			stop()
			if err != nil {
				conn.Close()
			}
		}()
		deadline := time.Now().Add(10 * time.Second)
		if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
		if err = conn.SetDeadline(deadline); err != nil {
			return nil, err
		}
		var header [2]byte
		binary.BigEndian.PutUint16(header[:], uint16(n))
		if _, err = conn.Write(header[:]); err != nil {
			return nil, err
		}
		var status [1]byte
		if _, err = io.ReadFull(conn, status[:]); err != nil {
			return nil, err
		}
		if status[0] != 0 {
			return nil, errors.New("fixture proxy rejected connection")
		}
		if !stop() || ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err = conn.SetDeadline(time.Time{}); err != nil {
			return nil, err
		}
		return &fixtureProxyConn{Conn: conn, peer: &net.TCPAddr{IP: ip, Port: int(n)}}, nil
	}
}
