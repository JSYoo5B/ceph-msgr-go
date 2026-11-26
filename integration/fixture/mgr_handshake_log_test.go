package main

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// The lines are shaped like pinned ProtocolV2.cc and AsyncConnection.cc
// output at debug_ms 5; addresses, pointers and values are synthetic. The
// connection 0x1 goes to the MGR endpoint 192.0.2.1:41000/77 and resets;
// 0x2 goes to another nonce on the same port; 0x3 is an accepted
// connection whose peer is the requested OSD endpoint.
const mgrHandshakeSampleLog = `2026-11-20T00:00:00.000+0000 7f0 0 ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable), process ceph-osd, pid 7
2026-11-20T00:00:01.000+0000 7f0  1 --2- [v2:192.0.2.20:6802/9,v1:192.0.2.20:6803/9] >> v2:192.0.2.1:41000/77 conn(0x1 0x11 unknown :-1 s=START_CONNECT pgs=0 gs=0 cs=0 l=1 c_cookie=0 s_cookie=0 reconnecting=0 rev1=0 crypto rx=0 tx=0 comp rx=0 tx=0).connect
2026-11-20T00:00:01.100+0000 7f0  5 --2- [v2:192.0.2.20:6802/9,v1:192.0.2.20:6803/9] >> v2:192.0.2.1:41000/77 conn(0x1 0x11 crc :-1 s=SESSION_CONNECTING pgs=0 gs=12 cs=0 l=1 c_cookie=0 s_cookie=0 reconnecting=0 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0).send_client_ident sending identification: addrs=[v2:192.0.2.20:6802/9,v1:192.0.2.20:6803/9] target=v2:192.0.2.1:41000/77 gid=0 global_seq=12 features_supported=3f01cfbffffdffff features_required=800000000002201 flags=1 cookie=0
2026-11-20T00:00:01.200+0000 7f0  5 --2- [v2:192.0.2.20:6802/9,v1:192.0.2.20:6803/9] >> v2:192.0.2.1:41000/77 conn(0x1 0x11 crc :-1 s=SESSION_CONNECTING pgs=0 gs=12 cs=0 l=1 c_cookie=0 s_cookie=0 reconnecting=0 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0).handle_server_ident received server identification: addrs=v2:192.0.2.1:41000/77 gid=4242 global_seq=3 features_supported=2a00040010a0a237 features_required=800000000200000 flags=0 cookie=a1b2c3d4
2026-11-20T00:00:01.300+0000 7f0  1 --2- [v2:192.0.2.20:6802/9,v1:192.0.2.20:6803/9] >> v2:192.0.2.1:41000/77 conn(0x1 0x11 crc :-1 s=READY pgs=3 gs=12 cs=0 l=0 c_cookie=0 s_cookie=a1b2c3d4 reconnecting=0 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0).ready entity=mgr.4242 client_cookie=0 server_cookie=a1b2c3d4 in_seq=0 out_seq=0
2026-11-20T00:00:01.400+0000 7f0  2 --2- [v2:192.0.2.20:6802/9,v1:192.0.2.20:6803/9] >> v2:192.0.2.1:41000/77 conn(0x1 0x11 crc :-1 s=READY pgs=3 gs=12 cs=0 l=0 c_cookie=0 s_cookie=a1b2c3d4 reconnecting=0 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0).write_message sending message m=0x77 seq=1 mgropen(osd.0 metadata) v3
2026-11-20T00:00:02.000+0000 7f0  1 --2- [v2:192.0.2.20:6802/9,v1:192.0.2.20:6803/9] >> v2:192.0.2.1:41000/77 conn(0x1 0x11 crc :-1 s=READY pgs=3 gs=12 cs=0 l=0 c_cookie=0 s_cookie=a1b2c3d4 reconnecting=0 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0).handle_read_frame_preamble_main read frame preamble failed r=-1
2026-11-20T00:00:02.001+0000 7f0  1 --2- [v2:192.0.2.20:6802/9,v1:192.0.2.20:6803/9] >> v2:192.0.2.1:41000/77 conn(0x1 0x11 crc :-1 s=READY pgs=3 gs=12 cs=0 l=0 c_cookie=0 s_cookie=a1b2c3d4 reconnecting=0 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0)._fault initiating reconnect
2026-11-20T00:00:02.200+0000 7f0  5 --2- [v2:192.0.2.20:6802/9,v1:192.0.2.20:6803/9] >> v2:192.0.2.1:41000/77 conn(0x1 0x11 crc :-1 s=SESSION_RECONNECTING pgs=3 gs=13 cs=1 l=0 c_cookie=0 s_cookie=a1b2c3d4 reconnecting=0 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0).send_reconnect reconnect to session: client_cookie=0 server_cookie=a1b2c3d4 gs=13 cs=1 ms=0
2026-11-20T00:00:02.300+0000 7f0  1 --2- [v2:192.0.2.20:6802/9,v1:192.0.2.20:6803/9] >> v2:192.0.2.1:41000/77 conn(0x1 0x11 crc :-1 s=SESSION_RECONNECTING pgs=3 gs=13 cs=1 l=0 c_cookie=0 s_cookie=a1b2c3d4 reconnecting=0 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0).handle_session_reset received session reset full=1
2026-11-20T00:00:02.301+0000 7f0  1 --2- [v2:192.0.2.20:6802/9,v1:192.0.2.20:6803/9] >> v2:192.0.2.1:41000/77 conn(0x1 0x11 crc :-1 s=SESSION_RECONNECTING pgs=3 gs=13 cs=1 l=0 c_cookie=0 s_cookie=a1b2c3d4 reconnecting=0 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0).reset_session
2026-11-20T00:00:03.000+0000 7f0  5 --2- [v2:192.0.2.20:6802/9,v1:192.0.2.20:6803/9] >> v2:192.0.2.1:41000/78 conn(0x2 0x21 crc :-1 s=SESSION_CONNECTING pgs=0 gs=14 cs=0 l=1 c_cookie=0 s_cookie=0 reconnecting=0 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0).send_client_ident sending identification: addrs=[v2:192.0.2.20:6802/9,v1:192.0.2.20:6803/9] target=v2:192.0.2.1:41000/78 gid=0 global_seq=14 features_supported=3f01cfbffffdffff features_required=800000000002201 flags=1 cookie=0
2026-11-20T00:00:03.100+0000 7f0  0 -- [v2:192.0.2.20:6802/9,v1:192.0.2.20:6803/9] >> v2:192.0.2.1:41000/77 conn(0x1 msgr2=0x11 crc :-1 s=STATE_CONNECTION_ESTABLISHED l=0)._try_send injecting socket failure
2026-11-20T00:00:04.000+0000 7f0  5 --2- v2:192.0.2.1:41000/77 >> v2:192.0.2.20:6802/9 conn(0x3 0x31 crc :6802 s=SESSION_ACCEPTING pgs=0 gs=0 cs=0 l=1 c_cookie=0 s_cookie=0 reconnecting=0 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0).handle_reconnect received reconnect: client_cookie=0 server_cookie=a1b2c3d4 gs=13 cs=1 ms=0
2026-11-20T00:00:04.001+0000 7f0  0 --2- v2:192.0.2.1:41000/77 >> v2:192.0.2.20:6802/9 conn(0x3 0x31 crc :6802 s=SESSION_ACCEPTING pgs=0 gs=0 cs=0 l=1 c_cookie=0 s_cookie=0 reconnecting=0 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0).handle_reconnect no existing connection exists, reseting client
2026-11-20T00:00:05.000+0000 7f0 0 ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable), process ceph-osd, pid 8
2026-11-20T00:00:06.000+0000 7f0  5 --2- [v2:192.0.2.20:6802/9,v1:192.0.2.20:6803/9] >> v2:192.0.2.1:41000/77 conn(0x1 0x11 crc :-1 s=SESSION_CONNECTING pgs=0 gs=2 cs=0 l=1 c_cookie=0 s_cookie=0 reconnecting=0 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0).send_client_ident sending identification: addrs=[v2:192.0.2.20:6802/9,v1:192.0.2.20:6803/9] target=v2:192.0.2.1:41000/77 gid=0 global_seq=2 features_supported=3f01cfbffffdffff features_required=800000000002201 flags=1 cookie=0
`

func TestMgrHandshakesKeepResetsTimesAndExactEndpoints(t *testing.T) {
	processes, events, err := scanMgrHandshakes(strings.NewReader(mgrHandshakeSampleLog), []string{"v2:192.0.2.1:41000/77", "v2:192.0.2.20:6802/9"}, 1<<20, 64)
	if err != nil {
		t.Fatal(err)
	}
	if len(processes) != 2 || processes[0].PID != "7" || processes[1].PID != "8" || processes[1].Process != 2 || processes[0].Name != "ceph-osd" {
		t.Fatalf("processes %+v", processes)
	}
	var kinds []string
	for _, e := range events {
		kinds = append(kinds, e.Kind)
		if e.Conn == "0x2" {
			t.Fatalf("another nonce on the same port was kept: %+v", e)
		}
	}
	want := []string{"connect", "client_ident_sent", "server_ident_received", "ready", "message_sent", "fault_reconnect", "reconnect_sent", "reset_received", "reset_session", "socket_failure_injected", "reconnect_received", "reset_sent", "client_ident_sent"}
	if !slices.Equal(kinds, want) {
		t.Fatalf("kinds %v, want %v", kinds, want)
	}
	byKind := func(kind string) mgrHandshakeEvent {
		return events[slices.IndexFunc(events, func(e mgrHandshakeEvent) bool { return e.Kind == kind })]
	}
	client, server, ready := byKind("client_ident_sent"), byKind("server_ident_received"), byKind("ready")
	if client.Time != "2026-11-20T00:00:01.100+0000" || client.Process != 1 || client.Endpoint != 0 || client.Target != "v2:192.0.2.1:41000/77" || client.GID != "0" || client.GlobalSeq != "12" || client.Flags != "1" || client.Cookie != "0" || client.Lossy != "1" {
		t.Fatalf("client %+v", client)
	}
	if server.GID != "4242" || server.Flags != "0" || server.Cookie != "k1" || server.Required != "800000000200000" {
		t.Fatalf("server %+v", server)
	}
	if ready.Entity != "mgr.4242" || ready.Lossy != "0" || ready.ServerCookie != "k1" || ready.PeerCookie != "k1" || ready.Cookie != "0" {
		t.Fatalf("ready %+v", ready)
	}
	if sent := byKind("message_sent"); sent.Message != "mgropen" || sent.Seq != "1" {
		t.Fatalf("message %+v", sent)
	}
	if r := byKind("reconnect_sent"); r.Cookie != "0" || r.PeerCookie != "k1" || r.ConnectSeq != "1" || r.GlobalSeq != "13" {
		t.Fatalf("reconnect %+v", r)
	}
	if r := byKind("reset_received"); r.Full != "1" || r.Conn != "0x1" || r.State != "SESSION_RECONNECTING" {
		t.Fatalf("reset %+v", r)
	}
	if r := byKind("socket_failure_injected"); r.Conn != "0x1" || r.Lossy != "0" {
		t.Fatalf("injected %+v", r)
	}
	if r := byKind("reconnect_received"); r.Endpoint != 1 || r.PeerCookie != "k1" {
		t.Fatalf("accepted reconnect %+v", r)
	}
	if r := byKind("reset_sent"); r.Full != "1" || r.Endpoint != 1 {
		t.Fatalf("reset decision %+v", r)
	}
	if last := events[len(events)-1]; last.Process != 2 || last.Conn != "0x1" || last.GlobalSeq != "2" {
		t.Fatalf("second process %+v", last)
	}
	if strings.Contains(strings.Join(kinds, " "), "a1b2c3d4") {
		t.Fatal("raw cookie published")
	}
}

func TestMgrHandshakesBoundsAndEndpoints(t *testing.T) {
	endpoints := []string{"v2:192.0.2.1:41000/77"}
	if _, _, err := scanMgrHandshakes(strings.NewReader(mgrHandshakeSampleLog), endpoints, 100, 64); !errors.Is(err, errContainerLogExceedsBound) {
		t.Fatal("oversized log", err)
	}
	if _, _, err := scanMgrHandshakes(strings.NewReader(mgrHandshakeSampleLog), endpoints, 1<<20, 3); !errors.Is(err, errHandshakeEventsExceedBound) {
		t.Fatal("event bound", err)
	}
	for in, want := range map[string]string{"192.0.2.1:41000/77": "v2:192.0.2.1:41000/77", "[2001:db8::1]:6800/3": "v2:[2001:db8::1]:6800/3"} {
		if got, err := parseHandshakeEndpoint(in); err != nil || got != want {
			t.Fatal(in, got, err)
		}
	}
	for _, bad := range []string{"192.0.2.1:41000", "192.0.2.1/77", "192.0.2.1:41000/x"} {
		if _, err := parseHandshakeEndpoint(bad); err == nil {
			t.Fatal("accepted", bad)
		}
	}
	if addressNames("[v2:192.0.2.1:41000/770,v1:192.0.2.1:41001/77]", "v2:192.0.2.1:41000/77") {
		t.Fatal("prefix of a longer nonce matched")
	}
}
