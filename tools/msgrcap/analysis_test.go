package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// These tests use synthetic captures only. Frames are produced by the
// product's internal/msgr writer, so they check that the separate capture
// walker, reassembly and passive derivation agree with that writer; they are
// not evidence of native interoperability (see CAPTURE_ANALYSIS.md).

type pcapBuilder struct {
	buf     bytes.Buffer
	t       time.Time
	snaplen uint32
}

func newPcap(snaplen uint32) *pcapBuilder {
	b := &pcapBuilder{t: time.Unix(1_700_000_000, 0), snaplen: snaplen}
	h := make([]byte, 24)
	binary.LittleEndian.PutUint32(h[0:], 0xa1b2c3d4)
	binary.LittleEndian.PutUint16(h[4:], 2)
	binary.LittleEndian.PutUint16(h[6:], 4)
	binary.LittleEndian.PutUint32(h[16:], snaplen)
	binary.LittleEndian.PutUint32(h[20:], linkEthernet)
	b.buf.Write(h)
	return b
}

func (b *pcapBuilder) tcp(src, dst netip.AddrPort, seq, ack uint32, flags uint8, payload []byte) {
	ip := make([]byte, 20+20+len(payload))
	ip[0] = 0x45
	binary.BigEndian.PutUint16(ip[2:], uint16(len(ip)))
	ip[8], ip[9] = 64, 6
	s4, d4 := src.Addr().As4(), dst.Addr().As4()
	copy(ip[12:], s4[:])
	copy(ip[16:], d4[:])
	t := ip[20:]
	binary.BigEndian.PutUint16(t[0:], src.Port())
	binary.BigEndian.PutUint16(t[2:], dst.Port())
	binary.BigEndian.PutUint32(t[4:], seq)
	binary.BigEndian.PutUint32(t[8:], ack)
	t[12], t[13] = 5<<4, flags
	copy(t[20:], payload)
	frame := append(make([]byte, 12), 0x08, 0x00)
	frame = append(frame, ip...)
	caplen := uint32(len(frame))
	if caplen > b.snaplen {
		caplen = b.snaplen
	}
	b.t = b.t.Add(time.Millisecond)
	h := make([]byte, 16)
	binary.LittleEndian.PutUint32(h[0:], uint32(b.t.Unix()))
	binary.LittleEndian.PutUint32(h[4:], uint32(b.t.Nanosecond()/1000))
	binary.LittleEndian.PutUint32(h[8:], caplen)
	binary.LittleEndian.PutUint32(h[12:], uint32(len(frame)))
	b.buf.Write(h)
	b.buf.Write(frame[:caplen])
}

var (
	cliAddr = netip.MustParseAddrPort("10.0.0.1:40000")
	monAddr = netip.MustParseAddrPort("10.0.0.2:3300")
)

// conversation writes a TCP connection carrying the two byte streams, split
// into small, interleaved segments with one retransmission.
func conversation(b *pcapBuilder, client, server netip.AddrPort, c2s, s2c []byte, chunk int, close bool) {
	const ci, si = 1000, 5000
	b.tcp(client, server, ci, 0, tcpSYN, nil)
	b.tcp(server, client, si, ci+1, tcpSYN|tcpACK, nil)
	b.tcp(client, server, ci+1, si+1, tcpACK, nil)
	var co, so int
	for co < len(c2s) || so < len(s2c) {
		if co < len(c2s) {
			n := min(chunk, len(c2s)-co)
			b.tcp(client, server, uint32(ci+1+co), uint32(si+1+so), tcpACK, c2s[co:co+n])
			if co == 0 {
				b.tcp(client, server, uint32(ci+1+co), uint32(si+1+so), tcpACK, c2s[co:co+n]) // retransmission
			}
			co += n
		}
		if so < len(s2c) {
			n := min(chunk+3, len(s2c)-so)
			b.tcp(server, client, uint32(si+1+so), uint32(ci+1+co), tcpACK, s2c[so:so+n])
			so += n
		}
	}
	if close {
		b.tcp(server, client, uint32(si+1+so), uint32(ci+1+co), tcpFIN|tcpACK, nil)
		b.tcp(client, server, uint32(ci+1+co), uint32(si+2+so), tcpFIN|tcpACK, nil)
	}
}

func encodedKey(kind uint16, secret []byte) string {
	e := wire.Encoder{}
	e.U16(kind)
	e.U32(1)
	e.U32(0)
	e.U16(uint16(len(secret)))
	e.Raw(secret)
	return base64.StdEncoding.EncodeToString(e.Data)
}

func mustKey(t *testing.T, kind uint16, fill byte, n int) (cephx.Key, []byte) {
	t.Helper()
	secret := bytes.Repeat([]byte{fill}, n)
	k, err := cephx.ParseKey(encodedKey(kind, secret))
	if err != nil {
		t.Fatal(err)
	}
	e := wire.Encoder{}
	e.U16(kind)
	e.U32(1)
	e.U32(0)
	e.U16(uint16(len(secret)))
	e.Raw(secret)
	return k, e.Data
}

// authDone builds a MON CephX AUTH_DONE payload: one AUTH ticket encrypted
// with the client key and a connection secret sealed with the session key.
func authDone(t *testing.T, client cephx.Key, sessionEncoded []byte, session cephx.Key, secret []byte) []byte {
	t.Helper()
	ticket := wire.Encoder{}
	ticket.U8(1)
	ticket.Raw(sessionEncoded)
	ticket.U32(3600)
	ticket.U32(0)
	sealed, err := client.Seal(4, ticket.Data)
	if err != nil {
		t.Fatal(err)
	}
	blob := wire.Encoder{}
	blob.U8(1)
	blob.U64(7)
	blob.Bytes([]byte("opaque-ticket"))
	e := wire.Encoder{}
	e.U16(0x100)
	e.U32(0)
	e.U8(1)
	e.U32(1)
	e.U32(0x20) // CEPH_ENTITY_TYPE_AUTH
	e.U8(1)
	e.Bytes(sealed)
	e.U8(0)
	e.Bytes(blob.Data)
	inner := wire.Encoder{}
	inner.Bytes(secret)
	conn, err := session.Seal(3, inner.Data)
	if err != nil {
		t.Fatal(err)
	}
	ce := wire.Encoder{}
	ce.Bytes(conn)
	e.Bytes(ce.Data)
	e.Bytes(nil)
	return e.Data
}

func banner() []byte {
	return msgr.Banner{Supported: msgr.Revision1 | msgr.Compression, Required: msgr.Revision1}.Encode()
}

func frame(t *testing.T, w *msgr.Writer, tag msgr.Tag, seg []byte) {
	t.Helper()
	if err := w.Write(msgr.Frame{Tag: tag, Segments: [][]byte{seg}}); err != nil {
		t.Fatal(err)
	}
}

// monStreams produces a MON session in the given mode, with one MESSAGE in
// each direction after SERVER_IDENT.
func monStreams(t *testing.T, mode uint32, payload, secret []byte) ([]byte, []byte) {
	var c2s, s2c bytes.Buffer
	c2s.Write(banner())
	s2c.Write(banner())
	cw, sw := msgr.NewWriter(&c2s, 0), msgr.NewWriter(&s2c, 0)
	frame(t, cw, msgr.Hello, []byte{8, 0})
	frame(t, sw, msgr.Hello, []byte{1, 0})
	req := wire.Encoder{}
	req.U32(2)
	req.U32(1)
	req.U32(mode)
	req.Bytes([]byte("initial"))
	frame(t, cw, msgr.AuthRequest, req.Data)
	frame(t, sw, msgr.AuthReplyMore, []byte("challenge"))
	frame(t, cw, msgr.AuthRequestMore, []byte("proof"))
	done := wire.Encoder{}
	done.U64(4242)
	done.U32(mode)
	done.Bytes(payload)
	frame(t, sw, msgr.AuthDone, done.Data)
	if mode == modeSecure {
		if err := cw.EnableSecure(secret[:16], secret[28:40]); err != nil {
			t.Fatal(err)
		}
		if err := sw.EnableSecure(secret[:16], secret[16:28]); err != nil {
			t.Fatal(err)
		}
	}
	frame(t, cw, msgr.AuthSignature, bytes.Repeat([]byte{1}, 32))
	frame(t, sw, msgr.AuthSignature, bytes.Repeat([]byte{2}, 32))
	frame(t, cw, msgr.ClientIdent, bytes.Repeat([]byte{3}, 70))
	frame(t, sw, msgr.ServerIdent, bytes.Repeat([]byte{4}, 70))
	m := msgr.MessageData{Sequence: 1, Transaction: 9, Type: msgr.MonCommandMessage, Version: 1, Front: []byte("front")}
	if err := cw.Write(m.Frame()); err != nil {
		t.Fatal(err)
	}
	m = msgr.MessageData{Sequence: 1, Transaction: 9, AckSequence: 1, Type: msgr.MonCommandReplyMessage, Version: 1, Front: bytes.Repeat([]byte{5}, 100), Data: []byte("{}")}
	if err := sw.Write(m.Frame()); err != nil {
		t.Fatal(err)
	}
	return c2s.Bytes(), s2c.Bytes()
}

func tags(frames []frameInfo) string {
	var s []string
	for _, f := range frames {
		s = append(s, f.Tag)
	}
	return strings.Join(s, ",")
}

func fixture(t *testing.T) (cephx.Key, []byte, []byte) {
	client, _ := mustKey(t, cephx.AES256K, 0x11, 32)
	session, sessionEncoded := mustKey(t, cephx.AES, 0x22, 16)
	secret := make([]byte, 40)
	for i := range secret {
		secret[i] = byte(i + 1)
	}
	return client, authDone(t, client, sessionEncoded, session, secret), secret
}

const wantClient = "HELLO,AUTH_REQUEST,AUTH_REQUEST_MORE,AUTH_SIGNATURE,CLIENT_IDENT,MESSAGE"
const wantServer = "HELLO,AUTH_REPLY_MORE,AUTH_DONE,AUTH_SIGNATURE,SERVER_IDENT,MESSAGE"

func TestSecureCaptureOpaqueWithoutKey(t *testing.T) {
	_, payload, secret := fixture(t)
	c2s, s2c := monStreams(t, modeSecure, payload, secret)
	b := newPcap(65535)
	conversation(b, cliAddr, monAddr, c2s, s2c, 7, true)
	rep, err := analyze("synthetic", &b.buf, analysisOptions{ServerPorts: []uint16{3300}, MaxBytes: 1 << 20, FrameLimit: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	c := rep.Connections[0]
	if c.ConMode != "secure" || c.TCPOutcome != "server FIN first" || c.C2S.Retransmitted != 1 {
		t.Fatalf("connection %+v", c)
	}
	if got := tags(c.C2S.Msgr.Frames); got != "HELLO,AUTH_REQUEST,AUTH_REQUEST_MORE" {
		t.Fatalf("client tags %s", got)
	}
	if c.S2C.Msgr.OpaqueBytes == 0 || c.C2S.Msgr.OpaqueBytes == 0 {
		t.Fatalf("secure traffic should be opaque: %+v %+v", c.S2C.Msgr, c.C2S.Msgr)
	}
}

func TestSecureCapturePassiveDerivation(t *testing.T) {
	client, payload, secret := fixture(t)
	c2s, s2c := monStreams(t, modeSecure, payload, secret)
	b := newPcap(65535)
	conversation(b, cliAddr, monAddr, c2s, s2c, 5, true)
	rep, err := analyze("synthetic", &b.buf, analysisOptions{ServerPorts: []uint16{3300}, MaxBytes: 1 << 20, FrameLimit: 1 << 20,
		Entity: "client.test", Key: &client})
	if err != nil {
		t.Fatal(err)
	}
	c := rep.Connections[0]
	if c.SecretState != "derived from captured AUTH_DONE" {
		t.Fatalf("secret state %q", c.SecretState)
	}
	if got := tags(c.C2S.Msgr.Frames); got != wantClient {
		t.Fatalf("client tags %s", got)
	}
	if got := tags(c.S2C.Msgr.Frames); got != wantServer {
		t.Fatalf("server tags %s", got)
	}
	last := c.S2C.Msgr.Frames[len(c.S2C.Msgr.Frames)-1]
	if last.Message == nil || last.Message.Type != msgr.MonCommandReplyMessage || last.Message.Tid != 9 || last.Message.AckSeq != 1 || !last.Encrypted {
		t.Fatalf("reply %+v", last)
	}
	if c.MsgrOutcome != "session established; messages exchanged" {
		t.Fatalf("outcome %q", c.MsgrOutcome)
	}
	// A wrong key must fail closed rather than mislabel frames.
	other, _ := mustKey(t, cephx.AES256K, 0x33, 32)
	b = newPcap(65535)
	conversation(b, cliAddr, monAddr, c2s, s2c, 5, true)
	rep, err = analyze("synthetic", &b.buf, analysisOptions{ServerPorts: []uint16{3300}, MaxBytes: 1 << 20, FrameLimit: 1 << 20,
		Entity: "client.test", Key: &other})
	if err != nil {
		t.Fatal(err)
	}
	if s := rep.Connections[0].SecretState; !strings.HasPrefix(s, "derivation failed") {
		t.Fatalf("wrong key state %q", s)
	}
}

func TestCRCCaptureDecodesWithoutKey(t *testing.T) {
	_, payload, _ := fixture(t)
	c2s, s2c := monStreams(t, modeCRC, payload, nil)
	b := newPcap(65535)
	conversation(b, cliAddr, monAddr, c2s, s2c, 11, false)
	rep, err := analyze("synthetic", &b.buf, analysisOptions{ServerPorts: []uint16{3300}, MaxBytes: 1 << 20, FrameLimit: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	c := rep.Connections[0]
	if c.ConMode != "crc" || c.TCPOutcome != "open at capture end" {
		t.Fatalf("connection %+v", c)
	}
	if tags(c.C2S.Msgr.Frames) != wantClient || tags(c.S2C.Msgr.Frames) != wantServer {
		t.Fatalf("tags %s / %s", tags(c.C2S.Msgr.Frames), tags(c.S2C.Msgr.Frames))
	}
}

func TestRefusedAndGapAndTruncation(t *testing.T) {
	_, payload, _ := fixture(t)
	c2s, s2c := monStreams(t, modeCRC, payload, nil)
	b := newPcap(65535)
	closed := netip.MustParseAddrPort("10.0.0.2:3301")
	b.tcp(cliAddr, closed, 77, 0, tcpSYN, nil)
	b.tcp(closed, cliAddr, 0, 78, tcpRST|tcpACK, nil)
	// A capture that lost one server segment which the client acknowledged.
	const ci, si = 1000, 5000
	other := netip.MustParseAddrPort("10.0.0.1:40001")
	b.tcp(other, monAddr, ci, 0, tcpSYN, nil)
	b.tcp(monAddr, other, si, ci+1, tcpSYN|tcpACK, nil)
	b.tcp(other, monAddr, ci+1, si+1, tcpACK, c2s[:26])
	b.tcp(monAddr, other, si+1, ci+27, tcpACK, s2c[:40])
	b.tcp(monAddr, other, si+61, ci+27, tcpACK, s2c[60:80]) // bytes 40..60 missing
	b.tcp(other, monAddr, ci+27, si+81, tcpACK, nil)
	rep, err := analyze("synthetic", &b.buf, analysisOptions{ServerPorts: []uint16{3300}, MaxBytes: 1 << 20, FrameLimit: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if got := rep.Connections[0].TCPOutcome; got != "refused (SYN answered by RST)" {
		t.Fatalf("refused outcome %q", got)
	}
	g := rep.Connections[1].S2C
	if len(g.Gaps) != 1 || g.Gaps[0] != (gap{40, 20}) || g.AckedNotCaptured != 40 {
		t.Fatalf("gap report %+v", g)
	}
	// Snaplen truncation is reported per record and as missing bytes.
	b = newPcap(80)
	conversation(b, cliAddr, monAddr, c2s, s2c, 64, false)
	rep, err = analyze("synthetic", &b.buf, analysisOptions{ServerPorts: []uint16{3300}, MaxBytes: 1 << 20, FrameLimit: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Pcap.Truncated == 0 || rep.Connections[0].C2S.SnaplenMissing == 0 || len(rep.Connections[0].C2S.Gaps) == 0 {
		t.Fatalf("truncation not reported: %+v %+v", rep.Pcap, rep.Connections[0].C2S)
	}
}

func TestRejectsPcapng(t *testing.T) {
	_, err := analyze("x", bytes.NewReader([]byte{0x0a, 0x0d, 0x0d, 0x0a, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}), analysisOptions{})
	if err != errPcapng {
		t.Fatalf("got %v", err)
	}
}

func TestMonLogCrossCheck(t *testing.T) {
	_, payload, _ := fixture(t)
	c2s, s2c := monStreams(t, modeCRC, payload, nil)
	b := newPcap(65535)
	conversation(b, cliAddr, monAddr, c2s, s2c, 9, true)
	rep, err := analyze("synthetic", &b.buf, analysisOptions{ServerPorts: []uint16{3300}, MaxBytes: 1 << 20, FrameLimit: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	prefix := "2026-01-01T00:00:00.000+0000 7f 10 --2- [v2:10.0.0.2:3300/0] >> v2:10.0.0.1:40000/0 conn(0xabc 0xdef crc :-1 s=READY pgs=0 cs=0 l=1).handle_read_frame_dispatch tag="
	var log strings.Builder
	for _, tag := range []int{1, 2, 5, 7, 8, 17} {
		log.WriteString(prefix)
		log.WriteString(string(rune('0' + tag/10)))
		log.WriteString(string(rune('0' + tag%10)))
		log.WriteString("\n")
	}
	log.WriteString("x 2 --2- [v2:10.0.0.2:3300/0] >> v2:10.0.0.1:40000/0 conn(0xabc 0xdef crc :-1 s=READY).write_message sending message m=0x1 seq=1 mon_command_ack([...]=0 v0) v1\n")
	logs, err := parseMonLog(strings.NewReader(log.String()))
	if err != nil {
		t.Fatal(err)
	}
	cross := crosscheck(rep, logs, 3300)
	if len(cross) != 1 || !cross[0].TagsEqual || !cross[0].ModeEqual || !cross[0].SeqsEqual {
		t.Fatalf("cross-check %+v", cross)
	}
}
