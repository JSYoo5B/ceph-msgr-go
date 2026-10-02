package cephmsgr

import (
	"bytes"
	"context"
	"crypto/hmac"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

type traceReader struct {
	io.Reader
	active bool
	data   bytes.Buffer
}

func (r *traceReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if r.active {
		r.data.Write(p[:n])
	}
	return n, err
}

type traceWriter struct {
	io.Writer
	active bool
	data   bytes.Buffer
}

func (w *traceWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if w.active {
		w.data.Write(p[:n])
	}
	return n, err
}

func mockCredential() (Key, []byte) {
	e := wire.Encoder{}
	e.U16(2)
	e.U32(0)
	e.U32(0)
	e.U16(32)
	e.Raw(bytes.Repeat([]byte{0x27}, 32))
	k, _ := ParseKey(base64.StdEncoding.EncodeToString(e.Data))
	return k, e.Data
}
func mockTicket(key cephx.Key, encoded []byte, service uint32) ([]byte, error) {
	e := wire.Encoder{}
	e.U8(1)
	e.Raw(encoded)
	e.U32(3600)
	e.U32(0)
	encrypted, err := key.Seal(4, e.Data)
	if err != nil {
		return nil, err
	}
	blob := wire.Encoder{}
	blob.U8(1)
	blob.U64(7)
	blob.Bytes([]byte("opaque"))
	out := wire.Encoder{}
	out.U8(1)
	out.U32(1)
	out.U32(service)
	out.U8(1)
	out.Bytes(encrypted)
	out.U8(0)
	out.Bytes(blob.Data)
	return out.Data, nil
}
func mockMonMap(release uint8, fsid [16]byte) []byte {
	e := wire.Encoder{}
	e.Raw(fsid[:])
	e.U32(1)
	e.Raw(make([]byte, 16))
	features := wire.Encoder{}
	features.U64(0)
	e.Struct(1, 1, features.Data)
	e.Struct(1, 1, features.Data)
	info := wire.Encoder{}
	info.String("a")
	msgr.EncodeAddresses(&info, []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")}})
	e.U32(1)
	e.String("a")
	e.Struct(6, 1, info.Data)
	e.U32(1)
	e.String("a")
	e.U8(release)
	out := wire.Encoder{}
	out.Struct(10, 6, e.Data)
	front := wire.Encoder{}
	front.Bytes(out.Data)
	return front.Data
}
func mockMgrMap(epoch uint32, id uint64, port uint16) []byte {
	e := wire.Encoder{}
	e.U32(epoch)
	msgr.EncodeAddresses(&e, []msgr.Address{{Type: 2, Endpoint: netip.AddrPortFrom(netip.MustParseAddr("192.0.2.1"), port)}})
	e.U64(id)
	e.U8(1)
	e.String("a")
	out := wire.Encoder{}
	out.Struct(14, 6, e.Data)
	return out.Data
}

type peerConfig struct {
	fsid     [16]byte
	release  byte
	role     uint8
	id       uint64
	clientID uint64
	command  func(msgr.MessageData)
	reply    func(*msgr.MessageData)
}

// The synthetic peer exercises actual CephX state transitions and client
// routing. It shares the Go frame codec and is not a real-Ceph wire oracle.
func mockAuthenticate(conn net.Conn, cfg peerConfig) (*msgr.Reader, *msgr.Writer, error) {
	if cfg.clientID == 0 {
		cfg.clientID = 42
	}
	rx := &traceReader{Reader: conn, active: true}
	tx := &traceWriter{Writer: conn, active: true}
	if _, err := msgr.ReadBanner(rx, msgr.Banner{Supported: 3, Required: 1}); err != nil {
		return nil, nil, err
	}
	if err := msgr.WriteFull(tx, (msgr.Banner{Supported: 3, Required: 1}).Encode()); err != nil {
		return nil, nil, err
	}
	r, w := msgr.NewReader(rx, 0), msgr.NewWriter(tx, 0)
	write := func(tag msgr.Tag, p []byte) error { return w.Write(msgr.Frame{Tag: tag, Segments: [][]byte{p}}) }
	read := func(tag msgr.Tag) ([]byte, error) {
		f, err := r.Read()
		if err != nil {
			return nil, err
		}
		if f.Tag != tag || len(f.Segments) != 1 {
			return nil, fmt.Errorf("expected tag %d, got %d", tag, f.Tag)
		}
		return f.Segments[0], nil
	}
	if _, err := read(msgr.Hello); err != nil {
		return nil, nil, err
	}
	hello := wire.Encoder{}
	hello.U8(cfg.role)
	msgr.Address{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.2:0")}.Encode(&hello)
	if err := write(msgr.Hello, hello.Data); err != nil {
		return nil, nil, err
	}
	p, err := read(msgr.AuthRequest)
	if err != nil {
		return nil, nil, err
	}
	d := wire.NewDecoder(p)
	if d.U32() != 2 || d.U32() != 1 || d.U32() != 2 {
		return nil, nil, errors.New("bad method/mode")
	}
	authPayload := d.Bytes()
	if err := d.Done(); err != nil {
		return nil, nil, err
	}
	key, encoded := mockCredential()
	secret := make([]byte, 40)
	for i := range secret {
		secret[i] = byte(i)
	}
	var final []byte
	if cfg.role == 1 {
		initial := wire.NewDecoder(authPayload)
		if initial.U8() != 10 || initial.U32() != 8 || initial.String() != "test" {
			return nil, nil, errors.New("identity")
		}
		initial.U64()
		if err := initial.Done(); err != nil {
			return nil, nil, err
		}
		challenge := wire.Encoder{}
		challenge.U8(1)
		challenge.U64(0x1234)
		more := wire.Encoder{}
		more.Bytes(challenge.Data)
		if err := write(msgr.AuthReplyMore, more.Data); err != nil {
			return nil, nil, err
		}
		p, err := read(msgr.AuthRequestMore)
		if err != nil {
			return nil, nil, err
		}
		container := wire.NewDecoder(p)
		proof := wire.NewDecoder(container.Bytes())
		if proof.U16() != 0x100 || proof.U8() != 3 {
			return nil, nil, errors.New("auth request version")
		}
		nonce, got := proof.U64(), proof.U64()
		proof.U8()
		proof.U64()
		proof.Bytes()
		if proof.U32() != cephx.ServiceAuth|cephx.ServiceMgr || proof.Done() != nil {
			return nil, nil, errors.New("ticket request")
		}
		challengeBlob := wire.Encoder{}
		challengeBlob.U64(0x1234)
		challengeBlob.U64(nonce)
		hash := key.value.Signature(challengeBlob.Data)
		var want uint64
		for i := 0; i < len(hash); i += 8 {
			want ^= binary.LittleEndian.Uint64(hash[i:])
		}
		if got != want {
			return nil, nil, errors.New("bad challenge proof")
		}
		tickets, err := mockTicket(key.value, encoded, cephx.ServiceAuth)
		if err != nil {
			return nil, nil, err
		}
		connSecret := wire.Encoder{}
		connSecret.Bytes(secret)
		encrypted, err := key.value.Seal(3, connSecret.Data)
		if err != nil {
			return nil, nil, err
		}
		wrapped := wire.Encoder{}
		wrapped.Bytes(encrypted)
		extra, err := mockTicket(key.value, encoded, cephx.ServiceMgr)
		if err != nil {
			return nil, nil, err
		}
		response := wire.Encoder{}
		response.U16(0x100)
		response.U32(0)
		response.Raw(tickets)
		response.Bytes(wrapped.Data)
		response.Bytes(extra)
		final = response.Data
	} else {
		parse := func(p []byte) (uint64, bool, uint64, error) {
			d := wire.NewDecoder(p)
			if d.U8() != 1 || d.U64() != cfg.clientID || d.U32() != cephx.ServiceMgr {
				return 0, false, 0, errors.New("mgr authorizer")
			}
			d.U8()
			d.U64()
			d.Bytes()
			plain, err := key.value.Open(0x10, d.Bytes())
			if err != nil {
				return 0, false, 0, err
			}
			body := wire.NewDecoder(plain)
			if body.U8() != 2 {
				return 0, false, 0, wire.ErrVersion
			}
			nonce, challenged, challenge := body.U64(), body.Bool(), body.U64()
			return nonce, challenged, challenge, body.Done()
		}
		nonce, challenged, _, err := parse(authPayload)
		if err != nil || challenged {
			return nil, nil, errors.New("mgr initial proof")
		}
		challenge := wire.Encoder{}
		challenge.U8(1)
		challenge.U64(57)
		encrypted, err := key.value.Seal(0x11, challenge.Data)
		if err != nil {
			return nil, nil, err
		}
		more := wire.Encoder{}
		more.Bytes(encrypted)
		if err := write(msgr.AuthReplyMore, more.Data); err != nil {
			return nil, nil, err
		}
		p, err := read(msgr.AuthRequestMore)
		if err != nil {
			return nil, nil, err
		}
		moreReply := wire.NewDecoder(p)
		actual, challenged, challengePlusOne, err := parse(moreReply.Bytes())
		if err != nil || actual != nonce || !challenged || challengePlusOne != 58 {
			return nil, nil, errors.New("mgr challenge proof")
		}
		reply := wire.Encoder{}
		reply.U8(2)
		reply.U64(nonce + 1)
		reply.Bytes(secret)
		encrypted, err = key.value.Seal(0x12, reply.Data)
		if err != nil {
			return nil, nil, err
		}
		out := wire.Encoder{}
		out.Bytes(encrypted)
		final = out.Data
	}
	done := wire.Encoder{}
	done.U64(cfg.clientID)
	done.U32(2)
	done.Bytes(final)
	if err := write(msgr.AuthDone, done.Data); err != nil {
		return nil, nil, err
	}
	sig := key.value.Signature(rx.data.Bytes())
	expected := key.value.Signature(tx.data.Bytes())
	rx.active, tx.active = false, false
	r.EnableSecure(secret[:16], secret[28:40])
	w.EnableSecure(secret[:16], secret[16:28])
	p, err = read(msgr.AuthSignature)
	if err != nil {
		return nil, nil, err
	}
	if !hmac.Equal(p, expected) {
		return nil, nil, errors.New("transcript")
	}
	if err := write(msgr.AuthSignature, sig); err != nil {
		return nil, nil, err
	}
	if _, err := read(msgr.CompressionRequest); err != nil {
		return nil, nil, err
	}
	noCompression := wire.Encoder{}
	noCompression.U8(0)
	noCompression.U32(0)
	if err := write(msgr.CompressionDone, noCompression.Data); err != nil {
		return nil, nil, err
	}
	p, err = read(msgr.ClientIdent)
	if err != nil {
		return nil, nil, err
	}
	clientIdent := wire.NewDecoder(p)
	msgr.DecodeAddresses(clientIdent)
	target := msgr.DecodeAddress(clientIdent)
	if err := clientIdent.Err(); err != nil {
		return nil, nil, err
	}
	ident := wire.Encoder{}
	msgr.EncodeAddresses(&ident, []msgr.Address{target})
	ident.U64(cfg.id)
	ident.U64(1)
	ident.U64(session.Features)
	ident.U64(session.RequiredFeatures)
	ident.U64(1)
	ident.U64(0)
	if err := write(msgr.ServerIdent, ident.Data); err != nil {
		return nil, nil, err
	}
	conn.SetDeadline(time.Time{})
	return r, w, nil
}

func mockDaemon(conn net.Conn, cfg peerConfig) error {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	r, w, err := mockAuthenticate(conn, cfg)
	if err != nil {
		return err
	}
	type incoming struct {
		frame msgr.Frame
		err   error
	}
	frames := make(chan incoming)
	closed := make(chan struct{})
	defer close(closed)
	go func() {
		for {
			f, err := r.Read()
			select {
			case frames <- incoming{f, err}:
			case <-closed:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	var sequence uint64
	send := func(m msgr.MessageData) error {
		sequence++
		m.Sequence = sequence
		m.Priority = 127
		m.Version = 1
		return w.Write(m.Frame())
	}
	if cfg.role == 1 {
		if err := send(msgr.MessageData{Type: msgr.MonMapMessage, Front: mockMonMap(cfg.release, cfg.fsid)}); err != nil {
			return err
		}
		if err := send(msgr.MessageData{Type: msgr.MgrMapMessage, Front: mockMgrMap(1, 99, 6800)}); err != nil {
			return err
		}
	}
	for {
		got := <-frames
		if got.err != nil {
			return got.err
		}
		f := got.frame
		if f.Tag != msgr.Message {
			continue
		}
		m, err := msgr.DecodeMessage(f)
		if err != nil {
			return err
		}
		if m.Type == msgr.SubscribeMessage {
			continue
		}
		if cfg.role == 1 && m.Type != msgr.MonCommandMessage || cfg.role == 16 && m.Type != msgr.MgrCommandMessage {
			return errors.New("wrong command daemon")
		}
		if cfg.command != nil {
			cfg.command(m)
		}
		if strings.Contains(string(m.Front), "drop") {
			return nil
		}
		if strings.Contains(string(m.Front), "switch") {
			if err := send(msgr.MessageData{Type: msgr.MgrMapMessage, Front: mockMgrMap(2, 100, 6801)}); err != nil {
				return err
			}
		}
		if strings.Contains(string(m.Front), "block") {
			continue
		}
		code := int32(0)
		if strings.Contains(string(m.Front), "deny") {
			code = -13
		}
		e := wire.Encoder{}
		typ := msgr.MgrCommandReplyMessage
		if cfg.role == 1 {
			msgr.Paxos(&e)
			typ = msgr.MonCommandReplyMessage
		}
		e.U32(uint32(code))
		e.String("status text")
		if cfg.role == 1 {
			e.U32(0)
		}
		data := m.Data
		if len(data) == 0 {
			data = []byte(`{"ok":true}`)
		}
		response := msgr.MessageData{Type: typ, Transaction: m.Transaction, Front: e.Data, Data: data}
		if cfg.reply != nil {
			cfg.reply(&response)
		}
		if err := send(response); err != nil {
			return err
		}
	}
}

func mockOptions(t *testing.T, release byte, fsid [16]byte) Options {
	t.Helper()
	key, _ := mockCredential()
	return Options{Monitors: []string{"192.0.2.1:3300"}, Identity: "client.test", Key: key, ConnectTimeout: time.Second, DialContext: func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		client, peer := net.Pipe()
		role, id := uint8(1), uint64(0)
		if strings.HasSuffix(endpoint, ":6800") {
			role, id = 16, 99
		}
		if strings.HasSuffix(endpoint, ":6801") {
			role, id = 16, 100
		}
		go func() { mockDaemon(peer, peerConfig{fsid: fsid, release: release, role: role, id: id}) }()
		return client, nil
	}}
}

func TestNativeMONAndMGRCommandPath(t *testing.T) {
	fsid := [16]byte{1, 2, 3}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, mockOptions(t, 20, fsid))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	input := []byte{0, 0xff, 0x7f}
	command := Command{JSON: []byte(`{"prefix":"status"}`), Input: input}
	for _, call := range []func(context.Context, Command) (Result, error){c.MonCommand, c.MgrCommand} {
		result, err := call(ctx, command)
		if err != nil || !bytes.Equal(result.Data, input) || result.Message != "status text" {
			t.Fatal(result, err)
		}
		result, err = call(ctx, Command{JSON: []byte(`{"prefix":"deny"}`)})
		var serverError *CommandError
		if result.Code != -13 || !errors.As(err, &serverError) || serverError.Code != -13 {
			t.Fatal("server code lost", result, err)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.MonCommand(ctx, command); !errors.Is(err, ErrClosed) {
		t.Fatal("closed client", err)
	}
}
func TestMinimumReleaseAndPinnedFSID(t *testing.T) {
	fsid := [16]byte{1}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if c, err := Dial(ctx, mockOptions(t, 19, fsid)); err == nil {
		c.Close()
		t.Fatal("Squid accepted")
	}
	options := mockOptions(t, 20, fsid)
	options.ExpectedFSID = "02000000-0000-0000-0000-000000000000"
	if c, err := Dial(ctx, options); err == nil {
		c.Close()
		t.Fatal("wrong cluster accepted")
	}
}
func TestManagerMapChangeSelectsNewDaemon(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, mockOptions(t, 20, [16]byte{1}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	command := Command{JSON: []byte(`{"prefix":"status"}`)}
	if _, err := c.MgrCommand(ctx, command); err != nil {
		t.Fatal(err)
	}
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"switch"}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.MgrCommand(ctx, command); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	id := c.mgrMap.GlobalID
	c.mu.Unlock()
	if id != 100 {
		t.Fatal("stale manager")
	}
}
