package session

import (
	"bytes"
	"crypto/hmac"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Source-derived synthetic service peer, not a native MDS oracle. In addition
// to the Messenger transcript it opens actual ServiceAuth proof ciphertext and
// checks service 0x02, identity, ticket and the authorizer-v2 challenge response.
// Provenance and excluded application protocols: MDS_FRESH_CONTRACT.md.
type freshMDSPeerConfig struct {
	role                          uint8
	rank, flags, cookie           uint64
	supported, required           uint64
	selected                      ConnectionMode
	addresses                     func(msgr.Address) []msgr.Address
	badNonce, badSignature        bool
	legacyReply                   bool
	truncateIdent                 int
	identTag                      msgr.Tag
	identPayload                  []byte
	clientIdent                   func(msgr.Address, uint64, uint64, uint64, uint64) error
	beforeServerIdent, afterIdent func(*msgr.Reader, *msgr.Writer) error
}

func freshMDSPeerDefaults() freshMDSPeerConfig {
	return freshMDSPeerConfig{role: 2, cookie: 17, supported: Features, required: RequiredFeatures}
}

func freshMDSNoTraffic(r *msgr.Reader) error {
	f, err := r.Read()
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("fresh MDS emitted unexpected control or application tag %d", f.Tag)
}

func freshMDSKey(kind uint16) (cephx.Key, error) {
	n := 16
	if kind == cephx.AES256K {
		n = 32
	}
	e := wire.Encoder{}
	e.U16(kind)
	e.U32(0)
	e.U32(0)
	e.U16(uint16(n))
	e.Raw(bytes.Repeat([]byte{0x71}, n))
	return cephx.ParseKey(base64.StdEncoding.EncodeToString(e.Data))
}

func freshMDSAuthorizer(kind uint16, mode ConnectionMode) (ServiceAuth, fixtureAuth, error) {
	key, err := freshMDSKey(kind)
	if err != nil {
		return ServiceAuth{}, fixtureAuth{}, err
	}
	c, err := cephx.NewClient("client.fresh-mds", key, cephx.ServiceAuth|cephx.ServiceMgr|cephx.ServiceMDS)
	if err != nil {
		return ServiceAuth{}, fixtureAuth{}, err
	}
	c.GlobalID = 42
	c.Tickets[cephx.ServiceMDS] = cephx.Ticket{Key: key, SecretID: 7, Blob: []byte("MDS ticket"), Expires: time.Now().Add(time.Hour)}
	a, err := c.Authorizer(cephx.ServiceMDS)
	fixture := fixtureAuth{key: key}
	if mode == SecureMode {
		fixture.secret = make([]byte, 40)
		for i := range fixture.secret {
			fixture.secret[i] = byte(i + 1)
		}
	}
	return ServiceAuth{Authorizer: a}, fixture, err
}

func freshMDSProof(key cephx.Key, payload []byte, challenge bool, nonce uint64) (uint64, error) {
	d := wire.NewDecoder(payload)
	if d.U8() != 1 || d.U64() != 42 || d.U32() != 0x02 || d.U8() != 1 || d.U64() != 7 || !bytes.Equal(d.Bytes(), []byte("MDS ticket")) {
		return 0, errors.New("wrong MDS service, ticket or identity")
	}
	proof := d.Bytes()
	if err := d.Done(); err != nil {
		return 0, err
	}
	plain, err := key.Open(0x10, proof)
	if err != nil {
		return 0, err
	}
	d = wire.NewDecoder(plain)
	version, gotNonce, challenged, response := d.U8(), d.U64(), d.Bool(), d.U64()
	if err := d.Done(); err != nil {
		return 0, err
	}
	if version != 2 || challenged != challenge || (challenge && (gotNonce != nonce || response != 58)) || (!challenge && response != 0) {
		return 0, errors.New("invalid MDS authorizer-v2 proof")
	}
	return gotNonce, nil
}

func freshMDSPeer(conn net.Conn, fixture fixtureAuth, mode ConnectionMode, cfg freshMDSPeerConfig) error {
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return err
	}
	rx := &recordingReader{Reader: conn, active: true}
	tx := &recordingWriter{Writer: conn, active: true}
	banner := msgr.Banner{Supported: msgr.Revision1 | msgr.Compression, Required: msgr.Revision1}
	if _, err := msgr.ReadBanner(rx, banner); err != nil {
		return err
	}
	if err := msgr.WriteFull(tx, banner.Encode()); err != nil {
		return err
	}
	r, w := msgr.NewReader(rx, 4096), msgr.NewWriter(tx, 4096)
	write := func(tag msgr.Tag, p []byte) error { return w.Write(msgr.Frame{Tag: tag, Segments: [][]byte{p}}) }
	f, err := r.Read()
	if err != nil {
		return err
	}
	if f.Tag != msgr.Hello || len(f.Segments) != 1 {
		return errors.New("client did not send HELLO")
	}
	d := wire.NewDecoder(f.Segments[0])
	clientRole := d.U8()
	msgr.DecodeAddress(d)
	if err := d.Done(); err != nil || clientRole != 8 {
		return errors.New("invalid CLIENT hello")
	}
	e := wire.Encoder{}
	e.U8(cfg.role)
	msgr.Address{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.2:0")}.Encode(&e)
	if err := write(msgr.Hello, e.Data); err != nil {
		return err
	}
	if cfg.role != 2 {
		return freshMDSNoTraffic(r)
	}
	f, err = r.Read()
	if err != nil {
		return err
	}
	if f.Tag != msgr.AuthRequest || len(f.Segments) != 1 {
		return errors.New("missing MDS service auth request")
	}
	d = wire.NewDecoder(f.Segments[0])
	method, n, offered := d.U32(), d.U32(), d.U32()
	payload := d.Bytes()
	if err := d.Done(); err != nil || method != 2 || n != 1 || offered != uint32(mode) {
		return errors.New("MDS auth method or mode changed")
	}
	nonce, err := freshMDSProof(fixture.key, payload, false, 0)
	if err != nil {
		return err
	}
	challenge := wire.Encoder{}
	challenge.U8(1)
	challenge.U64(57)
	cipher, err := fixture.key.Seal(0x11, challenge.Data)
	if err != nil {
		return err
	}
	e = wire.Encoder{}
	e.Bytes(cipher)
	if err := write(msgr.AuthReplyMore, e.Data); err != nil {
		return err
	}
	f, err = r.Read()
	if err != nil {
		return err
	}
	if f.Tag != msgr.AuthRequestMore || len(f.Segments) != 1 {
		return errors.New("missing MDS authorizer challenge response")
	}
	d = wire.NewDecoder(f.Segments[0])
	payload = d.Bytes()
	if err := d.Done(); err != nil {
		return err
	}
	if _, err := freshMDSProof(fixture.key, payload, true, nonce); err != nil {
		return err
	}
	reply := wire.Encoder{}
	if cfg.legacyReply {
		reply.U8(1)
	} else {
		reply.U8(2)
	}
	if cfg.badNonce {
		nonce++
	}
	reply.U64(nonce + 1)
	if !cfg.legacyReply {
		reply.Bytes(fixture.secret)
	}
	cipher, err = fixture.key.Seal(0x12, reply.Data)
	if err != nil {
		return err
	}
	envelope := wire.Encoder{}
	envelope.Bytes(cipher)
	selected := cfg.selected
	if selected == 0 {
		selected = mode
	}
	e = wire.Encoder{}
	e.U64(42)
	e.U32(uint32(selected))
	e.Bytes(envelope.Data)
	if err := write(msgr.AuthDone, e.Data); err != nil {
		return err
	}
	if selected != mode || cfg.badNonce || (cfg.legacyReply && mode == SecureMode) {
		return freshMDSNoTraffic(r)
	}
	sig, expected := fixture.key.Signature(rx.data.Bytes()), fixture.key.Signature(tx.data.Bytes())
	rx.active, tx.active = false, false
	if mode == SecureMode {
		if err := r.EnableSecure(fixture.secret[:16], fixture.secret[28:40]); err != nil {
			return err
		}
		if err := w.EnableSecure(fixture.secret[:16], fixture.secret[16:28]); err != nil {
			return err
		}
	}
	f, err = r.Read()
	if err != nil {
		return err
	}
	if f.Tag != msgr.AuthSignature || len(f.Segments) != 1 || !hmac.Equal(expected, f.Segments[0]) {
		return errors.New("invalid MDS client transcript signature")
	}
	if cfg.badSignature {
		sig[0] ^= 1
	}
	if err := write(msgr.AuthSignature, sig); err != nil {
		return err
	}
	if cfg.badSignature {
		return freshMDSNoTraffic(r)
	}
	f, err = r.Read()
	if err != nil {
		return err
	}
	if f.Tag != msgr.CompressionRequest || len(f.Segments) != 1 || !bytes.Equal(f.Segments[0], []byte{0, 0, 0, 0, 0}) {
		return errors.New("unexpected MDS compression offer")
	}
	if err := write(msgr.CompressionDone, []byte{0, 0, 0, 0, 0}); err != nil {
		return err
	}
	f, err = r.Read()
	if err != nil {
		return err
	}
	if f.Tag != msgr.ClientIdent || len(f.Segments) != 1 {
		return errors.New("fresh MDS did not send ClientIdent")
	}
	d = wire.NewDecoder(f.Segments[0])
	clientAddresses := msgr.DecodeAddresses(d)
	target := msgr.DecodeAddress(d)
	globalID, globalSeq := d.U64(), d.U64()
	supported, required, flags, cookie := d.U64(), d.U64(), d.U64(), d.U64()
	if err := d.Done(); err != nil {
		return err
	}
	if len(clientAddresses) != 1 || globalID != 42 || flags != 0 || cookie == 0 || supported != Features || required != RequiredFeatures {
		return fmt.Errorf("invalid fresh MDS ClientIdent masks/flags/cookie or identity")
	}
	if cfg.clientIdent != nil {
		if err := cfg.clientIdent(clientAddresses[0], globalSeq, supported, required, cookie); err != nil {
			return err
		}
	}
	if cfg.beforeServerIdent != nil {
		return cfg.beforeServerIdent(r, w)
	}
	if cfg.identTag != 0 {
		if err := write(cfg.identTag, cfg.identPayload); err != nil {
			return err
		}
		return freshMDSNoTraffic(r)
	}
	addresses := []msgr.Address{target}
	if cfg.addresses != nil {
		addresses = cfg.addresses(target)
	}
	e = wire.Encoder{}
	msgr.EncodeAddresses(&e, addresses)
	e.U64(cfg.rank)
	e.U64(19)
	e.U64(cfg.supported)
	e.U64(cfg.required)
	e.U64(cfg.flags)
	e.U64(cfg.cookie)
	if cfg.truncateIdent > 0 {
		e.Data = e.Data[:len(e.Data)-cfg.truncateIdent]
	}
	if err := write(msgr.ServerIdent, e.Data); err != nil {
		return err
	}
	if cfg.afterIdent != nil {
		return cfg.afterIdent(r, w)
	}
	return freshMDSNoTraffic(r)
}
