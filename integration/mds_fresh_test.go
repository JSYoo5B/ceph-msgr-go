package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

const mdsFreshSource = "7f793731f1b39eb4f465e960113d2363c311b964"
const mdsFreshLimit = uint32(1 << 20)

func mdsFreshExpectedKeyType(profile string) (uint16, error) {
	switch profile {
	case "aes":
		return cephx.AES, nil
	case "aes256k":
		return cephx.AES256K, nil
	default:
		return 0, errors.New("MDS fresh key and service profiles must each select aes or aes256k")
	}
}

func mdsFreshConnectionMode(profile string) (session.ConnectionMode, error) {
	// Public ConnectionMode uses zero for its default secure selection, while
	// this internal wire enum uses 2. Do not cast between these distinct enums.
	switch profile {
	case "crc":
		return session.CRCMode, nil
	case "secure":
		return session.SecureMode, nil
	default:
		return 0, errors.New("MDS fresh connection mode must explicitly select crc or secure")
	}
}

type nativeMDSFresh struct {
	Source         string `json:"source_commit"`
	Token          string `json:"token"`
	FSID           string `json:"fsid"`
	FSCID          int32  `json:"fscid"`
	FSName         string `json:"fs_name"`
	ClientIdentity string `json:"client_identity"`
	MDS            struct {
		Name      string `json:"name"`
		GID       uint64 `json:"gid"`
		Rank      int32  `json:"rank"`
		State     string `json:"state"`
		Endpoint  string `json:"endpoint"`
		Nonce     uint32 `json:"nonce"`
		Addresses []struct {
			Type  string `json:"type"`
			Addr  string `json:"addr"`
			Nonce uint32 `json:"nonce"`
		} `json:"addresses"`
	} `json:"mds"`
	BeforeSessions json.RawMessage `json:"before_sessions"`
	Filesystem     json.RawMessage `json:"filesystem"`
	FSDump         json.RawMessage `json:"fs_dump"`
}

type nativeMDSFreshSessions struct {
	Source     string          `json:"source_commit"`
	Token      string          `json:"token"`
	RequestID  int64           `json:"request_id"`
	FSID       string          `json:"fsid"`
	FSCID      int32           `json:"fscid"`
	FSName     string          `json:"fs_name"`
	MDSGID     uint64          `json:"mds_gid"`
	Rank       int32           `json:"rank"`
	GlobalID   uint64          `json:"expected_global_id"`
	Present    bool            `json:"expected_global_id_present"`
	Sessions   json.RawMessage `json:"sessions"`
	Filesystem json.RawMessage `json:"filesystem"`
	FSDump     json.RawMessage `json:"fs_dump"`
}

func readMDSFreshJSON(path string, target any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, int64(mdsFreshLimit)+1))
	if err != nil {
		return err
	}
	if len(data) > int(mdsFreshLimit) {
		return errors.New("native MDS fresh evidence exceeds limit")
	}
	return json.Unmarshal(data, target)
}

func observeMDSFresh(ctx context.Context, dir string, oracle nativeMDSFresh, globalID uint64) (nativeMDSFreshSessions, error) {
	var result nativeMDSFreshSessions
	path := filepath.Join(dir, "mds-fresh-session-result.json")
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	if err := testcluster.RequestControl(ctx, dir, "mds-fresh-observe", "mds-fresh-observed", oracle.Token, strconv.FormatUint(globalID, 10)); err != nil {
		return result, err
	}
	if err := readMDSFreshJSON(path, &result); err != nil {
		return result, err
	}
	if result.Source != mdsFreshSource || result.Token != oracle.Token || result.RequestID <= 0 || result.FSID != oracle.FSID || result.FSCID != oracle.FSCID || result.FSName != oracle.FSName || result.MDSGID != oracle.MDS.GID || result.Rank != oracle.MDS.Rank || result.GlobalID != globalID || result.Present {
		return result, errors.New("native MDS fresh observation scope differs or a CephFS session was opened")
	}
	var sessions []struct{ ID uint64 }
	if err := json.Unmarshal(result.Sessions, &sessions); err != nil || len(sessions) > 1024 {
		return result, errors.New("native MDS fresh session listing is invalid")
	}
	for _, s := range sessions {
		if s.ID == globalID {
			return result, errors.New("native listing contains fresh client's CephFS session")
		}
	}
	return result, nil
}

type mdsFreshEvidence struct {
	Source                  string                 `json:"source_commit"`
	Profile                 string                 `json:"osd_fixture_profile"`
	Identity                string                 `json:"identity"`
	GlobalID                uint64                 `json:"client_global_id"`
	ClientKeyType           uint16                 `json:"client_key_type"`
	MDSServiceKeyType       uint16                 `json:"mds_service_key_type"`
	ClientKeyProfile        string                 `json:"client_key_profile"`
	ServiceCipherProfile    string                 `json:"service_cipher_profile"`
	ConnectionMode          uint32                 `json:"connection_mode"`
	MONFeatures             uint64                 `json:"mon_features"`
	MDSFeatures             uint64                 `json:"mds_features"`
	Target                  msgr.Address           `json:"target"`
	ExpectedPeerRank        int32                  `json:"expected_server_ident_rank"`
	NativeDaemonGID         uint64                 `json:"native_daemon_gid"`
	NativeIdentityMeaning   string                 `json:"native_identity_meaning"`
	RequestedServices       uint32                 `json:"requested_services"`
	MONDialAttempts         uint32                 `json:"mon_dial_attempts"`
	MDSDialAttempts         uint32                 `json:"mds_dial_attempts"`
	SentFrameTags           []uint16               `json:"post_handshake_sent_frame_tags"`
	ReceivedFrameTags       []uint16               `json:"post_handshake_received_frame_tags"`
	KeepaliveHex            string                 `json:"keepalive_hex"`
	AckHex                  string                 `json:"ack_hex"`
	ACKVerified             bool                   `json:"ack_verified"`
	ApplicationMessagesSent uint32                 `json:"application_messages_sent"`
	TransportClosed         bool                   `json:"transport_closed"`
	NativeStart             nativeMDSFresh         `json:"native_start"`
	Before                  nativeMDSFreshSessions `json:"before"`
	After                   nativeMDSFreshSessions `json:"after"`
}

func mdsFreshKeepalive(ctx context.Context, tr *session.Transport, evidence *mdsFreshEvidence) error {
	// Handshake's setup context ceases to govern an admitted transport. This
	// separate I/O deadline and cancellation close bound the established wait.
	deadline := time.Now().Add(5 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := tr.Conn.SetDeadline(deadline); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { tr.Conn.Close() })
	defer stop()
	stamp := make([]byte, 8)
	now := time.Now()
	binary.LittleEndian.PutUint32(stamp, uint32(now.Unix()))
	binary.LittleEndian.PutUint32(stamp[4:], uint32(now.Nanosecond()))
	evidence.KeepaliveHex = hex.EncodeToString(stamp)
	if err := tr.Writer.Write(msgr.Frame{Tag: msgr.Keepalive, Segments: [][]byte{stamp}}); err != nil {
		return err
	}
	evidence.SentFrameTags = append(evidence.SentFrameTags, uint16(msgr.Keepalive))
	for i := 0; i < 8; i++ {
		frame, err := tr.Reader.Read()
		if err != nil {
			return err
		}
		evidence.ReceivedFrameTags = append(evidence.ReceivedFrameTags, uint16(frame.Tag))
		if len(frame.Segments) != 1 || len(frame.Segments[0]) != 8 || binary.LittleEndian.Uint32(frame.Segments[0][4:]) >= 1_000_000_000 {
			return errors.New("invalid post-authentication MDS keepalive envelope")
		}
		switch frame.Tag {
		case msgr.Keepalive:
			if err := tr.Writer.Write(msgr.Frame{Tag: msgr.KeepaliveAck, Segments: frame.Segments}); err != nil {
				return err
			}
			evidence.SentFrameTags = append(evidence.SentFrameTags, uint16(msgr.KeepaliveAck))
		case msgr.KeepaliveAck:
			evidence.AckHex = hex.EncodeToString(frame.Segments[0])
			if !bytes.Equal(frame.Segments[0], stamp) {
				return errors.New("MDS keepalive ACK timestamp differs")
			}
			evidence.ACKVerified = true
			return nil
		default:
			return fmt.Errorf("unexpected post-authentication MDS frame tag %d", frame.Tag)
		}
	}
	return errors.New("MDS did not acknowledge within the bounded control-frame count")
}

func TestCephMDSFreshIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_FRESH") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_FRESH=1 for owned native MDS authentication and keepalive acceptance")
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if dir == "" {
		t.Fatal("MDS fresh acceptance requires the disposable fixture controller")
	}
	clientKeyProfile, serviceProfile := os.Getenv("CEPH_MSGR_TEST_KEY_TYPE"), os.Getenv("CEPH_MSGR_TEST_SERVICE_CIPHER")
	wantClientKey, err := mdsFreshExpectedKeyType(clientKeyProfile)
	if err != nil {
		t.Fatal("select dedicated client key profile", err)
	}
	wantServiceKey, err := mdsFreshExpectedKeyType(serviceProfile)
	if err != nil {
		t.Fatal("select MDS service ticket key profile", err)
	}
	mode, err := mdsFreshConnectionMode(os.Getenv("CEPH_MSGR_TEST_CONNECTION_MODE"))
	if err != nil {
		t.Fatal("select MDS fresh connection mode", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var oracle nativeMDSFresh
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-fresh-oracle.json"), &oracle); err != nil {
		t.Fatal("read owned MDS fresh oracle", err)
	}
	identity := os.Getenv("CEPH_MSGR_MDS_FRESH_IDENTITY")
	token, tokenErr := hex.DecodeString(oracle.Token)
	if oracle.Source != mdsFreshSource || tokenErr != nil || len(token) != 16 || oracle.FSID != os.Getenv("CEPH_MSGR_FSID") || oracle.FSCID < 0 || oracle.FSName == "" || identity != "client.mds-fresh-"+oracle.Token || oracle.ClientIdentity != identity || oracle.MDS.GID == 0 || oracle.MDS.Rank != 0 || oracle.MDS.State != "up:active" || oracle.MDS.Name == "" || oracle.MDS.Nonce == 0 {
		t.Fatal("owned active-rank MDS fresh oracle identity or provenance differs")
	}
	endpoint, err := netip.ParseAddrPort(oracle.MDS.Endpoint)
	if err != nil || endpoint.Port() == 0 || endpoint.Addr().Zone() != "" {
		t.Fatal("MDS fresh oracle requires a numeric unscoped endpoint")
	}
	target := msgr.Address{Type: 2, Nonce: oracle.MDS.Nonce, Endpoint: endpoint}
	matched := false
	for _, a := range oracle.MDS.Addresses {
		matched = matched || a.Type == "v2" && a.Addr == oracle.MDS.Endpoint && a.Nonce == target.Nonce
	}
	if !matched {
		t.Fatal("MDS fresh target differs from owned native address vector")
	}
	keyBytes, err := os.ReadFile(os.Getenv("CEPH_MSGR_MDS_FRESH_KEY_FILE"))
	if err != nil {
		t.Fatal("read dedicated MDS fresh credential", err)
	}
	key, err := cephx.ParseKey(strings.TrimSpace(string(keyBytes)))
	if err != nil {
		t.Fatal("parse dedicated MDS fresh credential", err)
	}
	if key.Type() != wantClientKey {
		t.Fatalf("dedicated client key type %d differs from selected profile type %d", key.Type(), wantClientKey)
	}
	services := cephx.ServiceAuth | cephx.ServiceMgr | cephx.ServiceMDS
	auth, err := cephx.NewClient(identity, key, services)
	if err != nil {
		t.Fatal(err)
	}
	evidence := mdsFreshEvidence{Source: mdsFreshSource, Profile: os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE"), Identity: identity, ClientKeyType: key.Type(), ClientKeyProfile: clientKeyProfile, ServiceCipherProfile: serviceProfile, Target: target, ExpectedPeerRank: oracle.MDS.Rank, NativeDaemonGID: oracle.MDS.GID, RequestedServices: services, NativeStart: oracle,
		NativeIdentityMeaning: "native owned map identifies the daemon GID; ServerIdent identifies active rank 0; the MDS service key is shared and is not cryptographic proof of an individual daemon GID"}
	defer func() {
		data, err := json.Marshal(evidence)
		if err == nil && len(data) > 4<<20 {
			err = errors.New("MDS fresh public evidence exceeds output bound")
		}
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "mds-fresh-results.json"), data)
		}
		if err != nil {
			t.Error("preserve MDS fresh public evidence", err)
		}
	}()
	evidence.ConnectionMode = uint32(mode)
	monEndpoint := testcluster.PeerEndpoint("mon.a")
	monAddress, err := netip.ParseAddrPort(monEndpoint)
	if err != nil || monAddress.Port() == 0 {
		t.Fatal("fixture MON address is not numeric")
	}
	dial := testcluster.Dialer()
	evidence.MONDialAttempts++
	monConn, err := dial(ctx, "tcp", monEndpoint)
	if err != nil {
		t.Fatal("dial fixture MON once", err)
	}
	mon, err := session.Handshake(ctx, monConn, msgr.Address{Type: 2, Endpoint: monAddress}, 1, 0, session.MonAuth{Client: auth, ReplicatedPGV8: isReplicatedPGFixtureProfile()}, mode, mdsFreshLimit, 10*time.Second)
	if err != nil {
		t.Fatal("authenticate dedicated MDS fresh identity with MON", err)
	}
	defer mon.Conn.Close() // Keep this authenticated identity alive through MDS acceptance.
	evidence.GlobalID, evidence.MONFeatures = auth.GlobalID, mon.Features
	if auth.GlobalID == 0 || mon.GlobalID != auth.GlobalID || auth.Services() != services {
		t.Fatal("dedicated CephX service or GlobalID receipt differs")
	}
	authorizer, err := auth.Authorizer(cephx.ServiceMDS)
	if err != nil {
		t.Fatal("obtain parsed MDS service ticket", err)
	}
	evidence.MDSServiceKeyType = authorizer.Key().Type()
	if evidence.MDSServiceKeyType != wantServiceKey {
		t.Fatalf("MDS service ticket key type %d differs from selected profile type %d", evidence.MDSServiceKeyType, wantServiceKey)
	}
	evidence.Before, err = observeMDSFresh(ctx, dir, oracle, auth.GlobalID)
	if err != nil {
		t.Fatal("native session absence before MDS connection", err)
	}
	evidence.MDSDialAttempts++
	mdsConn, err := dial(ctx, "tcp", target.Endpoint.String())
	if err != nil {
		t.Fatal("dial owned active MDS once", err)
	}
	mds, err := session.HandshakeFreshMDS(ctx, mdsConn, target, oracle.MDS.Rank, session.ServiceAuth{Authorizer: authorizer}, mode, mdsFreshLimit, 10*time.Second)
	if err != nil {
		t.Fatal("authenticate fresh MDS Messenger connection", err)
	}
	defer func() { mds.Conn.Close(); evidence.TransportClosed = true }()
	evidence.MDSFeatures = mds.Features
	if mds.GlobalID != auth.GlobalID || mds.Features&^session.Features != 0 || mds.Features&session.RequiredFeatures != session.RequiredFeatures {
		t.Fatal("fresh MDS identity or fixed feature contract differs")
	}
	if err := mdsFreshKeepalive(ctx, mds, &evidence); err != nil {
		t.Fatal("fresh authenticated MDS keepalive", err)
	}
	// Observe while the Messenger connection remains open. Authentication and
	// keepalive must not create a CephFS SessionMap entry for this GlobalID.
	evidence.After, err = observeMDSFresh(ctx, dir, oracle, auth.GlobalID)
	if err != nil {
		t.Fatal("native session absence on live MDS connection", err)
	}
	if evidence.Before.RequestID == evidence.After.RequestID {
		t.Fatal("native before/after observations reused a controller receipt")
	}
}

func TestMDSFreshNativeAddressSchema(t *testing.T) {
	// These independent native entity_addr_t::dump shapes retain a textual
	// transport type. The private msgr.Address uses numeric type 2 only after
	// matching a v2 native address, not while decoding the fixture receipt.
	for _, sample := range []string{
		`{"mds":{"endpoint":"192.0.2.8:6800","nonce":123,"addresses":[{"type":"v2","addr":"192.0.2.8:6800","nonce":123},{"type":"v1","addr":"192.0.2.8:6801","nonce":123}]}}`,
		`{"mds":{"endpoint":"[fd00::8]:6800","nonce":456,"addresses":[{"type":"v2","addr":"[fd00::8]:6800","nonce":456}]}}`,
	} {
		var oracle nativeMDSFresh
		if err := json.Unmarshal([]byte(sample), &oracle); err != nil {
			t.Fatal("native textual address type cannot be decoded", err)
		}
		if len(oracle.MDS.Addresses) == 0 || oracle.MDS.Addresses[0].Type != "v2" || oracle.MDS.Addresses[0].Addr != oracle.MDS.Endpoint || oracle.MDS.Addresses[0].Nonce != oracle.MDS.Nonce {
			t.Fatal("native address identity changed")
		}
		if _, err := netip.ParseAddrPort(oracle.MDS.Endpoint); err != nil {
			t.Fatal("native address is not a numeric host:port", err)
		}
	}
}

func TestMDSFreshRequiresExplicitKeyProfiles(t *testing.T) {
	for _, tc := range []struct {
		profile string
		want    uint16
	}{{"aes", 1}, {"aes256k", 2}, {"", 0}, {"aes256", 0}, {"AES256K", 0}, {"unknown", 0}} {
		got, err := mdsFreshExpectedKeyType(tc.profile)
		if got != tc.want || (err != nil) != (tc.want == 0) {
			t.Fatal("key profile was silently substituted", tc.profile, got, err)
		}
	}
}

func TestMDSFreshRequiresExplicitWireModeMapping(t *testing.T) {
	for _, tc := range []struct {
		profile string
		want    session.ConnectionMode
	}{{"crc", 1}, {"secure", 2}, {"", 0}, {"CRC", 0}, {"unknown", 0}} {
		got, err := mdsFreshConnectionMode(tc.profile)
		if got != tc.want || (err != nil) != (tc.want == 0) {
			t.Fatal("test profile was not mapped to its exact wire mode", tc.profile, got, err)
		}
	}
}
