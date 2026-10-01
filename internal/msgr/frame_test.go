package msgr

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/netip"
	"os"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

type vector struct {
	Name        string
	Tag         Tag
	Segments    []string
	CRC, Secure string
}

func decodeHex(t testing.TB, s string) []byte {
	t.Helper()
	p, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func fixtures(t testing.TB) []vector {
	t.Helper()
	p, err := os.ReadFile("testdata/frames.json")
	if err != nil {
		t.Fatal(err)
	}
	var v []vector
	if err = json.Unmarshal(p, &v); err != nil {
		t.Fatal(err)
	}
	return v
}
func testKeyNonce() ([]byte, []byte) {
	p := make([]byte, 28)
	for i := range p {
		p[i] = byte(i)
	}
	return p[:16], p[16:]
}

func TestIndependentFrameVectors(t *testing.T) {
	for _, v := range fixtures(t) {
		for _, secure := range []bool{false, true} {
			t.Run(v.Name+map[bool]string{true: "/secure", false: "/crc"}[secure], func(t *testing.T) {
				f := Frame{Tag: v.Tag}
				for _, s := range v.Segments {
					f.Segments = append(f.Segments, decodeHex(t, s))
				}
				var out bytes.Buffer
				w := NewWriter(&out, 0)
				want := decodeHex(t, v.CRC)
				if secure {
					want = decodeHex(t, v.Secure)
					k, n := testKeyNonce()
					if err := w.EnableSecure(k, n); err != nil {
						t.Fatal(err)
					}
				}
				if err := w.Write(f); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(out.Bytes(), want) {
					t.Fatalf("wire mismatch\ngot %x\nwant %x", out.Bytes(), want)
				}
				r := NewReader(bytes.NewReader(want), 0)
				if secure {
					k, n := testKeyNonce()
					if err := r.EnableSecure(k, n); err != nil {
						t.Fatal(err)
					}
				}
				got, err := r.Read()
				if err != nil {
					t.Fatal(err)
				}
				if got.Tag != f.Tag || len(got.Segments) != len(f.Segments) {
					t.Fatal("wrong frame shape")
				}
				for i := range f.Segments {
					if !bytes.Equal(got.Segments[i], f.Segments[i]) {
						t.Fatalf("segment %d", i)
					}
				}
			})
		}
	}
}

func TestCorruptAndOversizedFrames(t *testing.T) {
	v := fixtures(t)[3]
	for _, offset := range []int{0, 32, 80} {
		p := decodeHex(t, v.CRC)
		p[offset] ^= 0x80
		_, err := NewReader(bytes.NewReader(p), 0).Read()
		if !errors.Is(err, ErrCRC) {
			t.Fatalf("CRC corruption at %d: %v", offset, err)
		}
	}
	for _, offset := range []int{0, 95, 100} {
		p := decodeHex(t, v.Secure)
		p[offset] ^= 0x80
		r := NewReader(bytes.NewReader(p), 0)
		k, n := testKeyNonce()
		r.EnableSecure(k, n)
		if _, err := r.Read(); !errors.Is(err, ErrAuthentication) {
			t.Fatalf("GCM corruption at %d: %v", offset, err)
		}
	}
	p := decodeHex(t, v.CRC)
	binary.LittleEndian.PutUint32(p[2:], math.MaxUint32)
	binary.LittleEndian.PutUint32(p[28:], CRC(0, p[:28]))
	input := bytes.NewReader(p)
	_, err := NewReader(input, 128).Read()
	if !errors.Is(err, wire.ErrLimit) || input.Len() != len(p)-32 {
		t.Fatal("did not reject size before payload read")
	}
}

func TestAbortedFrameDoesNotBreakStream(t *testing.T) {
	v := fixtures(t)[3]
	p := decodeHex(t, v.CRC)
	p[len(p)-13] = 1
	p = append(p, decodeHex(t, fixtures(t)[0].CRC)...)
	r := NewReader(bytes.NewReader(p), 0)
	if _, err := r.Read(); !errors.Is(err, ErrAborted) {
		t.Fatal(err)
	}
	if f, err := r.Read(); err != nil || f.Tag != Ack {
		t.Fatal("stream lost after abort", err)
	}
}

type partialWriter struct{ writes int }

func (w *partialWriter) Write(p []byte) (int, error) {
	w.writes++
	return min(2, len(p)), io.ErrClosedPipe
}
func TestWriterPoisonedAfterPartialWrite(t *testing.T) {
	output := &partialWriter{}
	w := NewWriter(output, 0)
	k, n := testKeyNonce()
	w.EnableSecure(k, n)
	f := Frame{Tag: Ack, Segments: [][]byte{{1}}}
	for i := 0; i < 2; i++ {
		if !errors.Is(w.Write(f), io.ErrClosedPipe) {
			t.Fatal("missing write error")
		}
	}
	if output.writes != 1 {
		t.Fatal("reused partially written stream")
	}
}

func TestSecureNonceProgressionAndExhaustion(t *testing.T) {
	k, n := testKeyNonce()
	var out bytes.Buffer
	w := NewWriter(&out, 0)
	w.EnableSecure(k, n)
	f := Frame{Tag: Message, Segments: [][]byte{make([]byte, 105), {1, 2, 3}}}
	if err := w.Write(f); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(f); err != nil {
		t.Fatal(err)
	}
	r := NewReader(&out, 0)
	r.EnableSecure(k, n)
	for i := 0; i < 2; i++ {
		got, err := r.Read()
		if err != nil || !reflect.DeepEqual(got.Segments, f.Segments) {
			t.Fatal(err)
		}
	}
	s, _ := newGCM(k, n)
	binary.LittleEndian.PutUint64(s.nonce[4:], math.MaxUint64)
	if _, err := s.seal(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.seal(nil); !errors.Is(err, ErrNonce) {
		t.Fatal("nonce reused")
	}
}

func TestBannerFixtureAndFeatureRejection(t *testing.T) {
	b := Banner{Supported: Revision1, Required: Revision1}
	const want = "636570682076320a100001000000000000000100000000000000"
	if hex.EncodeToString(b.Encode()) != want {
		t.Fatal("banner fixture")
	}
	for _, peer := range []Banner{{Supported: 0}, {Supported: 3, Required: 2}, {Supported: 1, Required: 4}} {
		if _, err := ReadBanner(bytes.NewReader(peer.Encode()), b); !errors.Is(err, ErrFeatures) {
			t.Fatal("feature mismatch accepted")
		}
	}
}

func FuzzFrameReader(f *testing.F) {
	f.Add(make([]byte, 32))
	for _, v := range fixtures(f) {
		f.Add(decodeHex(f, v.CRC))
	}
	p, err := os.ReadFile("testdata/ceph-mon-hello-v20.2.4.bin")
	if err != nil {
		f.Fatal(err)
	}
	f.Add(p[26:])
	f.Fuzz(func(t *testing.T, p []byte) { NewReader(bytes.NewReader(p), 4096).Read() })
}

func FuzzSecureFrameReader(f *testing.F) {
	f.Add([]byte{})
	for _, v := range fixtures(f) {
		f.Add(decodeHex(f, v.Secure))
	}
	f.Fuzz(func(t *testing.T, p []byte) {
		r := NewReader(bytes.NewReader(p), 4096)
		key, nonce := testKeyNonce()
		if err := r.EnableSecure(key, nonce); err != nil {
			t.Fatal(err)
		}
		r.Read()
	})
}

func TestCephCapturedBannerAndHello(t *testing.T) {
	p, err := os.ReadFile("testdata/ceph-mon-hello-v20.2.4.bin")
	if err != nil {
		t.Fatal(err)
	}
	input := bytes.NewReader(p)
	banner, err := ReadBanner(input, Banner{Supported: 3, Required: Revision1})
	if err != nil || banner != (Banner{Supported: 3}) {
		t.Fatal("Ceph banner", banner, err)
	}
	r := NewReader(input, 4096)
	f, err := r.Read()
	if err != nil || f.Tag != Hello || len(f.Segments) != 1 {
		t.Fatal("Ceph HELLO frame", err)
	}
	d := wire.NewDecoder(f.Segments[0])
	role, address := d.U8(), DecodeAddress(d)
	// The endpoint oracle is the capture socket's independent getsockname,
	// recorded before authentication; no Go encoder generated this fixture.
	want := Address{Type: 2, Endpoint: netip.MustParseAddrPort("127.0.0.1:34716")}
	if err := d.Done(); err != nil || role != 1 || address != want {
		t.Fatal("Ceph HELLO identity", role, address, err)
	}
	if _, err := r.Read(); !errors.Is(err, io.EOF) {
		t.Fatal("unconsumed capture bytes", err)
	}
	var encoded bytes.Buffer
	if err := NewWriter(&encoded, 4096).Write(f); err != nil || !bytes.Equal(encoded.Bytes(), p[26:]) {
		t.Fatal("writer differs from daemon-generated HELLO", err)
	}
}
