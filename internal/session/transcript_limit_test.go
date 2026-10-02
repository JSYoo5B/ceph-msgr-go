package session

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestRecordingReaderLimitCannotDisappearAtFullRead(t *testing.T) {
	for _, cumulative := range []bool{false, true} {
		name := "single read"
		if cumulative {
			name = "cumulative reads"
		}
		t.Run(name, func(t *testing.T) {
			payload := bytes.Repeat([]byte{0x41}, (1<<20)+1)
			input := bytes.NewReader(payload)
			r := &recordingReader{Reader: input, active: true}
			remaining, recorded := len(payload), 0
			if cumulative {
				recorded = (1 << 20) - 1
				if n, err := io.ReadFull(r, make([]byte, recorded)); n != recorded || err != nil {
					t.Fatal("valid transcript prefix failed", n, err)
				}
				remaining -= recorded
			}
			accepted := (1 << 20) - recorded
			if n, err := io.ReadFull(r, make([]byte, remaining)); n != accepted || !errors.Is(err, wire.ErrLimit) || r.data.Len() != 1<<20 || input.Len() != 1 {
				t.Fatal("transcript limit was suppressed or exposed unrecorded bytes", n, err, r.data.Len())
			}
			// Disabling recording must not permit a poisoned handshake to
			// resume after exceeding its transcript's capacity.
			r.active = false
			for range 2 {
				if n, err := r.Read(make([]byte, 1)); n != 0 || !errors.Is(err, wire.ErrLimit) || input.Len() != 1 {
					t.Fatal("transcript failure was not sticky", n, err)
				}
			}
		})
	}
}

func TestRecordingReaderExactLimitAndInactiveReads(t *testing.T) {
	for _, active := range []bool{false, true} {
		length := 1 << 20
		if !active {
			length++
		}
		payload := bytes.Repeat([]byte{0x37}, length)
		r := &recordingReader{Reader: bytes.NewReader(payload), active: active}
		output := make([]byte, length)
		if n, err := io.ReadFull(r, output); n != length || err != nil || !bytes.Equal(output, payload) {
			t.Fatal("valid read was rejected or changed", active, n, err)
		}
		recorded := 0
		if active {
			recorded = length
		}
		if r.data.Len() != recorded {
			t.Fatal("recording state changed", active, r.data.Len())
		}
		want := error(io.EOF)
		if active {
			want = wire.ErrLimit
		}
		if n, err := r.Read(make([]byte, 1)); n != 0 || !errors.Is(err, want) {
			t.Fatal("read beyond capacity was misclassified", active, n, err)
		}
	}

	// Reaching capacity is valid when authentication finishes recording
	// before the next read. Later frame bytes are no longer transcript data.
	payload := bytes.Repeat([]byte{0x37}, (1<<20)+1)
	r := &recordingReader{Reader: bytes.NewReader(payload), active: true}
	if n, err := io.ReadFull(r, make([]byte, 1<<20)); n != 1<<20 || err != nil {
		t.Fatal("exact-limit phase could not finish", n, err)
	}
	r.active = false
	last := make([]byte, 1)
	if n, err := io.ReadFull(r, last); n != 1 || err != nil || last[0] != 0x37 || r.data.Len() != 1<<20 {
		t.Fatal("inactive phase could not follow an exact-limit transcript", n, err, r.data.Len())
	}
}

type dataWithErrorReader struct{ cause error }

func (r dataWithErrorReader) Read(p []byte) (int, error) { return copy(p, "data"), r.cause }

type dataWithErrorWriter struct{ cause error }

func (w dataWithErrorWriter) Write(p []byte) (int, error) { return min(4, len(p)), w.cause }

func TestRecordingPreservesLegitimateTransportCountsAndErrors(t *testing.T) {
	cause := errors.New("transport failure")
	r := &recordingReader{Reader: dataWithErrorReader{cause: cause}, active: true}
	if n, err := r.Read(make([]byte, 8)); n != 4 || err != cause || r.data.String() != "data" {
		t.Fatal("read transport count/error or transcript changed", n, err, r.data.String())
	}
	w := &recordingWriter{Writer: dataWithErrorWriter{cause: cause}, active: true}
	if n, err := w.Write([]byte("data followed by incomplete bytes")); n != 4 || err != cause || w.data.String() != "data" {
		t.Fatal("write transport count/error or transcript changed", n, err, w.data.String())
	}
}

func TestRecordingWriterLimitDoesNotTransmitOverflow(t *testing.T) {
	for _, cumulative := range []bool{false, true} {
		var output bytes.Buffer
		w := &recordingWriter{Writer: &output, active: true}
		recorded := 0
		if cumulative {
			recorded = 1 << 20 // Equality is permitted.
			if n, err := w.Write(make([]byte, recorded)); n != recorded || err != nil {
				t.Fatal("exact limit rejected", n, err)
			}
		}
		length := (1 << 20) + 1
		if cumulative {
			length = 1
		}
		if n, err := w.Write(make([]byte, length)); n != 0 || !errors.Is(err, wire.ErrLimit) || output.Len() != recorded || w.data.Len() != recorded {
			t.Fatal("overflow was transmitted or changed the transcript", n, err, output.Len(), w.data.Len())
		}
	}
	var output bytes.Buffer
	w := &recordingWriter{Writer: &output}
	if n, err := w.Write(make([]byte, (1<<20)+1)); n != (1<<20)+1 || err != nil || w.data.Len() != 0 {
		t.Fatal("inactive writer was limited or recorded", n, err, w.data.Len())
	}
}

func TestOversizedHelloTranscriptStopsBeforeAuthentication(t *testing.T) {
	client, peer := net.Pipe()
	peerDone := make(chan bool, 1)
	go func() {
		defer peer.Close()
		peer.SetDeadline(time.Now().Add(2 * time.Second))
		banner := msgr.Banner{Supported: 3, Required: 1}
		if _, err := msgr.ReadBanner(peer, banner); err != nil {
			peerDone <- false
			return
		}
		if err := msgr.WriteFull(peer, banner.Encode()); err != nil {
			peerDone <- false
			return
		}
		r := msgr.NewReader(peer, 2<<20)
		if _, err := r.Read(); err != nil {
			peerDone <- false
			return
		}
		hello := wire.Encoder{}
		hello.U8(1)
		msgr.Address{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.2:0")}.Encode(&hello)
		// Extend the length-delimited address envelope with ignored fields.
		// The framing and fields used by HELLO remain valid; the transcript
		// limit must reject it before any authentication payload is sent.
		extra := bytes.Repeat([]byte{0x47}, 1<<20)
		size := binary.LittleEndian.Uint32(hello.Data[4:8])
		binary.LittleEndian.PutUint32(hello.Data[4:8], size+uint32(len(extra)))
		hello.Raw(extra)
		if err := msgr.NewWriter(peer, 2<<20).Write(msgr.Frame{Tag: msgr.Hello, Segments: [][]byte{hello.Data}}); err != nil {
			peerDone <- false
			return
		}
		f, err := r.Read()
		peerDone <- err == nil && f.Tag == msgr.AuthRequest
	}()
	_, err := Handshake(context.Background(), client, msgr.Address{Type: 2}, 1, 0, fixtureAuthData(), 2<<20, time.Second)
	authSent := <-peerDone
	if !errors.Is(err, wire.ErrLimit) || authSent {
		t.Fatal("oversized HELLO advanced past the transcript limit", err, authSent)
	}
}
