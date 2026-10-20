package msgr

import (
	"bytes"
	"errors"
	"testing"
)

// A frame written with compression enabled round-trips through the real
// preamble/CRC path and comes back byte-identical after decompression, and a
// large compressible frame actually goes onwire compressed (flag set, smaller).
func TestFrameCompressionRoundTrip(t *testing.T) {
	big := bytes.Repeat([]byte("ceph messenger compression payload\x00\xff"), 400)
	small := []byte("tiny")
	cases := []struct {
		name     string
		frame    Frame
		wantComp bool
	}{
		{"large single segment", Frame{Tag: Message, Segments: [][]byte{big}}, true},
		{"below min size", Frame{Tag: Message, Segments: [][]byte{small}}, false},
		{"multi segment", Frame{Tag: Message, Segments: [][]byte{big, []byte("seg1 content here"), bytes.Repeat([]byte("z"), 2000)}}, true},
	}
	for _, method := range SupportedCompressionMethods {
		for _, tc := range cases {
			var buf bytes.Buffer
			w := NewWriter(&buf, 1<<20)
			if err := w.EnableCompression(method, 512); err != nil {
				t.Fatal(err)
			}
			if err := w.Write(tc.frame); err != nil {
				t.Fatalf("method %d %s write: %v", method, tc.name, err)
			}
			onwire := buf.Bytes()
			if len(onwire) < 32 {
				t.Fatalf("method %d %s: short onwire", method, tc.name)
			}
			gotComp := onwire[26]&FrameEarlyDataCompressed != 0
			if gotComp != tc.wantComp {
				t.Fatalf("method %d %s: compressed flag %v, want %v", method, tc.name, gotComp, tc.wantComp)
			}
			if tc.wantComp {
				uncompressed := 0
				for _, s := range tc.frame.Segments {
					uncompressed += len(s)
				}
				if len(onwire) >= uncompressed {
					t.Fatalf("method %d %s: onwire %d not smaller than payload %d", method, tc.name, len(onwire), uncompressed)
				}
			}
			r := NewReader(&buf, 1<<20)
			if err := r.EnableCompression(method); err != nil {
				t.Fatal(err)
			}
			got, err := r.Read()
			if err != nil {
				t.Fatalf("method %d %s read: %v", method, tc.name, err)
			}
			want := tc.frame.Segments[:trailingSegments(tc.frame.Segments)]
			if len(got.Segments) != len(want) {
				t.Fatalf("method %d %s: %d segments, want %d", method, tc.name, len(got.Segments), len(want))
			}
			for i := range want {
				if !bytes.Equal(got.Segments[i], want[i]) {
					t.Fatalf("method %d %s: segment %d differs", method, tc.name, i)
				}
			}
		}
	}
}

// A reader that did not negotiate compression rejects a frame flagged
// compressed instead of returning raw bytes.
func TestFrameCompressionUnexpected(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1<<20)
	if err := w.EnableCompression(CompAlgZstd, 1); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(Frame{Tag: Message, Segments: [][]byte{bytes.Repeat([]byte("x"), 1000)}}); err != nil {
		t.Fatal(err)
	}
	r := NewReader(&buf, 1<<20) // no EnableCompression
	if _, err := r.Read(); !errors.Is(err, ErrFrame) {
		t.Fatalf("expected a framing error for an unexpected compressed frame, got %v", err)
	}
}

// With compression negotiated, an uncompressed frame (below min size, flag
// clear) still round-trips: the receiver must handle a mix.
func TestFrameCompressionMixed(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1<<20)
	w.EnableCompression(CompAlgSnappy, 512)
	frames := []Frame{
		{Tag: Message, Segments: [][]byte{[]byte("short")}},
		{Tag: Message, Segments: [][]byte{bytes.Repeat([]byte("compress me "), 100)}},
	}
	for _, f := range frames {
		if err := w.Write(f); err != nil {
			t.Fatal(err)
		}
	}
	r := NewReader(&buf, 1<<20)
	r.EnableCompression(CompAlgSnappy)
	for i, f := range frames {
		got, err := r.Read()
		if err != nil || !bytes.Equal(got.Segments[0], f.Segments[0]) {
			t.Fatalf("frame %d mixed round trip: %v", i, err)
		}
	}
}
