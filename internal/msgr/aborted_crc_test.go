package msgr

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
)

type crcLateStatusFixture struct {
	data                       []byte
	segments                   [][]byte
	payloadOffsets, crcOffsets []int
}

// Assemble the rev1 CRC layout directly. The pinned Tentacle decoder validates
// actual tail segments before late status and ignores unused CRC slots:
// src/msg/async/frames_v2.cc at 7f793731f1b39eb4f465e960113d2363c311b964.
func crcLateStatusFrame(n int, status byte) crcLateStatusFixture {
	f := crcLateStatusFixture{segments: [][]byte{make([]byte, 41), {1, 2, 3}, {4, 5}, {6, 7, 8, 9}}[:n]}
	f.data = make([]byte, 32)
	f.data[0], f.data[1] = byte(Message), byte(n)
	for i, seg := range f.segments {
		binary.LittleEndian.PutUint32(f.data[2+i*6:], uint32(len(seg)))
		binary.LittleEndian.PutUint16(f.data[6+i*6:], 8)
	}
	binary.LittleEndian.PutUint32(f.data[28:], CRC(0, f.data[:28]))
	for i, seg := range f.segments {
		f.payloadOffsets = append(f.payloadOffsets, len(f.data))
		f.data = append(f.data, seg...)
		if i == 0 {
			f.crcOffsets = append(f.crcOffsets, len(f.data))
			f.data = binary.LittleEndian.AppendUint32(f.data, CRC(^uint32(0), seg))
		}
	}
	f.data = append(f.data, status)
	for i := 1; i < 4; i++ {
		var value uint32
		if i < n {
			value = CRC(^uint32(0), f.segments[i])
			f.crcOffsets = append(f.crcOffsets, len(f.data))
		}
		f.data = binary.LittleEndian.AppendUint32(f.data, value)
	}
	return f
}

func TestCRCIntegrityPrecedesAbortedStatus(t *testing.T) {
	for n := 2; n <= 4; n++ {
		for segment := 0; segment < n; segment++ {
			for _, payload := range []bool{false, true} {
				t.Run(fmt.Sprintf("segments=%d/segment=%d/payload=%v", n, segment, payload), func(t *testing.T) {
					f := crcLateStatusFrame(n, 1)
					offset := f.crcOffsets[segment]
					if payload {
						offset = f.payloadOffsets[segment]
					}
					f.data[offset] ^= 0x80
					input := bytes.NewReader(append(f.data, crcLateStatusFrame(2, 0x0e).data...))
					r := NewReader(input, 4096)
					if _, err := r.Read(); !errors.Is(err, ErrCRC) {
						t.Fatal("aborted status suppressed segment corruption", err)
					}
					remaining := input.Len()
					if _, err := r.Read(); !errors.Is(err, ErrCRC) || input.Len() != remaining {
						t.Fatal("corrupt frame did not poison the reader", err, remaining, input.Len())
					}
				})
			}
		}
	}
}

func TestVerifiedAbortedCRCFramePreservesStream(t *testing.T) {
	for n := 2; n <= 4; n++ {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			aborted, complete := crcLateStatusFrame(n, 1), crcLateStatusFrame(n, 0x0e)
			input := bytes.NewReader(append(aborted.data, complete.data...))
			r := NewReader(input, 4096)
			if _, err := r.Read(); !errors.Is(err, ErrAborted) {
				t.Fatal("valid aborted frame was rejected", err)
			}
			got, err := r.Read()
			if err != nil || got.Tag != Message || len(got.Segments) != n || input.Len() != 0 {
				t.Fatal("valid abort disrupted the next complete frame", got.Tag, err, input.Len())
			}
			for i, want := range complete.segments {
				if !bytes.Equal(got.Segments[i], want) {
					t.Fatal("next complete frame changed", i)
				}
			}
		})
	}
}

func TestUnusedCRCSlotsDoNotAffectLateStatus(t *testing.T) {
	for n := 2; n <= 3; n++ {
		for _, status := range []byte{1, 0x0e} {
			t.Run(fmt.Sprintf("segments=%d/status=%x", n, status), func(t *testing.T) {
				f := crcLateStatusFrame(n, status)
				for segment := n; segment < 4; segment++ {
					offset := len(f.data) - 12 + (segment-1)*4
					binary.LittleEndian.PutUint32(f.data[offset:], 0xc0ffee00+uint32(segment))
				}
				input := bytes.NewReader(f.data)
				got, err := NewReader(input, 4096).Read()
				if status == 1 {
					if !errors.Is(err, ErrAborted) {
						t.Fatal("unused CRC slots changed aborted status", err)
					}
				} else if err != nil || len(got.Segments) != n {
					t.Fatal("unused CRC slots rejected a complete frame", err)
				}
				if input.Len() != 0 {
					t.Fatal("frame epilogue was not fully consumed", input.Len())
				}
			})
		}
	}
}
