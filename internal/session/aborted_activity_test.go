package session

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

// Signal the read after a whole frame has been consumed. The session has
// finished handling the preceding read before it can enter this next read.
type completedFrameReader struct {
	input     io.Reader
	remaining int
	processed chan struct{}
	once      sync.Once
}

func (r *completedFrameReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		r.once.Do(func() { close(r.processed) })
	}
	n, err := r.input.Read(p)
	r.remaining -= n
	return n, err
}

// Assemble the late-aborted frame directly from the pinned rev1 layout.
// Neither the product frame writer nor its message encoder generates it.
func abortedActivityFrame(t *testing.T, secure bool, key, nonce []byte) []byte {
	t.Helper()
	preamble := make([]byte, 32)
	preamble[0], preamble[1] = byte(msgr.Message), 2
	binary.LittleEndian.PutUint32(preamble[2:], 41)
	binary.LittleEndian.PutUint16(preamble[6:], 8)
	binary.LittleEndian.PutUint32(preamble[8:], 1)
	binary.LittleEndian.PutUint16(preamble[12:], 8)
	binary.LittleEndian.PutUint32(preamble[28:], msgr.CRC(0, preamble[:28]))
	header, front := make([]byte, 41), []byte{0xff}
	if !secure {
		frame := append(preamble, header...)
		frame = binary.LittleEndian.AppendUint32(frame, msgr.CRC(^uint32(0), header))
		frame = append(frame, front...)
		frame = append(frame, 1) // FRAME_LATE_STATUS_ABORTED
		frame = binary.LittleEndian.AppendUint32(frame, msgr.CRC(^uint32(0), front))
		return append(frame, make([]byte, 8)...)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	first := make([]byte, 80)
	copy(first, preamble)
	copy(first[32:], header)
	frame := aead.Seal(nil, nonce, first, nil)
	next := append([]byte(nil), nonce...)
	binary.LittleEndian.PutUint64(next[4:], binary.LittleEndian.Uint64(next[4:])+1)
	tail := make([]byte, 32)
	tail[0], tail[16] = front[0], 1
	return aead.Seal(frame, next, tail, nil)
}

func TestAbortedFrameCountsAsActivityWithoutDispatch(t *testing.T) {
	for _, test := range []struct {
		name            string
		secure, corrupt bool
		integrityError  error
	}{
		{"CRC", false, false, nil},
		{"secure", true, false, nil},
		{"invalid authentication tag", true, true, msgr.ErrAuthentication},
		{"invalid tail CRC", false, true, msgr.ErrCRC},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, peer := net.Pipe()
			peer.SetWriteDeadline(time.Now().Add(3 * time.Second))
			key, nonce := make([]byte, 16), make([]byte, 12)
			frame := abortedActivityFrame(t, test.secure, key, nonce)
			if test.corrupt {
				if test.secure {
					frame[len(frame)-1] ^= 0x80
				} else {
					frame[len(frame)-14] ^= 0x80 // corrupt the actual front segment
				}
			}
			input := &completedFrameReader{input: client, remaining: len(frame), processed: make(chan struct{})}
			reader := msgr.NewReader(input, 0)
			if test.secure {
				if err := reader.EnableSecure(key, nonce); err != nil {
					t.Fatal(err)
				}
			}
			var dispatched atomic.Int32
			s := New(&Transport{Conn: client, Reader: reader, Writer: msgr.NewWriter(client, 0)}, Config{WriteTimeout: time.Second, KeepaliveInterval: time.Hour, KeepaliveTimeout: 2 * time.Hour}, func(msgr.MessageData) error {
				dispatched.Add(1)
				return nil
			}, nil)
			t.Cleanup(func() { s.Fail(ErrClosed); peer.Close(); s.Wait() })
			s.Start()
			previous := time.Now().Add(-time.Minute)
			s.lastReceive.Store(&previous)
			if err := msgr.WriteFull(peer, frame); err != nil {
				t.Fatal(err)
			}
			if test.corrupt {
				select {
				case <-s.Done():
				case <-time.After(3 * time.Second):
					t.Fatal("corrupt frame was accepted as an aborted frame")
				}
				if !s.lastReceive.Load().Equal(previous) || s.Err() != test.integrityError || dispatched.Load() != 0 || s.received.Load() != 0 {
					t.Fatal("corrupt frame refreshed activity, dispatched, or lost its cause", s.Err())
				}
				return
			}
			select {
			case <-input.processed:
			case <-time.After(3 * time.Second):
				t.Fatal("aborted frame interrupted the receive stream", s.Err())
			}
			if !s.lastReceive.Load().After(previous) {
				t.Fatal("complete aborted frame did not refresh receive activity")
			}
			if dispatched.Load() != 0 || s.received.Load() != 0 || s.Err() != nil {
				t.Fatal("aborted frame was dispatched, acknowledged, or failed the healthy stream")
			}
		})
	}
}
