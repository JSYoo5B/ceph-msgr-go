package testcluster

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

// CRCMessageTapConn is a development-only observer for an explicitly selected
// CRC fixture. Arm only after the service handshake. Secure streams must never
// be passed to it. It forwards original I/O unchanged and retains only object
// request/reply messages, never authentication frames or connection secrets.
// These are actually transferred CRC frames, not an encoder projection.
type CRCMessageTapConn struct {
	net.Conn
	armed              atomic.Bool
	mu                 sync.Mutex
	outgoing, incoming []byte
	messages           []CRCObservedMessage
	err                error
}
type CRCObservedMessage struct {
	Direction string           `json:"direction"`
	Message   msgr.MessageData `json:"message"`
}

func (c *CRCMessageTapConn) Arm() { c.armed.Store(true) }
func (c *CRCMessageTapConn) Write(p []byte) (int, error) {
	armed := c.armed.Load()
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.capture("outgoing", p[:n], armed)
	}
	return n, err
}
func (c *CRCMessageTapConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.capture("incoming", p[:n], c.armed.Load())
	}
	return n, err
}
func (c *CRCMessageTapConn) capture(direction string, data []byte, armed bool) {
	if !armed {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return
	}
	pending := &c.outgoing
	if direction == "incoming" {
		pending = &c.incoming
	}
	if len(*pending)+len(data) > 1<<20 {
		c.err = errors.New("CRC object-message tap exceeds pending-byte bound")
		return
	}
	*pending = append(*pending, data...)
	for len(*pending) > 0 {
		input := bytes.NewReader(*pending)
		frame, err := msgr.NewReader(input, 1<<20).Read()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return
		}
		if err != nil {
			c.err = err
			return
		}
		consumed := len(*pending) - input.Len()
		if frame.Tag == msgr.Message {
			message, err := msgr.DecodeMessage(frame)
			if err != nil {
				c.err = err
				return
			}
			if message.Type == msgr.OSDOpMessage || message.Type == msgr.OSDOpReplyMessage {
				if len(c.messages) >= 128 {
					c.err = errors.New("CRC object-message tap exceeds message bound")
					return
				}
				message.Front = bytes.Clone(message.Front)
				message.Middle = bytes.Clone(message.Middle)
				message.Data = bytes.Clone(message.Data)
				c.messages = append(c.messages, CRCObservedMessage{direction, message})
			}
		}
		*pending = bytes.Clone((*pending)[consumed:])
	}
}
func (c *CRCMessageTapConn) Snapshot() ([]CRCObservedMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	records := append([]CRCObservedMessage(nil), c.messages...)
	for i := range records {
		records[i].Message.Front = bytes.Clone(records[i].Message.Front)
		records[i].Message.Middle = bytes.Clone(records[i].Message.Middle)
		records[i].Message.Data = bytes.Clone(records[i].Message.Data)
	}
	return records, c.err
}
