package testcluster

import (
	"bytes"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

// StreamRelay forwards opaque bytes through a net.Pipe, preserving its real
// deadlines. Tests can pause one large outgoing write and one incoming read,
// then resume without losing bytes or interpreting authentication material.
// Close releases both gates; Wait joins the development relay workers.
type StreamRelay struct {
	net.Conn
	peer, remote              net.Conn
	WriteBlocked, ReadBlocked <-chan struct{}
	writeBlocked, readBlocked chan struct{}
	writeResume, readResume   chan struct{}
	closed                    chan struct{}
	writePause, readPause     atomic.Bool
	writeOnce, readOnce       sync.Once
	closeOnce                 sync.Once
	wg                        sync.WaitGroup
	ReadChunks, WriteChunks   atomic.Uint64
}

func NewStreamRelay(remote net.Conn) *StreamRelay {
	client, peer := net.Pipe()
	p := &StreamRelay{
		Conn: client, peer: peer, remote: remote,
		writeBlocked: make(chan struct{}), readBlocked: make(chan struct{}),
		writeResume: make(chan struct{}), readResume: make(chan struct{}), closed: make(chan struct{}),
	}
	p.WriteBlocked, p.ReadBlocked = p.writeBlocked, p.readBlocked
	p.wg.Add(2)
	go p.send()
	go p.receive()
	return p
}

func (p *StreamRelay) LocalAddr() net.Addr  { return p.remote.LocalAddr() }
func (p *StreamRelay) RemoteAddr() net.Addr { return p.remote.RemoteAddr() }

// Fragment reads across preamble, authentication tag and payload boundaries.
func (p *StreamRelay) Read(b []byte) (int, error) {
	n, err := p.Conn.Read(b[:min(len(b), 37)])
	if n > 0 {
		p.ReadChunks.Add(1)
	}
	return n, err
}

func (p *StreamRelay) ArmWritePause() { p.writePause.Store(true) }
func (p *StreamRelay) ArmReadPause()  { p.readPause.Store(true) }
func (p *StreamRelay) ResumeWrite()   { p.writeOnce.Do(func() { close(p.writeResume) }) }
func (p *StreamRelay) ResumeRead()    { p.readOnce.Do(func() { close(p.readResume) }) }
func (p *StreamRelay) Wait()          { p.wg.Wait() }

func (p *StreamRelay) send() {
	defer p.wg.Done()
	defer p.Close()
	buffer := make([]byte, 4096)
	for {
		n, err := p.peer.Read(buffer)
		for sent := 0; sent < n; {
			// Split writes without violating net.Conn's short-write contract.
			end := min(sent+257, n)
			written, writeErr := p.remote.Write(buffer[sent:end])
			p.WriteChunks.Add(1)
			if writeErr != nil || written != end-sent {
				return
			}
			sent = end
		}
		if n == len(buffer) && p.writePause.CompareAndSwap(true, false) {
			// A prefix reached the server, but the bulk frame is incomplete.
			// Small ACKs and probes cannot trigger this gate.
			close(p.writeBlocked)
			select {
			case <-p.writeResume:
			case <-p.closed:
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *StreamRelay) receive() {
	defer p.wg.Done()
	defer p.Close()
	buffer := make([]byte, 4096)
	for {
		n, err := p.remote.Read(buffer)
		if n > 0 {
			if p.readPause.CompareAndSwap(true, false) {
				close(p.readBlocked)
				select {
				case <-p.readResume:
				case <-p.closed:
					return
				}
			}
			if _, writeErr := io.CopyN(p.peer, bytes.NewReader(buffer[:n]), int64(n)); writeErr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *StreamRelay) Close() error {
	p.closeOnce.Do(func() {
		close(p.closed)
		p.Conn.Close()
		p.peer.Close()
		p.remote.Close()
	})
	return nil
}
