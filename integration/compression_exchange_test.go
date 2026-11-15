package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"sync"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// compressionTap copies what one test client writes to and reads from every
// connection it dials, per connection and direction. It is test-only and
// changes no byte. A CRC-mode connection carries its frames in the clear, so
// the copies can be read back as banners and frames afterwards, however the
// socket split or joined them.
type compressionTap struct {
	mu    sync.Mutex
	conns []*compressionTapConn
}

type compressionTapConn struct {
	net.Conn
	tap      *compressionTap
	endpoint string
	out, in  []byte
	closed   bool
}

func (t *compressionTap) dialer(dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return conn, err
		}
		c := &compressionTapConn{Conn: conn, tap: t, endpoint: endpoint}
		t.mu.Lock()
		t.conns = append(t.conns, c)
		t.mu.Unlock()
		return c, nil
	}
}

func (c *compressionTapConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.tap.mu.Lock()
	c.out = append(c.out, p[:n]...)
	c.tap.mu.Unlock()
	return n, err
}

func (c *compressionTapConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.tap.mu.Lock()
	c.in = append(c.in, p[:n]...)
	c.tap.mu.Unlock()
	return n, err
}

func (c *compressionTapConn) Close() error {
	c.tap.mu.Lock()
	c.closed = true
	c.tap.mu.Unlock()
	return c.Conn.Close()
}

// compressionTapped is the copy of one connection at the time of the call.
type compressionTapped struct {
	endpoint string
	closed   bool
	out, in  []byte
}

func (t *compressionTap) connections() []compressionTapped {
	t.mu.Lock()
	defer t.mu.Unlock()
	var all []compressionTapped
	for _, c := range t.conns {
		all = append(all, compressionTapped{c.endpoint, c.closed, slices.Clone(c.out), slices.Clone(c.in)})
	}
	return all
}

// compressionExchange is what the two byte streams of one CRC-mode connection
// show about compression negotiation. It keeps tags and the decoded fields of
// the banner, CompressionRequest and CompressionDone only: no authentication
// payload, address or message is retained.
type compressionExchange struct {
	// peerSupported is the peer banner's supported feature mask.
	peerBanner    bool
	peerSupported uint64
	// localSupported is the mask this client sent.
	localBanner    bool
	localSupported uint64

	// request is the client's CompressionRequest (tag 21).
	request        bool
	requestEnabled bool
	requestMethods []uint32
	requestAfter   msgr.Tag // the client's frame before it

	// done is the peer's CompressionDone (tag 22).
	done        bool
	doneEnabled bool
	doneMethod  uint32
	doneAfter   msgr.Tag // the peer's frame before it

	// The frames of each direction after its negotiation frame, and how
	// many preambles of the direction carried the compressed-frame flag.
	laterOut, laterIn, compressedOut, compressedIn int

	// The message types of each direction, in order, and whether each
	// followed the direction's negotiation frame.
	messagesOut, messagesIn []compressionMessage

	// outEnd and inEnd say why reading a direction stopped: io.EOF at a
	// frame boundary, io.ErrUnexpectedEOF inside a frame that had not been
	// received in full, or a real framing error.
	outEnd, inEnd error
}

type compressionMessage struct {
	kind       uint16
	negotiated bool
}

// The limit of one recorded frame segment. A map can be large; the copies
// are bounded by what the test client actually exchanged.
const compressionTapFrameLimit = 1 << 28

// readCompressionExchange reads back the two directions of one connection.
// It reads each direction independently, so nothing depends on how the
// directions interleaved, and stops a direction at its first incomplete
// frame.
func readCompressionExchange(out, in []byte) compressionExchange {
	var x compressionExchange
	x.localSupported, x.localBanner, x.request, x.requestAfter, x.requestEnabled, x.requestMethods, x.laterOut, x.compressedOut, x.messagesOut, x.outEnd =
		readCompressionDirection(out, msgr.CompressionRequest)
	var method []uint32
	x.peerSupported, x.peerBanner, x.done, x.doneAfter, x.doneEnabled, method, x.laterIn, x.compressedIn, x.messagesIn, x.inEnd =
		readCompressionDirection(in, msgr.CompressionDone)
	if len(method) == 1 {
		x.doneMethod = method[0]
	}
	return x
}

func readCompressionDirection(stream []byte, subject msgr.Tag) (supported uint64, banner, found bool, after msgr.Tag, enabled bool, methods []uint32, later, compressed int, messages []compressionMessage, end error) {
	// The banner: the prefix, a 16-bit length of 16, then the supported
	// and required masks.
	const head = len(msgr.BannerPrefix) + 2
	if len(stream) < head+16 {
		return 0, false, false, 0, false, nil, 0, 0, nil, io.ErrUnexpectedEOF
	}
	if !bytes.HasPrefix(stream, []byte(msgr.BannerPrefix)) || binary.LittleEndian.Uint16(stream[len(msgr.BannerPrefix):]) != 16 {
		return 0, false, false, 0, false, nil, 0, 0, nil, errors.New("not a msgr2 banner")
	}
	supported, banner = binary.LittleEndian.Uint64(stream[head:]), true
	rest := stream[head+16:]
	var last msgr.Tag
	for len(rest) > 0 {
		// The compressed-frame flag is the flags byte of a preamble whose
		// own CRC holds. A plain reader refuses such a frame, and a
		// snapshot may end inside its body, so the complete preamble alone
		// counts.
		flagged := len(rest) >= 32 && rest[26]&msgr.FrameEarlyDataCompressed != 0 &&
			msgr.CRC(0, rest[:28]) == binary.LittleEndian.Uint32(rest[28:])
		input := bytes.NewReader(rest)
		f, err := msgr.NewReader(input, compressionTapFrameLimit).Read()
		if err != nil {
			if errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				// A frame cut inside its preamble or body.
				err = io.ErrUnexpectedEOF
			}
			if flagged {
				compressed++
			}
			return supported, banner, found, after, enabled, methods, later, compressed, messages, err
		}
		rest = rest[len(rest)-input.Len():]
		if flagged {
			compressed++
		}
		if f.Tag == msgr.Message {
			// The type field of the message header: after the sequence
			// and the transaction ID.
			if len(f.Segments) == 0 || len(f.Segments[0]) < 18 {
				return supported, banner, found, after, enabled, methods, later, compressed, messages, errors.New("a message frame without a header")
			}
			messages = append(messages, compressionMessage{binary.LittleEndian.Uint16(f.Segments[0][16:]), found})
		}
		switch {
		case found:
			later++
		case f.Tag == subject:
			found, after = true, last
			if len(f.Segments) != 1 {
				return supported, banner, found, after, false, nil, later, compressed, messages, fmt.Errorf("tag %d with %d segments", subject, len(f.Segments))
			}
			d := wire.NewDecoder(f.Segments[0])
			enabled = d.Bool()
			if subject == msgr.CompressionRequest {
				for n := d.Count(4, 64); n > 0; n-- {
					methods = append(methods, d.U32())
				}
			} else {
				methods = []uint32{d.U32()}
			}
			if err := d.Done(); err != nil {
				return supported, banner, found, after, enabled, methods, later, compressed, messages, fmt.Errorf("tag %d payload: %w", subject, err)
			}
		}
		last = f.Tag
	}
	return supported, banner, found, after, enabled, methods, later, compressed, messages, io.EOF
}

// declinedOffer reports why the exchange is not the one this client expects
// of a Tentacle daemon on a CRC-mode connection: both banners carry the
// compression bit, the client then offers zstd and snappy in that order
// right after its AUTH_SIGNATURE, the daemon answers right after its own
// AUTH_SIGNATURE with compression disabled and method 0, and no later frame
// of either direction is compressed. An empty result means every one of
// those observations is present on this one connection.
func (x compressionExchange) declinedOffer() []string {
	var missing []string
	add := func(format string, args ...any) { missing = append(missing, fmt.Sprintf(format, args...)) }
	switch {
	case !x.peerBanner:
		add("no peer banner (%v)", x.inEnd)
	case x.peerSupported&msgr.Compression == 0:
		add("the peer banner supports %#x, without the compression bit", x.peerSupported)
	}
	switch {
	case !x.localBanner:
		add("no client banner (%v)", x.outEnd)
	case x.localSupported&msgr.Compression == 0:
		add("the client banner supports %#x, without the compression bit", x.localSupported)
	}
	switch {
	case !x.request:
		add("no CompressionRequest from the client (%v)", x.outEnd)
	case !x.requestEnabled || !slices.Equal(x.requestMethods, []uint32{uint32(msgr.CompAlgZstd), uint32(msgr.CompAlgSnappy)}):
		add("the CompressionRequest was enabled=%v methods=%v, not an offer of zstd then snappy", x.requestEnabled, x.requestMethods)
	case x.requestAfter != msgr.AuthSignature:
		add("the CompressionRequest followed the client's tag %d, not its AUTH_SIGNATURE", x.requestAfter)
	}
	switch {
	case !x.done:
		add("no CompressionDone from the peer (%v)", x.inEnd)
	case x.doneEnabled || x.doneMethod != uint32(msgr.CompAlgNone):
		add("the CompressionDone was enabled=%v method=%d, not a refusal", x.doneEnabled, x.doneMethod)
	case x.doneAfter != msgr.AuthSignature:
		add("the CompressionDone followed the peer's tag %d, not its AUTH_SIGNATURE", x.doneAfter)
	}
	if x.compressedOut != 0 || x.compressedIn != 0 {
		add("%d sent and %d received frames carried the compressed-frame flag", x.compressedOut, x.compressedIn)
	}
	for direction, end := range map[string]error{"sent": x.outEnd, "received": x.inEnd} {
		if end != nil && !errors.Is(end, io.EOF) && !errors.Is(end, io.ErrUnexpectedEOF) {
			add("the %s stream is not a CRC-mode frame stream: %v", direction, end)
		}
	}
	slices.Sort(missing)
	return missing
}

// servedCommand finds, among the recorded connections of one daemon role, the
// one connection whose own bytes carry the command message and its reply,
// and reports why that connection does not show a refused offer followed by
// that command. Open or closed makes no difference: the command and the
// reply are read from the recording itself. A command or reply on more than
// one connection, or a reply without its command on the same connection,
// leaves the serving connection ambiguous and is reported, so an idle
// replacement with a complete negotiation cannot stand in for it.
func servedCommand(recorded []compressionTapped, selects func(string) bool, command, reply uint16) (index int, exchange compressionExchange, problems []string) {
	count := func(messages []compressionMessage, kind uint16) (all, negotiated int) {
		for _, m := range messages {
			if m.kind == kind {
				all++
				if m.negotiated {
					negotiated++
				}
			}
		}
		return all, negotiated
	}
	index = -1
	carried := 0
	for i, c := range recorded {
		if !selects(c.endpoint) {
			continue
		}
		x := readCompressionExchange(c.out, c.in)
		commands, _ := count(x.messagesOut, command)
		replies, _ := count(x.messagesIn, reply)
		if commands == 0 && replies == 0 {
			continue
		}
		carried++
		index, exchange = i, x
	}
	if carried != 1 {
		return -1, compressionExchange{}, []string{fmt.Sprintf("%d connections carried message type %d or %d; exactly one must have served the command", carried, command, reply)}
	}
	problems = exchange.declinedOffer()
	if commands, negotiated := count(exchange.messagesOut, command); commands == 0 || negotiated != commands {
		problems = append(problems, fmt.Sprintf("%d commands of type %d were sent, %d of them after the CompressionRequest", commands, command, negotiated))
	}
	if replies, negotiated := count(exchange.messagesIn, reply); replies == 0 || negotiated != replies {
		problems = append(problems, fmt.Sprintf("%d replies of type %d were received, %d of them after the CompressionDone", replies, reply, negotiated))
	}
	return index, exchange, problems
}

func (x compressionExchange) String() string {
	return fmt.Sprintf("peer banner supported=%#x; client banner supported=%#x; tag 21 present=%v enabled=%v methods=%v after tag %d; tag 22 present=%v enabled=%v method=%d after tag %d; later frames sent=%d received=%d, compressed sent=%d received=%d",
		x.peerSupported, x.localSupported, x.request, x.requestEnabled, x.requestMethods, x.requestAfter,
		x.done, x.doneEnabled, x.doneMethod, x.doneAfter, x.laterOut, x.laterIn, x.compressedOut, x.compressedIn)
}

// The reader finds the banner bit, the offer and the refusal of one
// connection whatever the socket did with the bytes, and each missing or
// different observation is reported.
func TestCompressionExchangeReadsOneConnection(t *testing.T) {
	banner := func(supported uint64) []byte {
		return msgr.Banner{Supported: supported, Required: msgr.Revision1}.Encode()
	}
	frames := func(t *testing.T, list ...msgr.Frame) []byte {
		t.Helper()
		var b bytes.Buffer
		w := msgr.NewWriter(&b, 0)
		for _, f := range list {
			if err := w.Write(f); err != nil {
				t.Fatal(err)
			}
		}
		return b.Bytes()
	}
	one := func(tag msgr.Tag, payload ...byte) msgr.Frame {
		return msgr.Frame{Tag: tag, Segments: [][]byte{payload}}
	}
	offer := func(enabled bool, methods ...uint32) msgr.Frame {
		e := wire.Encoder{}
		if enabled {
			e.U8(1)
		} else {
			e.U8(0)
		}
		e.U32(uint32(len(methods)))
		for _, m := range methods {
			e.U32(m)
		}
		return msgr.Frame{Tag: msgr.CompressionRequest, Segments: [][]byte{e.Data}}
	}
	answer := func(enabled bool, method uint32) msgr.Frame {
		e := wire.Encoder{}
		if enabled {
			e.U8(1)
		} else {
			e.U8(0)
		}
		e.U32(method)
		return msgr.Frame{Tag: msgr.CompressionDone, Segments: [][]byte{e.Data}}
	}
	both := msgr.Revision1 | msgr.Compression
	message := msgr.Frame{Tag: msgr.Message, Segments: [][]byte{bytes.Repeat([]byte{7}, 41), {1, 2}, nil, bytes.Repeat([]byte{9}, 600)}}
	// The handshake of a client, as far as tags go, with opaque payloads.
	sent := func(t *testing.T, supported uint64, request ...msgr.Frame) []byte {
		list := []msgr.Frame{one(msgr.Hello, 1), one(msgr.AuthRequest, 2), one(msgr.AuthRequestMore, 3), one(msgr.AuthSignature, 4)}
		list = append(list, request...)
		list = append(list, one(msgr.ClientIdent, 5), message, one(msgr.Keepalive, 6))
		return append(banner(supported), frames(t, list...)...)
	}
	received := func(t *testing.T, supported uint64, done ...msgr.Frame) []byte {
		list := []msgr.Frame{one(msgr.Hello, 1), one(msgr.AuthReplyMore, 2), one(msgr.AuthDone, 3), one(msgr.AuthSignature, 4)}
		list = append(list, done...)
		list = append(list, one(msgr.ServerIdent, 5), message)
		return append(banner(supported), frames(t, list...)...)
	}
	zstd, snappy := uint32(msgr.CompAlgZstd), uint32(msgr.CompAlgSnappy)
	out, in := sent(t, both, offer(true, zstd, snappy)), received(t, both, answer(false, 0))

	t.Run("a declined offer is read in full", func(t *testing.T) {
		x := readCompressionExchange(out, in)
		if problems := x.declinedOffer(); len(problems) != 0 {
			t.Fatal(problems, x)
		}
		if x.laterOut != 3 || x.laterIn != 2 || !errors.Is(x.outEnd, io.EOF) || errors.Is(x.outEnd, io.ErrUnexpectedEOF) {
			t.Fatal("later frames", x, x.outEnd, x.inEnd)
		}
	})
	t.Run("the socket may split and join the bytes", func(t *testing.T) {
		// The client writes whole and reads one byte at a time; the peer
		// writes one byte at a time. The copies must equal the streams.
		tap := &compressionTap{}
		client, server := net.Pipe()
		conn, err := tap.dialer(func(context.Context, string, string) (net.Conn, error) { return client, nil })(context.Background(), "tcp", "192.0.2.1:3300")
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 2)
		go func() {
			got, err := io.ReadAll(server)
			if err == nil && !bytes.Equal(got, out) {
				err = errors.New("the peer received other bytes")
			}
			done <- err
		}()
		go func() {
			for i := range in {
				if _, err := server.Write(in[i : i+1]); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()
		// Cut inside the first frame, the copy concludes nothing.
		const cut = 40
		if _, err := conn.Write(out[:cut]); err != nil {
			t.Fatal(err)
		}
		partial := readCompressionExchange(tap.connections()[0].out, nil)
		if !errors.Is(partial.outEnd, io.ErrUnexpectedEOF) || partial.request {
			t.Fatal("a cut sent stream", partial, partial.outEnd)
		}
		if _, err := conn.Write(out[cut:]); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, 0, len(in))
		buffer := make([]byte, 1)
		for len(got) < len(in) {
			n, err := conn.Read(buffer)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, buffer[:n]...)
		}
		conn.Close()
		for i := 0; i < 2; i++ {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
		recorded := tap.connections()
		if len(recorded) != 1 || recorded[0].endpoint != "192.0.2.1:3300" || !recorded[0].closed || !bytes.Equal(recorded[0].out, out) || !bytes.Equal(recorded[0].in, in) || !bytes.Equal(got, in) {
			t.Fatal("the tap changed or lost bytes")
		}
		if problems := readCompressionExchange(recorded[0].out, recorded[0].in).declinedOffer(); len(problems) != 0 {
			t.Fatal(problems)
		}
	})
	t.Run("every missing or different observation is reported", func(t *testing.T) {
		compressed := func(stream []byte) []byte {
			// Set the flag on the last frame's preamble the way a
			// compressing writer would mark it; the frame is then not a
			// plain frame any more.
			tail := frames(t, one(msgr.Keepalive, 6))
			marked := slices.Clone(stream)
			preamble := marked[len(marked)-len(tail):]
			preamble[26] |= msgr.FrameEarlyDataCompressed
			binary.LittleEndian.PutUint32(preamble[28:], msgr.CRC(0, preamble[:28]))
			return marked
		}
		for name, tc := range map[string]struct {
			out, in []byte
			want    int
		}{
			"the peer banner lacks the bit":       {out, received(t, msgr.Revision1, answer(false, 0)), 1},
			"the client banner lacks the bit":     {sent(t, msgr.Revision1, offer(true, zstd, snappy)), in, 1},
			"the client offered nothing":          {sent(t, both, offer(false)), in, 1},
			"the client offered another order":    {sent(t, both, offer(true, snappy, zstd)), in, 1},
			"the client sent no request":          {sent(t, both), in, 1},
			"the peer sent no answer":             {out, received(t, both), 1},
			"the peer enabled compression":        {out, received(t, both, answer(true, zstd)), 1},
			"the peer named a method but refused": {out, received(t, both, answer(false, snappy)), 1},
			"the request came before the signature": {append(banner(both), frames(t, one(msgr.Hello, 1), offer(true, zstd, snappy),
				one(msgr.AuthSignature, 4))...), in, 1},
			"only the sent direction":           {out, nil, 2},
			"only the received direction":       {nil, in, 2},
			"the received stream stops early":   {out, in[:60], 1},
			"a later sent frame was compressed": {compressed(out), in, 2},
			// The snapshot ends right after the flagged preamble, inside
			// the frame: the flag is direct evidence all the same.
			"a later sent frame was compressed and cut after its preamble": {compressed(out)[:len(out)-len(frames(t, one(msgr.Keepalive, 6)))+32], in, 1},
			"the received stream is not frames":                            {out, append(slices.Clone(in[:26]), bytes.Repeat([]byte{0xff}, 64)...), 2},
			"the sent stream is not a banner":                              {bytes.Repeat([]byte{1}, 64), in, 3},
		} {
			x := readCompressionExchange(tc.out, tc.in)
			if problems := x.declinedOffer(); len(problems) != tc.want {
				t.Errorf("%s: %d problems %q (%s)", name, len(problems), problems, x)
			}
		}
	})
	t.Run("the command binds the check to the connection that carried it", func(t *testing.T) {
		const command, reply = msgr.MonCommandMessage, msgr.MonCommandReplyMessage
		typed := func(kind uint16) msgr.Frame { return msgr.MessageData{Sequence: 1, Transaction: 1, Type: kind}.Frame() }
		// A connection up to and including session identification, with
		// optional negotiation frames and messages after it.
		stream := func(t *testing.T, client bool, negotiation []msgr.Frame, messages ...msgr.Frame) []byte {
			list := []msgr.Frame{one(msgr.Hello, 1), one(msgr.AuthRequest, 2), one(msgr.AuthSignature, 4)}
			ident := one(msgr.ClientIdent, 5)
			if !client {
				list, ident = []msgr.Frame{one(msgr.Hello, 1), one(msgr.AuthDone, 3), one(msgr.AuthSignature, 4)}, one(msgr.ServerIdent, 5)
			}
			list = append(append(append(list, negotiation...), ident), messages...)
			return append(banner(both), frames(t, list...)...)
		}
		offered, refused := []msgr.Frame{offer(true, zstd, snappy)}, []msgr.Frame{answer(false, 0)}
		idle := compressionTapped{endpoint: "192.0.2.1:3300", out: stream(t, true, offered), in: stream(t, false, refused)}
		serving := compressionTapped{endpoint: "192.0.2.1:3300", out: stream(t, true, offered, typed(command)), in: stream(t, false, refused, typed(reply))}
		// The command succeeded on a connection that never negotiated and
		// has since closed.
		silent := compressionTapped{endpoint: "192.0.2.1:3300", closed: true, out: stream(t, true, nil, typed(command)), in: stream(t, false, nil, typed(reply))}
		elsewhere := serving
		elsewhere.endpoint = "192.0.2.9:6800"
		all := func(string) bool { return true }
		mon := func(endpoint string) bool { return endpoint == "192.0.2.1:3300" }

		if index, _, problems := servedCommand([]compressionTapped{idle, serving, elsewhere}, mon, command, reply); index != 1 || len(problems) != 0 {
			t.Fatal("the serving connection", index, problems)
		}
		closed := serving
		closed.closed = true
		if index, _, problems := servedCommand([]compressionTapped{closed, idle}, mon, command, reply); index != 0 || len(problems) != 0 {
			t.Fatal("a closed serving connection still shows its own exchange", index, problems)
		}
		for name, tc := range map[string]struct {
			recorded []compressionTapped
			selects  func(string) bool
			index    int
		}{
			"the command ran on a closed connection without negotiation, beside an idle negotiated replacement": {[]compressionTapped{silent, idle}, mon, 0},
			"only idle connections":                {[]compressionTapped{idle, idle}, mon, -1},
			"the command on two connections":       {[]compressionTapped{serving, serving}, mon, -1},
			"the command on another role":          {[]compressionTapped{idle, elsewhere}, mon, -1},
			"the command and its reply apart":      {[]compressionTapped{{endpoint: idle.endpoint, out: serving.out, in: idle.in}, {endpoint: idle.endpoint, out: idle.out, in: serving.in}}, all, -1},
			"the reply never arrived":              {[]compressionTapped{{endpoint: idle.endpoint, out: serving.out, in: idle.in}}, all, 0},
			"the command preceded the negotiation": {[]compressionTapped{{endpoint: idle.endpoint, out: append(banner(both), frames(t, one(msgr.Hello, 1), one(msgr.AuthSignature, 4), typed(command), offer(true, zstd, snappy))...), in: serving.in}}, all, 0},
		} {
			index, x, problems := servedCommand(tc.recorded, tc.selects, command, reply)
			if index != tc.index || len(problems) == 0 {
				t.Errorf("%s: connection %d, problems %q (%s)", name, index, problems, x)
			}
		}
	})
	t.Run("a native capture that stops before authentication concludes nothing", func(t *testing.T) {
		// The archived Tentacle MON capture holds the daemon's banner and
		// HELLO only. No Go encoder produced it.
		capture, err := os.ReadFile("../internal/msgr/testdata/ceph-mon-hello-v20.2.4.bin")
		if err != nil {
			t.Fatal(err)
		}
		x := readCompressionExchange(nil, capture)
		if !x.peerBanner || x.peerSupported != msgr.Revision1|msgr.Compression || x.done || !errors.Is(x.inEnd, io.EOF) || errors.Is(x.inEnd, io.ErrUnexpectedEOF) {
			t.Fatal("the native banner", x, x.inEnd)
		}
		problems := x.declinedOffer()
		if !slices.ContainsFunc(problems, func(p string) bool { return bytes.Contains([]byte(p), []byte("no CompressionDone")) }) {
			t.Fatal("the capture was taken for a refusal", problems)
		}
	})
}
