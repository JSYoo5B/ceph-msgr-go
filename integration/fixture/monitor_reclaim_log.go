package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
)

// Bound memory for each streamed native log line independently of the amount
// of unrelated debug history produced by a long-running disposable MON.
const monitorReclaimLineLimit = 1 << 20

type monitorReclaimLogReader struct {
	ctx     context.Context
	reader  *bufio.Reader
	prefix  []byte
	line    []byte
	pending []byte
}

func newMonitorReclaimLogReader(ctx context.Context, input io.Reader, name string) io.Reader {
	return &monitorReclaimLogReader{ctx: ctx, reader: bufio.NewReaderSize(input, 64<<10),
		prefix: []byte("cephx server mgr." + name + ":  attempt to reclaim global_id ")}
}

// nextLine returns one line without its newline. A line longer than the
// limit is consumed in bounded chunks; only its first limit bytes are kept
// and overLong reports the truncation.
func (r *monitorReclaimLogReader) nextLine() (line []byte, overLong bool, err error) {
	r.line = r.line[:0]
	for {
		chunk, err := r.reader.ReadSlice('\n')
		room := monitorReclaimLineLimit - len(r.line)
		if len(chunk) > room {
			r.line = append(r.line, chunk[:room]...)
			overLong = true
		} else {
			r.line = append(r.line, chunk...)
		}
		switch {
		case err == nil:
			return trimNewline(r.line), overLong, nil
		case errors.Is(err, bufio.ErrBufferFull):
			if err := r.ctx.Err(); err != nil {
				return nil, false, err
			}
			continue
		case errors.Is(err, io.EOF) && (len(r.line) > 0 || overLong):
			return trimNewline(r.line), overLong, nil
		default:
			return nil, false, err
		}
	}
}

func trimNewline(line []byte) []byte {
	if n := len(line); n > 0 && line[n-1] == '\n' {
		return line[:n-1]
	}
	return line
}

func (r *monitorReclaimLogReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.pending) == 0 {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		line, overLong, err := r.nextLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0, io.EOF
			}
			return 0, fmt.Errorf("native MON reclaim candidate line scan: %w", err)
		}
		if !bytes.Contains(line, r.prefix) {
			// Unrelated history, including over-long debug dumps, is
			// streamed through in bounded chunks and never retained.
			continue
		}
		if overLong {
			return 0, fmt.Errorf("native MON reclaim candidate line exceeds %d bytes", monitorReclaimLineLimit)
		}
		// Preserve every candidate unchanged for the existing strict native
		// timestamp/entity/GID/suffix parser, including malformed candidates.
		r.pending = append(r.pending[:0], line...)
		r.pending = append(r.pending, '\n')
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}
