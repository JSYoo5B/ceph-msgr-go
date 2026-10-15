package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
)

// Bound memory for each streamed native log line independently of the amount
// of unrelated debug history produced by a long-running disposable MON.
const monitorReclaimLineLimit = 1 << 20

type monitorReclaimLogReader struct {
	ctx     context.Context
	scanner *bufio.Scanner
	prefix  string
	pending []byte
}

func newMonitorReclaimLogReader(ctx context.Context, input io.Reader, name string) io.Reader {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64<<10), monitorReclaimLineLimit+1)
	return &monitorReclaimLogReader{ctx: ctx, scanner: scanner,
		prefix: "cephx server mgr." + name + ":  attempt to reclaim global_id "}
}

func (r *monitorReclaimLogReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.pending) == 0 {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		if !r.scanner.Scan() {
			if err := r.scanner.Err(); err != nil {
				return 0, fmt.Errorf("native MON reclaim candidate line scan: %w", err)
			}
			return 0, io.EOF
		}
		if !strings.Contains(r.scanner.Text(), r.prefix) {
			continue
		}
		// Preserve every candidate unchanged for the existing strict native
		// timestamp/entity/GID/suffix parser, including malformed candidates.
		r.pending = append(r.pending[:0], r.scanner.Bytes()...)
		r.pending = append(r.pending, '\n')
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}
