package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync/atomic"
	"testing"
)

// paddedWriteLedger counts the writes of at least 4096 bytes that a test's
// connections issued, and separately those that returned complete: every byte
// accepted and no error. A short or failed write of that size is issued but
// not complete, so the issued count alone does not show that a padded command
// left the client in full.
type paddedWriteLedger struct {
	issued, complete atomic.Int32
}

type paddedWriteLedgerConn struct {
	net.Conn
	ledger *paddedWriteLedger
}

func (l *paddedWriteLedger) wrap(conn net.Conn) net.Conn {
	return &paddedWriteLedgerConn{Conn: conn, ledger: l}
}

func (c *paddedWriteLedgerConn) Write(p []byte) (int, error) {
	if len(p) < 4096 {
		return c.Conn.Write(p)
	}
	c.ledger.issued.Add(1)
	n, err := c.Conn.Write(p)
	if n == len(p) && err == nil {
		c.ledger.complete.Add(1)
	}
	return n, err
}

// once reports whether exactly one padded write was issued and it returned
// complete.
func (l *paddedWriteLedger) once() bool {
	return l.issued.Load() == 1 && l.complete.Load() == 1
}

// metLocallyEndedConnection reports whether a failed invocation is the one a
// test caused by closing a connection underneath a session that still held
// it: no result field is set, and the cause is the one a locally closed
// connection reports, bare or inside an unknown outcome. Any other failure,
// in particular a known-unsent refusal or a peer-side loss, is not admitted,
// so that a later answered invocation cannot be reported as recovery from it.
func metLocallyEndedConnection(result Result, err error) bool {
	if err == nil || result.Code != 0 || result.Message != "" || len(result.Data) != 0 {
		return false
	}
	return errors.Is(err, net.ErrClosed) && !errors.Is(err, ErrClosed)
}

// laterDials lists, in ascending order, the connections dialed to role after
// the held one. A dial is recorded before any handshake or command, so the
// list says the endpoint was dialed again and nothing about which connection
// carried a later command.
func laterDials(peers map[uint64]string, role string, held uint64) []uint64 {
	later := []uint64{}
	for id, peer := range peers {
		if peer == role && id > held {
			later = append(later, id)
		}
	}
	slices.Sort(later)
	return later
}

// Only the failure of the connection the test closed is tolerated before a
// recovery read; an unrelated error followed by an answered read is not.
func TestMetLocallyEndedConnectionAdmitsOnlyThatCause(t *testing.T) {
	closed := &net.OpError{Op: "set", Net: "tcp", Err: net.ErrClosed}
	for name, err := range map[string]error{
		"bare transport error": closed,
		"wrapped":              fmt.Errorf("write frame: %w", closed),
		"unknown outcome":      &OutcomeUnknownError{Cause: closed},
	} {
		if !metLocallyEndedConnection(Result{}, err) {
			t.Error("the ended connection's own failure was refused:", name, err)
		}
	}
	for name, c := range map[string]struct {
		result Result
		err    error
	}{
		"answered":                      {Result{}, nil},
		"known-unsent target selection": {Result{}, ErrManagerTargetChanged},
		"unrelated error":               {Result{}, errors.New("no active manager")},
		"peer closed":                   {Result{}, io.EOF},
		"unknown outcome, peer closed":  {Result{}, &OutcomeUnknownError{Cause: io.ErrUnexpectedEOF}},
		"canceled":                      {Result{}, context.Canceled},
		"unknown outcome, canceled":     {Result{}, &OutcomeUnknownError{Cause: context.Canceled}},
		"closed client":                 {Result{}, ErrClosed},
		"closed client and connection":  {Result{}, errors.Join(ErrClosed, closed)},
		"result code":                   {Result{Code: -22}, closed},
		"result message":                {Result{Message: "refused"}, closed},
		"result data":                   {Result{Data: []byte("{}")}, closed},
		"unknown outcome with message":  {Result{Message: "refused"}, &OutcomeUnknownError{Cause: closed}},
	} {
		if metLocallyEndedConnection(c.result, c.err) {
			t.Error("another failure was taken for the ended connection:", name, c.err)
		}
	}
}

// Every later dial to the target is listed; none is singled out as the
// connection of the recovery read.
func TestLaterDialsListsEveryLaterDialToTheTarget(t *testing.T) {
	peers := map[uint64]string{1: "mon.a", 2: "osd.0", 3: "mgr.a", 4: "osd.0", 5: "osd.0", 6: "osd.1"}
	// Two later dials: the read may have used either, so both are reported.
	if got := laterDials(peers, "osd.0", 2); !slices.Equal(got, []uint64{4, 5}) {
		t.Fatal("later dials to the target differ", got)
	}
	if got := laterDials(peers, "osd.0", 5); got == nil || len(got) != 0 {
		t.Fatal("a dial that is not later was listed", got)
	}
	if got := laterDials(peers, "mgr.b", 0); len(got) != 0 {
		t.Fatal("a dial to another daemon was listed", got)
	}
}

type scriptedWriteConn struct {
	net.Conn
	accept int
	err    error
}

func (c scriptedWriteConn) Write(p []byte) (int, error) {
	if c.accept >= 0 && c.accept < len(p) {
		return c.accept, c.err
	}
	return len(p), c.err
}

func TestPaddedWriteLedgerCountsOnlyCompleteWrites(t *testing.T) {
	padded, small := make([]byte, 8192), make([]byte, 4095)
	for _, tc := range []struct {
		name             string
		conn             scriptedWriteConn
		writes           [][]byte
		issued, complete int32
		once             bool
	}{
		{"complete", scriptedWriteConn{accept: -1}, [][]byte{small, padded}, 1, 1, true},
		{"below the size", scriptedWriteConn{accept: -1}, [][]byte{small}, 0, 0, false},
		{"short with an error", scriptedWriteConn{accept: 5000, err: io.ErrShortWrite}, [][]byte{padded}, 1, 0, false},
		{"short without an error", scriptedWriteConn{accept: 5000}, [][]byte{padded}, 1, 0, false},
		{"refused", scriptedWriteConn{accept: 0, err: net.ErrClosed}, [][]byte{padded}, 1, 0, false},
		{"every byte with an error", scriptedWriteConn{accept: -1, err: net.ErrClosed}, [][]byte{padded}, 1, 0, false},
		{"complete twice", scriptedWriteConn{accept: -1}, [][]byte{padded, padded}, 2, 2, false},
	} {
		var ledger paddedWriteLedger
		conn := ledger.wrap(tc.conn)
		for _, p := range tc.writes {
			_, _ = conn.Write(p)
		}
		if ledger.issued.Load() != tc.issued || ledger.complete.Load() != tc.complete || ledger.once() != tc.once {
			t.Errorf("%s: issued=%d complete=%d once=%t", tc.name, ledger.issued.Load(), ledger.complete.Load(), ledger.once())
		}
	}
	// A complete write followed by a failed resend is not one write.
	var ledger paddedWriteLedger
	_, _ = ledger.wrap(scriptedWriteConn{accept: -1}).Write(padded)
	_, _ = ledger.wrap(scriptedWriteConn{accept: 0, err: net.ErrClosed}).Write(padded)
	if ledger.once() || ledger.issued.Load() != 2 || ledger.complete.Load() != 1 {
		t.Error("a failed second padded write was not counted as issued", ledger.issued.Load(), ledger.complete.Load())
	}
}
