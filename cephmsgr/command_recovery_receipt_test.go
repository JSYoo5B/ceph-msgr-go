package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"testing"
)

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
