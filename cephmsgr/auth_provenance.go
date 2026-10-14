package cephmsgr

import (
	"context"
	"sync/atomic"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

// These nil-default private hooks support test-only consumer provenance. They
// are not a diagnostic API. Epochs are cached authenticated map observations;
// ticket receipt identities mean local parsing, never native key generations.
// Callbacks run outside Client.mu and may read Snapshot, but must not call
// Close synchronously: they can run on a worker Close must join. They must not
// retain credentials or payloads. Installation delivery can follow selection;
// observers must join by receipt identity rather than callback delivery order.
type authTraceContextKey struct{}
type authTraceHooks struct {
	cycle     *atomic.Int64
	installed func(authIssuerObservation, *cephx.Client)
	manager   func(int, *cephx.Client, maps.Mgr, msgr.Address, session.Authenticator) (session.Authenticator, func(error))
}

type authIssuerObservation struct {
	cycle    int
	sequence uint64
	session  *session.Session
	endpoint string
	monMap   maps.Mon
}

func authTraceFromContext(ctx context.Context) *authTraceHooks {
	hooks, _ := ctx.Value(authTraceContextKey{}).(*authTraceHooks)
	return hooks
}
