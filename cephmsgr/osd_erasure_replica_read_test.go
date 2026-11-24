package cephmsgr

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A replica-directed read with an erasure-coded placement is refused before
// shared admission and before a transaction ID is taken, on a connection to
// the PG's primary and on one to another OSD of its acting set, with or
// without primary-first numbering. The same flags with a replicated
// placement, and the erasure-coded placement without them, pass the check.
func TestOSDErasureReplicaDirectedReadRefusedBeforeAdmission(t *testing.T) {
	request := func(erasure bool, primary int32, shard uint8, first *OSDPrimaryFirst, balance, localize bool) OSDRequest {
		r := osdValidationRequest()
		r.Object.Key = ""
		r.Operations = []OSDOperation{{Code: OSDRead, Length: 8}}
		r.Placement = &OSDPlacement{PG: OSDPG{Pool: 7, Seed: 1}, Primary: primary, Erasure: erasure, Shard: shard, PrimaryFirst: first}
		r.BalanceReads, r.LocalizeReads = balance, localize
		return r
	}
	// backoffUnitConnection targets osd.3.
	for name, r := range map[string]OSDRequest{
		"balance at the primary":           request(true, 3, 0, nil, true, false),
		"localize at the primary":          request(true, 3, 0, nil, false, true),
		"both at the primary":              request(true, 3, 0, nil, true, true),
		"balance at another shard's OSD":   request(true, 5, 1, nil, true, false),
		"localize at another shard's OSD":  request(true, 5, 2, nil, false, true),
		"balance with primary-first shard": request(true, 3, 2, &OSDPrimaryFirst{PoolSize: 4, NonprimaryShards: 1}, true, false),
	} {
		t.Run(name, func(t *testing.T) {
			o := backoffUnitConnection()
			o.client.calls = make(chan struct{}, 1)
			o.client.calls <- struct{}{}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			got, err := o.Request(ctx, r)
			var unknown *OutcomeUnknownError
			var server *OSDError
			if !errors.Is(err, ErrUnsupportedOSDOperation) || errors.As(err, &unknown) || errors.As(err, &server) || got.Submission != (OSDSubmission{}) || o.client.osdNext.Load() != 0 || len(o.client.calls) != 1 {
				t.Fatal("an EC replica-directed read consumed shared admission or identity", got, err)
			}
		})
	}
	o := backoffUnitConnection()
	for name, r := range map[string]OSDRequest{
		"replicated balance at the primary":  request(false, 3, 0, nil, true, false),
		"replicated localize at a replica":   request(false, 5, 0, nil, false, true),
		"erasure-coded read without a flag":  request(true, 3, 0, nil, false, false),
		"erasure-coded primary-first, plain": request(true, 3, 2, &OSDPrimaryFirst{PoolSize: 4, NonprimaryShards: 1}, false, false),
	} {
		if err := o.validatePlacement(r); err != nil {
			t.Fatal("an admitted placement refused", name, err)
		}
	}
}
