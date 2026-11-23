package cephmsgr

import (
	"context"
	"errors"
	"testing"
	"time"
)

// An erasure-coded placement's vector of an extent read beside an operation
// with output is refused before shared admission and before a transaction ID
// is taken; the same vector on a replicated placement, and EC vectors of
// extent reads with output-free guards or of outputs without an extent read,
// pass this check.
func TestOSDErasureReexecutedVectorRefusedBeforeAdmission(t *testing.T) {
	read := OSDOperation{Code: OSDRead, Length: 8}
	call := OSDOperation{Code: OSDCall, Class: OSDClassInput{Class: "hello", Method: "say_hello", Mode: OSDCallRead, MaxOutputBytes: 64}}
	refused := map[string][]OSDOperation{
		"read-stat":       {read, {Code: OSDStat}},
		"stat-read":       {{Code: OSDStat}, read},
		"getxattrs-read":  {{Code: OSDGetXattrs, Xattr: OSDXattrInput{MaxOutputBytes: 64}}, read},
		"cmpext-getxattr": {{Code: OSDCompareExtent, Offset: 4, Length: 1, Data: []byte{1}}, {Code: OSDGetXattr, Xattr: OSDXattrInput{Name: "a", MaxOutputBytes: 64}}},
		"read-omap-keys":  {read, {Code: OSDGetOmapKeys, Omap: OSDOmapInput{MaxEntries: 2, MaxOutputBytes: 64}}},
		"read-call":       {read, call},
	}
	admitted := map[string][]OSDOperation{
		"read-cmpxattr": {read, {Code: OSDCompareXattr, Xattr: OSDXattrInput{Name: "a", Comparison: OSDXattrEqual, Mode: OSDXattrString}, Data: []byte("v")}},
		"assert-read":   {{Code: OSDAssertVersion, Version: 9}, read},
		"stat-call":     {{Code: OSDStat}, call},
		"read":          {read},
	}
	request := func(ops []OSDOperation, erasure bool) OSDRequest {
		r := osdValidationRequest()
		r.Object.Key = ""
		r.Operations = ops
		r.Placement = &OSDPlacement{PG: OSDPG{Pool: 7, Seed: 1}, Primary: 3, Erasure: erasure}
		return r
	}
	for name, ops := range refused {
		t.Run(name, func(t *testing.T) {
			o := backoffUnitConnection()
			o.client.calls = make(chan struct{}, 1)
			o.client.calls <- struct{}{}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			got, err := o.Request(ctx, request(ops, true))
			var unknown *OutcomeUnknownError
			if !errors.Is(err, ErrUnsupportedOSDOperation) || errors.As(err, &unknown) || got.Submission != (OSDSubmission{}) || o.client.osdNext.Load() != 0 || len(o.client.calls) != 1 {
				t.Fatal("an EC re-executed vector consumed shared admission or identity", got, err)
			}
			if err := o.validateErasureReexecution(request(ops, false)); err != nil {
				t.Fatal("the replicated vector refused", err)
			}
		})
	}
	o := backoffUnitConnection()
	for name, ops := range admitted {
		if err := o.validateErasureReexecution(request(ops, true)); err != nil {
			t.Fatal("an admitted EC vector refused", name, err)
		}
	}
}
