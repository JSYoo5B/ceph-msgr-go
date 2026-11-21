package msgr

import (
	"bytes"
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// TestMonMgrReportRefusesContentTheMONCannotHold covers the local refusals,
// each before any frame exists: they are synthetic, since a stock MON
// would end or silently drop instead.
func TestMonMgrReportRefusesContentTheMONCannotHold(t *testing.T) {
	v := mgrReportVectors(t)
	digest := unhex(t, v["digest-a"].Bytes)
	services := unhex(t, v["service-map"].Bytes)
	old := bytes.Clone(digest)
	old[0] = 3 // PGMapDigest::decode asserts struct_v >= 4.
	newer := bytes.Clone(digest)
	newer[1] = 6 // compat above the known version
	for name, r := range map[string]MonMgrReport{
		"no digest":              {},
		"trailing digest byte":   {Digest: append(bytes.Clone(digest), 0)},
		"digest version 3":       {Digest: old},
		"digest compat 6":        {Digest: newer},
		"duplicate health check": {Digest: digest, HealthChecks: []HealthCheck{{Code: "A"}, {Code: "A"}}},
		"unknown severity":       {Digest: digest, HealthChecks: []HealthCheck{{Code: "A", Severity: 3}}},
		"duplicate progress":     {Digest: digest, Progress: []ProgressEvent{{ID: "p"}, {ID: "p"}}},
		"cut service map":        {Digest: digest, ServiceMap: services[:len(services)-1]},
		"foreign service map":    {Digest: digest, ServiceMap: []byte("not a map")},
	} {
		if _, err := EncodeMonMgrReport(r, 1<<20); !errors.Is(err, ErrMgrReportContent) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := EncodeMonMgrReport(MonMgrReport{Digest: digest, HealthChecks: []HealthCheck{{Code: "BIG", Summary: string(make([]byte, 4096))}}}, 4096); !errors.Is(err, wire.ErrLimit) {
		t.Error("frame limit", err)
	}
	if _, err := EncodeMonMgrReport(MonMgrReport{Digest: digest}, uint32(len(digest))); !errors.Is(err, wire.ErrLimit) {
		t.Error("digest past the frame limit", err)
	}
	// A version 4 digest lacks the unavailable-PG map; PGMapDigest::decode
	// reads it, so this decoder does too.
	g, err := DecodePGMapDigest(digest, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.UnavailablePGs) != 1 {
		t.Fatal("version 5 field", g.UnavailablePGs)
	}
}

// TestMonMgrReportSortsKeysAndCopiesTheDigest: the encoder writes keys in
// std::map order whatever the caller's order, and the message keeps its own
// copy of the digest.
func TestMonMgrReportSortsKeysAndCopiesTheDigest(t *testing.T) {
	v := mgrReportVectors(t)
	digest := unhex(t, v["digest-empty"].Bytes)
	a, err := EncodeMonMgrReport(MonMgrReport{Digest: digest, GlobalID: 9, HealthChecks: []HealthCheck{{Code: "B"}, {Code: "A"}}, Progress: []ProgressEvent{{ID: "y"}, {ID: "x"}}}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	b, err := EncodeMonMgrReport(MonMgrReport{Digest: digest, GlobalID: 9, HealthChecks: []HealthCheck{{Code: "A"}, {Code: "B"}}, Progress: []ProgressEvent{{ID: "x"}, {ID: "y"}}}, 1<<20)
	if err != nil || !bytes.Equal(a.Front, b.Front) {
		t.Fatal("key order changed the encoding", err)
	}
	if gid := a.Front[len(a.Front)-8:]; !bytes.Equal(gid, []byte{9, 0, 0, 0, 0, 0, 0, 0}) {
		t.Fatal("GID is not the last front field", gid)
	}
	digest[0] ^= 0xff
	if a.Data[0] == digest[0] {
		t.Fatal("the message shares the caller's digest")
	}
}
