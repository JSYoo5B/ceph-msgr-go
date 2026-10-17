package cephmsgr

import (
	"context"
	"errors"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
)

func TestOSDProfilePlacementAdmission(t *testing.T) {
	o := backoffUnitConnection()
	o.client.options.OSDProfile = OSDProfileObjectHash
	request := OSDRequest{MapEpoch: 17, Object: OSDObject{Pool: 7, Name: "object", Hash: 0x89abcdef}, Operations: []OSDOperation{{Code: OSDStat}}}
	if err := o.validatePlacement(request); err != nil {
		t.Fatal(err)
	}
	request.Placement = &OSDPlacement{PG: OSDPG{Pool: 7, Seed: 3}, Primary: 3}
	if _, err := o.Request(context.Background(), request); !errors.Is(err, osd.ErrRequest) {
		t.Fatal("object-hash profile accepted actual PG", err)
	}
	o.client.options.OSDProfile = OSDProfileReplicatedPG
	if err := o.validatePlacement(request); err != nil {
		t.Fatal("actual PG should differ from raw hash", err)
	}
	for _, edit := range []func(*OSDRequest){
		func(r *OSDRequest) { r.Placement = nil },
		func(r *OSDRequest) { r.Placement.Primary++ },
		func(r *OSDRequest) { r.Placement.PG.Pool++ },
		func(r *OSDRequest) { r.Object.Pool = -1 },
	} {
		invalid := request
		invalid.Placement = cloneOSDPlacement(request.Placement)
		edit(&invalid)
		if reply, err := o.Request(context.Background(), invalid); !errors.Is(err, osd.ErrRequest) || reply.Submission.Transaction != 0 {
			t.Fatal("invalid placement admitted", reply, err)
		}
	}
	if o.client.osdNext.Load() != 0 {
		t.Fatal("invalid placement consumed identity")
	}
	copy := cloneOSDPlacement(request.Placement)
	request.Placement.PG.Seed++
	if copy.PG.Seed != 3 {
		t.Fatal("placement alias retained")
	}
}

func TestOSDProfileConnectionAdmission(t *testing.T) {
	if _, err := Dial(context.Background(), Options{EnableOSD: true, OSDProfile: 255}); !errors.Is(err, ErrUnsupportedFeatures) {
		t.Fatal("unknown profile admitted", err)
	}
	if _, err := Dial(context.Background(), Options{OSDProfile: OSDProfileReplicatedPG}); !errors.Is(err, ErrOSDDisabled) {
		t.Fatal("replicated profile without OSD enabled", err)
	}
}
