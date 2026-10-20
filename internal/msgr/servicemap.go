package msgr

import (
	"bytes"
	"fmt"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// MServiceMap and ServiceMap, independently translated from Ceph 20.2.4
// commit 7f793731f1b39eb4f465e960113d2363c311b964: messages/MServiceMap.h,
// mgr/ServiceMap.cc encode/decode, msg/Message.h MSG_SERVICE_MAP and
// mon/MgrStatMonitor.cc check_sub (LGPL-2.1; no code copied).
const ServiceMapMessage uint16 = 0x707

// ServiceMapSubscribe adds a continuous "servicemap" subscription while keeping
// the MON and MGR map subscriptions. next zero requests the current map; the
// MON then sends each later epoch. Keys stay in the sorted order of the
// encoded std::map.
func ServiceMapSubscribe(next uint64, hostname string) MessageData {
	e := wire.Encoder{}
	e.U32(3)
	e.String("mgrmap")
	e.U64(0)
	e.U8(0)
	e.String("monmap")
	e.U64(0)
	e.U8(0)
	e.String("servicemap")
	e.U64(next)
	e.U8(0)
	e.String(hostname)
	return MessageData{Type: SubscribeMessage, Version: 3, CompatVersion: 1, Priority: 127, Front: e.Data}
}

// ServiceDaemon is one registered daemon of a service.
type ServiceDaemon struct {
	GID                  uint64
	Address              Address
	StartEpoch           uint32
	StartSec, StartNsec  uint32
	Metadata, TaskStatus map[string]string
}

// Service is one named service: its daemons by name and the MGR's summary.
type Service struct {
	Daemons map[string]ServiceDaemon
	Summary string
}

// ServiceMap is a decoded map plus the raw ServiceMap bytes it came from.
type ServiceMap struct {
	Epoch                   uint32
	ModifiedSec, ModifiedNs uint32
	Services                map[string]Service
	Raw                     []byte
}

func decodeStringMap(d *wire.Decoder) map[string]string {
	n := d.Count(8, 1<<16)
	m := make(map[string]string, n)
	for i := 0; i < n && d.Err() == nil; i++ {
		k := d.String()
		m[k] = d.String()
	}
	return m
}

func joinChild(d, child *wire.Decoder) {
	if child.Err() != nil {
		d.Fail(child.Err())
	}
}

// DecodeServiceMap decodes one MServiceMap front. Newer struct versions are
// read when their compat version permits; appended fields are skipped.
func DecodeServiceMap(m MessageData, limit uint32) (result ServiceMap, err error) {
	defer func() {
		if err != nil {
			result = ServiceMap{}
			err = fmt.Errorf("%w: invalid service map: %w", ErrFrame, err)
		}
	}()
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if m.Type != ServiceMapMessage || len(m.Middle) != 0 || len(m.Data) != 0 {
		return ServiceMap{}, ErrFrame
	}
	if uint64(len(m.Front))+41 > uint64(limit) {
		return ServiceMap{}, wire.ErrLimit
	}
	d := wire.NewDecoderLimit(m.Front, limit)
	_, body := d.Struct(1)
	result.Epoch = body.U32()
	result.ModifiedSec, result.ModifiedNs = body.U32(), body.U32()
	n := body.Count(4, 1<<16)
	result.Services = make(map[string]Service, n)
	for i := 0; i < n && body.Err() == nil; i++ {
		name := body.String()
		_, svc := body.Struct(1)
		count := svc.Count(4, 1<<16)
		service := Service{Daemons: make(map[string]ServiceDaemon, count)}
		for j := 0; j < count && svc.Err() == nil; j++ {
			daemonName := svc.String()
			version, dm := svc.Struct(2)
			daemon := ServiceDaemon{GID: dm.U64(), Address: DecodeAddress(dm), StartEpoch: dm.U32()}
			daemon.StartSec, daemon.StartNsec = dm.U32(), dm.U32()
			daemon.Metadata = decodeStringMap(dm)
			if version >= 2 {
				daemon.TaskStatus = decodeStringMap(dm)
			}
			joinChild(svc, dm)
			service.Daemons[daemonName] = daemon
		}
		service.Summary = svc.String()
		joinChild(body, svc)
		result.Services[name] = service
	}
	joinChild(d, body)
	if err := d.Done(); err != nil {
		return ServiceMap{}, err
	}
	result.Raw = bytes.Clone(m.Front)
	return result, nil
}
