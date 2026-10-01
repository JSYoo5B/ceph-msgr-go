// Package maps reads the subset of MON/MGR maps needed by management clients.
package maps

import (
	"errors"
	"fmt"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

var ErrRelease = errors.New("ceph: monitor minimum release is earlier than Tentacle")

type Mon struct {
	FSID           [16]byte
	Epoch          uint32
	MinimumRelease uint8
	Addresses      []msgr.Address
}

func DecodeMon(front []byte) (Mon, error) {
	var m Mon
	outer := wire.NewDecoder(front)
	blob := outer.Bytes()
	if err := outer.Done(); err != nil {
		return m, err
	}
	d := wire.NewDecoder(blob)
	v, p := d.Struct(10)
	if err := d.Done(); err != nil {
		return m, err
	}
	if v < 7 {
		return m, wire.ErrVersion
	}
	copy(m.FSID[:], p.Raw(16))
	m.Epoch = p.U32()
	p.Raw(16) // last_changed and created
	for i := 0; i < 2; i++ {
		_, features := p.Struct(1)
		features.U64()
		p.Fail(features.Err())
	}
	n := p.Count(10, 1024)
	byName := make(map[string][]msgr.Address, n)
	for i := 0; i < n && p.Err() == nil; i++ {
		name := p.String()
		_, info := p.Struct(6)
		infoName := info.String()
		addrs := msgr.DecodeAddresses(info)
		p.Fail(info.Err())
		if name != infoName {
			return m, errors.New("ceph: inconsistent monitor name")
		}
		if _, exists := byName[name]; exists {
			return m, errors.New("ceph: duplicate monitor name")
		}
		byName[name] = addrs
	}
	ranks := p.Count(4, 1024)
	seen := make(map[string]bool)
	for i := 0; i < ranks; i++ {
		name := p.String()
		addrs, ok := byName[name]
		if !ok || seen[name] {
			return m, errors.New("ceph: invalid monitor ranks")
		}
		seen[name] = true
		for _, a := range addrs {
			if a.Type == 2 && a.Endpoint.IsValid() {
				m.Addresses = append(m.Addresses, a)
			}
		}
	}
	m.MinimumRelease = p.U8()
	if err := p.Err(); err != nil {
		return m, err
	}
	if m.MinimumRelease < 20 {
		return m, fmt.Errorf("%w: %d", ErrRelease, m.MinimumRelease)
	}
	if m.FSID == [16]byte{} || len(m.Addresses) == 0 {
		return m, errors.New("ceph: empty monitor map")
	}
	// The versioned envelope allows unrelated appended election/auth fields.
	return m, nil
}

type Mgr struct {
	Epoch     uint32
	GlobalID  uint64
	Available bool
	Name      string
	Addresses []msgr.Address
}

func DecodeMgr(front []byte) (Mgr, error) {
	var m Mgr
	d := wire.NewDecoder(front)
	v, p := d.Struct(14)
	if err := d.Done(); err != nil {
		return m, err
	}
	if v < 6 {
		return m, wire.ErrVersion
	}
	m.Epoch = p.U32()
	all := msgr.DecodeAddresses(p)
	m.GlobalID = p.U64()
	m.Available = p.Bool()
	m.Name = p.String()
	for _, a := range all {
		if a.Type == 2 && a.Endpoint.IsValid() {
			m.Addresses = append(m.Addresses, a)
		}
	}
	if err := p.Err(); err != nil {
		return m, err
	}
	if m.Available && (m.GlobalID == 0 || len(m.Addresses) == 0) {
		return m, errors.New("ceph: invalid active manager")
	}
	return m, nil // Standby/module metadata are outside the client's needs.
}
