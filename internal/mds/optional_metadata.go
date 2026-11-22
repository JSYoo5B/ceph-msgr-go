package mds

import (
	"bytes"
	"errors"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Independently written from pinned Ceph v20.2.4 commit
// 7f793731f1b39eb4f465e960113d2363c311b964: include/cephfs/types.h
// (charmap_md_t, unknown_md_t, optmetadata_singleton, optmetadata_multiton,
// optmetadata_client_t), include/encoding.h (DECODE_START, DECODE_UNKNOWN),
// messages/MClientReply.h (InodeStat::decode) and mds/CInode.cc
// (encode_inodestat). LGPL-2.1 sources; no code copied.
// https://github.com/ceph/ceph/blob/v20.2.4/src/include/cephfs/types.h

// OptionalMetadataCharmap is optmetadata_client_t::CHARMAP, the option kind
// the MDS adds to the InodeStat of a directory that has a charmap.
const OptionalMetadataCharmap uint64 = 1

// MaxOptionalMetadataOptions bounds the option count of one InodeStat. The
// pinned release defines one known kind and the MDS never repeats a kind.
const MaxOptionalMetadataOptions uint32 = 64

var ErrOptionalMetadata = errors.New("ceph mds: invalid InodeStat optional metadata")

// OptionalMetadataOption is one entry of the option vector, in wire order:
// its u64 kind and its versioned envelope, kept raw. Payload is the envelope
// body after the struct version, compat and length.
type OptionalMetadataOption struct {
	Kind            uint64
	Version, Compat uint8
	Payload         []byte
}

// Charmap is charmap_md_t, a directory's name policy as the MDS reports it:
// whether names are compared case-sensitively, and the normalization and
// encoding the stock client applies before it sends a name. These are raw
// server values; this library validates, normalizes and folds no name.
type Charmap struct {
	CaseSensitive bool
	Normalization string
	Encoding      string
}

// OptionalMetadata is the decoded option vector of an InodeStat. Options
// keeps every option raw in wire order, known or not. Charmap is the typed
// view of the CHARMAP option, nil when the inode has none.
type OptionalMetadata struct {
	Options []OptionalMetadataOption
	Charmap *Charmap
}

// DecodeOptionalMetadata decodes the option vector of a REPLY_ENCODING
// InodeStat (InodeStat.OptionalMetadata: the payload of the optmetadata
// multiton envelope). Each option is a u64 kind and a versioned envelope.
//
// It accepts what the pinned native client decoder accepts and refuses the
// rest, without reading past an envelope:
//   - An unknown kind (any kind but CHARMAP, kind 0 included) is kept raw
//     with any version, as unknown_md_t keeps it (DECODE_UNKNOWN).
//   - CHARMAP needs compat 1 or lower (DECODE_START(1)); a newer version is
//     read by its three leading fields and the rest of its envelope skipped.
//     Version 1 (or 0) must end after those fields.
//   - The option count is bounded before allocation, and a truncated kind,
//     envelope or field, or bytes after the last option, are refused.
//
// Two refusals are stricter than the native decoder, which never meets them
// from a pinned MDS: a repeated kind (the MDS keeps one option per kind; the
// native client would use the first) and a case-sensitivity byte other than
// 0 or 1.
func DecodeOptionalMetadata(raw []byte) (OptionalMetadata, error) {
	var m OptionalMetadata
	d := wire.NewDecoderLimit(raw, uint32(len(raw)))
	// Each option is at least a u64 kind and a six-byte envelope header.
	n := d.Count(8+6, MaxOptionalMetadataOptions)
	if n != 0 {
		m.Options = make([]OptionalMetadataOption, 0, n)
	}
	for range n {
		kind := d.U64()
		version, compat := d.U8(), d.U8()
		payload := d.Bytes()
		if d.Err() != nil {
			break
		}
		for _, o := range m.Options {
			if o.Kind == kind {
				d.Fail(ErrOptionalMetadata)
			}
		}
		m.Options = append(m.Options, OptionalMetadataOption{Kind: kind, Version: version, Compat: compat, Payload: bytes.Clone(payload)})
		if kind != OptionalMetadataCharmap {
			continue
		}
		if compat > 1 {
			d.Fail(wire.ErrVersion)
			break
		}
		b := wire.NewDecoderLimit(payload, uint32(len(payload)))
		c := Charmap{CaseSensitive: b.Bool(), Normalization: b.String(), Encoding: b.String()}
		if version <= 1 {
			d.Fail(b.Done())
		} else {
			d.Fail(b.Err())
		}
		m.Charmap = &c
	}
	if err := d.Done(); err != nil {
		return OptionalMetadata{}, err
	}
	return m, nil
}

// DecodeOptionalMetadata decodes s.OptionalMetadata; see the package
// function of the same name.
func (s *InodeStat) DecodeOptionalMetadata() (OptionalMetadata, error) {
	return DecodeOptionalMetadata(s.OptionalMetadata)
}
