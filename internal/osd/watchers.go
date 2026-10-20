package osd

import (
	"errors"
	"math"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Independently implemented from Ceph v20.2.4, commit
// 7f793731f1b39eb4f465e960113d2363c311b964: include/rados.h (LIST_WATCHERS),
// osdc/Objecter.h (list_watchers), osd/osd_types.h (watch_item_t and
// obj_list_watch_response_t), and osd/PrimaryLogPG.cc:6544-6569.
// LGPL-2.1 sources; no upstream implementation is copied.
const ListWatchers uint16 = 0x1209

type ListWatchersInput struct{ MaxOutputBytes uint32 }

type Watcher struct {
	NameType       uint8
	NameNumber     int64
	Cookie         uint64
	TimeoutSeconds uint32
	Address        msgr.Address
}

func validateListWatchersOperation(op Operation, limit uint32) error {
	if op.Flags != 0 {
		return ErrOperation
	}
	if op.Offset != 0 || op.Length != 0 || op.Version != 0 || len(op.Data) != 0 || op.Xattr != (XattrInput{}) || !omapInputEmpty(op.Omap) || op.Class != (ClassInput{}) || op.AllocHint != (AllocHintInput{}) || op.Snap != 0 || op.ListSnaps != (ListSnapsInput{}) || op.ListWatchers.MaxOutputBytes < 10 || op.ListWatchers.MaxOutputBytes > limit {
		return ErrRequest
	}
	return nil
}

// DecodeWatchers reads obj_list_watch_response_t v1 and current watch_item_t
// v2, including each entity_addr_t. Only newer compatible envelopes may carry
// appended fields. Returned entries preserve server order and raw identities.
func DecodeWatchers(data []byte) ([]Watcher, error) {
	if uint64(len(data)) > math.MaxUint32 {
		return nil, wire.ErrLimit
	}
	d := wire.NewDecoderLimit(data, uint32(len(data)))
	v, body := d.Struct(1)
	// Entry envelope + entity_name_t + cookie + timeout + minimal address.
	n := body.Count(6+9+8+4+19, math.MaxUint32)
	entries := make([]Watcher, n)
	for i := range entries {
		ev, item := body.Struct(2)
		if ev < 2 {
			item.Fail(wire.ErrVersion)
		}
		entries[i] = Watcher{NameType: item.U8(), NameNumber: int64(item.U64()), Cookie: item.U64(), TimeoutSeconds: item.U32()}
		entries[i].Address = msgr.DecodeAddress(item)
		if err := structDone(item, ev, 2); err != nil {
			body.Fail(err)
		}
	}
	if err := structDone(body, v, 1); err != nil {
		d.Fail(err)
	}
	if err := d.Done(); err != nil {
		return nil, err
	}
	return entries, nil
}

func ValidateListWatchersReply(requested Operation, reply OperationResult, success bool) error {
	if requested.Code != ListWatchers {
		return ErrOperation
	}
	if reply.Code != ListWatchers || reply.Offset != 0 || reply.Length != 0 || reply.TruncateSize != 0 || reply.TruncateSequence != 0 || reply.PayloadLength > requested.ListWatchers.MaxOutputBytes {
		return errors.New("OSD watcher listing reply differs from request or budget")
	}
	if !success {
		if reply.PayloadLength != 0 {
			return errors.New("OSD failed watcher listing returned output")
		}
		return nil
	}
	_, err := DecodeWatchers(reply.Data)
	return err
}
