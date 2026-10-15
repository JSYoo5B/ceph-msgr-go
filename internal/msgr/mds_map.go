package msgr

import (
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
	"strconv"
)

// FSMapUserSubscribe requests the cluster-wide raw filesystem directory.
func FSMapUserSubscribe(next uint64, hostname string) MessageData {
	return mdsMapSubscribe("fsmap.user", next, hostname)
}

// MDSMapSubscribe binds one explicitly supplied nonnegative filesystem ID.
// Its caller validates the ID; MMDSMap replies do not echo it.
func MDSMapSubscribe(filesystemID int32, next uint64, hostname string) MessageData {
	return mdsMapSubscribe("mdsmap."+strconv.FormatInt(int64(filesystemID), 10), next, hostname)
}

func mdsMapSubscribe(topic string, next uint64, hostname string) MessageData {
	e := wire.Encoder{}
	e.U32(1)
	e.String(topic)
	e.U64(next)
	e.U8(0)
	e.String(hostname)
	return MessageData{Type: SubscribeMessage, Version: 3, CompatVersion: 1, Priority: 127, Front: e.Data}
}
