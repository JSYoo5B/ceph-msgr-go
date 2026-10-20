package mds

import (
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

func FuzzDecodeReclaimReply(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0})
	f.Add(reclaimReplyMessage(-22, 7, nil).Front)
	f.Fuzz(func(t *testing.T, front []byte) {
		r, err := DecodeReclaimReply(msgr.MessageData{Type: ClientReclaimReplyMessage, Version: 1, CompatVersion: 1, Front: front}, 1<<16)
		if err == nil && len(r.Addresses) > 64 {
			t.Fatal("unbounded address vector")
		}
	})
}
