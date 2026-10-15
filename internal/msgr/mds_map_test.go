package msgr

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"
)

func TestMDSMapSubscriptionsUseOnlyFixedTopicsAndFullCursor(t *testing.T) {
	for _, tc := range []struct {
		name string
		msg  MessageData
		hex  string
	}{
		{
			name: "filesystem directory from zero",
			msg:  FSMapUserSubscribe(0, ""),
			hex:  "01000000 0a000000 66736d61702e75736572 0000000000000000 00 00000000",
		},
		{
			name: "filesystem directory full cursor and utf8 hostname",
			msg:  FSMapUserSubscribe(0x1122334455667788, "node-α"),
			hex:  "01000000 0a000000 66736d61702e75736572 8877665544332211 00 07000000 6e6f64652dceb1",
		},
		{
			name: "filesystem zero bound independently of cursor",
			msg:  MDSMapSubscribe(0, 1<<32, "fixture"),
			hex:  "01000000 08000000 6d64736d61702e30 0000000001000000 00 07000000 66697874757265",
		},
		{
			name: "largest filesystem id and cursor",
			msg:  MDSMapSubscribe(2147483647, ^uint64(0), ""),
			hex:  "01000000 11000000 6d64736d61702e32313437343833363437 ffffffffffffffff 00 00000000",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want, err := hex.DecodeString(strings.ReplaceAll(tc.hex, " ", ""))
			if err != nil {
				t.Fatal(err)
			}
			m := tc.msg
			if m.Type != SubscribeMessage || m.Version != 3 || m.CompatVersion != 1 || m.Priority != 127 || m.Transaction != 0 || m.Sequence != 0 || m.AckSequence != 0 || len(m.Middle) != 0 || len(m.Data) != 0 {
				t.Fatal("subscription envelope changed", m)
			}
			if !bytes.Equal(m.Front, want) {
				t.Fatalf("fixed one-topic subscription, flags or cursor changed\ngot:  %x\nwant: %x", m.Front, want)
			}
		})
	}
}
