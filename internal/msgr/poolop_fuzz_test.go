package msgr

import "testing"

func FuzzDecodePoolOpReply(f *testing.F) {
	f.Add(make([]byte, 18+16+4+4+1))
	f.Add(append(make([]byte, 18+16+8), 1, 8, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8))
	f.Fuzz(func(t *testing.T, front []byte) {
		r, err := DecodePoolOpReply(MessageData{Type: PoolOpReplyMessage, Version: 1, Front: front}, 1<<16)
		if err == nil && uint64(len(r.Data)) > uint64(len(front)) {
			t.Fatal("reply data larger than its frame")
		}
	})
}

func FuzzDecodeStatfsReply(f *testing.F) {
	f.Add(make([]byte, 56))
	f.Fuzz(func(t *testing.T, front []byte) {
		if _, err := DecodeStatfsReply(MessageData{Type: StatfsReplyMessage, Version: 1, Front: front}, 1<<16); err == nil && len(front) != 56 {
			t.Fatal("a reply that is not 56 bytes decoded")
		}
	})
}
