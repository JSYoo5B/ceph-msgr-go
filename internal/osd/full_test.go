package osd

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestFullFlagsEncodeV6V8AndRefuseMisuse(t *testing.T) {
	flagsOf := func(version int, r V8Request) (uint32, error) {
		if version == 6 {
			m, err := EncodeRequest(r.Request, oracleFeatures, 1024)
			if err != nil {
				return 0, err
			}
			return binary.LittleEndian.Uint32(m.Front[8:]), nil
		}
		m, err := EncodeRequestV8(r, v8OracleFeatures, 1024)
		if err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint32(m.Front[32:]), nil
	}
	for _, version := range []int{6, 8} {
		base := nativeV8Request("mutation")
		plain, err := flagsOf(version, base)
		if err != nil {
			t.Fatal(err)
		}
		try, force := base, base
		try.FullTry, force.FullForce = true, true
		gotTry, err1 := flagsOf(version, try)
		gotForce, err2 := flagsOf(version, force)
		if err1 != nil || err2 != nil || gotTry != plain|uint32(FlagFullTry) || gotForce != plain|uint32(FlagFullForce) {
			t.Fatal("full flags not encoded", version, plain, gotTry, gotForce, err1, err2)
		}
		both := base
		both.FullTry, both.FullForce = true, true
		read := nativeV8Request("read")
		read.FullTry = true
		for name, r := range map[string]V8Request{"both": both, "read vector": read} {
			if _, err := flagsOf(version, r); !errors.Is(err, ErrRequest) {
				t.Fatal("misuse accepted", name, version, err)
			}
		}
	}
}
