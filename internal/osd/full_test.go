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

func TestReplicaReadFlagsEncodeAndRefuseUnsafeVectors(t *testing.T) {
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
		read := nativeV8Request("read")
		plain, err := flagsOf(version, read)
		if err != nil {
			t.Fatal(err)
		}
		for name, edit := range map[string]func(*V8Request){
			"balance":  func(r *V8Request) { r.BalanceReads = true },
			"localize": func(r *V8Request) { r.LocalizeReads = true },
			"both":     func(r *V8Request) { r.BalanceReads, r.LocalizeReads = true, true },
		} {
			r := read
			edit(&r)
			got, err := flagsOf(version, r)
			want := plain
			if r.BalanceReads {
				want |= uint32(FlagBalanceReads)
			}
			if r.LocalizeReads {
				want |= uint32(FlagLocalizeReads)
			}
			if err != nil || got != want {
				t.Fatal("replica flags not encoded", name, version, plain, got, err)
			}
		}
		for name, ops := range map[string][]Operation{
			"write":   {{Code: Write, Length: 1, Data: []byte{1}}},
			"call":    {{Code: Call, Class: ClassInput{Class: "hello", Method: "say_hello", Mode: ClassRead, MaxOutputBytes: 8}}},
			"snaps":   {{Code: ListSnaps, ListSnaps: ListSnapsInput{MaxOutputBytes: 8}}},
			"watcher": {{Code: ListWatchers, ListWatchers: ListWatchersInput{MaxOutputBytes: 8}}},
		} {
			r := nativeV8Request("mutation")
			r.Operations, r.BalanceReads = ops, true
			if name != "write" {
				r.MTimeSeconds, r.MTimeNanoseconds = 0, 0
			}
			if _, err := flagsOf(version, r); !errors.Is(err, ErrRequest) && !errors.Is(err, ErrOperation) {
				t.Fatal("unsafe replica-read vector accepted", name, version, err)
			}
		}
	}
}
