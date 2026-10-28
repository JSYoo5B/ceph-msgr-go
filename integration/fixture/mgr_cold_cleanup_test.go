package main

import (
	"errors"
	"github.com/containerd/errdefs"
	"testing"
)

func TestColdBootstrapCleanupRejectsUnreadableOwnership(t *testing.T) {
	for _, data := range []string{`null`, `{}`, `{"containers":{},"network":"owned"}`, `{"containers":{"a":"id"}}`, `{"containers":{"a":"id","b":"id"},"network":"owned"}`, `{"containers":{"a":""},"network":"owned"}`} {
		if _, err := readColdBootstrapOwnership([]byte(data)); err == nil {
			t.Fatal("invalid ownership accepted", data)
		}
	}
}

func TestColdBootstrapCleanupRequiresIndependentNotFound(t *testing.T) {
	owned, err := readColdBootstrapOwnership([]byte(`{"containers":{"mon.a":"first","mgr.a":"late"},"network":"owned"}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range []error{nil, errors.New("inspector unavailable")} {
		if err := checkColdBootstrapAbsence(owned, func(string, string) error { return result }); err == nil {
			t.Fatal("unconfirmed absence accepted", result)
		}
	}
	seen := map[string]bool{}
	if err := checkColdBootstrapAbsence(owned, func(kind, name string) error { seen[kind+":"+name] = true; return errdefs.ErrNotFound }); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 3 || !seen["container:late"] || !seen["network:owned"] {
		t.Fatal("dynamic resources not independently inspected", seen)
	}
	if err := checkColdBootstrapAbsence(owned, func(kind, name string) error {
		if kind == "network" {
			return nil
		}
		return errdefs.ErrNotFound
	}); err == nil {
		t.Fatal("present network certified absent")
	}
}
