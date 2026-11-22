package main

import (
	"context"
	"strings"
	"testing"
)

func TestSetChunkControlIsClosedAndSelectedOnce(t *testing.T) {
	out := t.TempDir()
	request := func(body string) {
		t.Helper()
		if err := writeRequestFile(out, "verify-osd-set-chunk", body); err != nil {
			t.Fatal(err)
		}
	}
	c := &fixtureCluster{profile: profile{osd: "0"}}
	request("10 " + setChunkNativeAction)
	if err := c.controls(context.Background(), out); err == nil || !strings.Contains(err.Error(), "SET_CHUNK") || c.setChunkSelected {
		t.Fatal("SET_CHUNK checkpoint accepted without the OSD fixture", err)
	}
	c.profile.osd = "1"
	for _, body := range []string{"11 observe", "12 " + setChunkNativeAction + " extra"} {
		request(body)
		if err := c.controls(context.Background(), out); err == nil || !strings.Contains(err.Error(), "SET_CHUNK") || c.setChunkSelected {
			t.Fatal("foreign SET_CHUNK request accepted", body, err)
		}
	}
	c.setChunkSelected = true
	if err := c.setChunkNative(context.Background(), out); err == nil {
		t.Fatal("SET_CHUNK checkpoint was reissued")
	}
}
