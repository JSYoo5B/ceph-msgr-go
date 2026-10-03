package cephmsgr_test

import (
	"fmt"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func ExampleParseKeyring() {
	data := []byte(`[client.management]
    key = AgAAAAAAAAAAACAAbUBNN/r3n53w0zVo0yBmmADrSDZHLqigJtFrcYJGDFI=
    caps mon = "allow r"
`)
	identity := "client.management"
	key, err := cephmsgr.ParseKeyring(data, identity)
	if err != nil {
		panic(err)
	}
	options := cephmsgr.Options{Identity: identity, Key: key}
	fmt.Println(options.Identity, options.Key)
	// Output: client.management CephX key(type=2)
}
