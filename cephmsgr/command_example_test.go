package cephmsgr_test

import (
	"fmt"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func ExampleNewCommand() {
	command, err := cephmsgr.NewCommand("iostat", map[string]any{
		"print_header": true,
		"width":        80,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(string(command.JSON))
	// Output: {"prefix":"iostat","print_header":true,"width":80}
}
