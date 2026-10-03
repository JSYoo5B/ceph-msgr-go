package msgr

import (
	"bytes"
	"testing"
)

func TestHostnameRequestsIndependentVectors(t *testing.T) {
	for _, host := range []struct{ value, literal string }{
		{"", "00000000"},
		{" Host.雪\x00\xff ", "0c000000 20486f73742ee99baa00ff20"},
	} {
		for _, test := range []struct {
			name    string
			message MessageData
			literal string
			version uint16
		}{
			{"maps", Subscribe(0x11223344, 0x55667788, host.value), `02000000
06000000 6d67726d6170 8877665500000000 00
06000000 6d6f6e6d6170 4433221100000000 00 ` + host.literal, 3},
			{"logs", LogSubscribe("debug", 0x1122334455667788, host.value), `03000000
09000000 6c6f672d6465627567 8877665544332211 00
06000000 6d67726d6170 0000000000000000 00
06000000 6d6f6e6d6170 0000000000000000 00 ` + host.literal, 3},
			{"config", ConfigSubscribe(host.value), `03000000
06000000 636f6e666967 0000000000000000 00
06000000 6d67726d6170 0000000000000000 00
06000000 6d6f6e6d6170 0000000000000000 00 ` + host.literal, 3},
			{"digest", DigestSubscribe(host.value), `03000000
09000000 6d6772646967657374 0000000000000000 00
06000000 6d67726d6170 0000000000000000 00
06000000 6d6f6e6d6170 0000000000000000 00 ` + host.literal, 3},
			{"getconfig", GetConfig("client.client.scope", host.value), `08000000
0c000000 636c69656e742e73636f7065 ` + host.literal + ` 00000000`, 1},
		} {
			t.Run(test.name+"/"+host.literal, func(t *testing.T) {
				message := test.message
				typ := uint16(15)
				if test.name == "getconfig" {
					typ = 63
				}
				// These bytes follow MMonSubscribe v3 and MGetConfig v1 directly;
				// no product encoder or decoder constructs the expected payload.
				if message.Type != typ || message.Version != test.version || message.CompatVersion != 1 || message.Priority != 127 || message.Transaction != 0 || len(message.Middle) != 0 || len(message.Data) != 0 || !bytes.Equal(message.Front, configLiteral(t, test.literal)) {
					t.Fatal("hostname, authenticated entity, empty class or subscription fields changed")
				}
			})
		}
	}
}
