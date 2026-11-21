package main

import (
	"errors"
	"strings"
	"testing"
)

// The two lines are shaped like pinned ProtocolV2.cc debug_ms 5 output;
// addresses and values are synthetic.
const mgrAcceptorSampleLog = `2026-11-20T00:00:01.000+0000 7f0 5 --2- [v2:192.0.2.20:6802/9,v1:192.0.2.20:6803/9] >> [v2:192.0.2.1:41000/77] conn(0x1).send_client_ident sending identification: addrs=[v2:192.0.2.20:6802/9,v1:192.0.2.20:6803/9] target=v2:192.0.2.1:41000/77 gid=0 global_seq=12 features_supported=3f01cfbffffdffff features_required=800000000002201 flags=1 cookie=0
2026-11-20T00:00:01.000+0000 7f0 5 --2- >> conn(0x1).send_client_ident sending identification: addrs=[v2:192.0.2.20:6802/9] target=v2:192.0.2.3:3300/0 gid=0 global_seq=13 features_supported=3f01cfbffffdffff features_required=800000000002201 flags=1 cookie=0
2026-11-20T00:00:01.100+0000 7f0 5 --2- conn(0x1).handle_server_ident received server identification: addrs=[v2:192.0.2.1:41000/77] gid=4242 global_seq=3 features_supported=2a00040010a0a237 features_required=800000000200000 flags=1 cookie=0
`

func TestMgrAcceptorIdentsKeepOnlyTheMgrEndpoint(t *testing.T) {
	idents, err := scanMgrAcceptorIdents(strings.NewReader(mgrAcceptorSampleLog), "41000", 1<<20)
	if err != nil || len(idents) != 2 {
		t.Fatal(idents, err)
	}
	client, server := idents[0], idents[1]
	if client.Kind != "client" || client.Target != "v2:192.0.2.1:41000/77" || client.GID != "0" || client.GlobalSeq != "12" || client.Flags != "1" {
		t.Fatalf("client %+v", client)
	}
	if server.Kind != "server" || server.GID != "4242" || server.GlobalSeq != "3" || server.Supported != "2a00040010a0a237" || server.Required != "800000000200000" || server.Cookie != "0" {
		t.Fatalf("server %+v", server)
	}
	if _, err := scanMgrAcceptorIdents(strings.NewReader(mgrAcceptorSampleLog), "41000", 100); !errors.Is(err, errContainerLogExceedsBound) {
		t.Fatal("oversized log", err)
	}
}
