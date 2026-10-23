package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRGWNativeHistoryRequiresAttributedDecodedObjectReception(t *testing.T) {
	const client = "client.42"
	good := `{"ops":[{"description":"osd_op(client.42.0:9 3.0 3:abcd:::marker_small-binary:head [writefull 0~1031] snapc 0=[] ondisk+write e7)","type_data":{"client_info":{"client":"client.42","tid":9},"events":[{"event":"reached_pg"},{"event":"done"}]}}]}`
	objects := map[string]int64{"marker_small-binary": 3}
	operations, err := rgwNativeHistory([]byte(good), client, objects)
	if err != nil || len(operations) != 1 || operations[0].TID != 9 {
		t.Fatalf("native received operation: %+v %v", operations, err)
	}
	for name, raw := range map[string]string{
		"independent oracle":  strings.ReplaceAll(good, "client.42", "client.43"),
		"mismatched TID":      strings.Replace(good, `"tid":9`, `"tid":8`, 1),
		"partial object":      strings.ReplaceAll(good, "marker_small-binary", "prefix_marker_small-binary"),
		"no PG receipt":       strings.ReplaceAll(good, "reached_pg", "queued_for_pg"),
		"undecoded":           strings.ReplaceAll(good, "osd_op(client.42.0:9", "osd_op(undecoded client.42.0:9"),
		"same OID wrong pool": strings.ReplaceAll(good, "3.0 3:", "4.0 4:"),
		"wrong PG pool":       strings.ReplaceAll(good, "3.0 3:", "4.0 3:"),
		"wrong hobject pool":  strings.ReplaceAll(good, "3.0 3:", "3.0 4:"),
	} {
		t.Run(name, func(t *testing.T) {
			operations, err := rgwNativeHistory([]byte(raw), client, objects)
			if err != nil || len(operations) != 0 {
				t.Fatalf("unattributed operation admitted: %+v %v", operations, err)
			}
		})
	}
	for _, raw := range []string{`{}`, `{"ops":null}`, `{"ops":{}}`, `{"ops":[`} {
		if _, err := rgwNativeHistory([]byte(raw), client, objects); err == nil {
			t.Fatal("invalid native history schema admitted")
		}
	}
	tooMany, err := json.Marshal(map[string]any{"ops": make([]map[string]any, 4097)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rgwNativeHistory(tooMany, client, objects); err == nil {
		t.Fatal("oversized native history admitted")
	}
}

func TestRGWNativePutReceptionBindsIndexClassesToIndexPoolAndShard(t *testing.T) {
	makeOp := func(tid uint64, pg, object, vector string) map[string]any {
		return map[string]any{"description": "osd_op(client.42.0:" + strconv.FormatUint(tid, 10) + " " + pg + ".0 " + pg + ":abcd:::" + object + ":head [" + vector + "] snapc 0=[] ondisk+write e7)",
			"type_data": map[string]any{"client_info": map[string]any{"client": "client.42", "tid": tid}, "events": []map[string]string{{"event": "reached_pg"}}}}
	}
	write := makeOp(1, "7", "marker_small-binary", "writefull 0~1031")
	for name, ops := range map[string][]map[string]any{
		"correct index":       {write, makeOp(2, "6", ".dir.bucket.9", "call rgw.bucket_prepare_op"), makeOp(3, "6", ".dir.bucket.9", "call rgw.bucket_complete_op")},
		"class on head":       {write, makeOp(2, "7", "marker_small-binary", "call rgw.bucket_prepare_op"), makeOp(3, "7", "marker_small-binary", "call rgw.bucket_complete_op")},
		"wrong index pool":    {write, makeOp(2, "8", ".dir.bucket.9", "call rgw.bucket_prepare_op"), makeOp(3, "8", ".dir.bucket.9", "call rgw.bucket_complete_op")},
		"wrong method suffix": {write, makeOp(2, "6", ".dir.bucket.9", "call rgw.bucket_prepare_op_suffix"), makeOp(3, "6", ".dir.bucket.9", "call rgw.bucket_complete_op_suffix")},
		"write method suffix": {makeOp(1, "7", "marker_small-binary", "call rgw.not_write in=4b"), makeOp(2, "6", ".dir.bucket.9", "call rgw.bucket_prepare_op"), makeOp(3, "6", ".dir.bucket.9", "call rgw.bucket_complete_op")},
		"write xattr suffix":  {makeOp(1, "7", "marker_small-binary", "setxattr user.not_write (1)"), makeOp(2, "6", ".dir.bucket.9", "call rgw.bucket_prepare_op"), makeOp(3, "6", ".dir.bucket.9", "call rgw.bucket_complete_op")},
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{"ops": ops})
			if err != nil {
				t.Fatal(err)
			}
			ready, err := rgwNativePutPublished(raw, []rgwNativeClientGeneration{{Client: "client.42"}}, "marker_small-binary", "bucket", 7, 6)
			if err != nil || ready != (name == "correct index") {
				t.Fatalf("bound native index reception: ready=%t error=%v", ready, err)
			}
		})
	}
}

func TestRGWNativeS3DoesNotReplayOrDiscloseErrorBodies(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls++
		if request.Method != http.MethodPut || request.Header.Get("X-Amz-Content-Sha256") != rgwNativeHash([]byte("binary")) ||
			!strings.HasPrefix(request.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=test-access/") {
			t.Error("test-only S3 input was not signed")
		}
		w.Header().Set("Location", "/another-bucket")
		w.WriteHeader(http.StatusTemporaryRedirect)
		_, _ = w.Write([]byte("PRIVATE-S3-SECRET"))
	}))
	defer server.Close()
	client := rgwNativeS3{endpoint: server.URL, access: "test-access", secret: "test-secret", region: "us-east-1",
		http: &http.Client{Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	_, err := client.request(context.Background(), http.MethodPut, "/test-bucket/small-binary", []byte("binary"), http.StatusOK)
	if err == nil || strings.Contains(err.Error(), "PRIVATE") || calls != 1 {
		t.Fatalf("mutation replay/error privacy: calls=%d error=%v", calls, err)
	}
	if _, err := client.request(context.Background(), http.MethodPut, "/test-bucket/key:unsupported", nil, http.StatusOK); err == nil || calls != 1 {
		t.Fatal("checkpoint accepted an unsupported path")
	}
}

func TestRGWNativeGatewayInspectorDiscoversNonceNamedOwnedMessengers(t *testing.T) {
	const setup = `
import glob,os,io,builtins,subprocess,json,types
glob.glob=lambda _: ['/proc/1/fd/3']
os.readlink=lambda _: 'socket:[42]'
builtins.open=lambda _: io.StringIO('0: 2 0 10000 1 1 42 /var/run/ceph/owned.asok\n0: 2 0 10000 1 1 43 /var/run/ceph/unowned.asok\n')
def native(argv,**kwargs):
    assert argv[:5]==['ceph','--admin-daemon','/var/run/ceph/owned.asok','messenger','dump']
    if len(argv)==5:
        value={'messengers':['radosclient-12345','unrelated']}
    else:
        assert argv[5:] == ['radosclient-12345'], 'must discover names rather than hardcode radosclient'
        value={'status':{'messenger':{'my_name':{'type':'client','num':42},'nonce':12345}}}
    return types.SimpleNamespace(returncode=0,stdout=json.dumps(value).encode())
subprocess.run=native
`
	command := exec.Command("python3", "-c", setup+rgwNativeGatewayInspector)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("process-owned messenger inspector: %v: %s", err, output)
	}
	var result struct {
		Candidates int                         `json:"socket_candidates"`
		Messengers []rgwNativeClientGeneration `json:"messengers"`
	}
	if err := json.Unmarshal(output, &result); err != nil || result.Candidates != 1 || len(result.Messengers) != 1 || result.Messengers[0].Client != "client.42" || result.Messengers[0].Nonce != 12345 {
		t.Fatalf("incorrect owned named messenger result: %s %v", output, err)
	}
}

func TestRGWNativeHistoryPollWaitsForPublicationWithoutReplayingInput(t *testing.T) {
	fetches := 0
	result, err := rgwNativePollHistory(context.Background(), func(context.Context) []byte {
		fetches++
		if fetches == 1 {
			return []byte("partial native observation")
		}
		return []byte("complete native observation")
	}, func(output []byte) (bool, error) { return string(output) == "complete native observation", nil })
	if err != nil || fetches != 2 || string(result) != "complete native observation" {
		t.Fatalf("delayed receiver observation: fetches=%d error=%v", fetches, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = rgwNativePollHistory(ctx, func(context.Context) []byte { t.Fatal("cancelled observation executed"); return nil }, func([]byte) (bool, error) { return true, nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled history observation did not preserve cancellation", err)
	}
}
