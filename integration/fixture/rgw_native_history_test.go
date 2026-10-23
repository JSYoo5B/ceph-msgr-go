package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRGWNativeHistoryRequiresAttributedDecodedObjectReception(t *testing.T) {
	const client = "client.42"
	good := `{"ops":[{"description":"osd_op(client.42.0:9 3.0 3:abcd:::marker_small-binary:head [writefull 0~1031] snapc 0=[] ondisk+write e7)","type_data":{"client_info":{"client":"client.42","tid":9},"events":[{"event":"reached_pg"},{"event":"done"}]}}]}`
	objects := map[string]bool{"marker_small-binary": true}
	operations, err := rgwNativeHistory([]byte(good), client, objects)
	if err != nil || len(operations) != 1 || operations[0].TID != 9 {
		t.Fatalf("native received operation: %+v %v", operations, err)
	}
	for name, raw := range map[string]string{
		"independent oracle": strings.ReplaceAll(good, "client.42", "client.43"),
		"mismatched TID":     strings.Replace(good, `"tid":9`, `"tid":8`, 1),
		"partial object":     strings.ReplaceAll(good, "marker_small-binary", "prefix_marker_small-binary"),
		"no PG receipt":      strings.ReplaceAll(good, "reached_pg", "queued_for_pg"),
		"undecoded":          strings.ReplaceAll(good, "osd_op(client.42.0:9", "osd_op(undecoded client.42.0:9"),
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
