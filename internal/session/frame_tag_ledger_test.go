package session

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// TestFrameTagLedgerCoversEveryTag keeps FRAME_TAG_LEDGER.md aligned with the
// tag values the Messenger package defines.
func TestFrameTagLedgerCoversEveryTag(t *testing.T) {
	doc, err := os.ReadFile("FRAME_TAG_LEDGER.md")
	if err != nil {
		t.Fatal(err)
	}
	for tag, name := range map[msgr.Tag]string{
		msgr.Hello: "Hello", msgr.AuthRequest: "AuthRequest", msgr.AuthBadMethod: "AuthBadMethod",
		msgr.AuthReplyMore: "AuthReplyMore", msgr.AuthRequestMore: "AuthRequestMore", msgr.AuthDone: "AuthDone",
		msgr.AuthSignature: "AuthSignature", msgr.ClientIdent: "ClientIdent", msgr.ServerIdent: "ServerIdent",
		msgr.IdentMissingFeatures: "IdentMissingFeatures", msgr.SessionReconnect: "SessionReconnect",
		msgr.SessionReset: "SessionReset", msgr.SessionRetry: "SessionRetry", msgr.SessionRetryGlobal: "SessionRetryGlobal",
		msgr.SessionReconnectOK: "SessionReconnectOK", msgr.Wait: "Wait", msgr.Message: "Message",
		msgr.Keepalive: "Keepalive", msgr.KeepaliveAck: "KeepaliveAck", msgr.Ack: "Ack",
		msgr.CompressionRequest: "CompressionRequest", msgr.CompressionDone: "CompressionDone",
	} {
		if want := fmt.Sprintf("| %d | %s | ", tag, name); !strings.Contains(string(doc), want) {
			t.Errorf("tag %d %s has no ledger row", tag, name)
		}
	}
}
