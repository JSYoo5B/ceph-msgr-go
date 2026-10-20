package cephmsgr

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
)

// TestMessageLedgerCoversEveryType keeps MESSAGE_LEDGER.md aligned with the
// message type constants of the product packages.
func TestMessageLedgerCoversEveryType(t *testing.T) {
	doc, err := os.ReadFile("MESSAGE_LEDGER.md")
	if err != nil {
		t.Fatal(err)
	}
	for name, id := range map[string]uint16{
		"PingMessage": msgr.PingMessage, "MonMapMessage": msgr.MonMapMessage, "MonGetMapMessage": msgr.MonGetMapMessage,
		"MonGetOSDMapMessage": msgr.MonGetOSDMapMessage, "StatfsMessage": msgr.StatfsMessage,
		"StatfsReplyMessage": msgr.StatfsReplyMessage, "SubscribeMessage": msgr.SubscribeMessage,
		"SubscribeAckMessage": msgr.SubscribeAckMessage, "AuthMessage": msgr.AuthMessage,
		"AuthReplyMessage": msgr.AuthReplyMessage, "MonGetVersionMessage": msgr.MonGetVersionMessage,
		"MonGetVersionReplyMessage": msgr.MonGetVersionReplyMessage, "MDSMapMessageType": mds.MDSMapMessageType,
		"ClientSessionMessage": mds.ClientSessionMessage, "ClientReconnectMessage": mds.ClientReconnectMessage,
		"ClientRequestMessage": mds.ClientRequestMessage, "ClientRequestForwardMessage": mds.ClientRequestForwardMessage,
		"ClientReplyMessage": mds.ClientReplyMessage, "ClientReclaimMessage": mds.ClientReclaimMessage,
		"ClientReclaimReplyMessage": mds.ClientReclaimReplyMessage, "OSDMapMessage": msgr.OSDMapMessage,
		"OSDOpMessage": osd.RequestMessage, "OSDOpReplyMessage": osd.ReplyMessage,
		"WatchNotifyMessage": osd.WatchNotifyMessage, "PoolOpReplyMessage": msgr.PoolOpReplyMessage,
		"PoolOpMessage": msgr.PoolOpMessage, "MonCommandMessage": msgr.MonCommandMessage,
		"MonCommandReplyMessage": msgr.MonCommandReplyMessage, "LogMessage": msgr.LogMessage,
		"BackoffMessage": osd.BackoffMessage, "ConfigMessage": msgr.ConfigMessage, "GetConfigMessage": msgr.GetConfigMessage,
		"TellCommandMessage": msgr.TellCommandMessage, "TellCommandReplyMessage": msgr.TellCommandReplyMessage,
		"FSMapUserMessage": mds.FSMapUserMessage, "ClientCapsMessage": mds.ClientCapsMessage,
		"ClientLeaseMessage": mds.ClientLeaseMessage, "ClientSnapMessage": mds.ClientSnapMessage,
		"ClientCapReleaseMessage": mds.ClientCapReleaseMessage, "ClientQuotaMessage": mds.ClientQuotaMessage,
		"MgrMapMessage": msgr.MgrMapMessage, "MgrDigestMessage": msgr.MgrDigestMessage,
		"ServiceMapMessage": msgr.ServiceMapMessage, "MgrCommandMessage": msgr.MgrCommandMessage,
		"MgrCommandReplyMessage": msgr.MgrCommandReplyMessage,
	} {
		decimal, hex := fmt.Sprintf("| %d | %s", id, name), fmt.Sprintf("| %#x | %s", id, name)
		if !strings.Contains(string(doc), decimal) && !strings.Contains(string(doc), hex) {
			t.Errorf("%s (%d) has no ledger row", name, id)
		}
	}
}
