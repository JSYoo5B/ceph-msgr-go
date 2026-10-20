package cephx

import (
	"testing"
)

func TestTicketReceiptIsLocalParseIdentityAndAtomicOnFailure(t *testing.T) {
	key := rfcKey(t)
	c, err := NewClient("client.receipt", key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if (Ticket{}).ReceiptIdentity() != nil {
		t.Fatal("zero ticket fabricated provenance")
	}
	reply := testAuthReply(t, key, key, key, nil)
	if _, _, err := c.Finish(42, reply, true); err != nil {
		t.Fatal(err)
	}
	first := c.Tickets[ServiceMgr].ReceiptIdentity()
	if first == nil {
		t.Fatal("successful parse did not get an identity")
	}
	if _, _, err := c.Finish(42, reply, true); err != nil {
		t.Fatal(err)
	}
	second := c.Tickets[ServiceMgr].ReceiptIdentity()
	if second == nil || first == second {
		t.Fatal("identical service reply reused local parse identity")
	}
	if _, _, err := c.Finish(99, reply[:len(reply)-1], true); err == nil {
		t.Fatal("truncated reply accepted")
	}
	if c.GlobalID != 42 || c.Tickets[ServiceMgr].ReceiptIdentity() != second {
		t.Fatal("failed parse published receipt or client identity")
	}
}
