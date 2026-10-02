package cephmsgr

import (
	"errors"
	"fmt"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
)

// State is a local point-in-time view of the client. It contains no credentials
// or ticket contents. Map and identity fields retain their last known values
// across disconnection and Close; readiness does not promise command success.
type State struct {
	Closed        bool
	FSID          string
	GlobalID      uint64 // Authenticated client ID, distinct from Manager.GlobalID.
	Monitor       MonitorState
	Manager       ManagerState
	AuthTicket    TicketState
	MgrTicket     TicketState
	AuthRejection *AuthenticationError // A copy of an explicit MON rejection.
}

// MonitorState describes the authenticated MonMap and the held MON session.
type MonitorState struct {
	Ready          bool
	Endpoint       string // Peer reported by the held connection's RemoteAddr.
	MapEpoch       uint32
	MinimumRelease uint8
	Endpoints      []string // msgr2 dial endpoints from the last authenticated map; address family may differ from Endpoint.
}

// ManagerState separates advertised MGR availability from a ready client
// session. Ready also requires MON admission, which MGR operations wait for.
// MGR connections are opened lazily by MgrCommand, MgrTell or WaitMgrReady.
type ManagerState struct {
	Ready     bool
	Endpoint  string // Peer reported by the held connection's RemoteAddr.
	MapEpoch  uint32
	Available bool // Last received MgrMap value, not a live daemon probe.
	Name      string
	GlobalID  uint64
	Endpoints []string // msgr2 dial endpoints from the last authenticated map; address family may differ from Endpoint.
}

// TicketState exposes local renewal scheduling without keys or opaque proofs.
// A zero value means no ticket is available for new authentication. An older
// opaque proof can remain internal to renew tickets after service-key disposal.
type TicketState struct {
	Expires    time.Time
	RenewAfter time.Time
}

// Snapshot reads client state without sending a command, dialing, or waiting
// for recovery. The returned slices and rejection can be modified independently
// of the client. Ready is false after Close or an explicit auth rejection.
func (c *Client) Snapshot() State {
	c.mu.Lock()
	state := State{Closed: c.closed, GlobalID: c.auth.GlobalID}
	if c.fsid != [16]byte{} {
		state.FSID = fmt.Sprintf("%x-%x-%x-%x-%x", c.fsid[:4], c.fsid[4:6], c.fsid[6:8], c.fsid[8:10], c.fsid[10:])
	}
	state.Monitor.MapEpoch = c.monMap.Epoch
	state.Monitor.MinimumRelease = c.monMap.MinimumRelease
	for _, address := range c.monMap.Addresses {
		state.Monitor.Endpoints = append(state.Monitor.Endpoints, dialAddress(address))
	}
	state.Manager.MapEpoch = c.mgrMap.Epoch
	state.Manager.Available = c.mgrMap.Available
	state.Manager.Name = c.mgrMap.Name
	state.Manager.GlobalID = c.mgrMap.GlobalID
	for _, address := range c.mgrMap.Addresses {
		state.Manager.Endpoints = append(state.Manager.Endpoints, dialAddress(address))
	}
	authError := c.authErr
	admitted := !c.closed && authError == nil
	monSession, mgrSession := c.mon, c.mgr
	if monSession != nil {
		state.Monitor.Ready = admitted && c.monReady && monSession.Err() == nil
	}
	if mgrSession != nil {
		state.Manager.Ready = state.Monitor.Ready && mgrSession.Err() == nil
	}
	auth, mgr := c.auth.Tickets[cephx.ServiceAuth], c.auth.Tickets[cephx.ServiceMgr]
	state.AuthTicket = TicketState{Expires: auth.Expires, RenewAfter: auth.RenewAfter}
	state.MgrTicket = TicketState{Expires: mgr.Expires, RenewAfter: mgr.RenewAfter}
	c.mu.Unlock()
	// errors.As and RemoteAddr may invoke caller-supplied implementations.
	// Do not invoke custom code under the client lock.
	var rejection *AuthenticationError
	if errors.As(authError, &rejection) {
		copyRejection := *rejection
		state.AuthRejection = &copyRejection
	}
	if monSession != nil {
		if address := monSession.RemoteAddr(); address != nil {
			state.Monitor.Endpoint = address.String()
		}
	}
	if mgrSession != nil {
		if address := mgrSession.RemoteAddr(); address != nil {
			state.Manager.Endpoint = address.String()
		}
	}
	return state
}
