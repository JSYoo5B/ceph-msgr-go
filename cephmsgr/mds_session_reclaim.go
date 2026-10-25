package cephmsgr

import (
	"context"
	"errors"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
)

// MDSReclaimReply is the MDS's answer to Reclaim. Result is the signed code
// (zero on success). Addresses are the evicted session's addresses; an empty
// list with Result 0 means the MDS found no session with that uuid, which the
// native client reports as ENOENT. Epoch is the OSD map epoch the MDS held
// when it evicted the target, for the caller's blocklist wait.
type MDSReclaimReply struct {
	Result    int32
	Epoch     uint32
	Addresses []MDSMapAddress
}

// Reclaim asks the MDS to evict the session whose client metadata uuid is
// uuid and blocklist its address, then answer (MClientReclaim with
// CEPH_RECLAIM_RESET, as Client::start_reclaim sends it). The target must
// belong to the same CephX identity (the MDS answers EPERM otherwise) and this
// session must not use that uuid itself. The MDS must advertise
// RECLAIM_CLIENT. One reclaim may be pending per session. ctx bounds only the
// local wait; after the message is sent a cancelled wait or session end
// returns an OutcomeUnknownError because the target may already be evicted.
func (s *MDSSession) Reclaim(ctx context.Context, uuid string) (MDSReclaimReply, error) {
	if err := ctx.Err(); err != nil {
		return MDSReclaimReply{}, err
	}
	message, err := mds.EncodeReclaim(uuid, mds.ReclaimReset)
	if err != nil {
		return MDSReclaimReply{}, err
	}
	s.mu.Lock()
	state, terminal, features := s.state, s.err, s.open.SupportedFeatures
	switch {
	case state.terminal():
		s.mu.Unlock()
		return MDSReclaimReply{}, terminal
	case state == MDSSessionOpening:
		s.mu.Unlock()
		return MDSReclaimReply{}, errors.New("ceph: MDS session is not open")
	case !mds.FeatureBitsetHas(features, mds.FeatureReclaimClient):
		s.mu.Unlock()
		return MDSReclaimReply{}, ErrMDSReclaimUnsupported
	case uuid == s.uuid:
		s.mu.Unlock()
		return MDSReclaimReply{}, ErrMDSReclaimSelf
	case s.reclaim != nil:
		s.mu.Unlock()
		return MDSReclaimReply{}, ErrMDSReclaimPending
	case s.reclaimUncertain:
		s.mu.Unlock()
		return MDSReclaimReply{}, ErrMDSReclaimUncertain
	}
	wait := make(chan mds.ReclaimReply, 1)
	s.reclaim = wait
	s.mu.Unlock()
	clear := func(abandoned bool) {
		s.mu.Lock()
		if s.reclaim == wait {
			s.reclaim = nil
			if abandoned {
				s.reclaimUncertain = true
			}
		}
		s.mu.Unlock()
	}
	if err := s.conn.session.SendControl(ctx, message); err != nil {
		clear(false)
		return MDSReclaimReply{}, err
	}
	select {
	case reply := <-wait:
		return publicReclaimReply(reply), nil
	case <-s.done:
		clear(true)
		select {
		case reply := <-wait:
			return publicReclaimReply(reply), nil
		default:
		}
		return MDSReclaimReply{}, &OutcomeUnknownError{Cause: s.Err()}
	case <-ctx.Done():
		clear(true)
		select {
		case reply := <-wait:
			return publicReclaimReply(reply), nil
		default:
		}
		return MDSReclaimReply{}, &OutcomeUnknownError{Cause: ctx.Err()}
	}
}

// FinishReclaim sends MClientReclaim with only FLAG_FINISH, as
// Client::finish_reclaim does after a successful reclaim. The MDS sends no
// answer and, after a RESET reclaim, has nothing left to finish. ctx governs
// write admission only.
func (s *MDSSession) FinishReclaim(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	state, err, features := s.state, s.err, s.open.SupportedFeatures
	s.mu.Unlock()
	if state.terminal() {
		return err
	}
	if state == MDSSessionOpening {
		return errors.New("ceph: MDS session is not open")
	}
	if !mds.FeatureBitsetHas(features, mds.FeatureReclaimClient) {
		return ErrMDSReclaimUnsupported
	}
	message, err := mds.EncodeReclaim("", mds.ReclaimFinish)
	if err != nil {
		return err
	}
	return s.conn.session.SendControl(ctx, message)
}

func publicReclaimReply(r mds.ReclaimReply) MDSReclaimReply {
	out := MDSReclaimReply{Result: r.Result, Epoch: r.Epoch}
	for _, a := range r.Addresses {
		out.Addresses = append(out.Addresses, MDSMapAddress{Type: a.Type, Nonce: a.Nonce, Endpoint: a.Endpoint, FlowInfo: a.FlowInfo, ScopeID: a.ScopeID})
	}
	return out
}

// deliverReclaim hands a reply to the pending Reclaim. A reply with no waiter
// is a late answer to an abandoned wait and is ignored.
func (s *MDSSession) deliverReclaim(reply mds.ReclaimReply) {
	s.mu.Lock()
	wait := s.reclaim
	s.reclaim = nil
	s.mu.Unlock()
	if wait != nil {
		wait <- reply
	}
}
