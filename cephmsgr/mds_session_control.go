package cephmsgr

import (
	"context"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
)

// Renew sends one REQUEST_RENEWCAPS and waits for a RENEWCAPS reply with the
// same or a later sequence. ctx bounds only the local wait.
func (s *MDSSession) Renew(ctx context.Context) (MDSSessionRenewal, error) {
	if err := ctx.Err(); err != nil {
		return MDSSessionRenewal{}, err
	}
	seq, err := s.sendRenewal(ctx)
	if err != nil {
		return MDSSessionRenewal{}, err
	}
	for {
		s.mu.Lock()
		renewal, state, terminal, changed := s.renewal, s.state, s.err, s.changed
		s.mu.Unlock()
		if renewal.Sequence >= seq {
			return renewal, nil
		}
		if state.terminal() {
			return MDSSessionRenewal{}, terminal
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return MDSSessionRenewal{}, ctx.Err()
		}
	}
}

// FlushJournal asks the MDS to flush its journal now (REQUEST_FLUSH_MDLOG,
// as native Client::flush_mdlog does) so pending unsafe requests become safe
// sooner. The MDS sends no answer; safe replies remain the only completion
// signal. ctx governs write admission only.
func (s *MDSSession) FlushJournal(ctx context.Context) error {
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
	// Native clients send this only to MDSs advertising LUMINOUS (bit 7).
	if !mds.FeatureBitsetHas(features, 7) {
		return ErrMDSRequestFeatures
	}
	message, err := mds.EncodeSessionControl(mds.SessionRequestFlushMDLog, 0)
	if err != nil {
		return err
	}
	return s.conn.session.SendControl(ctx, message)
}

// AcknowledgeFlush sends FLUSHMSG_ACK for a FLUSHMSG event's sequence. The
// library never acknowledges a flush on the caller's behalf. ctx governs only
// write admission; success is not proof that the MDS processed the ack.
func (s *MDSSession) AcknowledgeFlush(ctx context.Context, seq uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	state, err := s.state, s.err
	s.mu.Unlock()
	if state.terminal() {
		return err
	}
	message, err := mds.EncodeSessionControl(mds.SessionFlushMessageAck, seq)
	if err != nil {
		return err
	}
	return s.conn.session.SendControl(ctx, message)
}

func (s *MDSSession) sendRenewal(ctx context.Context) (uint64, error) {
	s.mu.Lock()
	if s.state.terminal() {
		err := s.err
		s.mu.Unlock()
		return 0, err
	}
	if s.state == MDSSessionOpening {
		s.mu.Unlock()
		return 0, errors.New("ceph: MDS session is not open")
	}
	if s.renewSeq == ^uint64(0) {
		s.mu.Unlock()
		return 0, errors.New("ceph: MDS session renewal sequences exhausted")
	}
	s.renewSeq++
	seq := s.renewSeq
	s.renewSent[seq] = time.Now()
	// Bound outstanding renewals: a reply covers every older sequence.
	if len(s.renewSent) > 64 {
		delete(s.renewSent, slices.Min(slices.Collect(maps.Keys(s.renewSent))))
	}
	s.mu.Unlock()
	message, err := mds.EncodeSessionControl(mds.SessionRequestRenewCaps, seq)
	if err != nil {
		return 0, err
	}
	if err := s.conn.session.SendControl(ctx, message); err != nil {
		return 0, err
	}
	return seq, nil
}

func (s *MDSSession) renewLoop() {
	defer close(s.stopped)
	ticker := time.NewTicker(s.renewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
		case <-s.renewNow:
		case <-s.closeNow:
			// A counted push raised the sequence while closing; the MDS drops
			// a REQUEST_CLOSE that is behind, so resend it as natively.
			s.mu.Lock()
			state, seq := s.state, s.push
			s.mu.Unlock()
			if state != MDSSessionClosing {
				continue
			}
			message, err := mds.EncodeSessionControl(mds.SessionRequestClose, seq)
			if err != nil {
				return
			}
			ctx, cancel := context.WithTimeout(s.conn.ctx, s.renewInterval)
			err = s.conn.session.SendControl(ctx, message)
			cancel()
			if err != nil && s.conn.session.Err() != nil {
				return
			}
			continue
		}
		s.mu.Lock()
		state := s.state
		s.mu.Unlock()
		if state != MDSSessionOpen && state != MDSSessionStale {
			continue
		}
		ctx, cancel := context.WithTimeout(s.conn.ctx, s.renewInterval)
		_, err := s.sendRenewal(ctx)
		cancel()
		if err != nil && s.conn.session.Err() != nil {
			return
		}
	}
}

func (s *MDSSession) triggerRenewal() {
	select {
	case s.renewNow <- struct{}{}:
	default:
	}
}
