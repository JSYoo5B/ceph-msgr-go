package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

type pendingFlockWait struct {
	Safe              bool
	Started, Finished time.Time
	Reply             *cephmsgr.MDSReply
	Error             string `json:",omitempty"`
}

type pendingFlockRequest struct {
	Name               string
	Request            cephmsgr.MDSRequest
	Selected, Admitted time.Time
	Transaction        uint64
	SubmitError        string `json:",omitempty"`
	Waits              []pendingFlockWait
	applied            bool
}

type pendingFlockEvidence struct {
	Source                      string
	Observation                 string
	Ino, GlobalID               uint64
	BeforeSession, AfterSession cephmsgr.MDSSessionStatus
	ResumesBefore, ResumesAfter uint64
	BeforeInode, AfterInode     *cephmsgr.MDSInodeStat
	NativeBefore                *nativeMDSCephFSFlockState
	NativeBeforeSource          string
	NativeAfter                 *nativeMDSCephFSReceipt
	Requests                    []*pendingFlockRequest
	WaitTimeoutMillis           int64
	CleanupErrors               []string
	CleanupComplete             bool
}

// This leaf observes one application Submit for B, local wait expiry and a
// later reply of that same call. It does not observe an MDS waiting_locks
// entry or the server's wait branch before the local deadline. Messenger
// resumption is reported separately from application resubmission.
func cephfsPendingFlockLateGrant(t *testing.T, ctx context.Context, conn *cephmsgr.MDSConnection, s *cephmsgr.MDSSession,
	agent *cephfsCapAgent, control *mdsCephFSControl, ino uint64, epoch uint32, facts map[string]any) {
	t.Helper()
	e := &pendingFlockEvidence{Source: mdsFreshSource,
		Observation: "same-session ordered requests and local wait expiry; no direct native wait-queue observation",
		Ino:         ino, GlobalID: s.Status().GlobalID, BeforeSession: s.Status(), ResumesBefore: conn.Resumes(), WaitTimeoutMillis: 750,
		NativeBeforeSource: "filelock_nonblocking_native_conflict", CleanupErrors: []string{}}
	facts["pending_flock"] = e
	previousNative, ok := facts["flock_native_unchanged"].(*nativeMDSCephFSFlockState)
	if !ok || previousNative == nil || previousNative.Ino != ino {
		t.Fatal("pending flock requires the prior owned native file baseline")
	}
	copyNative := *previousNative
	e.NativeBefore = &copyNative

	request := func(name string, r cephmsgr.MDSRequest, admission context.Context) (*cephmsgr.MDSRequestCall, *pendingFlockRequest, error) {
		r.MDSMapEpoch = epoch
		r.Path = cephmsgr.MDSFilePath{Ino: ino}
		step := &pendingFlockRequest{Name: name, Request: r, Selected: time.Now()}
		e.Requests = append(e.Requests, step)
		call, err := s.Submit(admission, r)
		if err != nil {
			step.SubmitError = err.Error()
			return call, step, err
		}
		step.Admitted, step.Transaction = time.Now(), call.Transaction()
		return call, step, nil
	}
	wait := func(call *cephmsgr.MDSRequestCall, step *pendingFlockRequest, local context.Context, safe bool) (cephmsgr.MDSReply, error) {
		w := pendingFlockWait{Safe: safe, Started: time.Now()}
		var reply cephmsgr.MDSReply
		var err error
		if safe {
			reply, err = call.WaitSafe(local)
		} else {
			reply, err = call.Wait(local)
		}
		w.Finished = time.Now()
		if err != nil {
			w.Error = err.Error()
		} else {
			w.Reply = &reply
			if reply.Transaction != step.Transaction || reply.Op != step.Request.Op || safe && !reply.Safe {
				err = errors.New("pending flock reply identity or safe flag differs")
			} else if !step.applied {
				_, _, err = agent.applyReply(step.Name, reply)
				step.applied = err == nil
			}
			if err != nil {
				w.Error = err.Error()
			}
		}
		step.Waits = append(step.Waits, w)
		return reply, err
	}
	complete := func(name string, r cephmsgr.MDSRequest) cephmsgr.MDSReply {
		t.Helper()
		call, step, err := request(name, r, ctx)
		if err != nil {
			t.Fatal(name, err)
		}
		local, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		reply, err := wait(call, step, local, true)
		if err != nil || reply.Result != 0 {
			t.Fatal(name, reply.Result, err)
		}
		return reply
	}
	// Each unlock has one selection across the normal path and cleanup. A
	// timeout re-waits that call; it never creates another unlock request.
	type chosenUnlock struct {
		selected bool
		call     *cephmsgr.MDSRequestCall
		step     *pendingFlockRequest
		err      error
	}
	var unlockA, unlockB chosenUnlock
	unlock := func(choice *chosenUnlock, name string, owner uint64, local context.Context) error {
		bounded, cancel := context.WithTimeout(local, 15*time.Second)
		defer cancel()
		if !choice.selected {
			choice.selected = true
			choice.call, choice.step, choice.err = request(name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpSetFileLock,
				Args: cephfsFlockArgs(cephFlockUnlock, owner)}, bounded)
		}
		if choice.call == nil {
			return choice.err
		}
		reply, err := wait(choice.call, choice.step, bounded, true)
		if err == nil && reply.Result != 0 {
			err = fmt.Errorf("%s returned %d", name, reply.Result)
		}
		return err
	}
	ownerB := cephFlockGoOwner + 1
	var bCall *cephmsgr.MDSRequestCall
	var bStep *pendingFlockRequest
	var aAttempted, aReleased, bResolved, bGranted, bReleased bool
	agent.open(ino, cephFileModeRDWR)
	opened := true
	closeRef := func() error {
		if !opened {
			return nil
		}
		// close drops the local reference before sending any cap update.
		// Do not drop that reference twice if the update itself fails.
		opened = false
		_, _, err := agent.close(ino, cephFileModeRDWR)
		return err
	}
	t.Cleanup(func() {
		clean, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		fail := func(err error) {
			if err != nil {
				e.CleanupErrors = append(e.CleanupErrors, err.Error())
				t.Error("pending flock cleanup", err)
			}
		}
		if aAttempted && !aReleased {
			err := unlock(&unlockA, "pending-flock-unlock-a", cephFlockGoOwner, clean)
			aReleased = err == nil
			fail(err)
		}
		// Release A before waiting for the original B. This avoids a separate
		// cancellation-before-server-admission race and never replays acquire.
		if bCall != nil && !bResolved {
			reply, err := wait(bCall, bStep, clean, true)
			bResolved, bGranted = err == nil, err == nil && reply.Result == 0
			fail(err)
		}
		if bGranted && !bReleased {
			err := unlock(&unlockB, "pending-flock-unlock-b", ownerB, clean)
			bReleased = err == nil
			fail(err)
		}
		fail(closeRef())
		e.AfterSession, e.ResumesAfter = s.Status(), conn.Resumes()
		e.CleanupComplete = len(e.CleanupErrors) == 0 && (!aAttempted || aReleased) && (bCall == nil || bResolved) && (!bGranted || bReleased)
	})
	openedReply := complete("pending-flock-open", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpOpen, Args: cephfsOpenArgs(cephOpenReadWrite, 0, 0)})
	before, err := openedReply.DecodeTrace()
	if err != nil || before.Target == nil || before.Target.Ino != ino {
		t.Fatal("pending flock OPEN omitted the owned inode", err)
	}
	e.BeforeInode = before.Target
	aAttempted = true
	complete("pending-flock-acquire-a", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpSetFileLock, Args: cephfsFlockArgs(cephFlockExclusive, cephFlockGoOwner)})
	bArgs := cephfsFlockArgs(cephFlockExclusive, ownerB)
	bArgs[34] = 1 // wait on conflict; only the following local wait expires.
	bCall, bStep, err = request("pending-flock-acquire-b-once", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpSetFileLock, Args: bArgs}, ctx)
	if err != nil {
		t.Fatal("pending B Submit", err)
	}
	originalTID := bCall.Transaction()
	// This reply proves progress of an application request queued after B on
	// the same ordered writer, not direct observation of the MDS wait queue.
	complete("pending-flock-getattr-barrier", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpGetattr, Args: cephfsGetattrArgs(cephStatAll)})
	local, cancel := context.WithTimeout(ctx, time.Duration(e.WaitTimeoutMillis)*time.Millisecond)
	_, err = wait(bCall, bStep, local, false)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("B local Wait did not expire while A held the lock", err)
	}
	if err := unlock(&unlockA, "pending-flock-unlock-a", cephFlockGoOwner, ctx); err != nil {
		t.Fatal(err)
	}
	aReleased = true
	late, stopLate := context.WithTimeout(ctx, 15*time.Second)
	reply, err := wait(bCall, bStep, late, true)
	stopLate()
	bResolved, bGranted = err == nil, err == nil && reply.Result == 0
	if err != nil || !bGranted || bCall.Transaction() != originalTID || reply.Transaction != originalTID {
		t.Fatal("original B call did not receive its late grant", reply, err)
	}
	query := complete("pending-flock-query-b", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpGetFileLock,
		Args: cephfsFlockArgs(cephFlockExclusive, cephFlockGoOwner)})
	lock, err := cephfsDecodeFilelock(query.Extra)
	if err != nil || lock != (cephfsFilelock{Client: e.GlobalID, Owner: ownerB, Type: cephFlockExclusive}) {
		t.Fatal("late grant did not belong to owner B", lock, err)
	}
	if err := unlock(&unlockB, "pending-flock-unlock-b", ownerB, ctx); err != nil {
		t.Fatal(err)
	}
	bReleased = true
	native, err := control.request(ctx, "flock-ex-nb", "go-file")
	e.NativeAfter = &native
	if err != nil {
		t.Fatal("native probe after pending flock cleanup", err)
	}
	p := native.Native.Flock
	if p == nil || native.Mutation == nil || native.Mutation.State != "applied" || p.Errno != 0 || !p.Acquired || !p.Unlocked ||
		p.Owner != cephFlockNativeOwner || p.ElapsedNS <= 0 || p.Before != p.After || p.Before != *e.NativeBefore {
		t.Fatal("pending flock left a native conflict or changed the file", native.Native, native.Mutation)
	}
	afterReply := complete("pending-flock-getattr-after", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpGetattr, Args: cephfsGetattrArgs(cephStatAll)})
	after, err := afterReply.DecodeTrace()
	e.AfterInode = after.Target
	if err != nil || after.Target == nil || after.Target.Ino != ino || after.Target.Size != e.BeforeInode.Size ||
		after.Target.ChangeAttr != e.BeforeInode.ChangeAttr || after.Target.MTime != e.BeforeInode.MTime ||
		e.BeforeInode.ChangeAttr != e.NativeBefore.Version {
		t.Fatal("pending flock changed inode content metadata", e.BeforeInode, after.Target, err)
	}
	if err := closeRef(); err != nil {
		e.CleanupErrors = append(e.CleanupErrors, err.Error())
		t.Fatal("close pending flock handle", err)
	}
}
