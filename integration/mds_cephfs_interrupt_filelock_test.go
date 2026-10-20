package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// This leaf observes separate safe replies to an explicit FLOCK interrupt
// and the original acquire. The ordered barrier and local timeout do not
// directly observe a native wait-queue entry. A stays held until B returns
// -EINTR, excluding the grant race from the successful path.
func cephfsInterruptFlockWaiter(t *testing.T, ctx context.Context, conn *cephmsgr.MDSConnection, s *cephmsgr.MDSSession,
	agent *cephfsCapAgent, control *mdsCephFSControl, ino uint64, epoch uint32, facts map[string]any) {
	t.Helper()
	e := &pendingFlockEvidence{Source: mdsFreshSource,
		Observation: "same-session ordered barrier, local wait expiry, explicit interrupt and original acquire replies; no direct native wait-queue observation",
		Ino:         ino, GlobalID: s.Status().GlobalID, BeforeSession: s.Status(), ResumesBefore: conn.Resumes(), WaitTimeoutMillis: 750,
		NativeBeforeSource: "filelock_nonblocking_native_conflict", CleanupErrors: []string{}}
	facts["interrupt_flock"] = e
	previousNative, ok := facts["flock_native_unchanged"].(*nativeMDSCephFSFlockState)
	if !ok || previousNative == nil || previousNative.Ino != ino {
		t.Fatal("interrupt flock requires the prior owned native file baseline")
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
				err = errors.New("interrupt flock reply identity or safe flag differs")
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
	var iCall *cephmsgr.MDSRequestCall
	var iStep *pendingFlockRequest
	var aAttempted, aReleased, bResolved, bGranted, bReleased, iAttempted, iResolved bool
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
				t.Error("interrupt flock cleanup", err)
			}
		}
		if aAttempted && !aReleased {
			err := unlock(&unlockA, "interrupt-flock-unlock-a", cephFlockGoOwner, clean)
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
			err := unlock(&unlockB, "interrupt-flock-unlock-b", ownerB, clean)
			bReleased = err == nil
			fail(err)
		}
		// An interrupt never acquires a lock. Resolve its original call if
		// possible, without selecting another interrupt after uncertainty.
		if iCall != nil && !iResolved {
			reply, err := wait(iCall, iStep, clean, true)
			iResolved = err == nil
			if err == nil && reply.Result != 0 {
				err = fmt.Errorf("interrupt cleanup reply returned %d", reply.Result)
			}
			fail(err)
		}
		fail(closeRef())
		e.AfterSession, e.ResumesAfter = s.Status(), conn.Resumes()
		e.CleanupComplete = len(e.CleanupErrors) == 0 && (!aAttempted || aReleased) && (bCall == nil || bResolved) && (!bGranted || bReleased) && (!iAttempted || iResolved)
	})
	openedReply := complete("interrupt-flock-open", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpOpen, Args: cephfsOpenArgs(cephOpenReadWrite, 0, 0)})
	before, err := openedReply.DecodeTrace()
	if err != nil || before.Target == nil || before.Target.Ino != ino {
		t.Fatal("interrupt flock OPEN omitted the owned inode", err)
	}
	e.BeforeInode = before.Target
	aAttempted = true
	complete("interrupt-flock-acquire-a", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpSetFileLock, Args: cephfsFlockArgs(cephFlockExclusive, cephFlockGoOwner)})
	bArgs := cephfsFlockArgs(cephFlockExclusive, ownerB)
	bArgs[34] = 1 // wait on conflict; only the following local wait expires.
	bCall, bStep, err = request("interrupt-flock-acquire-b-once", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpSetFileLock, Args: bArgs}, ctx)
	if err != nil {
		t.Fatal("interrupt B Submit", err)
	}
	originalTID := bCall.Transaction()
	// This reply proves progress of an application request queued after B on
	// the same ordered writer, not direct observation of the MDS wait queue.
	complete("interrupt-flock-getattr-barrier", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpGetattr, Args: cephfsGetattrArgs(cephStatAll)})
	local, cancel := context.WithTimeout(ctx, time.Duration(e.WaitTimeoutMillis)*time.Millisecond)
	_, err = wait(bCall, bStep, local, false)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("B local Wait did not expire while A held the lock", err)
	}
	// Client::_interrupt_filelock copies all original arguments, including
	// wait=1, then changes only rule and type. Submit supplies a distinct TID.
	iRequest := bStep.Request
	iRequest.Args[0], iRequest.Args[1] = 4, cephFlockUnlock // FLOCK_INTR, UNLOCK
	iAttempted = true
	iCall, iStep, err = request("interrupt-flock-interrupt-b-once", iRequest, ctx)
	if err != nil {
		t.Fatal("interrupt Submit", err)
	}
	if iCall.Transaction() == originalTID {
		t.Fatal("interrupt reused the acquire transaction", originalTID)
	}
	// The calls retain replies independently. Waiting for I first imposes
	// no ordering requirement on the arrival of I and B's replies.
	interruptWait, stopInterrupt := context.WithTimeout(ctx, 15*time.Second)
	iReply, err := wait(iCall, iStep, interruptWait, true)
	stopInterrupt()
	iResolved = err == nil
	if err != nil || iReply.Result != 0 {
		t.Fatal("explicit interrupt did not succeed", iReply, err)
	}
	originalWait, stopOriginal := context.WithTimeout(ctx, 15*time.Second)
	reply, err := wait(bCall, bStep, originalWait, true)
	stopOriginal()
	bResolved, bGranted = err == nil, err == nil && reply.Result == 0
	if err != nil || reply.Result != -4 || bCall.Transaction() != originalTID || reply.Transaction != originalTID {
		t.Fatal("original B call did not return safe EINTR", reply, err)
	}
	query := complete("interrupt-flock-query-a-still-held", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpGetFileLock,
		Args: cephfsFlockArgs(cephFlockExclusive, ownerB)})
	lock, err := cephfsDecodeFilelock(query.Extra)
	if err != nil || lock != (cephfsFilelock{Client: e.GlobalID, Owner: cephFlockGoOwner, Type: cephFlockExclusive}) {
		t.Fatal("interrupt changed the original owner A's held lock", lock, err)
	}
	if err := unlock(&unlockA, "interrupt-flock-unlock-a", cephFlockGoOwner, ctx); err != nil {
		t.Fatal(err)
	}
	aReleased = true
	native, err := control.request(ctx, "flock-ex-nb", "go-file")
	e.NativeAfter = &native
	if err != nil {
		t.Fatal("native probe after explicit interrupt and A unlock", err)
	}
	p := native.Native.Flock
	if p == nil || native.Mutation == nil || native.Mutation.State != "applied" || p.Errno != 0 || !p.Acquired || !p.Unlocked ||
		p.Owner != cephFlockNativeOwner || p.ElapsedNS <= 0 || p.Before != p.After || p.Before != *e.NativeBefore {
		t.Fatal("interrupt flock left a native conflict or changed the file", native.Native, native.Mutation)
	}
	afterReply := complete("interrupt-flock-getattr-after", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpGetattr, Args: cephfsGetattrArgs(cephStatAll)})
	after, err := afterReply.DecodeTrace()
	e.AfterInode = after.Target
	if err != nil || after.Target == nil || after.Target.Ino != ino || after.Target.Size != e.BeforeInode.Size ||
		after.Target.ChangeAttr != e.BeforeInode.ChangeAttr || after.Target.MTime != e.BeforeInode.MTime ||
		e.BeforeInode.ChangeAttr != e.NativeBefore.Version {
		t.Fatal("interrupt flock changed inode content metadata", e.BeforeInode, after.Target, err)
	}
	if err := closeRef(); err != nil {
		e.CleanupErrors = append(e.CleanupErrors, err.Error())
		t.Fatal("close interrupt flock handle", err)
	}
}
