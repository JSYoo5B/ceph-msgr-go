package main

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

func TestRBDGroupPlanAndRequests(t *testing.T) {
	token := strings.Repeat("ab", 16)
	plan := rbdGroupPlanFor(token)
	if !plan.owned(token) || plan.owned(strings.Repeat("cd", 16)) {
		t.Fatal("RBD group plan is not bound to its token", plan)
	}
	for name, mutate := range map[string]func(*rbdGroupPlan){
		"base image as a member":        func(p *rbdGroupPlan) { p.Images[1].Name = "ceph-msgr-image-" + token },
		"member b in the first pool":    func(p *rbdGroupPlan) { p.Images[1].Pool = p.Pool },
		"member a in the second pool":   func(p *rbdGroupPlan) { p.Images[0].Pool = p.SecondPool },
		"foreign second pool":           func(p *rbdGroupPlan) { p.SecondPool = "ceph-msgr-rbd2-" + strings.Repeat("cd", 16) },
		"layering-only members":         func(p *rbdGroupPlan) { p.Features = 1 },
		"refused snapshot as the first": func(p *rbdGroupPlan) { p.RefusedSnapshot = p.GroupSnapshot },
		"successful quiesce answer":     func(p *rbdGroupPlan) { p.QuiesceRefusal = 0 },
		"default quiesce attempts":      func(p *rbdGroupPlan) { p.QuiesceAttempts = rbdGroupDefaultQuiesceAttempts },
		"single quiesce attempt":        func(p *rbdGroupPlan) { p.QuiesceAttempts = 1 },
		"retried as the timed-out":      func(p *rbdGroupPlan) { p.RetriedSnapshot = p.TimedOutSnapshot },
		"foreign over-limit snapshot":   func(p *rbdGroupPlan) { p.OverLimitSnapshot = "ceph-msgr-gsnap-overlimit" },
	} {
		foreign := rbdGroupPlanFor(token)
		foreign.Images = slices.Clone(foreign.Images)
		mutate(&foreign)
		if foreign.owned(token) {
			t.Fatal("RBD group plan admitted:", name)
		}
	}
	for i, action := range rbdGroupActions {
		id, got, err := parseRBDGroupRequest([]string{fmt.Sprint(i + 1), token, action}, token, int64(i), i)
		if err != nil || id != int64(i+1) || got != action {
			t.Fatal("ordered RBD group request refused", action, err)
		}
	}
	for _, fields := range [][]string{
		{"1", token, "modify"},     // out of order
		{"1", token, "observe"},    // not admitted
		{"0", token, "setup"},      // no identity
		{"01", token, "setup"},     // non-canonical identity
		{"1", "ab", "setup"},       // foreign token
		{"1", token, "setup", "x"}, // extra field
		{"1", strings.Repeat("zz", 16), "setup"},
	} {
		if _, _, err := parseRBDGroupRequest(fields, token, 0, 0); err == nil {
			t.Fatal("RBD group request admitted", fields)
		}
	}
	if _, _, err := parseRBDGroupRequest([]string{"2", token, "setup"}, token, 1, 1); err == nil {
		t.Fatal("RBD group setup admitted twice")
	}
	if _, _, err := parseRBDGroupRequest([]string{"4", token, "accepted"}, token, 3, 2); err == nil {
		t.Fatal("RBD group accepted creation admitted before the refused one")
	}
	if _, _, err := parseRBDGroupRequest([]string{"6", token, "retried"}, token, 5, 4); err == nil {
		t.Fatal("RBD group retried creation admitted before the timed-out one")
	}
	if _, _, err := parseRBDGroupRequest([]string{"8", token, "rollback"}, token, 7, 6); err == nil {
		t.Fatal("RBD group rollback admitted before the over-limit creation")
	}
	n := len(rbdGroupActions)
	if _, _, err := parseRBDGroupRequest([]string{fmt.Sprint(n + 1), token, "cleanup"}, token, int64(n), n); err == nil {
		t.Fatal("RBD group action admitted after the sequence ended")
	}
}

// rbdGroupSample is a structurally valid driver output for the plan.
func rbdGroupSample(action, token string, plan rbdGroupPlan) rbdGroupNative {
	sum := strings.Repeat("0", 64)
	n := rbdGroupNative{Action: action, Token: token, RequestID: 7, Pool: plan.Pool, PoolID: 3, SecondPoolID: 4, NativeGlobalID: 4100, ImagesListed: []string{}, GroupsListed: []string{}}
	call := func(object, class, method string) rbdWriteClassCall {
		return rbdWriteClassCall{Object: object, Class: class, Method: method, InputHex: "00", OutputHex: ""}
	}
	if action == "cleanup" {
		directory := call("rbd_group_directory", "rbd", "dir_get_id")
		directory.Code = -2
		n.Removed = &rbdGroupRemoval{GroupID: "g1", GroupHeader: "rbd_group_header.g1", GroupHeaderErrno: 2, DirectoryCall: &directory,
			GroupSnapshotsAfter: [][]rbdGroupSnap{{}, {}, {}}, MembersAfter: []rbdGroupMember{}}
		for i, image := range plan.Images {
			id := fmt.Sprint("i", i)
			n.Removed.Images = append(n.Removed.Images, rbdGroupRemovedImage{Key: image.Key, Name: image.Name, Pool: image.Pool, ImageID: id, Prefix: "rbd_data." + id, Objects: plan.Size >> plan.Order,
				SnapshotsAfter: []rbdGroupSnapshot{}, HeaderErrno: 2, IDErrno: 2, ObjectMapErrno: 2, DataErrno: []int{2, 2, 2}})
		}
		return n
	}
	snapshots := map[string]int{"accepted": 2, "timed-out": 2, "retried": 3, "over-limit": 3, "rollback": 3}[action]
	if snapshots == 0 {
		snapshots = 1
	}
	directory := call("rbd_group_directory", "rbd", "dir_get_id")
	n.Group = &rbdGroupView{Name: plan.Group, ID: "g1", HeaderObject: "rbd_group_header.g1", DirectoryCall: &directory}
	for s := range snapshots {
		snap := rbdGroupSnap{ID: fmt.Sprint("s", s), Name: plan.GroupSnapshot, State: 1}
		n.Group.Snapshots = append(n.Group.Snapshots, snap)
		n.Group.SnapshotInfos = append(n.Group.SnapshotInfos, snap)
	}
	for range snapshots + 1 {
		n.Group.ListCalls = append(n.Group.ListCalls, call("rbd_group_header.g1", "rbd", "group_snap_list"))
	}
	if slices.Contains(rbdGroupCreating, action) {
		c := &rbdGroupCreation{Snapshot: plan.createdSnapshot(action), GroupID: "g1", ImageIDs: map[string]string{"a": "i0", "b": "i1"},
			QuiesceAttempts: rbdGroupDefaultQuiesceAttempts, Errno: map[string]int{"refused": 16, "timed-out": 110, "over-limit": 122}[action], ElapsedNS: 5}
		if action == "timed-out" || action == "retried" {
			c.QuiesceAttempts = plan.QuiesceAttempts
		}
		if action == "over-limit" {
			set, removed := call("rbd_header.i0", "rbd", "snapshot_get_limit"), call("rbd_header.i0", "rbd", "snapshot_get_limit")
			c.SnapLimit = &rbdGroupSnapLimit{Image: "a", Snapshots: 3, Limit: 3, SetCall: &set, RemovedLimit: ^uint64(0), RemovedCall: &removed}
		}
		for i, label := range rbdGroupCreationPhases[action] {
			list := call("rbd_group_header.g1", "rbd", "group_snap_list")
			p := rbdGroupCreationPhase{Phase: label, AfterNS: int64(i + 1), ListCall: &list}
			if i == 0 {
				rollback := 22
				p.RollbackErrno = &rollback
			}
			if label == "member-snapshot-observed" {
				member := call("rbd_header.i1", "rbd", "snapshot_get")
				p.MemberSnapshotCall = &member
			}
			for j, image := range plan.Images {
				object := fmt.Sprintf("rbd_header.i%d", j)
				snapc, lock, limit := call(object, "rbd", "get_snapcontext"), call(object, "lock", "get_info"), call(object, "rbd", "snapshot_get_limit")
				p.Images = append(p.Images, rbdGroupCreationImage{Key: image.Key, SnapContextCall: &snapc, LockCall: &lock, LimitCall: &limit})
			}
			c.Phases = append(c.Phases, p)
		}
		n.Creation = c
	}
	if action == "rollback" {
		n.RolledBackTo = plan.GroupSnapshot
	}
	for i, image := range plan.Images {
		phase, extents := "", []rbdBaseExtent(nil)
		switch action {
		case "setup":
			phase, extents = "pre", image.PreWrites
		case "modify":
			phase, extents = "post", image.PostWrites
		}
		for _, e := range extents {
			n.Writes = append(n.Writes, rbdGroupWrite{Image: image.Key, Phase: phase, Offset: e.Offset, Length: e.Length, Written: e.Length, SHA256: sum})
		}
		id := fmt.Sprint("i", i)
		header := "rbd_header." + id
		snapc, lock := call(header, "rbd", "get_snapcontext"), call(header, "lock", "get_info")
		v := rbdGroupImageView{Key: image.Key, Name: image.Name, Pool: image.Pool, PoolID: int64(3 + i), ImageID: id, HeaderObject: header, IDObject: "rbd_id." + image.Name,
			Features: plan.Features | rbdFeatureOperations, OpFeatures: rbdOperationFeatureGroup, SnapContextCall: &snapc, LockCall: &lock,
			Stat: &rbdBaseStat{Size: plan.Size, ObjectSize: 1 << plan.Order, Objects: plan.Size >> plan.Order, Order: plan.Order, Prefix: "rbd_data." + id, ParentPool: ^uint64(0)}}
		view := func(label string, snap uint64) rbdGroupDataView {
			d := rbdGroupDataView{Label: label, SnapID: snap}
			var offset uint64
			for j := range plan.Size >> plan.Order {
				d.Objects = append(d.Objects, rbdGroupObjectView{Index: j, Exists: true, Size: 1 << plan.Order, Offset: offset, SHA256: sum})
				offset += 1 << plan.Order
			}
			d.Image = rbdBaseBlob{Name: rbdGroupBlobName(action, image.Key, label, "image"), Length: plan.Size, SHA256: sum}
			d.ObjectBytes = rbdBaseBlob{Name: rbdGroupBlobName(action, image.Key, label, "objects"), Length: offset, SHA256: sum}
			return d
		}
		v.Views = append(v.Views, view("head", 0))
		for s := range snapshots {
			id := uint64(4 + s)
			v.Snapshots = append(v.Snapshots, rbdGroupSnapshot{ID: id, Name: ".group.3_g1_s1", Size: plan.Size, Namespace: 1, Group: &rbdGroupNamespace{Pool: 3, Name: plan.Group, SnapName: plan.GroupSnapshot}})
			v.SnapshotCalls = append(v.SnapshotCalls, call(header, "rbd", "snapshot_get"))
			v.Views = append(v.Views, view(fmt.Sprint("s", id), id))
		}
		n.Images = append(n.Images, v)
	}
	return n
}

func TestRBDGroupNativeOutputShape(t *testing.T) {
	token := strings.Repeat("ab", 16)
	plan := rbdGroupPlanFor(token)
	for i, action := range rbdGroupActions {
		data, err := json.Marshal(rbdGroupSample(action, token, plan))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodeRBDGroupNative(action, token, 7, plan, data); err != nil {
			t.Fatal("valid RBD group output refused", action, err)
		}
		if _, err := decodeRBDGroupNative(action, token, 8, plan, data); err == nil {
			t.Fatal("RBD group output of another request admitted", action)
		}
		other := rbdGroupActions[(i+1)%len(rbdGroupActions)]
		if _, err := decodeRBDGroupNative(other, token, 7, plan, data); err == nil {
			t.Fatal("RBD group output of another action admitted", action, other)
		}
		var raw map[string]any
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Fatal(err)
		}
		raw["unexpected"] = true
		extra, _ := json.Marshal(raw)
		if _, err := decodeRBDGroupNative(action, token, 7, plan, extra); err == nil {
			t.Fatal("RBD group output with an unknown field admitted", action)
		}
	}
	for name, mutate := range map[string]func(*rbdGroupNative){
		"group header":     func(n *rbdGroupNative) { n.Group.HeaderObject = "rbd_header.g1" },
		"list call object": func(n *rbdGroupNative) { n.Group.ListCalls[0].Object = "rbd_group_header.other" },
		"list call method": func(n *rbdGroupNative) { n.Group.ListCalls[1].Method = "group_snap_set" },
		"three list calls": func(n *rbdGroupNative) { n.Group.ListCalls = append(n.Group.ListCalls, n.Group.ListCalls[0]) },
		"missing info":     func(n *rbdGroupNative) { n.Group.SnapshotInfos = nil },
		"member order":     func(n *rbdGroupNative) { n.Images[0], n.Images[1] = n.Images[1], n.Images[0] },
		"member pool":      func(n *rbdGroupNative) { n.Images[1].Pool = plan.Pool },
		"same pools":       func(n *rbdGroupNative) { n.SecondPoolID = n.PoolID },
		"snapshot call":    func(n *rbdGroupNative) { n.Images[0].SnapshotCalls[0].Object = n.Images[1].HeaderObject },
		"lock call class":  func(n *rbdGroupNative) { n.Images[0].LockCall.Class = "rbd" },
		"snapcontext call": func(n *rbdGroupNative) { n.Images[1].SnapContextCall = nil },
		"extra snapshot": func(n *rbdGroupNative) {
			n.Images[1].Snapshots = append(n.Images[1].Snapshots, n.Images[1].Snapshots[0])
		},
		"namespace":         func(n *rbdGroupNative) { n.Images[0].Snapshots[0].Group = nil },
		"object offset":     func(n *rbdGroupNative) { n.Images[1].Views[1].Objects[1].Offset++ },
		"view snapshot":     func(n *rbdGroupNative) { n.Images[1].Views[1].SnapID++ },
		"view label":        func(n *rbdGroupNative) { n.Images[0].Views[0].Label = "s0" },
		"blob name":         func(n *rbdGroupNative) { n.Images[0].Views[1].Image.Name = "rbd-group-setup-b-s4-image.bin" },
		"write extent":      func(n *rbdGroupNative) { n.Writes[0].Length++ },
		"write image":       func(n *rbdGroupNative) { n.Writes[0].Image = "b" },
		"missing write":     func(n *rbdGroupNative) { n.Writes = n.Writes[1:] },
		"removal on setup":  func(n *rbdGroupNative) { n.Removed = &rbdGroupRemoval{} },
		"creation on setup": func(n *rbdGroupNative) { n.Creation = &rbdGroupCreation{} },
		"rollback on setup": func(n *rbdGroupNative) { n.RolledBackTo = plan.GroupSnapshot },
		"foreign image key": func(n *rbdGroupNative) { n.Images[1].Key = "c" },
	} {
		sample := rbdGroupSample("setup", token, plan)
		mutate(&sample)
		data, _ := json.Marshal(sample)
		if _, err := decodeRBDGroupNative("setup", token, 7, plan, data); err == nil {
			t.Fatal("malformed RBD group setup output admitted:", name)
		}
	}
	for action, cases := range map[string]map[string]func(*rbdGroupNative){
		"refused": {
			"no creation":          func(n *rbdGroupNative) { n.Creation = nil },
			"other snapshot":       func(n *rbdGroupNative) { n.Creation.Snapshot = plan.SecondSnapshot },
			"second observation":   func(n *rbdGroupNative) { n.Creation.Phases = append(n.Creation.Phases, n.Creation.Phases[0]) },
			"no rollback attempt":  func(n *rbdGroupNative) { n.Creation.Phases[0].RollbackErrno = nil },
			"foreign header list":  func(n *rbdGroupNative) { n.Creation.Phases[0].ListCall.Object = "rbd_group_header.g2" },
			"member header lock":   func(n *rbdGroupNative) { n.Creation.Phases[0].Images[1].LockCall.Object = "rbd_header.i0" },
			"missing member image": func(n *rbdGroupNative) { delete(n.Creation.ImageIDs, "b") },
			"negative errno":       func(n *rbdGroupNative) { n.Creation.Errno = -16 },
		},
		"accepted": {
			"one observation": func(n *rbdGroupNative) { n.Creation.Phases = n.Creation.Phases[:1] },
			"phase order": func(n *rbdGroupNative) {
				n.Creation.Phases[0], n.Creation.Phases[1] = n.Creation.Phases[1], n.Creation.Phases[0]
			},
			"late rollback":        func(n *rbdGroupNative) { n.Creation.Phases[1].RollbackErrno = n.Creation.Phases[0].RollbackErrno },
			"refused snapshot":     func(n *rbdGroupNative) { n.Creation.Snapshot = plan.RefusedSnapshot },
			"unrecorded elapsed":   func(n *rbdGroupNative) { n.Creation.ElapsedNS = 0 },
			"snapshot views short": func(n *rbdGroupNative) { n.Images[0].Views = n.Images[0].Views[:2] },
			"retried attempts":     func(n *rbdGroupNative) { n.Creation.QuiesceAttempts = 2 },
			"snapshot limit":       func(n *rbdGroupNative) { n.Creation.SnapLimit = &rbdGroupSnapLimit{} },
		},
		"timed-out": {
			"default attempts":   func(n *rbdGroupNative) { n.Creation.QuiesceAttempts = rbdGroupDefaultQuiesceAttempts },
			"complete observed":  func(n *rbdGroupNative) { n.Creation.Phases = append(n.Creation.Phases, n.Creation.Phases[0]) },
			"refused snapshot":   func(n *rbdGroupNative) { n.Creation.Snapshot = plan.RefusedSnapshot },
			"member limit call":  func(n *rbdGroupNative) { n.Creation.Phases[0].Images[0].LimitCall = nil },
			"limit call method":  func(n *rbdGroupNative) { n.Creation.Phases[0].Images[1].LimitCall.Method = "snapshot_set_limit" },
			"limit call foreign": func(n *rbdGroupNative) { n.Creation.Phases[0].Images[1].LimitCall.Object = "rbd_header.i0" },
		},
		"retried": {
			"one observation":  func(n *rbdGroupNative) { n.Creation.Phases = n.Creation.Phases[:1] },
			"default attempts": func(n *rbdGroupNative) { n.Creation.QuiesceAttempts = rbdGroupDefaultQuiesceAttempts },
			"second snapshot":  func(n *rbdGroupNative) { n.Creation.Snapshot = plan.SecondSnapshot },
		},
		"over-limit": {
			"no snapshot limit":   func(n *rbdGroupNative) { n.Creation.SnapLimit = nil },
			"limit on member b":   func(n *rbdGroupNative) { n.Creation.SnapLimit.Image = "b" },
			"limit set call":      func(n *rbdGroupNative) { n.Creation.SnapLimit.SetCall.Object = "rbd_header.i1" },
			"limit removal call":  func(n *rbdGroupNative) { n.Creation.SnapLimit.RemovedCall = nil },
			"complete observed":   func(n *rbdGroupNative) { n.Creation.Phases[1].Phase = "complete-observed" },
			"retried attempts":    func(n *rbdGroupNative) { n.Creation.QuiesceAttempts = 2 },
			"four observed views": func(n *rbdGroupNative) { n.Images[1].Views = n.Images[1].Views[:3] },
			"no member snapshot":  func(n *rbdGroupNative) { n.Creation.Phases[1].MemberSnapshotCall = nil },
			"member a snapshot":   func(n *rbdGroupNative) { n.Creation.Phases[1].MemberSnapshotCall.Object = "rbd_header.i0" },
			"early member call": func(n *rbdGroupNative) {
				n.Creation.Phases[0].MemberSnapshotCall = n.Creation.Phases[1].MemberSnapshotCall
			},
		},
		"rollback": {
			"other target": func(n *rbdGroupNative) { n.RolledBackTo = plan.SecondSnapshot },
			"no target":    func(n *rbdGroupNative) { n.RolledBackTo = "" },
		},
		"cleanup": {
			"group header":          func(n *rbdGroupNative) { n.Removed.GroupHeader = "rbd_group_header.g2" },
			"errno":                 func(n *rbdGroupNative) { n.Removed.Images[0].DataErrno[1] = 5 },
			"object map errno":      func(n *rbdGroupNative) { n.Removed.Images[1].ObjectMapErrno = 5 },
			"object count":          func(n *rbdGroupNative) { n.Removed.Images[1].DataErrno = n.Removed.Images[1].DataErrno[1:] },
			"directory call":        func(n *rbdGroupNative) { n.Removed.DirectoryCall.Object = "rbd_directory" },
			"prefix":                func(n *rbdGroupNative) { n.Removed.Images[0].Prefix = "rbd_data.i1" },
			"pool":                  func(n *rbdGroupNative) { n.Removed.Images[1].Pool = plan.Pool },
			"two snapshot removals": func(n *rbdGroupNative) { n.Removed.GroupSnapshotsAfter = n.Removed.GroupSnapshotsAfter[:2] },
			"group view":            func(n *rbdGroupNative) { n.Group = &rbdGroupView{} },
		},
	} {
		for name, mutate := range cases {
			sample := rbdGroupSample(action, token, plan)
			mutate(&sample)
			data, _ := json.Marshal(sample)
			if _, err := decodeRBDGroupNative(action, token, 7, plan, data); err == nil {
				t.Fatalf("malformed RBD group %s output admitted: %s", action, name)
			}
		}
	}
}

// The embedded group driver must compile with the key prelude where an
// interpreter is available, read its key only from NATIVE_KEY and call only
// read methods through librados exec; the live acceptance runs it against
// native librbd.
func TestRBDGroupNativeDriverCompiles(t *testing.T) {
	script := rbdGroupNativePython
	if strings.Contains(script, ".keyring") || strings.Contains(script, "'keyring'") || !strings.Contains(script, "NATIVE_KEY") {
		t.Fatal("RBD group driver names a keyring path")
	}
	for _, method := range []string{"'group_snap_list'", "'snapshot_get'", "'dir_get_id'", "'get_snapcontext'", "'get_info'", "'snapshot_get_limit'"} {
		if !strings.Contains(script, method) {
			t.Fatal("RBD group driver lacks its read call", method)
		}
	}
	// Every exec names one of the read methods above.
	calls := strings.Count(script, "call(")
	reads := 0
	for _, method := range []string{"'group_snap_list'", "'snapshot_get'", "'dir_get_id'", "'get_snapcontext'", "'get_info'", "'snapshot_get_limit'"} {
		reads += strings.Count(script, method)
	}
	if strings.Contains(script, "group_snap_set") || strings.Contains(script, "write_full") || strings.Contains(script, "'lock', 'lock'") || calls-1 > reads {
		t.Fatal("RBD group driver issues an unexpected class call", calls, reads)
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	cmd := exec.Command(python, "-I", "-c", "import sys; compile(sys.stdin.read(), 'worker', 'exec')")
	cmd.Stdin = strings.NewReader(nativeKeyPrelude + script)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatal("native RBD group driver is not valid Python", err, string(output))
	}
}
