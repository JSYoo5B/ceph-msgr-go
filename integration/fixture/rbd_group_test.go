package main

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
)

func TestRBDGroupPlanAndRequests(t *testing.T) {
	token := strings.Repeat("ab", 16)
	plan := rbdGroupPlanFor(token)
	if !plan.owned(token) || plan.owned(strings.Repeat("cd", 16)) {
		t.Fatal("RBD group plan is not bound to its token", plan)
	}
	foreign := rbdGroupPlanFor(token)
	foreign.Images[1].Name = "ceph-msgr-image-" + token
	if foreign.owned(token) {
		t.Fatal("RBD group plan admits the base image as a member")
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
	if _, _, err := parseRBDGroupRequest([]string{"4", token, "cleanup"}, token, 3, 3); err == nil {
		t.Fatal("RBD group action admitted after the sequence ended")
	}
}

// rbdGroupSample is a structurally valid driver output for the plan.
func rbdGroupSample(action, token string, plan rbdGroupPlan) rbdGroupNative {
	sum := strings.Repeat("0", 64)
	n := rbdGroupNative{Action: action, Token: token, RequestID: 7, Pool: plan.Pool, PoolID: 3, NativeGlobalID: 4100, ImagesListed: []string{}, GroupsListed: []string{}}
	call := func(object, method string) rbdWriteClassCall {
		return rbdWriteClassCall{Object: object, Class: "rbd", Method: method, InputHex: "00", OutputHex: ""}
	}
	if action == "cleanup" {
		directory := call("rbd_group_directory", "dir_get_id")
		directory.Code = -2
		n.Removed = &rbdGroupRemoval{GroupID: "g1", GroupHeader: "rbd_group_header.g1", GroupHeaderErrno: 2, DirectoryCall: &directory, GroupSnapshotsAfter: []rbdGroupSnap{}, MembersAfter: []rbdGroupMember{}}
		for i, image := range plan.Images {
			id := fmt.Sprint("i", i)
			n.Removed.Images = append(n.Removed.Images, rbdGroupRemovedImage{Key: image.Key, Name: image.Name, ImageID: id, Prefix: "rbd_data." + id, Objects: plan.Size >> plan.Order,
				SnapshotsAfter: []rbdGroupSnapshot{}, HeaderErrno: 2, IDErrno: 2, DataErrno: []int{2, 2, 2}})
		}
		return n
	}
	phase, info := "pre", rbdGroupSnap{ID: "s1", Name: plan.GroupSnapshot, State: 1}
	if action == "modify" {
		phase = "post"
	}
	directory := call("rbd_group_directory", "dir_get_id")
	n.Group = &rbdGroupView{Name: plan.Group, ID: "g1", HeaderObject: "rbd_group_header.g1", SnapshotInfo: &info, DirectoryCall: &directory,
		ListCalls: []rbdWriteClassCall{call("rbd_group_header.g1", "group_snap_list"), call("rbd_group_header.g1", "group_snap_list")}}
	for i, image := range plan.Images {
		extents := image.PreWrites
		if action == "modify" {
			extents = image.PostWrites
		}
		for _, e := range extents {
			n.Writes = append(n.Writes, rbdGroupWrite{Image: image.Key, Phase: phase, Offset: e.Offset, Length: e.Length, Written: e.Length, SHA256: sum})
		}
		id := fmt.Sprint("i", i)
		v := rbdGroupImageView{Key: image.Key, Name: image.Name, ImageID: id, HeaderObject: "rbd_header." + id, IDObject: "rbd_id." + image.Name, Features: 1 | rbdFeatureOperations, OpFeatures: rbdOperationFeatureGroup,
			Stat:          &rbdBaseStat{Size: plan.Size, ObjectSize: 1 << plan.Order, Objects: plan.Size >> plan.Order, Order: plan.Order, Prefix: "rbd_data." + id, ParentPool: ^uint64(0)},
			Snapshots:     []rbdGroupSnapshot{{ID: 4, Name: ".group.3_g1_s1", Size: plan.Size, Namespace: 1, Group: &rbdGroupNamespace{Pool: 3, Name: plan.Group, SnapName: plan.GroupSnapshot}}},
			SnapshotCalls: []rbdWriteClassCall{call("rbd_header."+id, "snapshot_get")}, Blobs: map[string]rbdBaseBlob{}}
		var offset uint64
		for j := range plan.Size >> plan.Order {
			view := rbdBaseObjectView{Exists: true, Size: 1 << plan.Order, Offset: offset, SHA256: sum}
			offset += view.Size
			v.Objects = append(v.Objects, rbdBaseObject{Index: j, Name: fmt.Sprintf("rbd_data.%s.%016x", id, j), Head: view, Snap: view})
		}
		for key := range rbdGroupBlobViews {
			length := plan.Size
			if strings.HasPrefix(key, "objects") {
				length = offset
			}
			v.Blobs[key] = rbdBaseBlob{Name: rbdGroupBlobName(action, image.Key, key), Length: length, SHA256: sum}
		}
		n.Images = append(n.Images, v)
	}
	return n
}

func TestRBDGroupNativeOutputShape(t *testing.T) {
	token := strings.Repeat("ab", 16)
	plan := rbdGroupPlanFor(token)
	for _, action := range rbdGroupActions {
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
		other := map[string]string{"setup": "modify", "modify": "cleanup", "cleanup": "setup"}[action]
		if _, err := decodeRBDGroupNative(other, token, 7, plan, data); err == nil {
			t.Fatal("RBD group output of another action admitted", action)
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
		"member order":     func(n *rbdGroupNative) { n.Images[0], n.Images[1] = n.Images[1], n.Images[0] },
		"snapshot call":    func(n *rbdGroupNative) { n.Images[0].SnapshotCalls[0].Object = n.Images[1].HeaderObject },
		"extra snapshot": func(n *rbdGroupNative) {
			n.Images[1].Snapshots = append(n.Images[1].Snapshots, n.Images[1].Snapshots[0])
		},
		"namespace":     func(n *rbdGroupNative) { n.Images[0].Snapshots[0].Group = nil },
		"object offset": func(n *rbdGroupNative) { n.Images[1].Objects[1].Snap.Offset++ },
		"blob name": func(n *rbdGroupNative) {
			b := n.Images[0].Blobs["image_snap"]
			b.Name = "rbd-group-setup-b-image-snap.bin"
			n.Images[0].Blobs["image_snap"] = b
		},
		"write extent":      func(n *rbdGroupNative) { n.Writes[0].Length++ },
		"write image":       func(n *rbdGroupNative) { n.Writes[0].Image = "b" },
		"missing write":     func(n *rbdGroupNative) { n.Writes = n.Writes[1:] },
		"removal on setup":  func(n *rbdGroupNative) { n.Removed = &rbdGroupRemoval{} },
		"foreign image key": func(n *rbdGroupNative) { n.Images[1].Key = "c" },
	} {
		sample := rbdGroupSample("setup", token, plan)
		mutate(&sample)
		data, _ := json.Marshal(sample)
		if _, err := decodeRBDGroupNative("setup", token, 7, plan, data); err == nil {
			t.Fatal("malformed RBD group setup output admitted:", name)
		}
	}
	for name, mutate := range map[string]func(*rbdGroupNative){
		"group header":   func(n *rbdGroupNative) { n.Removed.GroupHeader = "rbd_group_header.g2" },
		"errno":          func(n *rbdGroupNative) { n.Removed.Images[0].DataErrno[1] = 5 },
		"object count":   func(n *rbdGroupNative) { n.Removed.Images[1].DataErrno = n.Removed.Images[1].DataErrno[1:] },
		"directory call": func(n *rbdGroupNative) { n.Removed.DirectoryCall.Object = "rbd_directory" },
		"prefix":         func(n *rbdGroupNative) { n.Removed.Images[0].Prefix = "rbd_data.i1" },
		"group view":     func(n *rbdGroupNative) { n.Group = &rbdGroupView{} },
	} {
		sample := rbdGroupSample("cleanup", token, plan)
		mutate(&sample)
		data, _ := json.Marshal(sample)
		if _, err := decodeRBDGroupNative("cleanup", token, 7, plan, data); err == nil {
			t.Fatal("malformed RBD group cleanup output admitted:", name)
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
	for _, method := range []string{"'group_snap_list'", "'snapshot_get'", "'dir_get_id'"} {
		if !strings.Contains(script, method) {
			t.Fatal("RBD group driver lacks its read call", method)
		}
	}
	if strings.Count(script, "call(ioctx,") != 6 || strings.Contains(script, "group_snap_set") || strings.Contains(script, "write_full") {
		t.Fatal("RBD group driver issues an unexpected class call")
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
