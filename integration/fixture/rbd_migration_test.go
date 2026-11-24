package main

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

func TestRBDMigrationPlanAndRequests(t *testing.T) {
	token := strings.Repeat("ab", 16)
	plan := rbdMigrationPlanFor(token)
	if !plan.owned(token) || plan.owned(strings.Repeat("cd", 16)) {
		t.Fatal("RBD migration plan is not bound to its token", plan)
	}
	for name, mutate := range map[string]func(*rbdMigrationPlan){
		"base image as the source":         func(p *rbdMigrationPlan) { p.Source = p.BaseImage },
		"destination in the first pool":    func(p *rbdMigrationPlan) { p.SecondPool = p.Pool },
		"abort destination as destination": func(p *rbdMigrationPlan) { p.AbortDestination = p.Destination },
		"foreign group":                    func(p *rbdMigrationPlan) { p.Group = "ceph-msgr-group-" + token },
		"layering-only images":             func(p *rbdMigrationPlan) { p.Features = 1 },
		"no post-snapshot writes":          func(p *rbdMigrationPlan) { p.PostWrites = nil },
		"another page size":                func(p *rbdMigrationPlan) { p.ListPage = 1 },
	} {
		foreign := rbdMigrationPlanFor(token)
		mutate(&foreign)
		if foreign.owned(token) {
			t.Fatal("RBD migration plan admitted:", name)
		}
	}
	for i, action := range rbdMigrationActions {
		id, got, err := parseRBDMigrationRequest([]string{fmt.Sprint(i + 1), token, action}, token, int64(i), i)
		if err != nil || id != int64(i+1) || got != action {
			t.Fatal("ordered RBD migration request refused", action, err)
		}
	}
	for _, fields := range [][]string{
		{"1", token, "prepare"},    // out of order
		{"1", token, "observe"},    // not admitted
		{"0", token, "setup"},      // no identity
		{"01", token, "setup"},     // non-canonical identity
		{"1", "ab", "setup"},       // foreign token
		{"1", token, "setup", "x"}, // extra field
		{"1", strings.Repeat("zz", 16), "setup"},
	} {
		if _, _, err := parseRBDMigrationRequest(fields, token, 0, 0); err == nil {
			t.Fatal("RBD migration request admitted", fields)
		}
	}
	if _, _, err := parseRBDMigrationRequest([]string{"5", token, "execute"}, token, 4, 3); err == nil {
		t.Fatal("RBD migration execute admitted before the prepare")
	}
	if _, _, err := parseRBDMigrationRequest([]string{"9", token, "abort"}, token, 8, 7); err == nil {
		t.Fatal("RBD migration abort admitted before the refused abort")
	}
	n := len(rbdMigrationActions)
	if _, _, err := parseRBDMigrationRequest([]string{fmt.Sprint(n + 1), token, "cleanup"}, token, int64(n), n); err == nil {
		t.Fatal("RBD migration action admitted after the sequence ended")
	}
	if _, _, err := parseRBDMigrationRequest([]string{"3", token, "watched"}, token, 2, 2); err == nil {
		t.Fatal("RBD migration watched prepare admitted twice")
	}
}

// rbdMigrationSample is a structurally valid driver output for the plan:
// every image with an ID, its header present and one snapshot.
func rbdMigrationSample(action, token string, plan rbdMigrationPlan) rbdMigrationNative {
	sum := strings.Repeat("0", 64)
	n := rbdMigrationNative{Action: action, Token: token, RequestID: 7, Pool: plan.Pool, PoolID: 3, SecondPoolID: 4, NativeGlobalID: 4100, ImagesListed: []string{}, GroupsListed: []string{}, TrashListed: []string{}}
	call := func(object, class, method string, code int32) *rbdWriteClassCall {
		return &rbdWriteClassCall{Object: object, Class: class, Method: method, InputHex: "00", Code: code}
	}
	if slices.Contains(rbdMigrationRefusing, action) {
		n.Refusal = &rbdMigrationRefusal{Errno: 16, ElapsedNS: 5}
	}
	if action == "group-snapshot" {
		n.SourceSnapshotsDuring = []rbdGroupSnapshot{{ID: 4, Name: plan.Snapshot, Size: plan.Size}}
	}
	if action == "cleanup" {
		n.Removed = &rbdMigrationRemoval{GroupID: "g1", GroupHeader: "rbd_group_header.g1", GroupHeaderErrno: 2, DirectoryCall: call("rbd_group_directory", "rbd", "dir_get_id", -2), MembersAfter: []rbdGroupMember{}}
		for i, key := range []string{"d", "a"} {
			name, pool := plan.image(key)
			n.Removed.Images = append(n.Removed.Images, rbdMigrationRemovedImage{Key: key, Name: name, Pool: pool, ImageID: fmt.Sprint("i", i), Objects: plan.Size >> plan.Order,
				HeaderErrno: 2, IDErrno: 2, ObjectMapErrno: 2, DataErrno: []int{2, 2, 2}})
		}
		return n
	}
	if action == "setup" {
		for _, w := range []struct {
			key, phase string
			extents    []rbdBaseExtent
		}{{"s", "pre", plan.PreWrites}, {"s", "post", plan.PostWrites}, {"a", "pre", plan.AbortWrites}} {
			for _, e := range w.extents {
				n.Writes = append(n.Writes, rbdMigrationWrite{Image: w.key, Phase: w.phase, Offset: e.Offset, Length: e.Length, Written: e.Length, SHA256: sum})
			}
		}
	}
	n.Group = &rbdMigrationGroupView{Name: plan.Group, ID: "g1", HeaderObject: "rbd_group_header.g1", Members: []rbdGroupMember{}, Snapshots: []string{},
		DirectoryCall: call("rbd_group_directory", "rbd", "dir_get_id", 0), ImageListCall: call("rbd_group_header.g1", "rbd", "group_image_list", 0)}
	for i, key := range rbdMigrationKeys {
		name, pool := plan.image(key)
		id := fmt.Sprint("i", i)
		header := "rbd_header." + id
		v := rbdMigrationImageView{Key: key, Name: name, Pool: pool, PoolID: int64(3 + i%2), ImageID: id, IDSource: "directory", DirectoryCall: call("rbd_directory", "rbd", "dir_get_id", 0),
			HeaderObject: header, TrashCall: call("rbd_trash", "rbd", "trash_get", -2), TrashErrno: 2, Calls: map[string]rbdWriteClassCall{},
			SnapshotCalls: []rbdWriteClassCall{*call(header, "rbd", "snapshot_get", 0)}}
		for _, method := range rbdMigrationHeaderMethods {
			v.Calls[method] = *call(header, "rbd", method, 0)
		}
		view := func(label string, snap uint64) rbdMigrationDataView {
			d := rbdMigrationDataView{Label: label, SnapID: snap}
			var offset uint64
			for j := range plan.Size >> plan.Order {
				d.Objects = append(d.Objects, rbdGroupObjectView{Index: j, Exists: true, Size: 1 << plan.Order, Offset: offset, SHA256: sum})
				offset += 1 << plan.Order
			}
			d.ObjectBytes = rbdBaseBlob{Name: "rbd-migration-" + action + "-" + key + "-" + label + "-objects.bin", Length: offset, SHA256: sum}
			return d
		}
		v.Views = []rbdMigrationDataView{view("head", 0), view("s4", 4)}
		head := rbdBaseBlob{Name: "rbd-migration-" + action + "-" + key + "-head-image.bin", Length: plan.Size, SHA256: sum}
		v.Reads = &rbdMigrationReads{Features: plan.Features, Snapshots: []rbdGroupSnapshot{{ID: 4, Name: plan.Snapshot, Size: plan.Size}}, Head: &head,
			SnapshotImages: []rbdBaseBlob{{Name: "rbd-migration-" + action + "-" + key + "-s4-image.bin", Length: plan.Size, SHA256: sum}}}
		n.Images = append(n.Images, v)
	}
	return n
}

func TestRBDMigrationNativeOutputShape(t *testing.T) {
	token := strings.Repeat("ab", 16)
	plan := rbdMigrationPlanFor(token)
	for _, action := range rbdMigrationActions {
		data, err := json.Marshal(rbdMigrationSample(action, token, plan))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodeRBDMigrationNative(action, token, 7, plan, data); err != nil {
			t.Fatal("valid RBD migration output refused", action, err)
		}
		if _, err := decodeRBDMigrationNative(action, token, 8, plan, data); err == nil {
			t.Fatal("RBD migration output of another request admitted", action)
		}
	}
	unknown := func(n *rbdMigrationNative) {}
	for name, c := range map[string]struct {
		action string
		mutate func(*rbdMigrationNative)
	}{
		"same pools":                   {"setup", func(n *rbdMigrationNative) { n.SecondPoolID = n.PoolID }},
		"no native identity":           {"setup", func(n *rbdMigrationNative) { n.NativeGlobalID = 0 }},
		"short write":                  {"setup", func(n *rbdMigrationNative) { n.Writes[0].Written-- }},
		"writes outside setup":         {"prepare", func(n *rbdMigrationNative) { n.Writes = []rbdMigrationWrite{{}} }},
		"refusal outside its action":   {"prepare", func(n *rbdMigrationNative) { n.Refusal = &rbdMigrationRefusal{Errno: 16, ElapsedNS: 1} }},
		"refused action without errno": {"watched", func(n *rbdMigrationNative) { n.Refusal = nil }},
		"group snapshot without view":  {"group-snapshot", func(n *rbdMigrationNative) { n.SourceSnapshotsDuring = nil }},
		"image missing":                {"prepare", func(n *rbdMigrationNative) { n.Images = n.Images[:3] }},
		"images out of order":          {"prepare", func(n *rbdMigrationNative) { n.Images[0], n.Images[1] = n.Images[1], n.Images[0] }},
		"foreign destination pool":     {"prepare", func(n *rbdMigrationNative) { n.Images[1].Pool = plan.Pool }},
		"header call on another object": {"prepare", func(n *rbdMigrationNative) {
			c := n.Images[0].Calls["migration_get"]
			c.Object = "rbd_header.x"
			n.Images[0].Calls["migration_get"] = c
		}},
		"header method missing": {"prepare", func(n *rbdMigrationNative) { delete(n.Images[0].Calls, "get_features") }},
		"write method in the calls": {"prepare", func(n *rbdMigrationNative) {
			n.Images[0].Calls["migration_set"] = rbdWriteClassCall{Object: "rbd_header.i0", Class: "rbd", Method: "migration_set"}
		}},
		"views without snapshot calls": {"prepare", func(n *rbdMigrationNative) { n.Images[0].SnapshotCalls = nil }},
		"object view offset":           {"prepare", func(n *rbdMigrationNative) { n.Images[0].Views[0].Objects[1].Offset = 7 }},
		"blob of another action": {"prepare", func(n *rbdMigrationNative) {
			n.Images[0].Views[0].ObjectBytes.Name = "rbd-migration-setup-s-head-objects.bin"
		}},
		"reads without a directory":      {"prepare", func(n *rbdMigrationNative) { n.Images[0].DirectoryCall.Code = -2 }},
		"image without ID with a header": {"prepare", func(n *rbdMigrationNative) { n.Images[0].ImageID, n.Images[0].IDSource = "", "" }},
		"status and status errno":        {"prepare", func(n *rbdMigrationNative) { n.Images[0].Status, n.Images[0].StatusErrno = &rbdMigrationStatus{}, 22 }},
		"trash and trash errno":          {"prepare", func(n *rbdMigrationNative) { n.Images[0].Trash = &rbdMigrationTrash{} }},
		"group header elsewhere":         {"prepare", func(n *rbdMigrationNative) { n.Group.HeaderObject = "rbd_group_header.x" }},
		"group view in cleanup":          {"cleanup", func(n *rbdMigrationNative) { n.Group = &rbdMigrationGroupView{} }},
		"removed image of another key":   {"cleanup", func(n *rbdMigrationNative) { n.Removed.Images[0].Key = "s" }},
		"removal data errno":             {"cleanup", func(n *rbdMigrationNative) { n.Removed.Images[1].DataErrno[0] = 5 }},
		"control sample":                 {"setup", unknown},
	} {
		n := rbdMigrationSample(c.action, token, plan)
		c.mutate(&n)
		data, err := json.Marshal(n)
		if err != nil {
			t.Fatal(err)
		}
		_, err = decodeRBDMigrationNative(c.action, token, 7, plan, data)
		if (name == "control sample") != (err == nil) {
			t.Fatal("RBD migration output judged wrongly:", name, err)
		}
	}
	data, _ := json.Marshal(rbdMigrationSample("setup", token, plan))
	if _, err := decodeRBDMigrationNative("setup", token, 7, plan, append(data[:len(data)-1], []byte(`,"extra":1}`)...)); err == nil {
		t.Fatal("RBD migration output with an unknown field admitted")
	}
}

// The embedded migration driver must compile with the key prelude where an
// interpreter is available, read its key only from NATIVE_KEY and call only
// read methods through librados exec; the live acceptance runs it against
// native librbd.
func TestRBDMigrationNativeDriverCompiles(t *testing.T) {
	script := rbdMigrationNativePython
	if strings.Contains(script, ".keyring") || strings.Contains(script, "'keyring'") || !strings.Contains(script, "NATIVE_KEY") {
		t.Fatal("RBD migration driver names a keyring path")
	}
	reads := []string{"'dir_get_id'", "'trash_get'", "'migration_get'", "'get_features'", "'get_snapcontext'", "'image_group_get'", "'snapshot_get'", "'group_image_list'"}
	count := 0
	for _, method := range reads {
		if !strings.Contains(script, method) {
			t.Fatal("RBD migration driver lacks its read call", method)
		}
		count += strings.Count(script, method)
	}
	// Every exec names one of the read methods above.
	if calls := strings.Count(script, "call("); calls-1 > count || strings.Contains(script, "migration_set") || strings.Contains(script, "write_full") {
		t.Fatal("RBD migration driver issues an unexpected class call", calls, count)
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	cmd := exec.Command(python, "-I", "-c", "import sys; compile(sys.stdin.read(), 'worker', 'exec')")
	cmd.Stdin = strings.NewReader(nativeKeyPrelude + script)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatal("native RBD migration driver is not valid Python", err, string(output))
	}
}
