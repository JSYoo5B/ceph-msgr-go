package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// This is a native oracle prerequisite, not a test of Go xattr support.
// Product xattr opcodes remain unsupported until their separate milestone.
func TestCephOSDXattrNativeOracleIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD") != "1" || os.Getenv("CEPH_MSGR_CONTROL_DIR") == "" {
		t.Skip("requires the opt-in disposable OSD fixture")
	}
	directory := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := testcluster.ControlDaemon(ctx, directory, "verify", "osd-xattr-native", "matrix"); err != nil {
		t.Fatal("native xattr prerequisite", err)
	}
	f, err := os.Open(filepath.Join(directory, "osd-xattr-native.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (512<<10)+1))
	if err != nil || len(data) > 512<<10 {
		t.Fatal("native xattr receipt exceeds its bound", err)
	}
	var receipt nativeXattrReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal("native xattr receipt", err)
	}
	metadata, err := os.ReadFile(filepath.Join(directory, "osd-oracle.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Pool   string `json:"pool"`
		ID     int64  `json:"pool_id"`
		Object string `json:"object"`
	}
	if err := json.Unmarshal(metadata, &fixture); err != nil {
		t.Fatal(err)
	}
	if receipt.Schema != 1 || receipt.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || receipt.Fixture.Pool != fixture.Pool || receipt.Fixture.ID != fixture.ID || receipt.Fixture.Object != fixture.Object || receipt.Object != fixture.Object+"-native-xattr" {
		t.Fatal("native xattr receipt identity differs from the owned fixture")
	}
	validateNativeXattrReceipt(t, receipt)
}

type nativeXattrReceipt struct {
	Schema  int    `json:"schema_version"`
	Commit  string `json:"source_commit"`
	Fixture struct {
		Pool   string `json:"pool"`
		ID     int64  `json:"pool_id"`
		Object string `json:"object"`
	} `json:"fixture"`
	Object string `json:"oracle_object"`
	Scope  struct {
		RawComparison bool `json:"raw_cmp_per_op_result_available"`
		CompoundGet   bool `json:"single_get_compound_available"`
	} `json:"scope"`
	Comparisons []nativeXattrComparison `json:"comparisons"`
	Inventory   nativeXattrSnapshot     `json:"binary_inventory"`
	SetRemove   nativeXattrVector       `json:"set_remove"`
	Rollback    nativeXattrVector       `json:"rollback"`
	FailOK      nativeXattrVector       `json:"failok"`
	Cleanup     struct {
		Removed bool `json:"object_removed"`
		Code    int  `json:"code"`
	} `json:"cleanup"`
}

type nativeXattrComparison struct {
	Kind      string              `json:"kind"`
	Operator  string              `json:"comparison"`
	Caller    []byte              `json:"caller"`
	Stored    []byte              `json:"stored"`
	Expected  bool                `json:"expected_boolean"`
	Aggregate int                 `json:"aggregate"`
	Written   bool                `json:"guard_written"`
	Before    nativeXattrSnapshot `json:"before"`
	After     nativeXattrSnapshot `json:"after"`
	Marker    []byte              `json:"marker"`
}

type nativeXattrEntry struct {
	Name  []byte `json:"name"`
	Value []byte `json:"value"`
}

type nativeXattrSnapshot struct {
	Code       int                `json:"code"`
	Version    uint64             `json:"version"`
	Seconds    int64              `json:"seconds"`
	Nanos      int64              `json:"nanos"`
	Data       []byte             `json:"data"`
	Attributes []nativeXattrEntry `json:"attributes"`
	Gets       []struct {
		Name  []byte `json:"name"`
		Value []byte `json:"value"`
		Code  int    `json:"code"`
	} `json:"gets"`
}

type nativeXattrVector struct {
	Aggregate int                 `json:"aggregate"`
	Before    nativeXattrSnapshot `json:"before"`
	After     nativeXattrSnapshot `json:"after"`
}

func xattrInventory(snapshot nativeXattrSnapshot) map[string]string {
	result := make(map[string]string, len(snapshot.Attributes))
	for _, entry := range snapshot.Attributes {
		result[string(entry.Name)] = string(entry.Value)
	}
	return result
}

func validateNativeXattrReceipt(t *testing.T, receipt nativeXattrReceipt) {
	t.Helper()
	if receipt.Scope.RawComparison || receipt.Scope.CompoundGet || !receipt.Cleanup.Removed || receipt.Cleanup.Code != 0 {
		t.Fatal("native C API scope or isolated object cleanup differs from its contract")
	}
	if len(receipt.Comparisons) != 36 {
		t.Fatal("native xattr oracle lacks the read/write six-operator comparison matrix")
	}
	baseline := []byte("\x00native-xattr-object\xff\n")
	wantAttrs := map[string]string{"binary": "\x00A\xffB\x00", "empty": "", "_prefixed": "\xff\x00prefix", "compare": "m\x00z"}
	if !bytes.Equal(receipt.Inventory.Data, baseline) || len(receipt.Inventory.Attributes) != 4 || !reflect.DeepEqual(xattrInventory(receipt.Inventory), wantAttrs) {
		t.Fatal("native binary, empty or prefixed attributes differ from their independent expected bytes")
	}
	if len(receipt.Inventory.Gets) != 6 {
		t.Fatal("native direct GET inventory lacks baseline and missing-attribute observations")
	}
	getNames := make(map[string]bool, 6)
	for _, get := range receipt.Inventory.Gets {
		name := string(get.Name)
		value, exists := wantAttrs[name]
		if getNames[name] || !exists && name != "guard" && name != "missing" {
			t.Fatal("native direct GET inventory has a duplicate or unrelated name", name)
		}
		getNames[name] = true
		if exists && (get.Code != len(value) || !bytes.Equal(get.Value, []byte(value))) || !exists && get.Code != -61 {
			t.Fatal("native direct GET bytes/length or ENODATA differ", string(get.Name), get.Code)
		}
	}
	delete(wantAttrs, "empty")
	wantAttrs["binary"] = "\xff\x00replacement\x00"
	if receipt.SetRemove.Aggregate != 0 || !reflect.DeepEqual(xattrInventory(receipt.SetRemove.After), wantAttrs) || receipt.SetRemove.After.Version <= receipt.SetRemove.Before.Version || !bytes.Equal(receipt.SetRemove.After.Data, baseline) {
		t.Fatal("native compound SET/RM effects or object content differ")
	}
	if receipt.Rollback.Aggregate != -125 || !reflect.DeepEqual(receipt.Rollback.Before, receipt.Rollback.After) {
		t.Fatal("ordinary native false guard did not preserve all prior bytes, attributes, version and stat2")
	}
	if receipt.FailOK.Aggregate != 0 || receipt.FailOK.After.Version <= receipt.FailOK.Before.Version || xattrInventory(receipt.FailOK.After)["binary"] != "staged\x00" || xattrInventory(receipt.FailOK.After)["guard"] != "after-failok" || !bytes.Equal(receipt.FailOK.After.Data, baseline) {
		t.Fatal("native FAILOK guard did not allow later metadata commit")
	}
	seen := make(map[string]bool, 36)
	for _, comparison := range receipt.Comparisons {
		ordering := bytes.Compare(comparison.Caller, comparison.Stored)
		expected := false
		switch comparison.Operator {
		case "eq":
			expected = ordering == 0
		case "ne":
			expected = ordering != 0
		case "gt":
			expected = ordering > 0
		case "gte":
			expected = ordering >= 0
		case "lt":
			expected = ordering < 0
		case "lte":
			expected = ordering <= 0
		default:
			t.Fatal("unknown native comparison operator", comparison.Operator)
		}
		if comparison.Kind != "read" && comparison.Kind != "write" {
			t.Fatal("unknown native comparison kind", comparison.Kind)
		}
		key := fmt.Sprintf("%s/%s/%d", comparison.Kind, comparison.Operator, ordering)
		if seen[key] {
			t.Fatal("duplicate native comparison matrix cell", key)
		}
		seen[key] = true
		code := 0
		if !expected {
			code = -125 // ECANCELED from an ordinary false CMPXATTR guard.
		}
		if comparison.Expected != expected || comparison.Aggregate != code || comparison.Written != (comparison.Kind == "write" && expected) {
			t.Fatalf("native caller-left comparison %s: expected=%t aggregate=%d written=%t", key, expected, comparison.Aggregate, comparison.Written)
		}
		if !bytes.Equal(comparison.Before.Data, baseline) || !bytes.Equal(comparison.After.Data, baseline) || xattrInventory(comparison.After)["compare"] != string(comparison.Stored) {
			t.Fatal("native comparison changed object bytes or stored operand", key)
		}
		if comparison.Written {
			if comparison.After.Version <= comparison.Before.Version || xattrInventory(comparison.After)["guard"] != string(comparison.Marker) {
				t.Fatal("native true guard did not commit its independent marker", key)
			}
		} else if !reflect.DeepEqual(comparison.Before, comparison.After) {
			t.Fatal("native read-only or false guard changed prior state", key)
		}
	}
}
