package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// legacyCRUSHDevice and legacyCRUSHItem are the single-device lines of
// legacy.crush.txt that the owned inventory extends.
const (
	legacyCRUSHDevice = "device 0 osd.0"
	legacyCRUSHItem   = "item osd.0 weight 1.00000"
	// legacyCRUSHWeight is 1.00000 in the 16.16 fixed point of a CRUSH dump.
	legacyCRUSHWeight = 0x10000
)

// ownedOSDInventory checks that the provider started exactly the selected
// topology's daemons, with IDs 0..daemons-1, and returns them sorted.
func ownedOSDInventory(started []int, topology osdTopology) ([]int, error) {
	if topology.daemons < 1 {
		return nil, errors.New("OSD fixture topology has no daemons")
	}
	ids := slices.Clone(started)
	slices.Sort(ids)
	if len(ids) != topology.daemons {
		return nil, fmt.Errorf("fixture owns %d OSD daemons, topology requires %d", len(ids), topology.daemons)
	}
	for i, id := range ids {
		if id != i {
			return nil, fmt.Errorf("owned OSD inventory %v is not the contiguous IDs 0..%d", ids, topology.daemons-1)
		}
	}
	return ids, nil
}

// legacyCRUSHForInventory adds every owned device after osd.0 to the legacy
// single-root template, each with the template's weight. The template's
// tunables, bucket algorithm and replicated rule stay unchanged.
func legacyCRUSHForInventory(template string, ids []int) (string, error) {
	if len(ids) == 0 || ids[0] != 0 {
		return "", errors.New("owned OSD inventory must start at osd.0")
	}
	if strings.Count(template, legacyCRUSHDevice) != 1 || strings.Count(template, legacyCRUSHItem) != 1 ||
		strings.Contains(template, "osd.1") {
		return "", errors.New("legacy CRUSH template is not the single-device template")
	}
	devices, items := legacyCRUSHDevice, legacyCRUSHItem
	for _, id := range ids[1:] {
		devices += fmt.Sprintf("\ndevice %d osd.%d", id, id)
		items += fmt.Sprintf("\n    item osd.%d weight 1.00000", id)
	}
	text := strings.Replace(template, legacyCRUSHDevice, devices, 1)
	return strings.Replace(text, legacyCRUSHItem, items, 1), nil
}

// verifyCRUSHInventory checks an installed `osd crush dump`: its devices are
// exactly the owned OSDs and its only bucket is the root that holds each of
// them once, in order, with the template weight.
func verifyCRUSHInventory(dump []byte, ids []int) error {
	var crush struct {
		Devices []struct {
			ID   *int    `json:"id"`
			Name *string `json:"name"`
		} `json:"devices"`
		Buckets []struct {
			Name     string `json:"name"`
			TypeName string `json:"type_name"`
			Items    []struct {
				ID     *int   `json:"id"`
				Weight *int64 `json:"weight"`
			} `json:"items"`
		} `json:"buckets"`
	}
	if len(dump) > 1<<20 {
		return errors.New("installed CRUSH dump exceeds 1 MiB")
	}
	if err := json.Unmarshal(dump, &crush); err != nil {
		return fmt.Errorf("installed CRUSH dump: %w", err)
	}
	if len(crush.Devices) != len(ids) {
		return fmt.Errorf("installed CRUSH map has %d devices, fixture owns %d", len(crush.Devices), len(ids))
	}
	for i, device := range crush.Devices {
		if device.ID == nil || device.Name == nil || *device.ID != ids[i] || *device.Name != fmt.Sprintf("osd.%d", ids[i]) {
			return fmt.Errorf("installed CRUSH device %d differs from owned osd.%d", i, ids[i])
		}
	}
	if len(crush.Buckets) != 1 || crush.Buckets[0].Name != "default" || crush.Buckets[0].TypeName != "root" ||
		len(crush.Buckets[0].Items) != len(ids) {
		return errors.New("installed CRUSH map is not one default root over the owned devices")
	}
	for i, item := range crush.Buckets[0].Items {
		if item.ID == nil || item.Weight == nil || *item.ID != ids[i] || *item.Weight != legacyCRUSHWeight {
			return fmt.Errorf("installed CRUSH root item %d differs from owned osd.%d", i, ids[i])
		}
	}
	return nil
}
