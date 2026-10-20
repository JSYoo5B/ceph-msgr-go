package msgr

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Pinned Ceph 20.2.4 7f793731f1b39eb4f465e960113d2363c311b964,
// src/msg/Message.h MSG_MGR_*: the MGR service-daemon lifecycle messages.
const (
	MgrOpenMessage      uint16 = 0x700
	MgrConfigureMessage uint16 = 0x701
	MgrReportMessage    uint16 = 0x702
	MgrCloseMessage     uint16 = 0x708
	MgrUpdateMessage    uint16 = 0x70b
)

// Bounds for caller-supplied service registration data. They apply to names,
// metadata/status maps and the whole encoded message, so a caller cannot make
// the client emit a frame the MGR must accept unbounded.
const (
	MaxServiceNameBytes   = 255
	MaxServiceMapEntries  = 1024
	MaxServiceStringBytes = 64 << 10
	MaxServiceMessageSize = 1 << 20
)

var ErrServiceRegistration = errors.New("ceph messenger: invalid MGR service registration data")

func validateServiceName(kind, name string) error {
	if name == "" || len(name) > MaxServiceNameBytes || strings.ContainsRune(name, 0) {
		return fmt.Errorf("%w: %s must be 1 to %d bytes without NUL", ErrServiceRegistration, kind, MaxServiceNameBytes)
	}
	return nil
}

func validateServiceMap(kind string, m map[string]string) error {
	if len(m) > MaxServiceMapEntries {
		return fmt.Errorf("%w: %s has %d entries, at most %d", ErrServiceRegistration, kind, len(m), MaxServiceMapEntries)
	}
	for k, v := range m {
		if len(k) > MaxServiceStringBytes || len(v) > MaxServiceStringBytes {
			return fmt.Errorf("%w: %s entry exceeds %d bytes", ErrServiceRegistration, kind, MaxServiceStringBytes)
		}
	}
	return nil
}

// encodeServiceMap writes std::map<std::string,std::string>: a u32 count and
// the entries in ascending bytewise key order, as the C++ map iterates.
func encodeServiceMap(e *wire.Encoder, m map[string]string) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	e.U32(uint32(len(keys)))
	for _, k := range keys {
		e.String(k)
		e.String(m[k])
	}
}

func checkServiceSize(e *wire.Encoder) error {
	if len(e.Data) > MaxServiceMessageSize {
		return fmt.Errorf("%w: encoded message exceeds %d bytes", ErrServiceRegistration, MaxServiceMessageSize)
	}
	return nil
}

func serviceIdentity(service, daemon string) error {
	if err := validateServiceName("service name", service); err != nil {
		return err
	}
	return validateServiceName("daemon name", daemon)
}

// MgrOpen builds MMgrOpen v3/compat 1 for a service daemon (service_daemon
// true): daemon and service names, caller metadata and status, and empty
// config and config-defaults buffers. The client reads no configuration, so it
// declares none; the MGR then keeps no config for this daemon.
func MgrOpen(service, daemon string, metadata, status map[string]string) (MessageData, error) {
	if err := serviceIdentity(service, daemon); err != nil {
		return MessageData{}, err
	}
	if err := errors.Join(validateServiceMap("metadata", metadata), validateServiceMap("status", status)); err != nil {
		return MessageData{}, err
	}
	e := wire.Encoder{}
	e.String(daemon)
	e.String(service)
	e.U8(1) // service_daemon
	encodeServiceMap(&e, metadata)
	encodeServiceMap(&e, status)
	e.U32(0) // config_bl
	e.U32(0) // config_defaults_bl
	if err := checkServiceSize(&e); err != nil {
		return MessageData{}, err
	}
	return MessageData{Type: MgrOpenMessage, Version: 3, CompatVersion: 1, Priority: 127, Front: e.Data}, nil
}

// MgrUpdate builds MMgrUpdate v2/compat 1 carrying replacement metadata
// (need_metadata_update true). The MGR ignores the status map of an update, so
// an empty one is sent; status travels in MgrReport.
func MgrUpdate(service, daemon string, metadata map[string]string) (MessageData, error) {
	if err := serviceIdentity(service, daemon); err != nil {
		return MessageData{}, err
	}
	if len(metadata) == 0 {
		return MessageData{}, fmt.Errorf("%w: the MGR ignores an update without metadata", ErrServiceRegistration)
	}
	if err := validateServiceMap("metadata", metadata); err != nil {
		return MessageData{}, err
	}
	e := wire.Encoder{}
	e.String(daemon)
	e.String(service)
	e.U8(1) // need_metadata_update
	encodeServiceMap(&e, metadata)
	encodeServiceMap(&e, nil)
	if err := checkServiceSize(&e); err != nil {
		return MessageData{}, err
	}
	return MessageData{Type: MgrUpdateMessage, Version: 2, CompatVersion: 1, Priority: 127, Front: e.Data}, nil
}

// MgrReport builds MMgrReport v9/compat 1 for a service daemon without perf
// counters. status and task are optional: nil omits the field (the MGR keeps
// the previous value), an empty non-nil map replaces it with an empty map.
// The packed counter buffer is the empty versioned envelope the MGR's
// DECODE_START requires; a report is also the liveness beacon the MGR expires
// after mgr_service_beacon_grace.
func MgrReport(service, daemon string, status, task map[string]string) (MessageData, error) {
	if err := serviceIdentity(service, daemon); err != nil {
		return MessageData{}, err
	}
	if err := errors.Join(validateServiceMap("status", status), validateServiceMap("task status", task)); err != nil {
		return MessageData{}, err
	}
	e := wire.Encoder{}
	e.String(daemon)
	e.U32(0) // declare_types
	packed := wire.Encoder{}
	packed.Struct(1, 1, nil)
	e.Bytes(packed.Data) // packed counters: empty versioned envelope
	e.U32(0)             // undeclare_types
	e.String(service)
	optionalMap := func(m map[string]string) {
		if m == nil {
			e.U8(0)
			return
		}
		e.U8(1)
		encodeServiceMap(&e, m)
	}
	optionalMap(status)
	e.U32(0) // daemon_health_metrics
	e.U32(0) // config_bl
	e.U32(0) // osd_perf_metric_reports
	optionalMap(task)
	e.U8(0) // metric_report_message absent
	if err := checkServiceSize(&e); err != nil {
		return MessageData{}, err
	}
	return MessageData{Type: MgrReportMessage, Version: 9, CompatVersion: 1, Priority: 127, Front: e.Data}, nil
}

// MgrClose builds MMgrClose v1/compat 1. The MGR echoes the same message back
// as its acknowledgement after it removes the daemon.
func MgrClose(service, daemon string) (MessageData, error) {
	if err := serviceIdentity(service, daemon); err != nil {
		return MessageData{}, err
	}
	e := wire.Encoder{}
	e.String(daemon)
	e.String(service)
	return MessageData{Type: MgrCloseMessage, Version: 1, CompatVersion: 1, Priority: 127, Front: e.Data}, nil
}

// MgrConfigure is the MGR's statistics configuration push (MMgrConfigure).
// Extension holds the undecoded bytes starting at the first field this client
// does not interpret (the OSD perf-metric query map, or the optional metric
// configuration message including its presence marker) when the MGR sent
// either; a service daemon peer normally gets neither.
type MgrConfigure struct {
	Version        uint16
	StatsPeriod    uint32 // seconds between reports; 0 stops reporting
	StatsThreshold uint32
	Extension      []byte
}

// DecodeMgrConfigure reads MMgrConfigure versions 1 through 4.
func DecodeMgrConfigure(m MessageData, limit uint32) (c MgrConfigure, err error) {
	defer func() {
		if err != nil {
			c = MgrConfigure{}
			err = fmt.Errorf("%w: invalid MGR configure: %w", ErrFrame, err)
		}
	}()
	if m.Type != MgrConfigureMessage || len(m.Middle) != 0 || len(m.Data) != 0 {
		return c, ErrFrame
	}
	if m.Version < 1 || m.Version > 4 || m.CompatVersion != 1 {
		return c, wire.ErrVersion
	}
	if uint64(len(m.Front))+41 > uint64(limit) {
		return c, wire.ErrLimit
	}
	d := wire.NewDecoderLimit(m.Front, limit)
	c.Version = m.Version
	c.StatsPeriod = d.U32()
	if m.Version >= 2 {
		c.StatsThreshold = d.U32()
	}
	remaining := func() []byte { return slices.Clone(m.Front[len(m.Front)-d.Remaining():]) }
	if m.Version >= 3 {
		rest := remaining()
		if queries := d.U32(); d.Err() == nil && queries != 0 {
			c.Extension = rest
			return c, nil
		}
	}
	if m.Version >= 4 {
		rest := remaining()
		switch present := d.U8(); {
		case d.Err() != nil:
		case present > 1:
			return c, ErrFrame
		case present == 1:
			c.Extension = rest
			return c, nil
		}
	}
	return c, d.Done()
}

// DecodeMgrClose reads the MGR's echo of MMgrClose.
func DecodeMgrClose(m MessageData, limit uint32) (service, daemon string, err error) {
	defer func() {
		if err != nil {
			service, daemon = "", ""
			err = fmt.Errorf("%w: invalid MGR close: %w", ErrFrame, err)
		}
	}()
	if m.Type != MgrCloseMessage || len(m.Middle) != 0 || len(m.Data) != 0 {
		return "", "", ErrFrame
	}
	if m.Version != 1 || m.CompatVersion != 1 {
		return "", "", wire.ErrVersion
	}
	if uint64(len(m.Front))+41 > uint64(limit) {
		return "", "", wire.ErrLimit
	}
	d := wire.NewDecoderLimit(m.Front, limit)
	daemon, service = d.String(), d.String()
	return service, daemon, d.Done()
}
