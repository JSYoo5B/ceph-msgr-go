package msgr

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

const (
	ConfigMessage    uint16 = 62
	GetConfigMessage uint16 = 63
)

// ConfigSubscribe requests continuous effective configuration updates without
// dropping the MON/MGR map subscriptions. Empty hostname means no host mask.
func ConfigSubscribe(hostname string) MessageData {
	return subscribe(0, 0, "config", 0, hostname)
}

// GetConfig requests a full map for the validated client.<id> identity used by
// CephX authentication and the caller's unchanged hostname. Device class is
// empty; this builder does not select another entity or infer settings from
// the local operating system.
// MConfig replies have no request transaction, service version or cursor.
func GetConfig(identity, hostname string) MessageData {
	e := wire.Encoder{}
	e.U32(8) // EntityName CLIENT type, distinct from a global-ID entity_name_t.
	e.String(strings.TrimPrefix(identity, "client."))
	e.String(hostname)
	e.String("")
	return MessageData{Type: GetConfigMessage, Version: 1, CompatVersion: 1, Priority: 127, Front: e.Data}
}

// DecodeConfig reads Tentacle's MConfig full replacement map. It preserves raw
// strings, including empty values and unknown option names, without applying
// them to the client. The logical frame limit includes the 41-byte Messenger
// message header. At most 65536 entries are accepted; failure returns no map.
func DecodeConfig(m MessageData, limit uint32) (config map[string]string, err error) {
	defer func() {
		if err != nil {
			config = nil
			err = fmt.Errorf("%w: invalid MON config: %w", ErrFrame, err)
		}
	}()
	if m.Type != ConfigMessage {
		return nil, ErrFrame
	}
	if m.Version < 1 || m.CompatVersion > 1 || m.CompatVersion > m.Version {
		return nil, wire.ErrVersion
	}
	if uint64(len(m.Front))+uint64(len(m.Middle))+uint64(len(m.Data))+41 > uint64(limit) {
		return nil, wire.ErrLimit
	}
	if len(m.Middle) != 0 || len(m.Data) != 0 {
		return nil, ErrFrame
	}
	d := wire.NewDecoderLimit(m.Front, limit)
	// Each key/value pair needs at least its two uint32 string lengths.
	n := d.Count(8, 65536)
	if err := d.Err(); err != nil {
		return nil, err
	}
	config = make(map[string]string, n)
	for i := 0; i < n; i++ {
		key, value := d.String(), d.String()
		if err := d.Err(); err != nil {
			return nil, err
		}
		if _, exists := config[key]; exists {
			return nil, errors.New("duplicate configuration key")
		}
		config[key] = value
	}
	return config, d.Done()
}
