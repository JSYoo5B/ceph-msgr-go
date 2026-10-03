package msgr

import "github.com/jsyoo5b/ceph-msgr-go/internal/wire"

const (
	MonMapMessage           uint16 = 4
	MonGetMapMessage        uint16 = 5
	SubscribeMessage        uint16 = 15
	SubscribeAckMessage     uint16 = 16
	AuthMessage             uint16 = 17
	AuthReplyMessage        uint16 = 18
	MonCommandMessage       uint16 = 50
	MonCommandReplyMessage  uint16 = 51
	TellCommandMessage      uint16 = 97
	TellCommandReplyMessage uint16 = 98
	MgrMapMessage           uint16 = 0x704
	MgrCommandMessage       uint16 = 0x709
	MgrCommandReplyMessage  uint16 = 0x70a
)

type MessageData struct {
	Sequence, Transaction, AckSequence     uint64
	Type, Priority, Version, CompatVersion uint16
	Front, Middle, Data                    []byte
}

func (m MessageData) Frame() Frame {
	e := wire.Encoder{}
	e.U64(m.Sequence)
	e.U64(m.Transaction)
	e.U16(m.Type)
	e.U16(m.Priority)
	e.U16(m.Version)
	e.U32(0)
	e.U16(0)
	e.U64(m.AckSequence)
	e.U8(1) // CEPH_MSG_FOOTER_COMPLETE
	e.U16(m.CompatVersion)
	e.U16(0)
	return Frame{Tag: Message, Segments: [][]byte{e.Data, m.Front, m.Middle, m.Data}, Alignment: []uint16{8, 8, 8, 4096}}
}
func DecodeMessage(f Frame) (MessageData, error) {
	var m MessageData
	if f.Tag != Message || len(f.Segments) < 1 || len(f.Segments[0]) != 41 {
		return m, ErrFrame
	}
	d := wire.NewDecoder(f.Segments[0])
	m.Sequence = d.U64()
	m.Transaction = d.U64()
	m.Type = d.U16()
	m.Priority = d.U16()
	m.Version = d.U16()
	padding := d.U32()
	d.U16()
	m.AckSequence = d.U64()
	d.U8()
	m.CompatVersion = d.U16()
	reserved := d.U16()
	if padding != 0 || reserved != 0 || m.CompatVersion > m.Version {
		return m, ErrFrame
	}
	if len(f.Segments) > 1 {
		m.Front = f.Segments[1]
	}
	if len(f.Segments) > 2 {
		m.Middle = f.Segments[2]
	}
	if len(f.Segments) > 3 {
		m.Data = f.Segments[3]
	}
	return m, d.Done()
}

func Paxos(e *wire.Encoder) { e.U64(0); e.U16(0xffff); e.U64(0) }

// GetMonMap requests a one-time MonMap without creating map subscriptions.
// Tentacle's MMonGetMap has the default version-1 header and no payload.
func GetMonMap() MessageData {
	return MessageData{Type: MonGetMapMessage, Version: 1, Priority: 127}
}

func CommandMessage(mgr bool, fsid [16]byte, commands []string, input []byte) MessageData {
	e := wire.Encoder{}
	typ := MonCommandMessage
	if mgr {
		typ = MgrCommandMessage
	} else {
		Paxos(&e)
	}
	e.Raw(fsid[:])
	e.U32(uint32(len(commands)))
	for _, cmd := range commands {
		e.String(cmd)
	}
	return MessageData{Type: typ, Version: 1, Front: e.Data, Data: input, Priority: 127}
}

// TellCommand encodes Tentacle's daemon-local MCommand. The caller supplies
// the authenticated FSID; Session.Call assigns the transaction in the header.
func TellCommand(fsid [16]byte, commands []string, input []byte) MessageData {
	e := wire.Encoder{}
	e.Raw(fsid[:])
	e.U32(uint32(len(commands)))
	for _, command := range commands {
		e.String(command)
	}
	return MessageData{Type: TellCommandMessage, Version: 1, Front: e.Data, Data: input, Priority: 127}
}

func CommandReply(m MessageData, limit uint32) (int32, string, error) {
	d := wire.NewDecoderLimit(m.Front, limit)
	if m.Type == MonCommandReplyMessage {
		d.U64()
		d.U16()
		d.U64()
	} else if m.Type != MgrCommandReplyMessage && m.Type != TellCommandReplyMessage {
		return 0, "", ErrFrame
	}
	if m.CompatVersion > 1 {
		return 0, "", wire.ErrVersion
	}
	code, text := int32(d.U32()), d.String()
	if m.Type == MonCommandReplyMessage {
		n := d.Count(4, 1024)
		for i := 0; i < n; i++ {
			_ = d.Bytes()
		}
	}
	return code, text, d.Done()
}

func Subscribe(monEpoch, mgrEpoch uint32) MessageData {
	return subscribe(monEpoch, mgrEpoch, "", 0)
}

// LogSubscribe adds a continuous log subscription without dropping the MON/MGR
// map subscriptions. The caller validates the level: debug, info, sec, warn,
// or error. next is the 64-bit MON log-service cursor, not an entry sequence.
func LogSubscribe(level string, next uint64) MessageData {
	return subscribe(0, 0, "log-"+level, next)
}

func subscribe(monEpoch, mgrEpoch uint32, extraKey string, next uint64) MessageData {
	e := wire.Encoder{}
	if extraKey != "" {
		e.U32(3)
	} else {
		e.U32(2)
	}
	// Ceph maps encode in lexicographic key order.
	if extraKey != "" {
		e.String(extraKey)
		e.U64(next)
		e.U8(0)
	}
	e.String("mgrmap")
	e.U64(uint64(mgrEpoch))
	e.U8(0)
	e.String("monmap")
	e.U64(uint64(monEpoch))
	e.U8(0)
	e.String("")
	return MessageData{Type: SubscribeMessage, Version: 3, CompatVersion: 1, Priority: 127, Front: e.Data}
}
