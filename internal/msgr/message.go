package msgr

import "github.com/JSYoo5B/ceph-msgr-go/internal/wire"

const (
	// MonMapMessage is received; header compat above 1 is refused, and the inner
	// map envelope carries its own version.
	MonMapMessage uint16 = 4
	// MonGetMapMessage is sent with header 1/0.
	MonGetMapMessage uint16 = 5
	// MonGetOSDMapMessage is sent with header 1/0.
	MonGetOSDMapMessage uint16 = 6
	// SubscribeMessage is sent with header 3/1.
	SubscribeMessage uint16 = 15
	// SubscribeAckMessage is defined only: no encoder or decoder exists.
	SubscribeAckMessage uint16 = 16
	// AuthMessage is defined only: CephX runs inside msgr2 authentication
	// frames, not this message.
	AuthMessage uint16 = 17
	// AuthReplyMessage is defined only, as AuthMessage.
	AuthReplyMessage uint16 = 18
	// OSDMapMessage is received with header 1/1 or 4/3.
	OSDMapMessage uint16 = 41
	// OSDOpMessage is sent with header 6 (base), 8 or 9 and compat 3, by the
	// selected OSD profile; it shares type 42 with osd.RequestMessage.
	OSDOpMessage uint16 = 42
	// OSDOpReplyMessage is received with header exactly 6/2 (base) or 8/2
	// (reply-v8 profile); it shares type 43 with osd.ReplyMessage.
	OSDOpReplyMessage uint16 = 43
	// MonCommandMessage is sent with header 1/0.
	MonCommandMessage uint16 = 50
	// MonCommandReplyMessage is received with header compat at most 1 and
	// matched by type and transaction.
	MonCommandReplyMessage uint16 = 51
	// TellCommandMessage is sent with header 1/0 (MCommand to a MON, MGR, OSD or
	// MDS).
	TellCommandMessage uint16 = 97
	// TellCommandReplyMessage is received with header compat at most 1 and
	// matched by type and transaction.
	TellCommandReplyMessage uint16 = 98
	// MgrMapMessage is received; header compat above 1 is refused.
	MgrMapMessage uint16 = 0x704
	// MgrCommandMessage is sent with header 1/0.
	MgrCommandMessage uint16 = 0x709
	// MgrCommandReplyMessage is received with header compat at most 1 and
	// matched by type and transaction.
	MgrCommandReplyMessage uint16 = 0x70a
)

// MessageData is one Messenger message (Ceph v20.2.4 msg/Message.h header2
// and its segments). Each message type constant in this package and in the
// mds and osd packages states on its own declaration whether the client sends
// it, receives it or only defines it, and the header versions it emits or
// accepts. Every received message first requires compat at most its header
// version; further checks vary by decoder and are stated per type. Those
// statements bound acceptance only: they do not advertise support for, or
// promise a refusal of, every other header version a daemon may send. Types
// without a constant, such as CephFS client metrics (29) and the full FSMap
// (45), and daemon-to-daemon types are not implemented.
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

// GetOSDMaps requests inclusive full/incremental ranges. Tentacle returns one
// possibly empty or truncated MOSDMap, without copying a transaction ID.
func GetOSDMaps(fullFirst, fullLast, incrementalFirst, incrementalLast uint32) MessageData {
	e := wire.Encoder{}
	Paxos(&e)
	e.U32(fullFirst)
	e.U32(fullLast)
	e.U32(incrementalFirst)
	e.U32(incrementalLast)
	return MessageData{Type: MonGetOSDMapMessage, Version: 1, Priority: 127, Front: e.Data}
}

// OSDMapSubscribe updates only osdmap. Other subscriptions on the connection
// are retained by MON. Zero requests the latest full map; positive next asks
// for history starting at that epoch. ONETIME removes this server subscription
// after its first publication cycle, which can contain multiple messages.
func OSDMapSubscribe(next uint64, onetime bool, hostname string) MessageData {
	e := wire.Encoder{}
	e.U32(1)
	e.String("osdmap")
	e.U64(next)
	var flags uint8
	if onetime {
		flags = 1
	}
	e.U8(flags)
	e.String(hostname)
	return MessageData{Type: SubscribeMessage, Version: 3, CompatVersion: 1, Priority: 127, Front: e.Data}
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

func Subscribe(monEpoch, mgrEpoch uint32, hostname string) MessageData {
	return subscribe(monEpoch, mgrEpoch, "", 0, hostname)
}

// LogSubscribe adds a continuous log subscription without dropping the MON/MGR
// map subscriptions. The caller validates the level: debug, info, sec, warn,
// or error. next is the 64-bit MON log-service cursor, not an entry sequence.
func LogSubscribe(level string, next uint64, hostname string) MessageData {
	return subscribe(0, 0, "log-"+level, next, hostname)
}

func subscribe(monEpoch, mgrEpoch uint32, extraKey string, next uint64, hostname string) MessageData {
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
	e.String(hostname)
	return MessageData{Type: SubscribeMessage, Version: 3, CompatVersion: 1, Priority: 127, Front: e.Data}
}
