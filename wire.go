package mongonet

const (
	OP_REPLY         = 1
	OP_MSG_LEGACY    = 1000
	OP_UPDATE        = 2001
	OP_INSERT        = 2002
	RESERVED         = 2003
	OP_QUERY         = 2004
	OP_GET_MORE      = 2005
	OP_DELETE        = 2006
	OP_KILL_CURSORS  = 2007
	OP_COMMAND       = 2010
	OP_COMMAND_REPLY = 2011
	OP_MSG           = 2013
)

type MessageHeader struct {
	Size       int32 // total message size
	RequestID  int32
	ResponseTo int32
	OpCode     int32
}

func (h *MessageHeader) WriteInto(buf []byte) {
	writeInt32(h.Size, buf, 0)
	writeInt32(h.RequestID, buf, 4)
	writeInt32(h.ResponseTo, buf, 8)
	writeInt32(h.OpCode, buf, 12)
}

// ------------

type Message interface {
	Header() MessageHeader
	Serialize() []byte
	HasResponse() bool
}

// OP_REPLY
type ReplyMessage struct {
	header MessageHeader

	Flags          int32
	CursorId       int64
	StartingFrom   int32
	NumberReturned int32

	Docs []SimpleBSON
}

// OP_UPDATE
type UpdateMessage struct {
	header MessageHeader

	Reserved  int32
	Namespace string
	Flags     int32

	Filter SimpleBSON
	Update SimpleBSON
}

// OP_QUERY
type QueryMessage struct {
	header MessageHeader

	Flags     int32
	Namespace string
	Skip      int32
	NReturn   int32

	Query   SimpleBSON
	Project SimpleBSON
}

// OP_GET_MORE
type GetMoreMessage struct {
	header MessageHeader

	Reserved  int32
	Namespace string
	NReturn   int32
	CursorId  int64
}

// OP_INSERT
type InsertMessage struct {
	header MessageHeader

	Flags     int32
	Namespace string

	Docs []SimpleBSON
}

// OP_DELETE
type DeleteMessage struct {
	header MessageHeader

	Reserved  int32
	Namespace string
	Flags     int32

	Filter SimpleBSON
}

// OP_KILL_CURSORS
type KillCursorsMessage struct {
	header MessageHeader

	Reserved   int32
	NumCursors int32
	CursorIds  []int64
}

// OP_COMMAND
type CommandMessage struct {
	header MessageHeader

	DB          string
	CmdName     string
	CommandArgs SimpleBSON
	Metadata    SimpleBSON
	InputDocs   []SimpleBSON
}

// OP_COMMAND_REPLY
type CommandReplyMessage struct {
	header MessageHeader

	CommandReply SimpleBSON
	Metadata     SimpleBSON
	OutputDocs   []SimpleBSON
}

// OP_MSG
// Note that checksum is not implemented
type MessageMessage struct {
	header MessageHeader

	FlagBits int32
	Sections []MessageMessageSection
}
