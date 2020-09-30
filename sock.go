package mongonet

import "io"

const MaxInt32 = 2147483647

func sendBytes(writer io.Writer, buf []byte) error {
	for {
		written, err := writer.Write(buf)
		if err != nil {
			return NewStackErrorf("error writing to client: %s", err)
		}

		if written == len(buf) {
			return nil
		}

		buf = buf[written:]
	}

}

func getMessage(header MessageHeader, body []byte) (Message, error) {
	switch header.OpCode {
	case OP_REPLY:
		return parseReplyMessage(header, body)
	case OP_UPDATE:
		return parseUpdateMessage(header, body)
	case OP_INSERT:
		return parseInsertMessage(header, body)
	case OP_QUERY:
		return parseQueryMessage(header, body)
	case OP_GET_MORE:
		return parseGetMoreMessage(header, body)
	case OP_DELETE:
		return parseDeleteMessage(header, body)
	case OP_KILL_CURSORS:
		return parseKillCursorsMessage(header, body)
	case OP_COMMAND:
		return parseCommandMessage(header, body)
	case OP_COMMAND_REPLY:
		return parseCommandReplyMessage(header, body)
	case OP_MSG:
		return parseMessageMessage(header, body)
	default:
		return nil, NewStackErrorf("unknown op code: %v", header.OpCode)
	}
}

func ReadMessageFromBytes(src []byte) (Message, error) {
	// header
	header := MessageHeader{}
	header.Size = readInt32(src[0:4])

	if header.Size > int32(200*1024*1024) {
		if header.Size == 542393671 {
			return nil, NewStackErrorf("message too big, probably http request %d", header.Size)
		}
		return nil, NewStackErrorf("message too big %d", header.Size)
	}
	if header.Size-4 < 0 || header.Size-4 > MaxInt32 {
		return nil, NewStackErrorf("message header has invalid size (%v).", header.Size)
	}
	if header.Size-4 < 12 {
		return nil, NewStackErrorf("invalid message header. either header.Size = %v is shorter than message length, or message is missing RequestId, ResponseTo, or OpCode fields.", header.Size)
	}
	header.RequestID = readInt32(src[4:8])
	header.ResponseTo = readInt32(src[8:12])
	header.OpCode = readInt32(src[12:16])

	body := src[16:]
	return getMessage(header, body)

}

func ReadMessage(reader io.Reader) (Message, error) {
	// read header
	sizeBuf := make([]byte, 4)
	n, err := reader.Read(sizeBuf)
	if err != nil {
		return nil, err
	}
	if n != 4 {
		return nil, NewStackErrorf("didn't read message size from socket, got %d", n)
	}

	header := MessageHeader{}

	header.Size = readInt32(sizeBuf)

	if header.Size > int32(200*1024*1024) {
		if header.Size == 542393671 {
			return nil, NewStackErrorf("message too big, probably http request %d", header.Size)
		}
		return nil, NewStackErrorf("message too big %d", header.Size)
	}

	if header.Size-4 < 0 || header.Size-4 > MaxInt32 {
		return nil, NewStackErrorf("message header has invalid size (%v).", header.Size)
	}
	restBuf := make([]byte, header.Size-4)

	for read := 0; int32(read) < header.Size-4; {
		n, err := reader.Read(restBuf[read:])
		if err != nil {
			return nil, err
		}
		if n == 0 {
			break
		}
		read += n
	}

	if len(restBuf) < 12 {
		return nil, NewStackErrorf("invalid message header. either header.Size = %v is shorter than message length, or message is missing RequestId, ResponseTo, or OpCode fields.", header.Size)
	}
	header.RequestID = readInt32(restBuf)
	header.ResponseTo = readInt32(restBuf[4:])
	header.OpCode = readInt32(restBuf[8:])

	body := restBuf[12:]

	return getMessage(header, body)

}

func SendMessage(m Message, writer io.Writer) error {
	return sendBytes(writer, m.Serialize())
}
