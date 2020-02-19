package mongonet

import "io"

const MaxInt32 = 2147483647

func sendBytes(writer io.Writer, buf []byte) error {
	origBuf := buf
	defer BufferPoolPut(origBuf)
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

// caller must call BufferPoolPut() on bufToReclaim when done
func ReadMessage(reader io.Reader) (m Message, bufToReclaim []byte, err error) {
	// read header
	sizeBuf := make([]byte, 4)
	n, err := reader.Read(sizeBuf)
	if err != nil {
		return nil, nil, err
	}
	if n != 4 {
		return nil, nil, NewStackErrorf("didn't read message size from socket, got %d", n)
	}

	header := MessageHeader{}

	header.Size = readInt32(sizeBuf)

	if header.Size > int32(200*1024*1024) {
		if header.Size == 542393671 {
			return nil, nil, NewStackErrorf("message too big, probably http request %d", header.Size)
		}
		return nil, nil, NewStackErrorf("message too big %d", header.Size)
	}

	if header.Size-4 < 0 || header.Size-4 > MaxInt32 {
		return nil, nil, NewStackErrorf("message header has invalid size (%v).", header.Size)
	}
	restBuf := BufferPoolGet(int(header.Size - 4))
	bufToReclaim = restBuf

	for read := 0; int32(read) < header.Size-4; {
		n, err := reader.Read(restBuf[read:])
		if err != nil {
			return nil, nil, err
		}
		if n == 0 {
			break
		}
		read += n
	}

	if len(restBuf) < 12 {
		return nil, nil, NewStackErrorf("invalid message header. either header.Size = %v is shorter than message length, or message is missing RequestId, ResponseTo, or OpCode fields.", header.Size)
	}
	header.RequestID = readInt32(restBuf)
	header.ResponseTo = readInt32(restBuf[4:])
	header.OpCode = readInt32(restBuf[8:])

	body := restBuf[12:]

	switch header.OpCode {
	case OP_REPLY:
		m, err = parseReplyMessage(header, body)
	case OP_UPDATE:
		m, err = parseUpdateMessage(header, body)
	case OP_INSERT:
		m, err = parseInsertMessage(header, body)
	case OP_QUERY:
		m, err = parseQueryMessage(header, body)
	case OP_GET_MORE:
		m, err = parseGetMoreMessage(header, body)
	case OP_DELETE:
		m, err = parseDeleteMessage(header, body)
	case OP_KILL_CURSORS:
		m, err = parseKillCursorsMessage(header, body)
	case OP_COMMAND:
		m, err = parseCommandMessage(header, body)
	case OP_COMMAND_REPLY:
		m, err = parseCommandReplyMessage(header, body)
	case OP_MSG:
		m, err = parseMessageMessage(header, body)
	default:
		return nil, nil, NewStackErrorf("unknown op code: %v", header.OpCode)
	}
	return
}

func SendMessage(m Message, writer io.Writer) error {
	return sendBytes(writer, m.Serialize())
}
