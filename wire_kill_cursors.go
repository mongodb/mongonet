package mongonet

func (m *KillCursorsMessage) HasResponse() bool {
	return false
}

func (m *KillCursorsMessage) Header() MessageHeader {
	return m.header
}

func (m *KillCursorsMessage) Serialize() []byte {
	size := 16 /* header */ + 8 /* header */ + (8 * int(m.NumCursors))

	m.header.Size = int32(size)

	buf := make([]byte, size)
	m.header.WriteInto(buf)

	writeInt32(0, buf, 16)
	writeInt32(m.NumCursors, buf, 20)

	loc := 24

	for _, c := range m.CursorIds {
		writeInt64(c, buf, loc)
		loc += 8
	}

	return buf
}

func parseKillCursorsMessage(header MessageHeader, buf []byte) (Message, error) {
	m := &KillCursorsMessage{}
	m.header = header

	loc := 0

	if len(buf) < 8 {
		return m, NewStackErrorf("invalid kill cursors message -- message must have length of at least 8 bytes.")
	}
	m.Reserved = readInt32(buf)
	loc += 4

	m.NumCursors = readInt32(buf[loc:])
	loc += 4

	if len(buf[loc:]) < int(m.NumCursors)*8 {
		return m, NewStackErrorf("invalid kill cursors message -- NumCursors = %v is larger than number of cursors in message", m.NumCursors)
	}
	m.CursorIds = make([]int64, int(m.NumCursors))

	for i := 0; i < int(m.NumCursors); i++ {
		m.CursorIds[i] = readInt64(buf[loc:])
		loc += 8
	}

	return m, nil
}
