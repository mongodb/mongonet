package mongonet

func (m *GetMoreMessage) HasResponse() bool {
	return true
}

func (m *GetMoreMessage) Header() MessageHeader {
	return m.header
}

func (m *GetMoreMessage) Serialize() []byte {
	size := 16 /* header */ + 16 /* query header */
	size += len(m.Namespace) + 1

	m.header.Size = int32(size)

	buf := make([]byte, size)
	m.header.WriteInto(buf)

	writeInt32(0, buf, 16)

	loc := 20
	writeCString(m.Namespace, buf, &loc)
	writeInt32(m.NReturn, buf, loc)
	loc += 4

	writeInt64(m.CursorId, buf, loc)
	loc += 8

	return buf
}

func parseGetMoreMessage(header MessageHeader, buf []byte) (Message, error) {
	qm := &GetMoreMessage{}
	qm.header = header

	loc := 0

	if len(buf) < 4 {
		return m, NewStackErrorf("invalid get more message -- length of message bytes = %v is less than 4", len(buf))
	}
	qm.Reserved = readInt32(buf)
	loc += 4

	tmp, err := readCString(buf[loc:])
	qm.Namespace = tmp
	if err != nil {
		return nil, err
	}
	loc += len(qm.Namespace) + 1

	if len(buf) < loc+12 {
		return m, NewStackErrorf("invalid get more message -- expected message length to be at least %v, but it was %v", loc+12, len(buf))
	}
	qm.NReturn = readInt32(buf[loc:])
	loc += 4

	qm.CursorId = readInt64(buf[loc:])

	return qm, nil
}
