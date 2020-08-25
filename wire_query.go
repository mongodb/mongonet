package mongonet

func (m *QueryMessage) HasResponse() bool {
	return true
}

func (m *QueryMessage) IsExhaust() bool {
	return m.Flags&(1<<6) != 0
}

func (m *QueryMessage) Header() MessageHeader {
	return m.header
}

func (m *QueryMessage) Serialize() []byte {
	size := 16 /* header */ + 12 /* query header */
	size += len(m.Namespace) + 1
	size += int(m.Query.Size)
	size += int(m.Project.Size)

	m.header.Size = int32(size)

	buf := make([]byte, size)
	m.header.WriteInto(buf)

	writeInt32(m.Flags, buf, 16)

	loc := 20
	writeCString(m.Namespace, buf, &loc)
	writeInt32(m.Skip, buf, loc)
	loc += 4

	writeInt32(m.NReturn, buf, loc)
	loc += 4

	m.Query.Copy(&loc, buf)
	m.Project.Copy(&loc, buf)

	return buf
}

func parseQueryMessage(header MessageHeader, buf []byte) (Message, error) {
	qm := &QueryMessage{}
	qm.header = header

	loc := 0

	if len(buf) < 4 {
		return qm, NewStackErrorf("invalid query message -- message must have length of at least 4 bytes.")
	}
	qm.Flags = readInt32(buf)
	loc += 4

	tmp, err := readCString(buf[loc:])
	qm.Namespace = tmp
	if err != nil {
		return nil, err
	}
	loc += len(qm.Namespace) + 1

	if len(buf) < loc+8 {
		return qm, NewStackErrorf("invalid query message -- message length is too short.")
	}
	qm.Skip = readInt32(buf[loc:])
	loc += 4

	qm.NReturn = readInt32(buf[loc:])
	loc += 4

	qm.Query, err = parseSimpleBSON(buf[loc:])
	if err != nil {
		return nil, err
	}
	loc += int(qm.Query.Size)
	if loc < len(buf) {
		qm.Project, err = parseSimpleBSON(buf[loc:])
		if err != nil {
			return nil, err
		}
		loc += int(qm.Project.Size)
	}

	return qm, nil
}

func NewQueryMessage(ns string, flags int32, skip int32, toReturn int32, query SimpleBSON, project SimpleBSON) *QueryMessage {
	qm := &QueryMessage{}
	qm.header.RequestID = 17 // TODO
	qm.header.OpCode = OP_QUERY
	qm.Flags = flags
	qm.Namespace = ns
	qm.Skip = skip
	qm.NReturn = toReturn
	qm.Query = query
	qm.Project = project
	return qm
}
