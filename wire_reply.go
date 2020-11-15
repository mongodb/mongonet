package mongonet

import "go.mongodb.org/mongo-driver/x/bsonx/bsoncore"

func (m *ReplyMessage) HasResponse() bool {
	return false // because its a response
}

func (m *ReplyMessage) IsExhaust() bool {
	return false
}

func (m *ReplyMessage) Header() MessageHeader {
	return m.header
}

func (m *ReplyMessage) Serialize() []byte {
	size := 16 /* header */ + 20 /* reply header */
	for _, d := range m.Docs {
		size += int(d.Size)
	}
	m.header.Size = int32(size)

	buf := make([]byte, size)
	m.header.WriteInto(buf)

	writeInt32(m.Flags, buf, 16)
	writeInt64(m.CursorId, buf, 20)
	writeInt32(m.StartingFrom, buf, 28)
	writeInt32(m.NumberReturned, buf, 32)

	loc := 36
	for _, d := range m.Docs {
		copy(buf[loc:], d.BSON)
		loc += len(d.BSON)
	}

	return buf
}

func parseReplyMessage(header MessageHeader, buf []byte) (Message, error) {
	rm := &ReplyMessage{}
	rm.header = header

	loc := 0

	if len(buf) < 20 {
		return rm, NewStackErrorf("invalid reply message -- message must have length of at least 20 bytes.")
	}
	rm.Flags = readInt32(buf[loc:])
	loc += 4

	rm.CursorId = readInt64(buf[loc:])
	loc += 8

	rm.StartingFrom = readInt32(buf[loc:])
	loc += 4

	rm.NumberReturned = readInt32(buf[loc:])
	loc += 4

	for loc < len(buf) {
		doc, err := parseSimpleBSON(buf[loc:])
		if err != nil {
			return nil, err
		}
		rm.Docs = append(rm.Docs, doc)
		loc += int(doc.Size)
	}
	if len(rm.Docs) > 0 {
		rm.CommandDoc = bsoncore.Document(rm.Docs[0].BSON)
	}
	return rm, nil
}
