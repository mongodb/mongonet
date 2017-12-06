package mongonet

func (m *MessageMessage) HasResponse() bool {
	return true
}

func (m *MessageMessage) Header() MessageHeader {
	return m.header
}

func (m *MessageMessage) Serialize() []byte {
	size := 16 // header
	size += 4  // FlagBits
	for _, s := range m.Sections {
		size += s.Size()
	}
	m.header.Size = int32(size)

	buf := make([]byte, size)
	m.header.WriteInto(buf)

	writeInt32(m.FlagBits, buf, 16)

	loc := 20

	for _, s := range m.Sections {
		s.WriteInto(buf, &loc)
	}

	return buf
}

type MessageMessageSection interface {
	Size() int32
	WriteInto(buf []byte, loc *int)
}

type BodySection SimpleBSON

func (bs BodySection) Size() int32 {
	// 1 byte for kind byte
	return 1 + SimpleBSON(bs).Size
}

func (bs BodySection) WriteInto(buf []byte, loc *int) {
	
