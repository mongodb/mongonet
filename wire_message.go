package mongonet

const (
	BodySectionKind             = 0
	DocumentSequenceSectionKind = 1
)

func (m *MessageMessage) HasResponse() bool {
	return true
}

func (m *MessageMessage) Header() MessageHeader {
	return m.header
}

func (m *MessageMessage) Serialize() []byte {
	size := int32(16) // header
	size += 4         // FlagBits
	for _, s := range m.Sections {
		size += s.Size()
	}
	m.header.Size = size

	buf := make([]byte, size)
	m.header.WriteInto(buf)

	flags := m.FlagBits
	flags &^= 1 // clear checksumPresent field
	writeInt32(flags, buf, 16)

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

type BodySection struct {
	Body SimpleBSON
}

func (bs *BodySection) Size() int32 {
	// 1 byte for kind byte
	return 1 + bs.Body.Size
}

func (bs *BodySection) WriteInto(buf []byte, loc *int) {
	buf[*loc] = BodySectionKind
	(*loc)++
	bs.Body.Copy(loc, buf)
}

type DocumentSequenceSection struct {
	SequenceId string
	Documents  []SimpleBSON
}

func (dss *DocumentSequenceSection) Size() int32 {
	size := int32(5)                       // 1 byte for kind, 4 bytes for size
	size += int32(len(dss.SequenceId)) + 1 // 1 byte for null termination
	for _, d := range dss.Documents {
		size += d.Size
	}
	return size
}

func (dss *DocumentSequenceSection) WriteInto(buf []byte, loc *int) {
	buf[*loc] = DocumentSequenceSectionKind
	(*loc)++
	writeInt32(dss.Size()-1, buf, *loc) // subtract 1 byte for kind byte (not included in size written)
	(*loc) += 4
	writeCString(dss.SequenceId, buf, loc)
	for _, d := range dss.Documents {
		d.Copy(loc, buf)
	}
}

func parseMessageMessage(header MessageHeader, buf []byte) (Message, error) {
	msg := &MessageMessage{}
	msg.header = header

	if len(buf) < 4 {
		return msg, NewStackErrorf("invalid message message -- message must have a length of at least 4 bytes.")
	}

	msg.FlagBits = readInt32(buf)
	loc := 4

	sections := make([]MessageMessageSection, 0)
	for (len(buf) - loc) >= 5 { // need at least 5 bytes left for kind byte and size
		section, err := parseMessageMessageSection(buf, &loc)
		if err != nil {
			return msg, err
		}
		sections = append(sections, section)
	}

	msg.Sections = sections

	return msg, nil
}

func parseMessageMessageSection(buf []byte, loc *int) (MessageMessageSection, error) {
	kind := buf[*loc]
	(*loc)++
	switch kind {
	case BodySectionKind:
		return parseBodySection(buf, loc)
	case DocumentSequenceSectionKind:
		return parseDocumentSequenceSection(buf, loc)
	default:
		return nil, NewStackErrorf("invalid message message -- unknown section kind: %v", buf[0])
	}
}

func parseBodySection(buf []byte, loc *int) (MessageMessageSection, error) {
	bs := &BodySection{}

	var err error

	bs.Body, err = parseSimpleBSON(buf[*loc:])
	if err != nil {
		return bs, err
	}

	(*loc) += int(bs.Body.Size)

	return bs, nil
}

func parseDocumentSequenceSection(buf []byte, loc *int) (MessageMessageSection, error) {
	dss := &DocumentSequenceSection{}

	if len(buf[*loc:]) < 4 {
		return dss, NewStackErrorf("invalid Document Sequence section -- section must have a length of at least 4 bytes.")
	}

	expectedSizeRemaining := readInt32(buf[*loc:])

	if int(expectedSizeRemaining) > len(buf[*loc:]) {
		return dss, NewStackErrorf("invalid Document Sequence section -- section size is larger than message.")
	}

	(*loc) += 4
	expectedSizeRemaining -= 4

	var err error

	dss.SequenceId, err = readCString(buf[*loc:])
	if err != nil {
		return dss, err
	}

	(*loc) += len(dss.SequenceId) + 1
	expectedSizeRemaining -= int32((len(dss.SequenceId) + 1))

	docs := make([]SimpleBSON, 0)
	docNum := 0
	for expectedSizeRemaining > 0 {
		doc, err := parseSimpleBSON(buf[*loc:])
		if err != nil {
			return dss, err
		}

		if expectedSizeRemaining < doc.Size {
			return dss, NewStackErrorf("invalid Document Sequence section -- size of document %v (starting from 0) (%v bytes) overruns end of section (%v bytes left)", docNum, doc.Size, expectedSizeRemaining)
		}

		expectedSizeRemaining -= doc.Size
		(*loc) += int(doc.Size)
		docs = append(docs, doc)
		docNum++
	}

	dss.Documents = docs

	return dss, nil
}
