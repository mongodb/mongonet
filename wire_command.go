package mongonet

func (m *CommandMessage) HasResponse() bool {
	return true
}

func (m *CommandMessage) Header() MessageHeader {
	return m.header
}

func (m *CommandMessage) Serialize() []byte {
	size := 16 /* header */
	size += len(m.DB) + 1
	size += len(m.CmdName) + 1
	size += int(m.CommandArgs.Size)
	size += int(m.Metadata.Size)
	for _, d := range m.InputDocs {
		size += int(d.Size)
	}
	m.header.Size = int32(size)

	buf := make([]byte, size)
	m.header.WriteInto(buf)

	loc := 16

	writeCString(m.DB, buf, &loc)
	writeCString(m.CmdName, buf, &loc)
	m.CommandArgs.Copy(&loc, buf)
	m.Metadata.Copy(&loc, buf)

	for _, d := range m.InputDocs {
		d.Copy(&loc, buf)
	}

	return buf
}

func parseCommandMessage(header MessageHeader, buf []byte) (Message, error) {

	cmd := &CommandMessage{}
	cmd.header = header

	var err error

	cmd.DB, err = readCString(buf)
	if err != nil {
		return cmd, err
	}
	if len(buf) < len(cmd.DB)+1 {
		return cmd, NewStackErrorf("invalid command message -- message length is too short.")
	}
	buf = buf[len(cmd.DB)+1:]

	cmd.CmdName, err = readCString(buf)
	if err != nil {
		return cmd, err
	}
	if len(buf) < len(cmd.CmdName)+1 {
		return cmd, NewStackErrorf("invalid command message -- message length is too short.")
	}
	buf = buf[len(cmd.CmdName)+1:]

	cmd.CommandArgs, err = parseSimpleBSON(buf)
	if err != nil {
		return cmd, err
	}
	if len(buf) < int(cmd.CommandArgs.Size) {
		return cmd, NewStackErrorf("invalid command message -- message length is too short.")
	}
	buf = buf[cmd.CommandArgs.Size:]

	cmd.Metadata, err = parseSimpleBSON(buf)
	if err != nil {
		return cmd, err
	}
	if len(buf) < int(cmd.Metadata.Size) {
		return cmd, NewStackErrorf("invalid command message -- message length is too short.")
	}
	buf = buf[cmd.Metadata.Size:]

	for len(buf) > 0 {
		doc, err := parseSimpleBSON(buf)
		if err != nil {
			return cmd, err
		}
		buf = buf[doc.Size:]
		cmd.InputDocs = append(cmd.InputDocs, doc)
	}

	return cmd, nil
}
