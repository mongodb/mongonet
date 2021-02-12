package mongonet

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
)

type Session struct {
	server     *Server
	conn       io.ReadWriteCloser
	remoteAddr net.Addr

	logger *slogger.Logger

	SSLServerName string
	tlsConn       *tls.Conn
}

var ErrUnknownOpcode = errors.New("unknown opcode")

// ------------------
func (s *Session) Connection() io.ReadWriteCloser {
	return s.conn
}

func (s *Session) GetTLSConnection() *tls.Conn {
	return s.tlsConn
}

func (s *Session) Logf(level slogger.Level, messageFmt string, args ...interface{}) (*slogger.Log, []error) {
	return s.logger.Logf(level, messageFmt, args...)
}

func (s *Session) SetRemoteAddr(v net.Addr) {
	s.remoteAddr = v
}

func (s *Session) ReadMessage() (Message, error) {
	return ReadMessage(s.conn)
}

func (s *Session) Run(conn net.Conn) {
	var err error
	s.conn = s.server.workerFactory.GetConnection(conn)

	var worker ServerWorker
	defer func() {
		s.conn.Close()
		if worker != nil {
			worker.Close()
		}

		// server has sessions that will receive from the sessionCtx.Done() channel
		// decrement the session wait group
		if _, ok := s.server.contextualWorkerFactory(); ok {
			s.server.sessionManager.sessionWG.Done()
		}
	}()

	switch c := conn.(type) {
	case *tls.Conn:
		// we do this here so that we can get the SNI server name
		err = c.Handshake()
		if err != nil {
			s.logger.Logf(slogger.WARN, "error doing tls handshake %s", err)
			return
		}
		s.tlsConn = c
		s.SSLServerName = strings.TrimSuffix(c.ConnectionState().ServerName, ".")
	}

	s.logger.Logf(slogger.INFO, "new connection SSLServerName [%s]", s.SSLServerName)

	defer s.logger.Logf(slogger.INFO, "socket closed")

	if cwf, ok := s.server.contextualWorkerFactory(); ok {
		worker, err = cwf.CreateWorkerWithContext(s, s.server.sessionManager.ctx)
	} else {
		worker, err = s.server.workerFactory.CreateWorker(s)
	}

	if err != nil {
		s.logger.Logf(slogger.WARN, "error creating worker %s", err)
		return
	}

	worker.DoLoopTemp()
}

func (s *Session) RespondToCommandMakeBSON(clientMessage Message, args ...interface{}) error {
	if len(args)%2 == 1 {
		return fmt.Errorf("magic bson has to be even # of args, got %d", len(args))
	}

	gotOk := false

	doc := bson.D{}
	for idx := 0; idx < len(args); idx += 2 {
		name, ok := args[idx].(string)
		if !ok {
			return fmt.Errorf("got a non string for bson name: %t", args[idx])
		}
		doc = append(doc, bson.E{name, args[idx+1]})
		if name == "ok" {
			gotOk = true
		}
	}

	if !gotOk {
		doc = append(doc, bson.E{"ok", 1})
	}

	doc2, err := SimpleBSONConvert(doc)
	if err != nil {
		return err
	}
	return s.RespondToCommand(clientMessage, doc2)
}

// do not call with OP_GET_MORE since we never added support for that
func (s *Session) RespondToCommand(clientMessage Message, doc SimpleBSON) error {
	switch clientMessage.Header().OpCode {

	case OP_QUERY:
		rm := &ReplyMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_REPLY},
			0, // flags - error bit
			0, // cursor id
			0, // StartingFrom
			1, // NumberReturned
			[]SimpleBSON{doc},
		}
		return SendMessage(rm, s.conn)

	case OP_INSERT, OP_UPDATE, OP_DELETE:
		// For MongoDB 2.6+, and wpv 3+, these are only used for unacknowledged writes, so do nothing
		return nil

	case OP_COMMAND:
		rm := &CommandReplyMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_COMMAND_REPLY},
			doc,
			SimpleBSONEmpty(),
			[]SimpleBSON{},
		}
		return SendMessage(rm, s.conn)

	case OP_MSG:
		rm := &MessageMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_MSG},
			0,
			[]MessageMessageSection{
				&BodySection{
					doc,
				},
			},
		}
		return SendMessage(rm, s.conn)

	case OP_GET_MORE:
		return errors.New("Internal error.  Should not be passing a GET_MORE message here.")

	default:
		return ErrUnknownOpcode
	}

}

func (s *Session) RespondWithError(clientMessage Message, err error) error {
	s.logger.Logf(slogger.INFO, "RespondWithError %v", err)
	var errBSON bson.D
	if err == nil {
		errBSON = bson.D{{"ok", 1}}
	} else if mongoErr, ok := err.(MongoError); ok {
		errBSON = mongoErr.ToBSON()
	} else {
		errBSON = bson.D{{"ok", 0}, {"errmsg", err.Error()}}
	}

	doc, myErr := SimpleBSONConvert(errBSON)
	if myErr != nil {
		return myErr
	}

	switch clientMessage.Header().OpCode {
	case OP_QUERY, OP_GET_MORE:
		rm := &ReplyMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_REPLY},

			// We should not set the error bit because we are
			// responding with errmsg instead of $err
			0, // flags - error bit

			0, // cursor id
			0, // StartingFrom
			1, // NumberReturned
			[]SimpleBSON{doc},
		}
		return SendMessage(rm, s.conn)

	case OP_INSERT, OP_UPDATE, OP_DELETE:
		// For MongoDB 2.6+, and wpv 3+, these are only used for unacknowledged writes, so do nothing
		return nil

	case OP_COMMAND:
		rm := &CommandReplyMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_COMMAND_REPLY},
			doc,
			SimpleBSONEmpty(),
			[]SimpleBSON{},
		}
		return SendMessage(rm, s.conn)

	case OP_MSG:
		rm := &MessageMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_MSG},
			0,
			[]MessageMessageSection{
				&BodySection{
					doc,
				},
			},
		}
		return SendMessage(rm, s.conn)

	default:
		return ErrUnknownOpcode
	}

}
