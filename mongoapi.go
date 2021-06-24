package mongonet

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// RunCommandUsingRawBSON - runs a command using low-level writeWireMessage API
// Notes:
// 1. caller is expected to cleanup mongo.Client object
// 2. Uses primary read preference
// 3. Expects an OP_MSG response back from the server
func RunCommandUsingRawBSON(cmd bson.D, client *mongo.Client, goctx context.Context) (bson.D, error) {
	topology := extractTopology(client)
	srv, err := topology.SelectServer(goctx, description.ReadPrefSelector(readpref.Primary()))
	if err != nil {
		return nil, err
	}
	conn, err := srv.Connection(goctx)
	if err != nil {
		return nil, err
	}

	sb, err := SimpleBSONConvert(cmd)
	if err != nil {
		return nil, err
	}

	newmsg := &MessageMessage{
		MessageHeader{
			0,
			17,
			1,
			OP_MSG},
		0,
		[]MessageMessageSection{
			&BodySection{
				sb,
			},
		},
	}

	if err := conn.WriteWireMessage(goctx, newmsg.Serialize()); err != nil {
		return nil, err
	}

	ret, err := conn.ReadWireMessage(goctx, nil)
	if err != nil {
		return nil, err
	}
	resp, err := ReadMessageFromBytes(ret)
	if err != nil {
		return nil, err
	}
	if mm, ok := resp.(*MessageMessage); ok {
		for _, sec := range mm.Sections {
			if bodySection, ok := sec.(*BodySection); ok && bodySection != nil {
				respBsonD, err := bodySection.Body.ToBSOND()
				if err != nil {
					return nil, err
				}
				return respBsonD, nil
			}
		}
	} else {
		return nil, fmt.Errorf("expected an OP_MSG response but got %T", resp)
	}
	return nil, fmt.Errorf("couldn't find a body section in response")
}
