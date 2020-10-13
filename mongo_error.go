package mongonet

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

type MongoError struct {
	err      error
	code     int
	codeName string
}

func NewMongoError(err error, code int, codeName string) MongoError {
	return MongoError{err, code, codeName}
}

func (me MongoError) ToBSON() bson.D {
	doc := bson.D{{"ok", 0}}

	if me.err != nil {
		doc = append(doc, bson.E{"errmsg", me.err.Error()})
	}

	doc = append(doc,
		bson.E{"code", me.code},
		bson.E{"codeName", me.codeName})

	return doc
}

func (me MongoError) Error() string {
	return fmt.Sprintf(
		"code=%v codeName=%v errmsg = %v",
		me.code,
		me.codeName,
		me.err.Error(),
	)
}
