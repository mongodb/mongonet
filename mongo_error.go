package mongonet

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
)

type MongoError struct {
	err      error
	code     int
	codeName string
	labels   []string
}

func NewMongoError(err error, code int, codeName string) MongoError {
	return MongoError{err, code, codeName, nil}
}

func NewMongoErrorWithLabels(err error, code int, codeName string, labels []string) MongoError {
	return MongoError{err, code, codeName, labels}
}

func (me MongoError) HasLabel(label string) bool {
	for _, val := range me.labels {
		if val == label {
			return true
		}
	}
	return false
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

func (me MongoError) GetCode() int {
	return me.code
}

func (me MongoError) GetCodeName() string {
	return me.codeName
}

func (me MongoError) Error() string {
	if me.err != nil {
		return fmt.Sprintf(
			"code=%v codeName=%v errmsg = %v",
			me.code,
			me.codeName,
			me.err.Error(),
		)
	} else {
		return fmt.Sprintf(
			"code=%v codeName=%v",
			me.code,
			me.codeName,
		)
	}
}
