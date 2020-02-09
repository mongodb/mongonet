package mongonet

import (
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type SimpleBSON struct {
	Size int32
	BSON []byte
}

func SimpleBSONConvert(d interface{}) (SimpleBSON, error) {
	raw, err := bson.Marshal(d)
	if err != nil {
		return SimpleBSON{}, err
	}
	return SimpleBSON{int32(len(raw)), raw}, nil
}

func SimpleBSONConvertOrPanic(d interface{}) SimpleBSON {
	raw, err := bson.Marshal(d)
	if err != nil {
		panic(err)
	}
	return SimpleBSON{int32(len(raw)), raw}
}

func (sb SimpleBSON) ToBSOND() (bson.D, error) {
	t := bson.D{}
	err := bson.Unmarshal(sb.BSON, &t)
	return t, err
}

func (sb SimpleBSON) Copy(loc *int, buf []byte) {
	copy(buf[*loc:], sb.BSON)
	*loc = *loc + int(sb.Size)
}

func parseSimpleBSON(b []byte) (SimpleBSON, error) {
	if len(b) < 4 {
		return SimpleBSON{}, NewStackErrorf("invalid bson -- length of bytes must be at least 4, not %v", len(b))
	}
	size := readInt32(b)
	if int(size) == 0 {
		// shortcut in wire protocol
		return SimpleBSON{4, b}, nil
	}

	if int(size) > (128*1024*1024) || int(size) < 0 {
		return SimpleBSON{}, NewStackErrorf("bson size invalid %d", size)
	}

	if int(size) > len(b) {
		return SimpleBSON{}, NewStackErrorf("invalid bson -- size = %v is greater than length of bytes = %v", size, len(b))
	}

	return SimpleBSON{size, b[0:int(size)]}, nil
}

func SimpleBSONEmpty() SimpleBSON {
	return SimpleBSON{int32(5), []byte{5, 0, 0, 0, 0}}
}

// ---------

func BSONIndexOf(doc bson.D, name string) int {
	for i, elem := range doc {
		if elem.Key == name {
			return i
		}
	}
	return -1
}

func GetAsString(elem bson.E) (string, string, error) {
	tipe := fmt.Sprintf("%T", elem.Value)
	switch val := elem.Value.(type) {
	case string:
		return val, tipe, nil
	default:
		return "", tipe, NewStackErrorf("not a string %T %s", val, val)
	}
}

func GetAsInt(elem bson.E) (int, string, error) {
	tipe := fmt.Sprintf("%T", elem.Value)
	switch val := elem.Value.(type) {
	case int:
		return val, tipe, nil
	case int32:
		return int(val), tipe, nil
	case int64:
		return int(val), tipe, nil
	case float64:
		return int(val), tipe, nil
	default:
		return 0, tipe, NewStackErrorf("not a number %T %s", val, val)
	}
}

func GetAsBool(elem bson.E) (bool, string, error) {
	tipe := fmt.Sprintf("%T", elem.Value)
	switch val := elem.Value.(type) {
	case bool:
		return val, tipe, nil
	case int:
		return val != 0, tipe, nil
	case int32:
		return int(val) != 0, tipe, nil
	case int64:
		return int(val) != 0, tipe, nil
	case float64:
		return val != 0.0, tipe, nil
	default:
		return false, tipe, NewStackErrorf("not a bool %T %s", val, val)
	}
}

func GetAsBSON(elem bson.E) (bson.D, string, error) {
	tipe := fmt.Sprintf("%T", elem.Value)
	switch val := elem.Value.(type) {
	case bson.D:
		return val, tipe, nil
	default:
		return bson.D{}, tipe, NewStackErrorf("not bson %T %s", val, val)
	}
}

func getAsBsonDocsArray(val []interface{}, tipe string) ([]bson.D, string, error) {
	a := make([]bson.D, len(val))
	for num, raw := range val {
		switch fixed := raw.(type) {
		case bson.D:
			a[num] = fixed
		default:
			return []bson.D{}, tipe, NewStackErrorf("not bson.D %T %s", raw, raw)
		}
	}
	return a, tipe, nil
}

func GetAsBSONDocs(elem bson.E) ([]bson.D, string, error) {
	tipe := fmt.Sprintf("%T", elem.Value)
	switch val := elem.Value.(type) {
	case []bson.D:
		return val, tipe, nil
	case primitive.A:
		return getAsBsonDocsArray([]interface{}(val), tipe)
	case []interface{}:
		return getAsBsonDocsArray(val, tipe)
	default:
		return []bson.D{}, tipe, NewStackErrorf("not an array %T", elem.Value)
	}
}

// ---

var DELETE_ME = fmt.Errorf("delete_me")

type BSONWalkVisitor interface {
	/**
	change value
	set Name = "" to delete
	*/
	Visit(elem *bson.E) error
}

// BSONWalkAll - recursively walks the BSON doc and applies the visitor when encountering the fieldName
// If delete_me is encountered, it'll return an empty document for that element
func BSONWalkAll(doc bson.D, fieldName string, visitor BSONWalkVisitor) (bson.D, error) {
	current := doc
	for i, elem := range current {
		elemDoc := &(current)[i]
		if elem.Key == fieldName {
			err := visitor.Visit(elemDoc)
			if err != nil {
				if err == DELETE_ME {
					return nil, nil
				}
				return nil, fmt.Errorf("error visiting node %v", err)
			}
		}
		var valToUse []interface{}
		switch val := elem.Value.(type) {
		case bson.D:
			newDoc, err := BSONWalkAll(val, fieldName, visitor)
			if err != nil {
				return nil, fmt.Errorf("error going deeper into doc %v", err)
			}
			elem.Value = newDoc
		case []bson.D:
			for arrayOffset, sub := range val {
				newDoc, err := BSONWalkAll(sub, fieldName, visitor)
				if err != nil {
					return nil, fmt.Errorf("error going deeper into array %v", err)
				}
				val[arrayOffset] = newDoc
			}
		case primitive.A:
			valToUse = []interface{}(val)
		case []interface{}:
			valToUse = val
		}
		if len(valToUse) == 0 {
			continue
		}
		for arrayOffset, subRaw := range valToUse {
			switch sub := subRaw.(type) {
			case bson.D:
				newDoc, err := BSONWalkAll(sub, fieldName, visitor)
				if err != nil {
					return nil, fmt.Errorf("error going deeper into doc %v", err)
				}
				valToUse[arrayOffset] = newDoc
			default:
				// won't alter nested arrays (e.g. [[1,2,3],[4,5,6]]) - will set them as-is
				valToUse[arrayOffset] = sub
			}
		}

	}
	return doc, nil
}

// BSONWalk - applies the visitor on the select path
func BSONWalk(doc bson.D, pathString string, visitor BSONWalkVisitor) (bson.D, error) {
	path := strings.Split(pathString, ".")
	return BSONWalkHelp(doc, path, visitor, false)
}

func BSONWalkHelp(doc bson.D, path []string, visitor BSONWalkVisitor, inArray bool) (bson.D, error) {
	prev := doc
	current := doc

	docPath := []int{}

	for pieceOffset, piece := range path {
		idx := BSONIndexOf(current, piece)
		//fmt.Printf("XX %d %s %d\n", pieceOffset, piece, idx)

		if idx < 0 {
			return doc, nil
		}
		docPath = append(docPath, idx)

		elem := &(current)[idx]

		if pieceOffset == len(path)-1 {
			// this is the end
			if len(elem.Key) == 0 {
				panic("this is not ok right now")
			}
			err := visitor.Visit(elem)
			if err != nil {
				if err == DELETE_ME {
					if inArray {
						return bson.D{}, DELETE_ME
					}
					fixed := append(current[0:idx], current[idx+1:]...)
					if pieceOffset == 0 {
						return fixed, nil
					}

					prev[docPath[len(docPath)-2]].Value = fixed
					return doc, nil
				}

				return nil, fmt.Errorf("error visiting node %s", err)
			}

			return doc, nil
		}

		// more to walk down
		var valToUse []interface{}
		switch val := elem.Value.(type) {
		case bson.D:
			prev = current
			current = val
		case []bson.D:
			numDeleted := 0

			for arrayOffset, sub := range val {
				newDoc, err := BSONWalkHelp(sub, path[pieceOffset+1:], visitor, true)
				if err == DELETE_ME {
					newDoc = nil
					numDeleted++
				} else if err != nil {
					return nil, fmt.Errorf("error going deeper into array %s", err)
				}

				val[arrayOffset] = newDoc
			}

			if numDeleted > 0 {
				newArr := make([]bson.D, len(val)-numDeleted)
				pos := 0
				for _, sub := range val {
					if sub != nil {
						newArr[pos] = sub
						pos++
					}
				}
				current[idx].Value = newArr
			}

			return doc, nil
		case primitive.A:
			valToUse = []interface{}(val)
		case []interface{}:
			valToUse = val
		default:
			fmt.Printf("*** bsonWalk - this is a different type - %v %T\n", val, val)
			return doc, nil
		}
		if len(valToUse) == 0 {
			continue
		}
		numDeleted := 0
		for arrayOffset, subRaw := range valToUse {
			switch sub := subRaw.(type) {
			case bson.D:
				newDoc, err := BSONWalkHelp(sub, path[pieceOffset+1:], visitor, true)
				if err == DELETE_ME {
					newDoc = nil
					numDeleted++
				} else if err != nil {
					return nil, fmt.Errorf("error going deeper into array %s", err)
				}

				valToUse[arrayOffset] = newDoc
			default:
				valToUse[arrayOffset] = sub
			}
		}

		if numDeleted > 0 {
			newArr := make([]interface{}, len(valToUse)-numDeleted)
			pos := 0
			for _, sub := range valToUse {
				if sub != nil && len(sub.(bson.D)) > 0 {
					newArr[pos] = sub
					pos++
				}
			}
			current[idx].Value = newArr
		}
		return doc, nil
	}

	return doc, nil
}
