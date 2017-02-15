package mongonet

import "fmt"
import "strings"

import "gopkg.in/mgo.v2/bson"

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

	if int(size) > (128 * 1024 * 1024) {
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
		if elem.Name == name {
			return i
		}
	}
	return -1
}

func GetAsString(elem bson.DocElem) (string, error) {
	switch val := elem.Value.(type) {
	case string:
		return val, nil
	default:
		return "", NewStackErrorf("not a string %T %s", val, val)
	}
}

func GetAsInt(elem bson.DocElem) (int, error) {
	switch val := elem.Value.(type) {
	case int:
		return val, nil
	case int32:
		return int(val), nil
	case int64:
		return int(val), nil
	case float64:
		return int(val), nil
	default:
		return 0, NewStackErrorf("not a number %T %s", val, val)
	}
}

func GetAsBool(elem bson.DocElem) (bool, error) {
	switch val := elem.Value.(type) {
	case bool:
		return val, nil
	case int:
		return val != 0, nil
	case int32:
		return int(val) != 0, nil
	case int64:
		return int(val) != 0, nil
	case float64:
		return val != 0.0, nil
	default:
		return false, NewStackErrorf("not a bool %T %s", val, val)
	}
}

func GetAsBSON(elem bson.DocElem) (bson.D, error) {
	switch val := elem.Value.(type) {
	case bson.D:
		return val, nil
	default:
		return bson.D{}, NewStackErrorf("not bson %T %s", val, val)
	}
}

func GetAsBSONDocs(elem bson.DocElem) ([]bson.D, error) {
	switch val := elem.Value.(type) {
	case []bson.D:
		return val, nil

	case []interface{}:
		a := make([]bson.D, len(val))
		for num, raw := range val {
			switch fixed := raw.(type) {
			case bson.D:
				a[num] = fixed
			default:
				return []bson.D{}, NewStackErrorf("not bson.D %T %s", raw, raw)
			}
		}
		return a, nil

	default:
		return []bson.D{}, NewStackErrorf("not an array %T", elem.Value)
	}
}

// ---

type BSONWalkVisitor interface {
	/**
	change value
	set Name = "" to delete
	*/
	Visit(elem *bson.DocElem) error
}

func BSONWalk(doc bson.D, pathString string, visitor BSONWalkVisitor) (bson.D, error) {
	path := strings.Split(pathString, ".")
	return BSONWalkHelp(doc, path, visitor, false)
}

var DELETE_ME = fmt.Errorf("delete_me")

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
			if len(elem.Name) == 0 {
				panic("this is not ok right now")
			}
			err := visitor.Visit(elem)
			if err != nil {
				if err == DELETE_ME {
					if inArray {
						return bson.D{}, DELETE_ME
					} else {
						fixed := append(current[0:idx], current[idx+1:]...)
						if pieceOffset == 0 {
							return fixed, nil
						}

						prev[docPath[len(docPath)-2]].Value = fixed
						return doc, nil
					}
				}

				return nil, fmt.Errorf("error visiting node %s", err)
			}

			return doc, nil
		}

		// more to walk down

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
		case []interface{}:
			numDeleted := 0

			for arrayOffset, subRaw := range val {

				switch sub := subRaw.(type) {
				case bson.D:
					newDoc, err := BSONWalkHelp(sub, path[pieceOffset+1:], visitor, true)
					if err == DELETE_ME {
						newDoc = nil
						numDeleted++
					} else if err != nil {
						return nil, fmt.Errorf("error going deeper into array %s", err)
					}

					val[arrayOffset] = newDoc
				default:
					return nil, fmt.Errorf("bad type going deeper into array %s", sub)
				}
			}

			if numDeleted > 0 {
				newArr := make([]interface{}, len(val)-numDeleted)
				pos := 0
				for _, sub := range val {
					if sub != nil && len(sub.(bson.D)) > 0 {
						newArr[pos] = sub
						pos++
					}
				}
				current[idx].Value = newArr
			}

			return doc, nil
		default:
			//fmt.Printf("hi %#v\n", elem.Value)
			return doc, nil
		}
	}

	return doc, nil
}
