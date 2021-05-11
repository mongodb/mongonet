package mongonet

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/go-test/deep"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestBSONIndexOf(test *testing.T) {
	doc := bson.D{{"a", 1}, {"b", 3}}

	if BSONIndexOf(doc, "a") != 0 {
		test.Errorf("index of a is wrong")
	}

	if BSONIndexOf(doc, "b") != 1 {
		test.Errorf("index of b is wrong")
	}

	if BSONIndexOf(doc, "c") != -1 {
		test.Errorf("index of c is wrong")
	}
}

func TestGetAsStringArray(test *testing.T) {
	val := bson.A{"test1", "test2"}
	doc := bson.E{"a", val}
	res, _, _ := GetAsStringArray(doc)
	if len(res) != 2 {
		test.Errorf("result should of length 2, but got %v", len(res))
	}
	if res[0] != "test1" {
		test.Errorf("expected test1, but got %v", res[0])
	}
	if res[1] != "test2" {
		test.Errorf("expected test2, but got %v", res[0])
	}
}

type testWalker struct {
	seen []bson.E
}

func (tw *testWalker) Visit(elem *bson.E) error {
	tw.seen = append(tw.seen, *elem)
	if elem.Value.(int) == 111 {
		return DELETE_ME
	}
	elem.Value = 17
	return nil
}

func TestBSONWalk1(test *testing.T) {
	doc := bson.D{{"a", 1}, {"b", 3}}
	walker := &testWalker{}
	doc, err := BSONWalk(doc, "b", walker)
	if err != nil {
		test.Errorf("why did we get an error %s", err)
	}
	if len(walker.seen) != 1 {
		test.Errorf("wrong # saw")
	}
	if walker.seen[0].Key != "b" {
		test.Errorf("name wrong %s", walker.seen[0].Key)
	}
	if doc[1].Value.(int) != 17 {
		test.Errorf("we didn't change it %d", doc[1].Value.(int))
	}
}

func TestBSONWalk2(test *testing.T) {
	doc := bson.D{{"a", 1}, {"b", 3}, {"c", bson.D{{"x", 5}, {"y", 7}}}}
	walker := &testWalker{}
	doc, err := BSONWalk(doc, "c.y", walker)
	if err != nil {
		test.Errorf("why did we get an error %s", err)
		return
	}
	if len(walker.seen) != 1 {
		test.Errorf("wrong # saw")
		return
	}
	if walker.seen[0].Value != 7 {
		test.Errorf("number wrong %d", walker.seen[0].Value)
	}
	if doc[2].Value.(bson.D)[1].Value.(int) != 17 {
		test.Errorf("we didn't change it")
	}
}

func TestBSONWalk3(test *testing.T) {
	doc := bson.D{{"a", 1}, {"b", 3}, {"c", []bson.D{{{"x", 5}}, {{"x", 7}}}}}
	walker := &testWalker{}
	doc, err := BSONWalk(doc, "c.x", walker)
	if err != nil {
		test.Errorf("why did we get an error %s", err)
		return
	}
	if len(walker.seen) != 2 {
		test.Errorf("wrong # saw %d", len(walker.seen))
		return
	}
	if walker.seen[0].Value != 5 {
		test.Errorf("number wrong %d", walker.seen[0].Value)
	}
	if walker.seen[1].Value != 7 {
		test.Errorf("number wrong %d", walker.seen[1].Value)
	}
}

func TestBSONWalk4(test *testing.T) {
	doc := bson.D{{"a", 1}, {"b", 3}, {"c", []interface{}{bson.D{{"x", 5}}, bson.D{{"x", 7}}}}}
	walker := &testWalker{}
	doc, err := BSONWalk(doc, "c.x", walker)
	if err != nil {
		test.Errorf("why did we get an error %s", err)
		return
	}
	if len(walker.seen) != 2 {
		test.Errorf("wrong # saw %d", len(walker.seen))
		return
	}
	if walker.seen[0].Value != 5 {
		test.Errorf("number wrong %d", walker.seen[0].Value)
	}
	if walker.seen[1].Value != 7 {
		test.Errorf("number wrong %d", walker.seen[1].Value)
	}
}

func TestBSONWalk5(test *testing.T) {
	doc := bson.D{{"a", 1}, {"b", 3}, {"c", []interface{}{bson.D{{"x", 5}}, bson.D{{"x", 111}, {"y", 3}}, bson.D{{"x", 7}}}}}
	walker := &testWalker{}
	doc, err := BSONWalk(doc, "c.x", walker)
	if err != nil {
		test.Errorf("why did we get an error %s", err)
		return
	}
	if len(walker.seen) != 3 {
		test.Errorf("wrong # saw %d", len(walker.seen))
		return
	}
	if walker.seen[0].Value != 5 {
		test.Errorf("number wrong %d", walker.seen[0].Value)
		return
	}
	if walker.seen[2].Value != 7 {
		test.Errorf("number wrong %d", walker.seen[1].Value)
		return
	}
	if len(doc[2].Value.([]interface{})) != 2 {
		test.Errorf("did not remove %s", doc[2])
		return
	}

}

func TestBSONWalk6(test *testing.T) {
	doc := bson.D{{"a", 1}, {"b", 3}, {"c", []bson.D{{{"x", 5}}, {{"x", 111}, {"y", 3}}, {{"x", 7}}}}}
	walker := &testWalker{}
	doc, err := BSONWalk(doc, "c.x", walker)
	if err != nil {
		test.Errorf("why did we get an error %s", err)
		return
	}
	if len(walker.seen) != 3 {
		test.Errorf("wrong # saw %d", len(walker.seen))
		return
	}
	if walker.seen[0].Value != 5 {
		test.Errorf("number wrong %d", walker.seen[0].Value)
		return
	}
	if walker.seen[2].Value != 7 {
		test.Errorf("number wrong %d", walker.seen[1].Value)
		return
	}
	if len(doc[2].Value.([]bson.D)) != 2 {
		test.Errorf("did not remove %s", doc[2])
		return
	}

}

func TestBSONWalk7(test *testing.T) {
	doc := bson.D{{"a", 111}, {"b", 3}}
	walker := &testWalker{}
	doc, err := BSONWalk(doc, "a", walker)
	if err != nil {
		test.Errorf("why did we get an error %s", err)
	}
	if len(doc) != 1 {
		test.Errorf("didn't delete 1 %s", doc)
	}
	if doc[0].Key != "b" {
		test.Errorf("deleted wrong one? %s", doc)
	}

}

func TestBSONWalk8(test *testing.T) {
	doc := bson.D{{"b", 11}, {"a", 111}}
	walker := &testWalker{}
	doc, err := BSONWalk(doc, "a", walker)
	if err != nil {
		test.Errorf("why did we get an error %s", err)
	}
	if len(doc) != 1 {
		test.Errorf("didn't delete 1 %s", doc)
	}
	if doc[0].Key != "b" {
		test.Errorf("deleted wrong one? %s", doc)
	}

}

func TestBSONWalk9(test *testing.T) {
	doc := bson.D{{"b", 11}, {"a", 111}, {"c", 12}}
	walker := &testWalker{}
	doc, err := BSONWalk(doc, "a", walker)
	if err != nil {
		test.Errorf("why did we get an error %s", err)
	}
	if len(doc) != 2 {
		test.Errorf("didn't delete 1 %s", doc)
	}
	if doc[0].Key != "b" {
		test.Errorf("deleted wrong one? %s", doc)
	}
	if doc[1].Key != "c" {
		test.Errorf("deleted wrong one? %s", doc)
	}

}

func TestBSONWalk10(test *testing.T) {
	doc := bson.D{{"b", 11}, {"a", bson.D{{"x", 1}, {"y", 111}}}, {"c", 12}}
	walker := &testWalker{}
	doc, err := BSONWalk(doc, "a.y", walker)
	if err != nil {
		test.Errorf("why did we get an error %s", err)
	}
	if len(doc) != 3 {
		test.Errorf("what did i do! %s", doc)
	}

	if doc[1].Key != "a" {
		test.Errorf("what did i do! %s", doc)
	}

	sub := doc[1].Value.(bson.D)
	if len(sub) != 1 {
		test.Errorf("didn't delete %s", doc)
	}
	if sub[0].Key != "x" {
		test.Errorf("deleted wrong one? %s", doc)
	}

}

func TestBSONWalkAll1(test *testing.T) {
	doc := bson.D{{"a", 10}}
	walker := &testWalker{}
	doc, err := BSONWalkAll(doc, "a", walker)
	if err != nil {
		test.Errorf("why did we get an error %s", err)
	}
	if len(doc) != 1 {
		test.Errorf("incorrect doc length")
	}
	if doc[0].Key != "a" {
		test.Errorf("incorrect doc structure")
	}
	if doc[0].Value != 17 {
		test.Errorf("incorrect revised doc value")
	}
}

func TestBSONWalkAll2(test *testing.T) {
	doc := bson.D{{"a", 10}, {"b", bson.D{{"a", 1}}}}
	walker := &testWalker{}
	doc, err := BSONWalkAll(doc, "a", walker)
	if err != nil {
		test.Errorf("why did we get an error %s", err)
	}
	if len(doc) != 2 {
		test.Errorf("incorrect doc length")
	}
	if doc[0].Key != "a" {
		test.Errorf("incorrect doc structure")
	}
	if doc[0].Value != 17 {
		test.Errorf("incorrect revised doc value")
	}
	sub := doc[1].Value.(bson.D)
	if len(sub) != 1 {
		test.Errorf("incorrect sub-doc structure")
	}
	if sub[0].Value != 17 {
		test.Errorf("incorrect sub-doc value")
	}
}

func TestBSONWalkAll3(test *testing.T) {
	doc := bson.D{{"a", 10}, {"b", bson.D{{"a", 1}}}, {"c", []bson.D{{{"x", 5}}, {{"a", 1}}}}}
	walker := &testWalker{}
	doc, err := BSONWalkAll(doc, "a", walker)
	if err != nil {
		test.Errorf("why did we get an error %s", err)
	}
	if len(doc) != 3 {
		test.Errorf("incorrect doc length")
	}
	if doc[0].Key != "a" {
		test.Errorf("incorrect doc structure")
	}
	if doc[0].Value != 17 {
		test.Errorf("incorrect revised doc value")
	}
	sub := doc[1].Value.(bson.D)
	if len(sub) != 1 {
		test.Errorf("incorrect sub-doc structure")
	}
	if sub[0].Value != 17 {
		test.Errorf("incorrect sub-doc value")
	}
	if len(walker.seen) != 3 {
		test.Errorf("wrong # saw %d", len(walker.seen))
	}
}

func TestBSONWalkAll4(test *testing.T) {
	doc := bson.D{{"a", 1}, {"b", 3}, {"c", []interface{}{bson.D{{"x", 5}}, bson.D{{"a", 2}, {"y", 3}}, bson.D{{"a", 7}}}}}
	walker := &testWalker{}
	doc, err := BSONWalkAll(doc, "a", walker)
	if err != nil {
		test.Errorf("why did we get an error %s", err)
	}
	if len(walker.seen) != 3 {
		test.Errorf("wrong # saw %d", len(walker.seen))
	}
	arr := doc[2].Value.([]interface{})
	val := arr[1].(bson.D)
	if val[0].Value != 17 {
		test.Errorf("incorrect sub-doc value")
	}
	val2 := arr[0].(bson.D)
	if val2[0].Value != 5 {
		test.Errorf("incorrect sub-doc value")
	}
}

func TestBSONWalkAll5(test *testing.T) {
	doc := bson.D{
		{"a", 1},
		{"b", 3},
		{"c", []interface{}{bson.D{{"x", 5}}, bson.D{{"a", 2}, {"y", 3}}, bson.D{{"a", 7}}}},
		{"d", []interface{}{"1", "2"}},
	}
	walker := &testWalker{}
	doc, err := BSONWalkAll(doc, "a", walker)
	if err != nil {
		test.Errorf("why did we get an error %s", err)
	}
	if len(walker.seen) != 3 {
		test.Errorf("wrong # saw %d", len(walker.seen))
	}
	arr := doc[2].Value.([]interface{})
	val := arr[1].(bson.D)
	if val[0].Value != 17 {
		test.Errorf("incorrect sub-doc value")
	}
	val2 := arr[0].(bson.D)
	if val2[0].Value != 5 {
		test.Errorf("incorrect sub-doc value")
	}
}

func TestBSONWalkAll6(test *testing.T) {
	doc := bson.D{
		{"a", 1},
		{"b", 3},
		{"c", []interface{}{bson.D{{"x", 5}}, bson.D{{"a", 2}, {"y", 3}}, bson.D{{"a", 111}}}},
		{"d", []interface{}{"1", "2"}},
	}
	walker := &testWalker{}
	doc, err := BSONWalkAll(doc, "a", walker)
	if err != nil {
		test.Errorf("why did we get an error %s", err)
	}
	if len(walker.seen) != 3 {
		test.Errorf("wrong # saw %d", len(walker.seen))
	}
	arr := doc[2].Value.([]interface{})
	val := arr[1].(bson.D)
	if val[0].Value != 17 {
		test.Errorf("incorrect sub-doc value")
	}
	val2 := arr[0].(bson.D)
	if val2[0].Value != 5 {
		test.Errorf("incorrect sub-doc value")
	}
	val3 := arr[2].(bson.D)
	if len(val3) != 0 {
		test.Errorf("element should've been deleted %s", doc)
	}
}

func docToRaw(doc bson.D) bson.Raw {
	return bson.Raw(SimpleBSONConvertOrPanic(doc).BSON)
}

func TestTranslatePathsWithArrays(t *testing.T) {
	doc := bson.D{
		{"x", 1},
		{"a", primitive.A{}},
	}
	var visitor visitorFunc = func(elem *bson.E) error {
		elem.Value = 2
		return nil
	}

	expected := bson.D{
		{"x", 1},
		{"a", primitive.A{}},
	}
	actual, err := translatePaths(visitor, doc, []string{"a.x"})
	if err != nil {
		t.Fatal(err)
	}

	if deep.Equal(expected, actual) != nil {
		t.Fatalf("expected %v, got %v", docToRaw(expected), docToRaw(actual))
	}
}

// a[].b.c
func TestTranslatePathsWithNestedBsonInArray(t *testing.T) {
	doc := bson.D{
		{"x", 1},
		{"a", primitive.A{
			bson.D{{"b", bson.D{{"c", 1}}}},
		}},
	}
	var visitor visitorFunc = func(elem *bson.E) error {
		elem.Value = 2
		return nil
	}

	expected := bson.D{
		{"x", 1},
		{"a", primitive.A{
			bson.D{{"b", bson.D{{"c", 2}}}},
		}},
	}
	actual, err := translatePaths(visitor, doc, []string{"a.b.c"})
	if err != nil {
		t.Fatal(err)
	}

	if deep.Equal(expected, actual) != nil {
		t.Fatalf("expected %v, got %v", docToRaw(expected), docToRaw(actual))
	}
}

func translatePaths(v BSONWalkVisitor, doc bson.D, path []string) (bson.D, error) {
	var err error
	for _, p := range path {
		doc, err = BSONWalk(doc, p, v)
		if err != nil {
			return doc, err
		}
	}
	return doc, nil
}

func TestBSONGetValueByNestedPathForTests(t *testing.T) {
	doc := bson.D{
		{"x", 1},
		{"a", bson.D{
			{"b", bson.D{{"c", 2}}},
			{"d", primitive.A{bson.D{{"x", 5}}, bson.D{{"x", 2}, {"x", 3}}, bson.D{{"x", 111}}}},
		}},
	}
	v := BSONGetValueByNestedPathForTests(doc, "x", -1)
	if v.(int) != 1 {
		t.Fatalf("expected %v to equal 1", v)
	}
	v = BSONGetValueByNestedPathForTests(doc, "a.b.c", -1)
	if v.(int) != 2 {
		t.Fatalf("expected %v to equal 1", v)
	}
	v = BSONGetValueByNestedPathForTests(doc, "a.d.x", 1)
	if v.(int) != 2 {
		t.Fatalf("expected %v to equal 1", v)
	}
	v = BSONGetValueByNestedPathForTests(doc, "a.b", -1)
	exp := bson.D{{"c", 2}}
	if !reflect.DeepEqual(v, exp) {
		t.Fatalf("expected %v to equal %v", v, exp)
	}
}

// visitorFunc is a function implementation of BSONWalkVisitor
type visitorFunc func(*bson.E) error

var _ BSONWalkVisitor = (visitorFunc)(nil)

func (vf visitorFunc) Visit(elem *bson.E) error {
	return vf(elem)
}

func BenchmarkSimpleBSONConvertEmptyDoc(b *testing.B) {
	b.ReportAllocs()
	doc := bson.D{}
	for i := 0; i < b.N; i++ {
		_, err := SimpleBSONConvert(doc)
		if err != nil {
			b.Error(err)
		}
	}
}
func BenchmarkSimpleBSONConvertSmallDoc(b *testing.B) {
	b.ReportAllocs()
	doc := bson.D{
		{"ok", 1},
	}
	for i := 0; i < b.N; i++ {
		_, err := SimpleBSONConvert(doc)
		if err != nil {
			b.Error(err)
		}
	}
}

func getDocOfSize(n int) bson.D {
	doc := bson.D{}
	for i := 0; i < n; i++ {
		doc = append(doc, bson.E{fmt.Sprintf("field%v", i), "blabla"})
	}
	return doc
}

func BenchmarkSimpleBSONConvertLarge10Doc(b *testing.B) {
	b.ReportAllocs()
	doc := getDocOfSize(10)
	for i := 0; i < b.N; i++ {
		_, err := SimpleBSONConvert(doc)
		if err != nil {
			b.Error(err)
		}
	}
}
func BenchmarkSimpleBSONConvertLarge50Doc(b *testing.B) {
	b.ReportAllocs()
	doc := getDocOfSize(50)
	for i := 0; i < b.N; i++ {
		_, err := SimpleBSONConvert(doc)
		if err != nil {
			b.Error(err)
		}
	}
}
func BenchmarkSimpleBSONConvertLarge100Doc(b *testing.B) {
	b.ReportAllocs()
	doc := getDocOfSize(100)
	for i := 0; i < b.N; i++ {
		_, err := SimpleBSONConvert(doc)
		if err != nil {
			b.Error(err)
		}
	}
}
func BenchmarkSimpleBSONConvertLarge500Doc(b *testing.B) {
	b.ReportAllocs()
	doc := getDocOfSize(500)
	for i := 0; i < b.N; i++ {
		_, err := SimpleBSONConvert(doc)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkSimpleBSONConvertLarge1000Doc(b *testing.B) {
	b.ReportAllocs()
	doc := getDocOfSize(1000)
	for i := 0; i < b.N; i++ {
		_, err := SimpleBSONConvert(doc)
		if err != nil {
			b.Error(err)
		}
	}
}

func getIsMaster() bson.D {
	return bson.D{
		{"ismaster", true},
		{"maxBsonObjectSize", 16777216},
		{"maxMessageSizeBytes", 48000000},
		{"maxWriteBatchSize", 100000},
		{"localTime", time.Now()},
		{"logicalSessionTimeoutMinutes", 30},
		{"minWireVersion", 0},
		{"maxWireVersion", 6},
		{"readOnly", false},
		{"hostsBsonD", primitive.A{
			primitive.E{"host", "blabla1"},
			primitive.E{"host", "blabla2"},
			primitive.E{"host", "blabla3"},
		}},
		{"hostsIf", []interface{}{
			bson.D{{"host", "blabla1"}},
			bson.D{{"host", "blabla2"}},
			bson.D{{"host", "blabla3"}},
		}},
	}
}

func BenchmarkSimpleBSONConvertIsMasterResponse(b *testing.B) {
	b.ReportAllocs()
	doc := getIsMaster()
	for i := 0; i < b.N; i++ {
		_, err := SimpleBSONConvert(doc)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkSimpleBSONConvertIsMasterRequest(b *testing.B) {
	b.ReportAllocs()
	doc := bson.D{
		{"ismaster", 1},
		{"db", "$db"},
	}
	for i := 0; i < b.N; i++ {
		_, err := SimpleBSONConvert(doc)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkSimpleBSONConvertFindOne(b *testing.B) {
	b.ReportAllocs()
	doc := bson.D{
		{"ismaster", true},
		{"maxBsonObjectSize", 16777216},
		{"maxMessageSizeBytes", 48000000},
		{"maxWriteBatchSize", 100000},
		{"localTime", time.Now()},
		{"logicalSessionTimeoutMinutes", 30},
		{"minWireVersion", 0},
		{"maxWireVersion", 6},
		{"readOnly", false},
		{"hostsBsonD", primitive.A{
			primitive.E{"host", "blabla1"},
			primitive.E{"host", "blabla2"},
			primitive.E{"host", "blabla3"},
		}},
		{"hostsIf", []interface{}{
			bson.D{{"host", "blabla1"}},
			bson.D{{"host", "blabla2"}},
			bson.D{{"host", "blabla3"}},
		}},
	}
	for i := 0; i < b.N; i++ {
		_, err := SimpleBSONConvert(doc)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkToBSONEmpty(b *testing.B) {
	b.ReportAllocs()
	doc := bson.D{}
	simple, err := SimpleBSONConvert(doc)
	if err != nil {
		b.Error(err)
	}
	for i := 0; i < b.N; i++ {
		_, err := simple.ToBSOND()
		if err != nil {
			b.Error(err)
		}
	}
}
func BenchmarkToBSONSmall(b *testing.B) {
	b.ReportAllocs()
	doc := bson.D{
		{"ok", 1},
	}
	simple, err := SimpleBSONConvert(doc)
	if err != nil {
		b.Error(err)
	}
	for i := 0; i < b.N; i++ {
		_, err := simple.ToBSOND()
		if err != nil {
			b.Error(err)
		}
	}
}
func BenchmarkToBSONLarge10(b *testing.B) {
	b.ReportAllocs()
	doc := getDocOfSize(10)
	simple, err := SimpleBSONConvert(doc)
	if err != nil {
		b.Error(err)
	}
	for i := 0; i < b.N; i++ {
		_, err := simple.ToBSOND()
		if err != nil {
			b.Error(err)
		}
	}
}
func BenchmarkToBSONLarge50(b *testing.B) {
	b.ReportAllocs()
	doc := getDocOfSize(50)
	simple, err := SimpleBSONConvert(doc)
	if err != nil {
		b.Error(err)
	}
	for i := 0; i < b.N; i++ {
		_, err := simple.ToBSOND()
		if err != nil {
			b.Error(err)
		}
	}
}
func BenchmarkToBSONLarge100(b *testing.B) {
	b.ReportAllocs()
	doc := getDocOfSize(100)
	simple, err := SimpleBSONConvert(doc)
	if err != nil {
		b.Error(err)
	}
	for i := 0; i < b.N; i++ {
		_, err := simple.ToBSOND()
		if err != nil {
			b.Error(err)
		}
	}
}
func BenchmarkToBSONLarge500(b *testing.B) {
	b.ReportAllocs()
	doc := getDocOfSize(500)
	simple, err := SimpleBSONConvert(doc)
	if err != nil {
		b.Error(err)
	}
	for i := 0; i < b.N; i++ {
		_, err := simple.ToBSOND()
		if err != nil {
			b.Error(err)
		}
	}
}
func BenchmarkToBSONLarge1000(b *testing.B) {
	b.ReportAllocs()
	doc := getDocOfSize(1000)
	simple, err := SimpleBSONConvert(doc)
	if err != nil {
		b.Error(err)
	}
	for i := 0; i < b.N; i++ {
		_, err := simple.ToBSOND()
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkToBSONIsMasterResponse(b *testing.B) {
	b.ReportAllocs()
	doc := getIsMaster()
	simple, err := SimpleBSONConvert(doc)
	if err != nil {
		b.Error(err)
	}
	for i := 0; i < b.N; i++ {
		_, err := simple.ToBSOND()
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkToBSONFindOneRequest(b *testing.B) {
	b.ReportAllocs()
	doc := bson.D{
		{"find", "bla"},
		{"$db", "test"},
		{"filter", bson.D{{"b", 1}}},
		{"limit", float64(1)},
		{"singleBatch", true},
		{"lsid", bson.D{
			{"id", primitive.Binary{
				Subtype: uint8(4),
				Data:    []byte("blalblalbalblablalabl"),
			}},
		}},
		{"$clusterTime", bson.D{
			{"clusterTime", primitive.Timestamp{
				T: uint32(1593340459),
				I: uint32(1),
			}},
			{"signature", bson.D{
				{"hash", primitive.Binary{
					Subtype: uint8(4),
					Data:    []byte("blalblalbalblablalablibibibibibibibi"),
				}},
				{"keyId", int64(6843344346754842627)},
			}},
		}},
	}
	simple, err := SimpleBSONConvert(doc)
	if err != nil {
		b.Error(err)
	}
	for i := 0; i < b.N; i++ {
		_, err := simple.ToBSOND()
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkToBSONFindOneResponse(b *testing.B) {
	b.ReportAllocs()
	doc := bson.D{
		{"cursor", bson.D{
			{"id", int64(0)},
			{"ns", "eliot1-bla.test"},
			{"firstBatch", bson.A{
				bson.D{
					{"_id", primitive.NewObjectID()},
					{"a", 1},
				},
			}},
		}},
		{"$db", "test"},
		{"ok", 1},
		{"lsid", bson.D{
			{"id", primitive.Binary{
				Subtype: uint8(4),
				Data:    []byte("blalblalbalblablalabl"),
			}},
		}},
		{"$clusterTime", bson.D{
			{"clusterTime", primitive.Timestamp{
				T: uint32(1593340459),
				I: uint32(1),
			}},
			{"signature", bson.D{
				{"hash", primitive.Binary{
					Subtype: uint8(4),
					Data:    []byte("blalblalbalblablalablibibibibibibibi"),
				}},
				{"keyId", int64(6843344346754842627)},
			}},
		}},
		{"operationTime", primitive.Timestamp{
			T: uint32(1593340459),
			I: uint32(1),
		}},
	}
	simple, err := SimpleBSONConvert(doc)
	if err != nil {
		b.Error(err)
	}
	for i := 0; i < b.N; i++ {
		_, err := simple.ToBSOND()
		if err != nil {
			b.Error(err)
		}
	}
}
