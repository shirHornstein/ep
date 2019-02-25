package ep_test

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/compare"
	"sort"
	"strconv"
)

var _ = ep.Types.
	Register("string", str).
	Register("integer", integer)

var str = &strType{}
var integer = &integerType{}

type strType struct{}

func (s *strType) String() string        { return s.Name() }
func (*strType) Name() string            { return "string" }
func (*strType) Size() uint              { return 8 }
func (*strType) Data(n int) ep.Data      { return make(strs, n) }
func (*strType) DataEmpty(n int) ep.Data { return make(strs, 0, n) }
func (*strType) Builder() ep.DataBuilder {
	return &strBuilder{}
}

type strBuilder struct {
	ds  []strs
	len int
}

func (b *strBuilder) Append(data ep.Data) {
	strData := data.(strs)
	b.ds = append(b.ds, strData)
	b.len += strData.Len()
}

func (b *strBuilder) Data() ep.Data {
	res := make(strs, 0, b.len)
	for _, d := range b.ds {
		res = append(res, d...)
	}
	return res
}

type strs []string

func (strs) Type() ep.Type         { return str }
func (vs strs) Len() int           { return len(vs) }
func (vs strs) Less(i, j int) bool { return vs[i] < vs[j] }
func (vs strs) Swap(i, j int)      { vs[i], vs[j] = vs[j], vs[i] }
func (vs strs) LessOther(thisRow int, other ep.Data, otherRow int) bool {
	data := other.(strs)
	return vs[thisRow] < data[otherRow]
}
func (vs strs) Slice(s, e int) ep.Data { return vs[s:e] }
func (vs strs) Duplicate(t int) ep.Data {
	ans := make(strs, 0, vs.Len()*t)
	for i := 0; i < t; i++ {
		ans = append(ans, vs...)
	}
	return ans
}
func (vs strs) IsNull(i int) bool { return vs[i] == "" }
func (vs strs) MarkNull(i int)    {}
func (vs strs) Nulls() []bool     { return make([]bool, vs.Len()) }
func (vs strs) Equal(other ep.Data) bool {
	// for efficiency - avoid reflection and check address of underlying arrays
	return fmt.Sprintf("%p", vs) == fmt.Sprintf("%p", other)
}

func (vs strs) Compare(other ep.Data) ([]compare.Result, error) {
	otherData := other.(strs)
	res := make([]compare.Result, vs.Len())
	for i := 0; i < vs.Len(); i++ {
		switch {
		case vs.IsNull(i) && otherData.IsNull(i):
			res[i] = compare.BothNulls
		case vs.IsNull(i) || otherData.IsNull(i):
			res[i] = compare.Null
		case vs[i] == otherData[i]:
			res[i] = compare.Equal
		case vs[i] > otherData[i]:
			res[i] = compare.Greater
		case vs[i] < otherData[i]:
			res[i] = compare.Less
		}
	}
	return res, nil
}

func (vs strs) Copy(from ep.Data, fromRow, toRow int) {
	src := from.(strs)
	vs[toRow] = src[fromRow]
}
func (vs strs) Strings() []string { return vs }

type integerType struct{}

func (s *integerType) String() string        { return s.Name() }
func (*integerType) Name() string            { return "integer" }
func (*integerType) Size() uint              { return 4 }
func (*integerType) Data(n int) ep.Data      { return make(integers, n) }
func (*integerType) DataEmpty(n int) ep.Data { return make(integers, 0, n) }
func (*integerType) Builder() ep.DataBuilder {
	return &integerBuilder{}
}

type integerBuilder struct {
	ds  []integers
	len int
}

func (b *integerBuilder) Append(data ep.Data) {
	intData := data.(integers)
	b.ds = append(b.ds, intData)
	b.len += intData.Len()
}

func (b *integerBuilder) Data() ep.Data {
	res := make(integers, 0, b.len)
	for _, d := range b.ds {
		res = append(res, d...)
	}
	return res
}

type integers []int

func (integers) Type() ep.Type         { return integer }
func (vs integers) Len() int           { return len(vs) }
func (vs integers) Less(i, j int) bool { return vs[i] < vs[j] }
func (vs integers) Swap(i, j int)      { vs[i], vs[j] = vs[j], vs[i] }
func (vs integers) LessOther(thisRow int, other ep.Data, otherRow int) bool {
	data := other.(integers)
	return vs[thisRow] < data[otherRow]
}
func (vs integers) Slice(s, e int) ep.Data { return vs[s:e] }
func (vs integers) Duplicate(t int) ep.Data {
	ans := make(integers, 0, vs.Len()*t)
	for i := 0; i < t; i++ {
		ans = append(ans, vs...)
	}
	return ans
}
func (vs integers) IsNull(i int) bool { return false }
func (vs integers) MarkNull(i int)    {}
func (vs integers) Nulls() []bool     { return make([]bool, vs.Len()) }
func (vs integers) Equal(other ep.Data) bool {
	// for efficiency - avoid reflection and check address of underlying arrays
	return fmt.Sprintf("%p", vs) == fmt.Sprintf("%p", other)
}

func (vs integers) Compare(other ep.Data) ([]compare.Result, error) {
	otherData := other.(integers)
	res := make([]compare.Result, vs.Len())
	for i := 0; i < vs.Len(); i++ {
		switch {
		case vs.IsNull(i) && otherData.IsNull(i):
			res[i] = compare.BothNulls
		case vs.IsNull(i) || otherData.IsNull(i):
			res[i] = compare.Null
		case vs[i] == otherData[i]:
			res[i] = compare.Equal
		case vs[i] > otherData[i]:
			res[i] = compare.Greater
		case vs[i] < otherData[i]:
			res[i] = compare.Less
		}
	}
	return res, nil
}

func (vs integers) Copy(from ep.Data, fromRow, toRow int) {
	src := from.(integers)
	vs[toRow] = src[fromRow]
}
func (vs integers) Strings() []string {
	s := make([]string, vs.Len())
	for i, v := range vs {
		s[i] = strconv.Itoa(v)
	}
	return s
}

func ExampleData() {
	var strs ep.Data = strs([]string{"hello", "world", "foo", "bar"})
	sort.Sort(strs)
	strs = strs.Slice(0, 2)
	fmt.Println(strs.Strings())

	// Output: [bar foo]
}

func ExampleData_Compare_strings() {
	d1 := strs([]string{"a", "a@a.a", "bb", "c8", "x", " ", "", " "})
	d2 := strs([]string{"a", "a", "b", "c9", "", "x", "", " "})
	comparisonResult, _ := d1.Compare(d2)
	fmt.Println(comparisonResult)

	// Output: [1 4 4 5 3 5 2 1]
}
func ExampleData_Compare_integers() {
	d1 := integers{1, 2, 5}
	d2 := integers{1, 3, 4}
	comparisonResult, _ := d1.Compare(d2)
	fmt.Println(comparisonResult)

	// Output: [1 5 4]
}
