package ep

import (
	"fmt"
	"sort"
)

var str = &strType{}

type strType struct{}

func (*strType) Name() string         { return "string" }
func (s *strType) String() string     { return s.Name() }
func (*strType) Data(n int) Data      { return make(strs, n) }
func (*strType) DataEmpty(n int) Data { return make(strs, 0, n) }

type strs []string

func (strs) Type() Type             { return str }
func (vs strs) Len() int            { return len(vs) }
func (vs strs) Less(i, j int) bool  { return vs[i] < vs[j] }
func (vs strs) Swap(i, j int)       { vs[i], vs[j] = vs[j], vs[i] }
func (vs strs) Slice(s, e int) Data { return vs[s:e] }
func (vs strs) Strings() []string   { return vs }
func (vs strs) Append(o Data) Data  { return append(vs, o.(strs)...) }
func (vs strs) Duplicate(t int) Data {
	ans := make(strs, 0, vs.Len()*t)
	for i := 0; i < t; i++ {
		ans = append(ans, vs...)
	}
	return ans
}

func ExampleData() {
	var strs Data = strs([]string{"hello", "world", "foo", "bar"})
	sort.Sort(strs)
	strs = strs.Slice(0, 2)
	fmt.Println(strs.Strings()) // [bar foo]

	// Output: [bar foo]
}
