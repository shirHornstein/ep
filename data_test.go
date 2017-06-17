package ep

import (
    "sort"
    "fmt"
)

type StrType struct {}
func (*StrType) Name() string { return "string" }
func (*StrType) Data(n uint) Data { return make(Strs, n) }

type Strs []string
func (Strs) Type() Type { return &StrType{} }
func (vs Strs) Len() int { return len(vs) }
func (vs Strs) Less(i, j int) bool { return vs[i] < vs[j] }
func (vs Strs) Swap(i, j int) { vs[i], vs[j] = vs[j], vs[i] }
func (vs Strs) Slice(s, e int) Data { return vs[s:e] }
func (vs Strs) Strings() []string { return vs }
func (vs Strs) Append(o Data) Data { return append(vs, o.(Strs)...) }


func ExampleData() {
    var strs Data = Strs([]string{"hello", "world", "foo", "bar"})
    sort.Sort(strs)
    strs = strs.Slice(0, 2)
    fmt.Println(strs.Strings())
    // Output: [bar foo]
}

func ExampleClone() {
    var strs1 Data = Strs([]string{"hello", "world"})
    strs2 := Clone(strs1).(Strs)
    
    strs2[0] = "foo"
    strs2[1] = "bar"
    fmt.Println(strs2) // clone modified
    fmt.Println(strs1) // original left intact

    // Output:
    // [foo bar]
    // [hello world]
}
