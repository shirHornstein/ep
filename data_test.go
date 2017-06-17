package ep

type strType struct {}
func (*strType) Name() string { return "str" }
func (*strType) Data(n uint) Data { return make(Strs, n) }

type Strs []string
func (Strs) Type() Type { return &strType{} }
func (vs Strs) Len() int { return len(vs) }
func (vs Strs) Swap(i, j int) { vs[i], vs[j] = vs[j], vs[i] }
func (vs Strs) Less(i, j int) bool { return vs[i] < vs[j] }
func (vs Strs) Slice(start, end int) Data { return vs[start:end] }
func (vs Strs) Append(data Data) Data { return append(vs, data.(Strs)...) }
func (vs Strs) Strings() []string { return vs }

func ExampleData() {

}
