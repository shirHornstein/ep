package ep

type intType struct {}
func (*intType) Name() string { return "int4" }
func (*intType) Data(n uint) Data { return make(Ints, n) }

type Ints []int
func (Ints) Type() Type { return &intType{} }
func (vs Ints) Len() int { return len(vs) }
func (vs Ints) Swap(i, j int) { vs[i], vs[j] = vs[j], vs[i] }
func (vs Ints) Less(i, j int) bool { return vs[i] < vs[j] }
func (vs Ints) Slice(start, end int) Data { return Ints(vs[start:end]) }
func (vs Ints) Append(data Data) Data { return append(vs, data.(Ints)...) }
func (vs Ints) Strings() []string {
    s := make([]string, len(vs))
    for i, d := range vs {
        s[i] = strconv.Itoa(d)
    }
    return s
}

func ExampleData() {

}
