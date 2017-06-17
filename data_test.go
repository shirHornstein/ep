package ep

type intType struct {}
func (*intType) Name() string { return "int4" }
func (*intType) Data(n uint) ep.Data { return make(Ints, n) }

type Ints []int
func (Ints) Type() ep.Type { return &intType{} }
func (vs Ints) Len() int { return len(vs) }
func (vs Ints) Swap(i, j int) { vs[i], vs[j] = vs[j], vs[i] }
func (vs Ints) Less(i, j int) bool { return vs[i] < vs[j] }
func (vs Ints) Slice(start, end int) ep.Data { return Ints(vs[start:end]) }

func (vs Ints) Append(data ep.Data) ep.Data {
    return append(vs, data.(Ints)...)
}

func (vs Ints) Strings() []string {
    s := make([]string, len(vs))
    for i, d := range vs {
        s[i] = strconv.Itoa(d)
    }
    return s
}

func ExampleData() {

}
