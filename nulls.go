package ep

// Nulls is a Type representing NULL values. Use Nulls.Data(n) to create Data
// instances of `n` nulls
var Null = &nullType{}

type nullType struct {}
func (*nullType) Data(n int) Data { return nulls(n) }
func (*nullType) Name() string { return "NULL" }

// nulls is implemented to satisfy both the Type and Data interfaces
type nulls int // number of nulls in the set
func (nulls) Type() Type { return Null }
func (nulls) Less(int, int) bool { return false }
func (nulls) Swap(int, int) {}
func (nulls) Slice(i, j int) Data { return nulls(j - i) }
func (vs nulls) Append(data Data) Data { return vs + data.(nulls) }
func (vs nulls) Len() int { return int(vs) }
func (vs nulls) Strings() []string { return make([]string, vs) }
