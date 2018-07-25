package ep

// Null is a Type representing NULL values. Use Null.Data(n) to create Data
// instances of `n` nulls
var Null = &nullType{}

var _ = Types.Register("NULL", Null)

type nullType struct{}

func (t *nullType) String() string { return t.Name() }
func (*nullType) Name() string     { return "NULL" }
func (*nullType) Data(n int) Data {
	if n < 0 {
		return variadicNulls{-1}
	}
	return nulls(n)
}
func (t *nullType) DataEmpty(n int) Data { return variadicNulls{-1} }
func (*nullType) Is(t Type) bool {
	return t.Name() == "NULL"
}

type nulls int                              // number of nulls in the set
func (nulls) Type() Type                    { return Null }
func (vs nulls) Len() int                   { return int(vs) }
func (nulls) Less(int, int) bool            { return false }
func (nulls) Swap(int, int)                 {}
func (nulls) LessOther(int, Data, int) bool { return false }
func (nulls) Slice(i, j int) Data           { return nulls(j - i) }
func (vs nulls) Append(data Data) Data      { return vs + data.(nulls) }
func (vs nulls) Duplicate(t int) Data       { return vs * nulls(t) }
func (vs nulls) IsNull(int) bool            { return true }
func (vs nulls) MarkNull(int)               {}
func (vs nulls) Nulls() []bool {
	res := make([]bool, vs.Len())
	for i := range res {
		res[i] = true
	}
	return res
}
func (vs nulls) Equal(data Data) bool {
	d, ok := data.(nulls)
	if !ok {
		return false
	}
	return d == vs
}
func (vs nulls) Copy(Data, int, int) {}
func (vs nulls) Strings() []string   { return make([]string, vs) }

// variadicNulls inherits nulls to allow nulls with flexible length
type variadicNulls struct{ nulls }

func (variadicNulls) Len() int                 { return -1 }
func (vs variadicNulls) Append(data Data) Data { return vs }
func (variadicNulls) Strings() []string        { return []string{} }
func (vs variadicNulls) Nulls() []bool         { return []bool{} }
func (variadicNulls) Equal(data Data) bool {
	_, ok := data.(variadicNulls)
	return ok
}
