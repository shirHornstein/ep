package ep

var dummy = &dummyType{}
var _ = registerGob(dummy, variadicDummies(-1))

type dummyType struct{}

func (t *dummyType) String() string       { return t.Name() }
func (*dummyType) Name() string           { return "dummy" }
func (*dummyType) Data(n int) Data        { return variadicDummies(n) }
func (t *dummyType) DataEmpty(n int) Data { return variadicDummies(n) }

type variadicDummies int

func (variadicDummies) Type() Type                    { return dummy }
func (vs variadicDummies) Len() int                   { return int(vs) }
func (variadicDummies) Less(int, int) bool            { return false }
func (variadicDummies) Swap(int, int)                 {}
func (variadicDummies) LessOther(int, Data, int) bool { return false }
func (vs variadicDummies) Slice(i, j int) Data        { return vs }
func (vs variadicDummies) Append(data Data) Data      { return vs }
func (vs variadicDummies) Duplicate(t int) Data       { return vs }
func (variadicDummies) IsNull(int) bool               { return true }
func (variadicDummies) MarkNull(int)                  {}
func (variadicDummies) Nulls() []bool                 { return []bool{} }
func (variadicDummies) Equal(data Data) bool {
	_, ok := data.(variadicDummies)
	return ok
}
func (variadicDummies) Copy(Data, int, int) {}
func (variadicDummies) Strings() []string   { return []string{} }
