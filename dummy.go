package ep

import "github.com/panoplyio/ep/compare"

var dummy = &dummyType{}
var _ = registerGob(dummy, &variadicDummies{})

type dummyType struct{}

func (t *dummyType) String() string     { return t.Name() }
func (*dummyType) Name() string         { return "dummy" }
func (*dummyType) Size() uint           { return 0 }
func (*dummyType) Data(n int) Data      { return &variadicDummies{} }
func (*dummyType) DataEmpty(n int) Data { return &variadicDummies{} }

type variadicDummies struct{}

func (*variadicDummies) Type() Type                    { return dummy }
func (*variadicDummies) Len() int                      { return -1 }
func (*variadicDummies) Less(int, int) bool            { return false }
func (*variadicDummies) Swap(int, int)                 {}
func (*variadicDummies) LessOther(int, Data, int) bool { return false }
func (vs *variadicDummies) Slice(i, j int) Data        { return vs }
func (vs *variadicDummies) Append(data Data) Data      { return vs }
func (vs *variadicDummies) Duplicate(t int) Data       { return vs }
func (*variadicDummies) IsNull(int) bool               { return true }
func (*variadicDummies) MarkNull(int)                  {}
func (*variadicDummies) Nulls() []bool                 { return []bool{} }
func (*variadicDummies) Equal(data Data) bool {
	_, ok := data.(*variadicDummies)
	return ok
}

// Compare implements ep.Data
func (vs *variadicDummies) Compare(other Data) ([]compare.Result, error) {
	otherData := other.(*variadicDummies)
	res := make([]compare.Result, vs.Len())
	for i := 0; i < vs.Len(); i++ {
		switch {
		case vs.IsNull(i) && otherData.IsNull(i):
			res[i] = compare.BothNulls
		case vs.IsNull(i) || otherData.IsNull(i):
			res[i] = compare.Null
		case vs.Equal(otherData):
			res[i] = compare.Equal
		}
	}
	return res, nil
}

func (*variadicDummies) Copy(Data, int, int) {}
func (*variadicDummies) Strings() []string   { return []string{} }
