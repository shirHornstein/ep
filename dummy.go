package ep

import "github.com/panoplyio/ep/compare"

var dummy = &dummyType{}
var dummyData = &variadicDummies{}
var dummyBuilder = &dummyDataBuilder{}
var _ = registerGob(dummy, dummyData)

type dummyType struct{}

func (t *dummyType) String() string     { return t.Name() }
func (*dummyType) Name() string         { return "dummy" }
func (*dummyType) Size() uint           { return 0 }
func (*dummyType) Data(int) Data        { return dummyData }
func (*dummyType) Builder() DataBuilder { return dummyBuilder }

type dummyDataBuilder struct{}

func (*dummyDataBuilder) Append(data Data) {}
func (*dummyDataBuilder) Data() Data       { return dummyData }

type variadicDummies struct{}

func (*variadicDummies) Type() Type                    { return dummy }
func (*variadicDummies) Len() int                      { return -1 }
func (*variadicDummies) Less(int, int) bool            { return false }
func (*variadicDummies) Swap(int, int)                 {}
func (*variadicDummies) LessOther(int, Data, int) bool { return false }
func (vs *variadicDummies) Slice(int, int) Data        { return vs }
func (vs *variadicDummies) Duplicate(int) Data         { return vs }
func (*variadicDummies) IsNull(int) bool               { return true }
func (*variadicDummies) MarkNull(int)                  {}
func (*variadicDummies) Nulls() []bool                 { return []bool{} }
func (*variadicDummies) Equal(data Data) bool {
	// note we must use cast and not compare to singleton to handle
	// distributed dummies data
	_, ok := data.(*variadicDummies)
	return ok
}

// Compare implements ep.Data
func (vs *variadicDummies) Compare(other Data) ([]compare.Result, error) {
	panic("dummies are not comparable")
}

func (*variadicDummies) Copy(Data, int, int) {}
func (*variadicDummies) Strings() []string   { return []string{} }
