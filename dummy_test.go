package ep

import (
	"testing"
)

type variadicDummiesOldVersion struct{}

func (variadicDummiesOldVersion) Type() Type                    { return dummy }
func (vs variadicDummiesOldVersion) Len() int                   { return -1 }
func (variadicDummiesOldVersion) Less(int, int) bool            { return false }
func (variadicDummiesOldVersion) Swap(int, int)                 {}
func (variadicDummiesOldVersion) LessOther(int, Data, int) bool { return false }
func (vs variadicDummiesOldVersion) Slice(i, j int) Data        { return vs }
func (vs variadicDummiesOldVersion) Append(data Data) Data      { return vs }
func (vs variadicDummiesOldVersion) Duplicate(t int) Data       { return vs }
func (variadicDummiesOldVersion) IsNull(int) bool               { return true }
func (variadicDummiesOldVersion) MarkNull(int)                  {}
func (variadicDummiesOldVersion) Nulls() []bool                 { return []bool{} }
func (variadicDummiesOldVersion) Equal(data Data) bool {
	_, ok := data.(variadicDummiesOldVersion)
	return ok
}
func (variadicDummiesOldVersion) Copy(Data, int, int) {}
func (variadicDummiesOldVersion) Strings() []string   { return []string{} }

func BenchmarkBefore_structReceiver(b *testing.B) {
	v := variadicDummiesOldVersion{}
	var d Data = v
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		d.Copy(nil, 0, 0)
	}
}

func BenchmarkAfter_pointerReceiver(b *testing.B) {
	v := &variadicDummies{}
	var d Data = v
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		d.Copy(nil, 0, 0)
	}
}
