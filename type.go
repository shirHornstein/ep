package ep

import "fmt"

// Wildcard is a pseudo-type used to denote types that are dependent on their
// input type. For example, a function returning [Wildcard, Int] effectively
// returns its input followed by an int column. It should never be used in the
// datasets themselves, but only in API declaration.
var Wildcard = &wildcardType{}

// WildcardMinusTail returns wildcard pseduo-type as described above, with the
// ability to denote subset of their input type. For example, a function returning
// [WildcardMinusTail(1), Int] effectively returns its input without input's last
// column followed by an int column. It should never be used in the datasets
// themselves, but only in API declaration.
func WildcardMinusTail(n int) Type {
	return &wildcardType{nil, n}
}

// Any is a pseudo-type used to denote unknown or a varied data type that might
// change from batch to batch. The actual batches must be typed (thus be
// concrete Data implementations), as Any is not instantiatable, but no one type
// can be determined ahead of time. Examples are JSON parser functions that may
// produce different data types for each row. Some functions (CAST, COUNT, etc.)
// may be able to support Any as input.
// However, no runner should return type Any, but only concrete type.
var Any = &anyType{}

var _ = registerGob(&modifierType{}, Wildcard, Any)

// Type is an interface that represents specific data types
type Type interface {
	fmt.Stringer

	// Name returns the name of the type
	Name() string

	// Data returns a new Data object of this type, containing `n` zero-values
	Data(n int) Data

	// DataEmpty returns a new empty Data object of this type, with allocated size 'n'
	DataEmpty(n int) Data
}

// AreEqualTypes compares types and returns true if types arrays are deep equal
func AreEqualTypes(ts1, ts2 []Type) bool {
	if len(ts1) != len(ts2) {
		return false // mismatching number of types
	}

	for i, t1 := range ts1 {
		if t1.Name() != ts2[i].Name() && !isAny(t1) && !isAny(ts2[i]) {
			return false // mismatching type name
		}
	}

	return true
}

// see Wildcard above.
type wildcardType struct {
	Idx         *int
	CutFromTail int
}

func (*wildcardType) String() string             { return "*" }
func (*wildcardType) Name() string               { return "*" }
func (*wildcardType) Data(int) Data              { panic("wildcard has no concrete type") }
func (*wildcardType) DataEmpty(int) Data         { panic("wildcard has no concrete type") }
func (w *wildcardType) At(idx int) *wildcardType { return &wildcardType{&idx, w.CutFromTail} }

type anyType struct{}

func (*anyType) String() string     { return "?" }
func (*anyType) Name() string       { return "?" }
func (*anyType) Data(int) Data      { panic("any has no concrete type") }
func (*anyType) DataEmpty(int) Data { panic("any has no concrete type") }
func isAny(t Type) bool {
	return t.Name() == "?"
}

type modifier interface {
	Type
	getModifier(k interface{}) interface{}
}

type modifierType struct {
	Type
	K interface{}
	V interface{}
}

func (t *modifierType) getModifier(k interface{}) interface{} {
	if t.K == k {
		return t.V
	}
	return Modifier(t.Type, k)
}

// Modify returns a new Type that's assigned a key-value pair. Useful for adding
// modifiers/context to types (like type-length, format (binary/text), alias, flags, etc.)
func Modify(t Type, k, v interface{}) Type {
	// we're only casting it here to benefit from compile-time verification that
	// the interface isn't broken
	return modifier(&modifierType{t, k, v})
}

// Modifier is useful for getting pre-added modifiers/context of types, using Modify function
func Modifier(t Type, k interface{}) interface{} {
	modifier, ok := t.(modifier)
	if !ok || modifier == nil {
		return nil
	}

	return modifier.getModifier(k)
}
