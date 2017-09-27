package ep

// Wildcard is a pseduo-type used to denote types that are dependent on their
// input type. For example, a function returning [Wildcard, Int] effectively
// returns its input followed by an int column. It should never be used in the
// datasets themselves, but only in API declaration.
var Wildcard = &wildcardType{}

// Any is a pseduo-type used to denote unknown or a varied data type that might
// change from batch to batch. The actual batches must be typed (thus be
// concrete Data implementations), as Any is not instantiatable, but no one type
// can be determined ahead of time. Examples are JSON parser functions that may
// produce different data types for each row. Some functions (CAST, COUNT, etc.)
// may be able to support Any as input.
var Any = &anyType{}

var _ = registerGob(&modifierType{}, Wildcard, Any)

// Type is an interface that represents specific data types
type Type interface {
	Name() string

	// Data returns a new Data object of this type, containing `n` zero-values
	Data(n int) Data
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
type wildcardType struct{ Idx *int }

func (*wildcardType) String() string           { return "*" }
func (*wildcardType) Name() string             { return "*" }
func (*wildcardType) Data(int) Data            { panic("wildcard has no concrete data") }
func (*wildcardType) At(idx int) *wildcardType { return &wildcardType{&idx} }

type anyType struct{}

func (*anyType) String() string { return "?" }
func (*anyType) Name() string   { return "?" }
func (*anyType) Data(int) Data  { panic("any has no concrete data") }
func isAny(t Type) bool {
	return t.Name() == "?"
}

// Modifier returns a new Type that's assigned a key-value pair:
//
//  type modifier interface {
//      Modifier(k interface) interface{}
//  }

// Modify is useful for adding more modifiers/context to types, like type-length,
// format (binary/text), alias, flags, etc.
func Modify(t Type, k, v interface{}) Type {
	// we're only casting it here to benefit from compile-time verification that
	// the interface isn't broken
	return modifier(&modifierType{t, k, v})
}

type modifierType struct {
	Type
	K interface{}
	V interface{}
}

func (t *modifierType) Modifier(k interface{}) interface{} {
	if t.K == k {
		return t.V
	}

	modifier, ok := t.Type.(interface {
		Modifier(k interface{}) interface{}
	})
	if ok && modifier != nil {
		return modifier.Modifier(k)
	}

	return nil
}

type modifier interface {
	Type
	Modifier(k interface{}) interface{}
}
