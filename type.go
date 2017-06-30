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

var _ = registerGob(asType{}, Wildcard, Any)

// Type is an interface that represnts specific data types
type Type interface {
    Name() string

    // Data returns a new Data object of this type, containing `n` zero-values
    Data(n int) Data
}

// see Wildcard above.
type wildcardType struct { Idx *int }
func (*wildcardType) Name() string { return "*" }
func (*wildcardType) Data(int) Data { panic("wildcard has no concrete data") }
func (*wildcardType) At(idx int) *wildcardType { return &wildcardType{&idx} }

type anyType struct {}
func (*anyType) Name() string { return "?" }
func (*anyType) Data(int) Data { panic("any has no concrete data") }

// As returns a new Type that's assigned a name, Useful for cases where the name
// of the data represented by the type matters for later referencing. To fetch
// the name, cast the type to `interface { As() string }`
func As(t Type, name string) Type {
    return &asType{t, name}
}

type asType struct {
    Type
    AsName string
}

func (t *asType) String() string { return t.Type.Name() }
func (t *asType) As() string { return t.AsName }
