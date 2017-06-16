package ep

// Wildcard is a pseduo-type used to denote types that are dependent on their
// input type. For example, a function returning [Wildcard, Int] effectively
// returns its input followed by an int column. It should never be used in the
// datasets themselves, but only in API declaration.
var Wildcard = &wildcardType{}
var _ = registerGob(asType{}, Wildcard)

// Type is an interface that represnts specific data types
type Type interface {
    Name() string

    // Data returns a new Data object of this type, containing `n` zero-values
    Data(n uint) Data
}

// see Wildcard above.
type wildcardType struct {}
func (*wildcardType) Name() string { return "*" }
func (*wildcardType) Data(uint) Data { panic("wildcard has no concrete data") }

// EqualTypes determines if two types are the same, and returns the first if
// they are, or nil if they aren't.
func EqualType(t1, t2 Type) (t Type, ok bool) {
    ok := t1.Name() == t2.Name()
    if ok {
        return t1, true
    } else {
        return nil, false
    }
}

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

func (t *asType) As() string {
    return t.AsName
}
