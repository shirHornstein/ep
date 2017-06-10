package ep

// Type is an interface that represnts specific data types
type Type interface {
    Name() string
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
