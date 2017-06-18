package ep

// Registry is a utility object that stores Types and Runners by an arbitrary
// key argument. Its purpose is to destignate a lookup key used for finding the
// relevant Runner or Type to be used. You can think of it like a key-value
// store with the caveat that - if it's not a string - the key is first
// reflected to the type name of the object, which is then used instead. This
// means that we can register and use key objects, instead of simple strings.
//
// For example, if we're using SQL to construct Runners, we can bind an AST
// node:
//
//      Registry.Runners(ast.SelectStmt{}, &SelectRunner{})
//      Register.Runners(ast.SelectStmt{}) // returns &SelectRunner{}
//
type Registry interface {

    // Type registers and returns a list of Types, marked by a key. Calling this
    // function without any types will just return all of the registered types
    Types(k interface{}, types ...Type) []Type

    // Runner registers and returns a list of Runners, marked by a key. Calling
    // this function without any Runners will just return all of the registered
    // runners.
    Runners(k interface{}, runners ...Runner) []Runner
}
