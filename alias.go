package ep

var _ = registerGob(&alias{})
var _ = registerGob(&scope{})

// UnnamedColumn used as default name for columns without an alias
const UnnamedColumn = "?column?"

// NewAlias wraps given runner's single return type with given alias.
// Useful for planners that need to externally wrap a runner with alias
// see Aliasing and Scoping
func NewAlias(r Runner, label string) Runner {
	return &alias{r, label}
}

type alias struct {
	Runner
	Label string
}

// Returns implements ep.Runner
func (a *alias) Returns() []Type {
	inpTypes := a.Runner.Returns()
	if len(inpTypes) == 1 {
		return []Type{SetAlias(inpTypes[0], a.Label)}
	}
	panic("Invalid usage of alias. Consider use scope")
}

// SetAlias sets an alias for the given typed column.
// Useful for runner that need aliasing each column internally
func SetAlias(col Type, alias string) Type {
	return Modify(col, "Alias", alias)
}

// GetAlias returns the alias of the given typed column
func GetAlias(col Type) string {
	m, ok := col.(modifier)
	if ok {
		alias, ok := m.Modifier("Alias").(string)
		if ok {
			return alias
		}
	}
	return ""
}

// NewScope wraps given runner with scope alias to allow runner aliasing.
// Useful to mark all returned columns with runner alias by planners that need
// to externally wrap a runner with scope
// see Aliasing and Scoping
func NewScope(r Runner, label string) Runner {
	return &scope{r, label}
}

type scope struct {
	Runner
	Label string
}

// Returns implements ep.Runner
func (a *scope) Returns() []Type {
	inpTypes := a.Runner.Returns()
	outTypes := make([]Type, len(inpTypes))
	for i, t := range inpTypes {
		if a.Label != "" {
			t = Modify(t, "Scope", a.Label)
		}
		outTypes[i] = t
	}
	return outTypes
}

// GetScope returns the scope alias of the given typed column
func GetScope(col Type) string {
	m, ok := col.(modifier)
	if ok {
		alias, ok := m.Modifier("Scope").(string)
		if ok {
			return alias
		}
	}
	return ""
}
