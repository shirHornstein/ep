package ep

var _ = registerGob(&Alias{})
var _ = registerGob(&Scope{})

// UnnamedColumn used as default name for columns without an alias
const UnnamedColumn = "?column?"

// Alias wraps internal runner's single return type with alias
type Alias struct {
	Runner
	Label string
}

// Returns implements ep.Runner
func (a *Alias) Returns() []Type {
	inpTypes := a.Runner.Returns()
	if len(inpTypes) == 1 {
		return []Type{SetAlias(inpTypes[0], a.Label)}
	}
	panic("Invalid usage of alias. Consider use scope")
}

// SetAlias sets an alias for the given typed column
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

// Scope wraps internal runner with scope alias
type Scope struct {
	Runner
	Label string
}

// Returns implements ep.Runner
func (a *Scope) Returns() []Type {
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
