package ep

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
	return "?column?" // un-named column
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
