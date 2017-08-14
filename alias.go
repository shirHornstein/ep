package ep

func SetAlias(col Type, alias string) Type {
	return Modify(col, "Alias", alias)
}
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

type Scope struct {
	Runner
	Label string
}

func (a Scope) Returns() []Type {
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
