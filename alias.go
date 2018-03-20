package ep

import "fmt"

var _ = registerGob(&alias{})
var _ = registerGob(&scope{})

// UnnamedColumn used as default name for columns without an alias
const UnnamedColumn = "?column?"

// Alias wraps given runner's single return type with given alias.
// Useful for planners that need to externally wrap a runner with alias
// see Aliasing and Scoping
func Alias(r Runner, label string) Runner {
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

// Filter implements ep.FilterRunner
func (a *alias) Filter(keep []bool) {
	if f, isFilterable := a.Runner.(FilterRunner); isFilterable {
		f.Filter(keep)
	} else {
		// TODO remove: temporary print for detecting non filterable runners
		fmt.Printf("WARN: can't filter alias: %T\n", a.Runner)
	}
}

// SetAlias sets an alias for the given typed column.
// Useful for runner that need aliasing each column internally
func SetAlias(col Type, alias string) Type {
	return Modify(col, "Alias", alias)
}

// GetAlias returns the alias of the given typed column
func GetAlias(col Type) string {
	alias, ok := Modifier(col, "Alias").(string)
	if ok {
		return alias
	}
	return ""
}

// Scope wraps given runner with scope alias to allow runner aliasing.
// Useful to mark all returned columns with runner alias by planners that need
// to externally wrap a runner with scope
// see Aliasing and Scoping
func Scope(r Runner, label string) Runner {
	return &scope{r, label}
}

type scope struct {
	Runner
	Label string
}

// Returns implements ep.Runner
func (s *scope) Returns() []Type {
	inpTypes := s.Runner.Returns()
	outTypes := make([]Type, len(inpTypes))
	for i, t := range inpTypes {
		if s.Label != "" {
			t = Modify(t, "Scope", s.Label)
		}
		outTypes[i] = t
	}
	return outTypes
}

// Filter implements ep.FilterRunner
func (s *scope) Filter(keep []bool) {
	if f, isFilterable := s.Runner.(FilterRunner); isFilterable {
		f.Filter(keep)
	} else {
		// TODO remove: temporary print for detecting non filterable runners
		fmt.Printf("WARN: can't filter scope: %T\n", s.Runner)
	}
}

// GetScope returns the scope alias of the given typed column
func GetScope(col Type) string {
	scope, ok := Modifier(col, "Scope").(string)
	if ok {
		return scope
	}
	return ""
}
