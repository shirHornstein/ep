package ep

var _ = registerGob(&alias{})
var _ = registerGob(&scope{})

// UnnamedColumn used as default name for columns without an alias
const UnnamedColumn = "?column?"

// Alias wraps given runner's single return type with given alias.
// Useful for planners that need to externally wrap a runner with alias
// see Aliasing and Scoping
func Alias(r Runner, label string) Runner {
	if cmp, ok := r.(*compose); ok {
		cmp.Ts[0] = SetAlias(cmp.Ts[0], label)
		return r
	}
	return &alias{r, label}
}

type alias struct {
	Runner
	Label string
}

func (a *alias) Push(conds ScopesRunner) bool {
	return a.Runner.(PushRunner).Push(conds)
}

func (a *alias) Scopes() map[string]bool {
	if r, ok := a.Runner.(ScopesRunner); ok {
		return r.Scopes()
	}
	return map[string]bool{}
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
	if cmp, ok := r.(*compose); ok {
		cmp.Ts = SetScope(cmp.Ts, label)
		return r
	}
	return &scope{r, label}
}

type scope struct {
	Runner
	Label string
}

func (s *scope) Push(conds ScopesRunner) bool {
	return s.Runner.(PushRunner).Push(conds)
}

func (s *scope) Scopes() map[string]bool {
	return map[string]bool{s.Label: true}
}

// Returns implements ep.Runner
func (s *scope) Returns() []Type {
	inpTypes := s.Runner.Returns()
	return SetScope(inpTypes, s.Label)
}

// Filter implements ep.FilterRunner
func (s *scope) Filter(keep []bool) {
	if f, isFilterable := s.Runner.(FilterRunner); isFilterable {
		f.Filter(keep)
	}
}

// SetScope sets a scope for the given columns
func SetScope(cols []Type, scope string) []Type {
	if scope == "" {
		return cols
	}
	types := make([]Type, len(cols))
	for i := 0; i < len(cols); i++ {
		types[i] = Modify(cols[i], "Scope", scope)
	}
	return types
}

// GetScope returns the scope alias of the given typed column
func GetScope(col Type) string {
	scope, ok := Modifier(col, "Scope").(string)
	if ok {
		return scope
	}
	return ""
}

func IsScoped(scopes, other map[string]bool) bool {
	isScoped := true
	for s, val := range other {
		if !val {
			continue
		}
		isScoped = isScoped && scopes[s]
	}
	return isScoped && len(other) != 0
}
