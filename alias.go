package ep

var _ = registerGob(&alias{})
var _ = registerGob(&scope{})

// UnnamedColumn used as default name for columns without an alias
const UnnamedColumn = "?column?"

type AliasSetter interface {
	SetAlias(name string)
}

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

func (a *alias) Scopes() StringsSet {
	if r, ok := a.Runner.(ScopesRunner); ok {
		return r.Scopes()
	}
	return StringsSet{}
}

func (a *alias) Push(toPush ScopesRunner) bool {
	if p, ok := a.Runner.(PushRunner); ok {
		return p.Push(toPush)
	}
	return false
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

func (s *scope) Scopes() StringsSet {
	return StringsSet{s.Label: struct{}{}}
}

func (s *scope) Push(toPush ScopesRunner) bool {
	if p, ok := s.Runner.(PushRunner); ok {
		return p.Push(toPush)
	}
	return false
}

func (s *scope) ApproxSize() int {
	if sizer, ok := s.Runner.(ApproxSizer); ok {
		return sizer.ApproxSize()
	}
	return UnknownSize
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
