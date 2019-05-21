package ep

import (
	"context"
)

var _ = registerGob(&compose{}, &composeProject{})

// BatchFunction is a function that transforms a single Dataset.
// BatchFunction is not blocking: it returns immediately and does not wait for
// more batches.
type BatchFunction func(Dataset) (Dataset, error)

// Composable is a type that holds a BatchFunction implementation and can be
// used to Compose runners.
type Composable interface {
	equals
	returns // Composable must declare its return types

	BatchFunction() BatchFunction
}

// Compose returns a Runner with the the provided scopes.
// Note: The caller's responsibility to maintain a valid set of scopes.
// This Runner passes its input through every Composable's BatchFunction
// implementation, where every following BatchFunction receives the output
// of the previous one. This Runner is also a Composable, which means that
// its BatchFunction can be retrieved and used in another Compose call.
func Compose(scopes StringsSet, cmps ...Composable) Runner {
	return &compose{Scps: scopes, Cmps: cmps}
}

type compose struct {
	Alias    string
	RetScope string
	Scps     StringsSet
	Cmps     []Composable
}

func (c *compose) Equals(other interface{}) bool {
	r, ok := other.(*compose)
	if !ok || len(c.Cmps) != len(r.Cmps) {
		return false
	}

	for i, cmp := range c.Cmps {
		if !cmp.Equals(r.Cmps[i]) {
			return false
		}
	}

	return true
}
func (c *compose) Returns() []Type {
	last := len(c.Cmps) - 1
	ret := returnsOne(last, c.getIReturns)

	if c.Alias != "" {
		if len(ret) > 1 {
			panic("Invalid usage of alias. Consider use scope")
		}
		ret[0] = SetAlias(ret[0], c.Alias)
	}
	if c.RetScope != "" {
		ret = SetScope(ret, c.RetScope)
	}
	return ret
}
func (c *compose) getIReturns(i int) returns { return c.Cmps[i] }

func (c *compose) Run(ctx context.Context, inp, out chan Dataset) error {
	batchFunction := c.BatchFunction()

	for data := range inp {
		res, err := batchFunction(data)
		if err != nil {
			return err
		}
		out <- res
	}
	return nil
}
func (c *compose) BatchFunction() BatchFunction {
	funcs := make([]BatchFunction, len(c.Cmps))
	for i := 0; i < len(c.Cmps); i++ {
		funcs[i] = c.Cmps[i].BatchFunction()
	}

	return func(data Dataset) (Dataset, error) {
		var err error
		for i := 0; i < len(funcs); i++ {
			data, err = funcs[i](data)
			if err != nil {
				return nil, err
			}
		}
		return data, nil
	}
}

func (c *compose) Scopes() StringsSet   { return c.Scps }
func (c *compose) SetAlias(name string) { c.Alias = name }

func (c *compose) Filter(keep []bool) {
	last := c.Cmps[len(c.Cmps)-1]
	if f, isFilterable := last.(interface{ Filter(keep []bool) }); isFilterable {
		f.Filter(keep)
	}
}

// ComposeProject returns a special Composable which forwards its input as-is
// to every Composable's BatchFunction, combining their outputs into a single
// Dataset. It is a functional implementation of ep.Project.
func ComposeProject(cmps ...Composable) Composable {
	if len(cmps) == 0 {
		panic("at least 1 Composable is required for project composables")
	}
	if len(cmps) == 1 {
		return cmps[0]
	}
	return composeProject(cmps)
}

type composeProject []Composable

func (cs composeProject) Equals(other interface{}) bool {
	otherP, ok := other.(composeProject)
	if !ok || len(cs) != len(otherP) {
		return false
	}

	for i, r := range cs {
		if !r.Equals(otherP[i]) {
			return false
		}
	}

	return true
}

// Returns a concatenation of all composables' return types
func (cs composeProject) Returns() []Type {
	var types []Type
	for _, c := range cs {
		types = append(types, c.Returns()...)
	}
	return types
}
func (cs composeProject) BatchFunction() BatchFunction {
	var resultWidth, resultLen int
	funcs := make([]BatchFunction, len(cs))
	for i := 0; i < len(cs); i++ {
		resultWidth = 0
		funcs[i] = cs[i].BatchFunction()
	}

	return func(data Dataset) (Dataset, error) {
		result := make([]Data, 0, resultWidth)
		resultLen = -1

		for col := 0; col < len(funcs); col++ {
			res, err := funcs[col](data)
			if err != nil {
				return nil, err
			}
			resultLen, err = verifySameLength(resultLen, res.Len())
			if err != nil {
				return nil, err
			}
			result = append(result, res.(dataset)...)
		}
		resultWidth = len(result)
		return NewDataset(result...), nil
	}
}

func (cs composeProject) Filter(keep []bool) {
	currIdx := 0
	for i, c := range cs {
		returnLen := len(c.Returns())
		// simplest (and most common for project) case - c return single value
		if returnLen == 1 {
			if !keep[currIdx] {
				cs[i] = dummyRunnerSingleton
			}
		} else {
			if r, isFilterable := c.(interface{ Filter(keep []bool) }); isFilterable {
				r.Filter(keep[currIdx : currIdx+returnLen])
			}
		}
		currIdx += returnLen
	}
}

func createComposeRunner(runners []Runner) (Runner, bool) {
	var ok bool
	scopes := make(StringsSet)
	cmps := make([]Composable, len(runners))
	for i, r := range runners {
		if scp, ok := r.(ScopesRunner); ok {
			scopes.AddAll(scp.Scopes())
		}
		if cmps[i], ok = r.(Composable); !ok {
			return nil, false
		}
	}
	return Compose(scopes, cmps...), true
}

func createComposeProjectRunner(runners []Runner) (Runner, bool) {
	scopes := make(StringsSet)
	var flat composeProject = make([]Composable, 0, len(runners))
	for _, r := range runners {
		if scp, ok := r.(ScopesRunner); ok {
			scopes.AddAll(scp.Scopes())
		}
		c, isCmp := r.(Composable)
		if !isCmp {
			return nil, false
		}

		if p, isProject := isComposeProject(r); isProject {
			flat = append(flat, p...)
		} else {
			flat = append(flat, c)
		}
	}
	return Compose(scopes, ComposeProject(flat...)), true
}

func isComposeProject(r returns) (composeProject, bool) {
	comp, isCmp := r.(*compose)
	if isCmp && len(comp.Cmps) == 1 {
		p, isProject := comp.Cmps[0].(composeProject)
		return p, isProject
	}
	return nil, false
}
