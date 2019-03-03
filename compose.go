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
	BatchFunction() BatchFunction
}

// Compose returns a Runner with the provided return types & the provided scopes.
// Note: The caller's responsibility to maintain a valid set of scopes.
// This Runner passes its input through every Composable's BatchFunction
// implementation, where every following BatchFunction receives the output
// of the previous one. This Runner is also a Composable, which means that
// its BatchFunction can be retrieved and used in another Compose call.
func Compose(returns []Type, scopes StringsSet, cmps ...Composable) Runner {
	return &compose{returns, scopes, cmps}
}

type compose struct {
	ReturnTs []Type
	Scps     StringsSet
	Cmps     []Composable
}

func (c *compose) Returns() []Type { return c.ReturnTs }
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

func (c *compose) Scopes() StringsSet {
	return c.Scps
}

func (c *compose) SetAlias(name string) {
	if len(c.ReturnTs) > 1 {
		panic("Invalid usage of alias. Consider use scope")
	}
	c.ReturnTs[0] = SetAlias(c.ReturnTs[0], name)
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
	return &composeProject{cmps}
}

type composeProject struct {
	Cmps []Composable
}

func (p *composeProject) BatchFunction() BatchFunction {
	funcs := make([]BatchFunction, len(p.Cmps))
	for i := 0; i < len(p.Cmps); i++ {
		funcs[i] = p.Cmps[i].BatchFunction()
	}

	return func(data Dataset) (Dataset, error) {
		result := NewDataset()
		for col := 0; col < len(funcs); col++ {
			res, err := funcs[col](data)
			if err != nil {
				return nil, err
			}
			result, err = result.Expand(res)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	}
}
