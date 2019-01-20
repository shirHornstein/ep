package ep

import (
	"context"
)

var _ = registerGob(&composition{}, &composeProject{})

// BatchFunction is a function that transforms a single Dataset.
// BatchFunction is not blocking: it returns immediately and does not wait for
// more batches.
type BatchFunction func(Dataset) (Dataset, error)

// Composable is a type that holds a BatchFunction implementation and can be
// used to Compose runners.
type Composable interface {
	BatchFunction() BatchFunction
}

// Compose returns a Runner with the provided return types. This Runner passes
// its input through every Composable's BatchFunction implementation, where
// every following BatchFunction receives the output of the previous one. This
// Runner is also a Composable, which means that its BatchFunction can be
// retrieved and used in another Compose call.
func Compose(ts []Type, cmps ...Composable) Runner {
	return &composition{ts, cmps}
}

type composition struct {
	Ts   []Type
	Cmps []Composable
}

func (c *composition) Returns() []Type { return c.Ts }
func (c *composition) Run(ctx context.Context, inp, out chan Dataset) error {
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
func (c *composition) BatchFunction() BatchFunction {
	funcs := make([]BatchFunction, len(c.Cmps))
	for i := 0; i < len(c.Cmps); i++ {
		funcs[i] = c.Cmps[i].BatchFunction()
	}

	return func(data Dataset) (Dataset, error) {
		res, err := funcs[0](data)
		if err != nil {
			return nil, err
		}

		for i := 1; i < len(funcs); i++ {
			res, err = funcs[i](res)
			if err != nil {
				return nil, err
			}
		}
		return res, nil
	}
}

// ComposeProject returns a special Composable which forwards its input as-is
// to every Composable's BatchFunction, combining their outputs into a single
// Dataset. It is a functional implementation of ep.Project.
func ComposeProject(cmps ...Composable) Composable {
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
		var result Dataset
		for col := 0; col < len(funcs); col++ {
			res, err := funcs[col](data)
			if err != nil {
				return nil, err
			}
			if result == nil {
				result = res
			} else {
				result, err = result.Expand(res)
				if err != nil {
					return nil, err
				}
			}
		}
		return result, nil
	}
}
