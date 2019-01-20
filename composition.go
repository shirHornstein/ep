package ep

import (
	"context"
)

var _ = registerGob(&composition{}, &composeProject{})

// BatchFunction is a function that transforms a single Dataset.
// BatchFunction is not blocking: it returns immediately and does not wait for
// more batches.
type BatchFunction func(Dataset) (Dataset, error)

// Composable is a type that holds a BatchFunction implementation.
type Composable interface {
	BatchFunction() BatchFunction
}

// Composer is a type that creates a list of BatchFunctions held by Composables based on
// other Composers that it receives.
type Composer interface {
	Compose(...Composer) []Composable
}

// Composition returns a Runner with the provided return types. This Runner
// passes its input through every Composable's BatchFunction implementation, where every
// following Composable receives the output of the previous one. This Runner is also
// a Composer, which means that its Composable can be retrieved and used in another
// Composition.
func Composition(ts []Type, pcs ...Composable) Runner {
	return &composition{ts, pcs}
}

type composition struct {
	Ts  []Type
	Pcs []Composable
}

func (c *composition) Returns() []Type { return c.Ts }
func (c *composition) Run(ctx context.Context, inp, out chan Dataset) error {
	funcs := make([]BatchFunction, len(c.Pcs))
	for i := 0; i < len(c.Pcs); i++ {
		funcs[i] = c.Pcs[i].BatchFunction()
	}

	for data := range inp {
		res, err := funcs[0](data)
		if err != nil {
			return err
		}

		for i := 1; i < len(funcs); i++ {
			res, err = funcs[i](res)
			if err != nil {
				return err
			}
		}
		out <- res
	}
	return nil
}
func (c *composition) Compose(...Composer) []Composable {
	return c.Pcs
}

// ComposeProject returns a special Composable which forwards its input as-is to
// every Composer, combining their outputs into a single Dataset. It is a
// functional implementation of ep.Project.
func ComposeProject(cmps ...Composer) Composable {
	return &composeProject{cmps}
}

type composeProject struct {
	Cmps []Composer
}

func (p *composeProject) BatchFunction() BatchFunction {
	funcs := make([][]BatchFunction, len(p.Cmps))
	for i := 0; i < len(p.Cmps); i++ {
		pcs := p.Cmps[i].Compose()
		funcs[i] = make([]BatchFunction, len(pcs))
		for j := 0; j < len(pcs); j++ {
			funcs[i][j] = pcs[j].BatchFunction()
		}
	}

	return func(data Dataset) (Dataset, error) {
		var result Dataset
		for col := 0; col < len(funcs); col++ {
			res, err := funcs[col][0](data)
			if err != nil {
				return nil, err
			}
			for i := 1; i < len(funcs[col]); i++ {
				res, err = funcs[col][i](res)
				if err != nil {
					return nil, err
				}
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
