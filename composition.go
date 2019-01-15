package ep

import (
	"context"
)

var _ = registerGob(&composition{}, &composeProject{})

// Function is a function that transforms Datasets. Any function that accepts a
// Dataset and returns a Dataset with the same length is a Function.
type Function func(Dataset) (Dataset, error)

// Piece is a type that holds a Function implementation.
type Piece interface {
	Function() Function
}

// Composer is a type that creates a list of Functions held by Pieces based on
// other Composers that it receives.
type Composer interface {
	Compose(...Composer) []Piece
}

// Composition returns a Runner with the provided return types. This Runner
// passes its input through every Piece's Function implementation, where every
// following Piece receives the output of the previous one. This Runner is also
// a Composer, which means that its pieces can be retrieved and used in another
// Composition.
func Composition(ts []Type, pcs ...Piece) Runner {
	return &composition{ts, pcs}
}

type composition struct {
	Ts  []Type
	Pcs []Piece
}

func (c *composition) Returns() []Type { return c.Ts }
func (c *composition) Run(ctx context.Context, inp, out chan Dataset) error {
	funcs := make([]Function, len(c.Pcs))
	for i := 0; i < len(c.Pcs); i++ {
		funcs[i] = c.Pcs[i].Function()
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
func (c *composition) Compose(...Composer) []Piece {
	return c.Pcs
}

// ComposeProject returns a special Piece which forwards its input as-is to
// every Composer, combining their outputs into a single Dataset. It is a
// functional implementation of ep.Project.
func ComposeProject(cmps ...Composer) Piece {
	return &composeProject{cmps}
}

type composeProject struct {
	Cmps []Composer
}

func (p *composeProject) Function() Function {
	funcs := make([][]Function, len(p.Cmps))
	for i := 0; i < len(p.Cmps); i++ {
		pcs := p.Cmps[i].Compose()
		funcs[i] = make([]Function, len(pcs))
		for j := 0; j < len(pcs); j++ {
			funcs[i][j] = pcs[j].Function()
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
