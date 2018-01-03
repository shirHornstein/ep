package ep

import (
	"context"
)

var _ = registerGob(strs{})

// errRunner is a Runner that immediately returns an error
type errRunner struct{ error }

func (*errRunner) Returns() []Type { return []Type{} }
func (r *errRunner) Run(ctx context.Context, inp, out chan Dataset) error {
	return r.error
}

// infinityRunner infinitely emits data until it's canceled
type infinityRunner struct{ Running bool }

func (*infinityRunner) Returns() []Type { return []Type{str} }
func (r *infinityRunner) Run(ctx context.Context, inp, out chan Dataset) error {
	// running flag helps tests ensure that the go-routine didn't leak
	r.Running = true
	defer func() { r.Running = false }()

	// infinitely produce data, until canceled
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			out <- NewDataset(strs{"data"})
		}
	}
}

type dataRunner struct {
	Dataset
}

func (r *dataRunner) Returns() []Type {
	types := []Type{}
	for i := 0; i < r.Dataset.Len(); i++ {
		types = append(types, r.Dataset.At(i).Type())
	}
	return types
}
func (r *dataRunner) Run(ctx context.Context, inp, out chan Dataset) error {
	for range inp {
	} // drains input
	out <- r.Dataset
	return nil
}
