package ep

import (
	"context"
)

var _ = registerGob(Strs{})

// ErrRunner is a Runner that immediately returns an error
type ErrRunner struct{ error }

func (*ErrRunner) Returns() []Type { return []Type{} }
func (r *ErrRunner) Run(ctx context.Context, inp, out chan Dataset) error {
	return r.error
}

// InfinityRunner infinitely emits data until it's canceled
type InfinityRunner struct{ Running bool }

func (*InfinityRunner) Returns() []Type { return []Type{Str} }
func (r *InfinityRunner) Run(ctx context.Context, inp, out chan Dataset) error {
	// running flag helps tests ensure that the go-routine didn't leak
	r.Running = true
	defer func() { r.Running = false }()

	// infinitely produce data, until canceled
	for {
		select {
		case _, _ = <-ctx.Done():
			return nil
		default:
			out <- NewDataset(Strs{"data"})
		}
	}
}

type DataRunner struct {
	Dataset
}

func (r *DataRunner) Returns() []Type {
	types := []Type{}
	for i := 0; i < r.Dataset.Len(); i++ {
		types = append(types, r.Dataset.At(i).Type())
	}
	return types
}

func (r *DataRunner) Run(ctx context.Context, inp, out chan Dataset) error {
	for range inp {
	} // drains input
	out <- r.Dataset
	return nil
}
