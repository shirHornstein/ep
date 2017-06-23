package ep

import (
    "context"
)

// ErrRunner is a Runner that immediately returns an error
type ErrRunner struct { error }
func (*ErrRunner) Returns() []Type { return []Type{} }
func (r *ErrRunner) Run(ctx context.Context, inp, out chan Dataset) error {
    return r.error
}

// InfinityRunner infinitely emits data until it's canceled
type InfinityRunner struct { Running bool }
func (*InfinityRunner) Returns() []Type { return []Type{Str} }
func (r *InfinityRunner) Run(ctx context.Context, inp, out chan Dataset) error {
    // running flag helps tests ensure that the go-routine didn't leak
    r.Running = true
    defer func() { r.Running = false }()

    // infinitely produce data, until canceled
    for {
        select {
        case _, _ = <- ctx.Done():
            return nil
        default:
            out <- NewDataset(Strs{"data"})
        }
    }
}
