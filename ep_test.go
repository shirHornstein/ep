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
