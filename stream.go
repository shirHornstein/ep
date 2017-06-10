package ep

import (
    "context"
)

type Runner interface {
    Run(ctx context.Context, inp, out chan Dataset) error
}
