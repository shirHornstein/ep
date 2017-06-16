package ep

import (
    "sync"
    "context"
)

// Pipeline returns a vertical composite pipeline runner where the output of
// any one stream is passed as input to the next
func Pipeline(runners ...Runner) Runner {
    return pipeline(runners)
}

type pipeline []Runner

func (rs pipeline) Run(ctx context.Context, inp, out chan Dataset) (err error) {
    var wg sync.WaitGroup
    defer wg.Wait()

    // don't let go-routines leak, for example LIMIT-clause may exit early
    ctx, cancel := context.WithCancel(ctx)
    defer cancel()

    l := len(rs)
    head, tail1 := rs[:l - 1], rs[l - 1:][0]

    for _, r := range head {
        wg.Add(1)
        middle := make(chan Dataset)
        go func(r Runner, inp, out chan Dataset) {
            defer wg.Done()
            defer close(out)
            err1 := r.Run(ctx, inp, out)
            if err1 != nil && err == nil {
                err = err1
            }
        }(r, inp, middle)
        inp = middle
    }

    err1 := tail1.Run(ctx, inp, out)
    if err1 != nil && err == nil {
        err = err1
    }
    return err
}

func (r pipeline) Returns() []Type {
    panic("incorrect implementation")
    return []Type{Wildcard}
}
