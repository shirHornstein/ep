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

    wg.Wait()
    return err
}

func (rs pipeline) Returns() []Type {
    res := rs[len(rs) - 1].Returns() // start with the last returns

    // walk backwards on all of the runners in the pipeline until no more
    // wildcards are found. In many cases, this loop will just run once.
    for j := len(rs) - 2; j >= 0; j-- {
        hasWildcard := false

        // fetch the types of the previous stream in the pipeline
        prev := rs[j].Returns()

        // check all of the types for wildcards, and replace as needed. Walk
        // backwards to allow adding types without messing the iteration
        for i := len(res) - 1; i >= 0; i-- {
            if res[i] == Wildcard {
                // wildcard found - replace it with the columns from the
                // previous stream (which might also contain Wildcards)
                res = append(res[:i], append(prev, res[i+1:]...)...)
                hasWildcard = true
            }
        }

        if !hasWildcard {
            break // no wildcards remain to replace.
        }

    }

    return res
}
