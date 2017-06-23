package ep

import (
    "sync"
    "context"
)

var _ = registerGob(pipeline{})

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
                // This mid-stream runner has halted unexpectedly, which other
                // previous runners might still be pushing data in, we might end
                // up with a blocked channel. Cancel the context, to stop all
                // producing runners and drain the channel of all blocking data.
                cancel()
                for _ = range inp {}
                err = err1
            }
        }(r, inp, middle)
        inp = middle
    }

    err1 := tail1.Run(ctx, inp, out)
    if err1 != nil && err == nil {
        for _ = range inp {}
        err = err1
    }

    wg.Wait()
    return err
}

// The implementation isn't trivial because it has to account for Wildcard types
// which indicate that the actual types should be retrieved from the input, thus
// this function has to walk backwards from the last runner and replace all
// Wildcard entries with the entire types from the previous runner. This is done
// repetitively until there are no more Wildcard, or all of the runners were
// accounted for.
// see Runner & Wildcard
func (rs pipeline) Returns() []Type {
    res := rs[len(rs) - 1].Returns() // start with the last runner

    // walk backwards over all of the runners in the pipeline until no more
    // wildcards are found. In many cases, this loop will just run once.
    for j := len(rs) - 2; j >= 0; j-- {
        hasWildcard := false

        // fetch the types of the previous runner in the pipeline
        prev := rs[j].Returns()

        // check for wildcards, and replace as needed. Walk backwards to allow
        // adding types in-place without messing the iteration
        for i := len(res) - 1; i >= 0; i-- {
            if res[i] == Wildcard {
                // wildcard found - replace it with the types from the previous
                // runner (which might also contain Wildcards)
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
