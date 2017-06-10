package ep

// NewPipeline returns a vertical composite pipeline runner that works as a
// pipeline: the output of any one stream is passed as input to the next
func NewPipeline(runners ...Runner) Runner {
    return pipeline(runners)
}

type pipeline []Runner

func (streams pipeline) Run(ctx Ctx, inp, out chan Dataset) (err error) {
    var wg sync.WaitGroup
    defer wg.Wait()

    // don't let go-routines leak, for example LIMIT-clause may exit early
    ctx, cancel := context.WithCancel(ctx)
    defer cancel()

    l := len(streams)
    head, tail1 := streams[:l - 1], streams[l - 1:][0]

    for _, s := range head {
        wg.Add(1)
        middle := make(chan Dataset)
        Debugf("Run pipe: init stream %s %p %+v with inp %p and out %p\n", s.Name(), s, s, inp, middle)
        go func(s Stream, inp, out chan Dataset) {
            defer wg.Done()
            defer close(out)
            err1 := s.Run(ctx, inp, out)
            if err1 != nil && err == nil {
                err = err1
            }
        }(s, inp, middle)
        inp = middle
    }

    Debugf("Run pipe: init tail %s with inp %p and out %p\n", tail1, inp, out)
    err1 := tail1.Run(ctx, inp, out)
    if err1 != nil && err == nil {
        err = err1
    }
    return err
}
