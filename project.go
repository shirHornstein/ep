package ep

import (
    "context"
)

var _ = registerGob(project{})

// Project returns a horizontal composite projection runner that dispatches
// its input to all of the internal runners, and joins the result into a single
// dataset to return.
func Project(runners ...Runner) Runner {
    if len(runners) == 0 {
        panic("ep: at least 1 runner is required for projecting")
    } else if len(runners) == 1 {
        return runners[0]
    }

    head := runners[:len(runners) - 1]
    tail1 := runners[len(runners) - 1]
    var from Runner
    if len(head) == 1 {
        from = head[0]
    } else {
        from = Project(head...)
    }

    return &project{from, tail1}
}

type project struct { Left Runner; Right Runner }

// Returns a concatenation of the left and right return types
func (rs *project) Returns() []Type {
    types := []Type{}
    types = append(types, rs.Left.Returns()...)
    types = append(types, rs.Right.Returns()...)
    return types
}

// Run dispatches the same input to all inner runners, and then collects and
// joins their results into a single dataset output
func (rs *project) Run(ctx context.Context, inp, out chan Dataset) (err error) {
    // choose the error out from the Left and Right errors.
    var err1 error
    defer func() { if err == nil && err1 != nil { err = err1 } }()

    //
    inpLeft := make(chan Dataset)
    left := make(chan Dataset)
    defer func() { for _ = range left {} }()

    inpRight := make(chan Dataset)
    right := make(chan Dataset)
    defer func() { for _ = range right {} }()

    // cancel the From runner when we're done - just in case it's still running.
    ctx, cancel := context.WithCancel(ctx)
    defer cancel()

    go func() {
        defer close(left)
        err1 = rs.Left.Run(ctx, inpLeft, left)
    }()

    go func() {
        defer close(right)
        err = rs.Right.Run(ctx, inpRight, right)
    }()

    // dispatch (duplicate) input to both left and right runners
    go func() {
        defer close(inpLeft)
        defer close(inpRight)
        for data := range inp {
            inpLeft <- data
            inpRight <- data
        }
    }()

    // collect & join the output from the Left and Right runners, in order.
    for {
        result := dataset{}
        dataLeft, okLeft := <- left
        dataRight, okRight := <- right

        if !okLeft || !okRight {
            return // TODO: what if just one is done? error?
        }

        // TODO: what if there's a mismatch in Len()?
        for i := 0; okLeft && i < dataLeft.Width(); i++ {
            result = append(result, dataLeft.At(i))
        }

        for i := 0; okRight && i < dataRight.Width(); i++ {
            result = append(result, dataRight.At(i))
        }

        out <- result
    }

    return
}
