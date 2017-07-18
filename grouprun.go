package ep

import (
	"context"
)

// GroupRun wraps around another Runner and executes its .Run() method once
// for every repeated distinct value in the input (assumed to be the last Data
// in the input Dataset). The results of all the individual runs are then
// unioned together in the output
func GroupRun(runner Runner) Runner {
	return &groupRun{runner}
}

type groupRun struct{ Runner }

func (r *groupRun) Run(ctx context.Context, inp, out chan Dataset) error {
	grps := map[string]chan Dataset{}
	errs := map[string]chan error{}
	for data := range inp {
		grouping := data.At(data.Width() - 1)
		for i, s := range grouping.Strings() {
			if grps[s] == nil {
				errs[s] = make(chan error)
				grps[s] = make(chan Dataset)
				go func(s string) {
					errs[s] <- r.Runner.Run(ctx, grps[s], out)
				}(s)
			}

			grps[s] <- data.Slice(i, i+1).(Dataset)
		}
	}

	// close all groups
	for _, grp := range grps {
		close(grp)
	}

	// wait for all group runs to end
	var err error
	for _, errChan := range errs {
		err1 := <-errChan
		if err1 != nil {
			err = err1
		}
	}
	return err
}
