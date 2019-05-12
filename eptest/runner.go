package eptest

import (
	"context"
	"github.com/panoplyio/ep"
)

var _ = ep.Runners.
	Register("errRunner", &errRunner{})

// errRunner is a Runner that returns an error upon first input or inp closing
type errRunner struct {
	error
	// name is unused field, defined to allow gob-ing errRunner between peers
	name string
}

// NewErrRunner returns new errRunner
func NewErrRunner(e error) ep.Runner {
	return &errRunner{error: e, name: "err"}
}

func (r *errRunner) Equals(other interface{}) bool {
	o, ok := other.(*errRunner)
	return ok && r.name == o.name && r.error.Error() == o.error.Error()
}
func (*errRunner) Returns() []ep.Type { return []ep.Type{} }
func (r *errRunner) Run(ctx context.Context, inp, out chan ep.Dataset) error {
	for range inp {
		return r.error
	}
	return r.error
}
