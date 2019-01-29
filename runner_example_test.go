package ep_test

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
)

func ExampleRunner() {
	upper := &upper{}
	data := ep.NewDataset(strs([]string{"hello", "world"}))
	data, err := eptest.Run(upper, data)
	fmt.Println(data.Strings(), err)

	// Output:
	// [(HELLO) (WORLD)] <nil>
}

func ExampleFilterRunner_Filter() {
	runner := ep.Project(&question{}, &upper{}, &question{}).(ep.FilterRunner)
	runner.Filter([]bool{false, true, false})

	data := ep.NewDataset(strs([]string{"hello", "world"}))
	res, err := eptest.Run(runner, data)
	fmt.Println(res.Strings(), err)

	// Output:
	// [(HELLO) (WORLD)] <nil>
}

func ExampleScopesRunner_Scopes() {
	runner := ep.Pipeline(&question{}, &upper{})
	fmt.Println(runner.(ep.ScopesRunner).Scopes())

	// Output:
	// map[]
}

func ExamplePushRunner_Push() {
	runner := ep.Pipeline(&question{}, &upper{})
	toPush := ep.Project(ep.PassThrough(), ep.PassThrough()).(ep.ScopesRunner)
	fmt.Println(runner.(ep.PushRunner).Push(toPush))

	// Output:
	// false
}
