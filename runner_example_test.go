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
	// [[HELLO WORLD]] <nil>
}

func ExampleFilterRunner_Filter() {
	runner := ep.Project(&question{}, &upper{}, &question{}).(ep.FilterRunner)
	err := runner.Filter([]bool{false, true, false})
	fmt.Println(err)

	data := ep.NewDataset(strs([]string{"hello", "world"}))
	data, err = eptest.Run(runner, data)
	fmt.Println(data.Strings(), err)

	// Output:
	// <nil>
	// [[] [HELLO WORLD] []] <nil>
}
