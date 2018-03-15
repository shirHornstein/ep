package ep_test

import (
	"fmt"
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
)

func ExampleUnion() {
	runner, _ := ep.Union(&upper{}, &question{})
	data := ep.NewDataset(strs([]string{"hello", "world"}))
	data, err := eptest.TestRunner(runner, data)
	fmt.Println(data.Strings(), err)

	// Output:
	// [[HELLO WORLD is hello? is world?]] <nil>
}
