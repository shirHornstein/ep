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
