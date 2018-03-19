package ep

import (
	"fmt"
)

func ExampleUnion() {
	runner, _ := Union(&upper{}, &question{})
	data := NewDataset(strs([]string{"hello", "world"}))
	data, err := TestRunner(runner, data)
	fmt.Println(data.Strings(), err)

	// Output:
	// [[HELLO WORLD is hello? is world?]] <nil>
}
