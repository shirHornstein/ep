package ep

import (
	"fmt"
)

func ExampleUnion() {
	runner, _ := Union(&Upper{}, &Question{})
	data := NewDataset(Strs([]string{"hello", "world"}))
	data, err := testRun(runner, data)
	fmt.Println(data, err)

	// Output:
	// [[HELLO WORLD is hello? is world?]] <nil>
}
