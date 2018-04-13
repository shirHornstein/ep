package ep_test

import (
    "fmt"
    "context"
    "github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
)

func ExampleMapInpToOut() {
    // breakChars is a runner that breaks the input strings into characters (one
    // character per row).
    r := ep.MapInpToOut(&breakChars{})
    data := ep.NewDataset(strs([]string{"XY", "ABC"}))
    data, err := eptest.Run(r, data)
    fmt.Println(data.Strings(), err)

    // Output:
	// [[XY XY ABC ABC ABC] [X Y A B C]] <nil>
}

// runner that accepts strings, and breaks up the characters in the string to
// one character per row.
type breakChars struct{}
func (*breakChars) Returns() []ep.Type { return []ep.Type{str} }
func (*breakChars) Run(ctx context.Context, inp, out chan ep.Dataset) error {
    for data := range inp {
        for _, s := range data.At(0).(strs) {
			res := make(strs, len(s))
			for i, c := range s { // iterate on the characters
				res[i] = string(c)
			}
			out <- ep.NewDataset(res)
		}
    }
    return nil
}
