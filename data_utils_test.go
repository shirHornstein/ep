package ep

import (
    "fmt"
)

func ExampleClone() {
    var d1 Data = Strs([]string{"hello", "world"})
    d2 := Clone(d1).(Strs)

    d2[0] = "foo"
    d2[1] = "bar"
    fmt.Println(d2) // clone modified
    fmt.Println(d1) // original left intact

    // Output:
    // [foo bar]
    // [hello world]
}

func ExampleCut() {
    var d Data = Strs([]string{"hello", "world", "foo", "bar"})
    data := Cut(d, 1, 3)
    fmt.Println(data)
    // Output:
    // [[hello] [world foo] [bar]]
}
