package ep

func ExampleClone() {
    var strs1 Data = Strs([]string{"hello", "world"})
    strs2 := Clone(strs1).(Strs)

    strs2[0] = "foo"
    strs2[1] = "bar"
    fmt.Println(strs2) // clone modified
    fmt.Println(strs1) // original left intact

    // Output:
    // [foo bar]
    // [hello world]
}
