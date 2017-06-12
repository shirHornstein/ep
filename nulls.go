package ep

// Nulls is a Type representing NULL values. Use Nulls.Data(n) to create Data
// instances of `n` nulls
var Nulls Type = nulls(0)

// nulls is implemented to satisfy both the Type and Data interfaces
type nulls int // number of nulls in the set
