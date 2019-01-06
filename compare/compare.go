package compare

// Comparison is an enum for comparing data. We chose to use type byte,
// due to memory constraints.
type Comparison byte

const (
	// Equal represents equal to other data object, e.g. 1 == 1
	Equal Comparison = iota + 1
	// BothNulls represents a case in which the current & the other data objects are null, e.g. null == null
	BothNulls
	// Null represents a case in which only 1 of the data objects is null, e.g. 1 == null
	Null
	// Greater represents greater than other data object, e.g. 2 > 1
	Greater
	// Less represents less than other data object, e.g. 1 < 2
	Less
)
