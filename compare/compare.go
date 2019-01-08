package compare

// Result is an enum for comparison results.
type Result byte

const (
	// Equal represents equality to other data object, e.g. 1 == 1
	Equal Result = iota + 1
	// BothNulls represents a case in which the current & the other data objects
	// are null, e.g. null == null
	// Note: The data is the specific data in the column, and not the whole row
	BothNulls
	// Null represents a case in which only 1 of the data objects is null,
	// e.g. 1 == null
	// Note: The data is the specific data in the column, and not the whole row
	Null
	// Greater represents greater than other data object, e.g. 2 > 1
	Greater
	// Less represents smaller value than value, e.g. 1 < 2
	Less
)
