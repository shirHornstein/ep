package compare

// Comparison is an enum for comparing data. We chose to use type byte, due to memory constraints. The 'a', 'b', 'c'... is arbitrary.
type Comparison byte

const (
	// Equal represents equal to other data object, e.g. 1 == 1
	Equal Comparison = 'a'
	// BothNulls represents a case in which the current & the other data objects are null, e.g. null == null
	BothNulls Comparison = 'b'
	// Null represents a case in which only 1 of the data objects is null, e.g. 1 == null
	Null Comparison = 'c'
	// Greater represents greater than other data object, e.g. 2 > 1
	Greater Comparison = 'd'
	// Less represents less than other data object, e.g. 1 < 2
	Less Comparison = 'e'
)
