package compare

// Result is an enum for comparison results.
type Result byte

const (
	// NOTE: Result const (Equal / BothNulls / Null...) is the outcome of
	// a comparison between 2 Rows.
	//
	// Rows - is the Value inside the Data.
	// Data - is all the column's data.
	// Data Object - is the column.
	//
	// E.g. ((1,'Bob'), (2,'Danny'), (3,'John')) AS t1(id,name)
	// Data Object: name
	// Data: 'Bob', 'Danny', 'John'
	// Row: 'Danny'

	// Equal represents equality of the current value to other value
	// e.g. 1 == 1
	Equal Result = iota + 1
	// BothNulls represents a case in which the value and the other value
	// are both nulls, e.g. null == null
	BothNulls
	// Null represents a case in which only 1 of the values is null
	// e.g. 1 == null
	Null
	// Greater represents greater than other value
	// e.g. 2 > 1
	Greater
	// Less represents smaller value than other value
	// e.g. 1 < 2
	Less
)
