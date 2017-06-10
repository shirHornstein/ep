package ep

// NewNulls creates a new data object representing a list of `n` null values
func NewNulls(n int) Data {
    return nulls(n)
}

type nulls int // number of nulls in the set
