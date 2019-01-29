package ep

// StringsSet holds set of unique strings
type StringsSet map[string]struct{}

// Contains checks if other StringsSet included in the current StringsSet
func (r StringsSet) Contains(other StringsSet) bool {
	for s := range other {
		if _, ok := r[s]; !ok {
			return false
		}
	}
	return true
}
