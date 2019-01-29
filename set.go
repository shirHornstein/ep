package ep

// StringsSet holds set of unique strings
type StringsSet map[string]struct{}

// Contains gets two maps and checks if other map includes in the scopes map
func (r StringsSet) Contains(other StringsSet) bool {
	for s := range other {
		if _, ok := r[s]; !ok {
			return false
		}
	}
	return true
}
