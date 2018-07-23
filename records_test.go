package ep_test

import (
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
)

func TestRecordsInvariant(t *testing.T) {
	var d1 ep.Data = strs([]string{"1", "2", "4", "0", "3", "1", "1"})
	var d2 ep.Data = strs([]string{"a", "b", "c", "", "e", "f", "g"})

	eptest.VerifyDataInterfaceInvariant(t, ep.NewRecords(d1, d2))
}

func TestRecords_sorting(t *testing.T) {
	expected := []string{"(0,,)", "(1,a,a)", "(1,a,a2)", "(1,g,g)", "(1,z,z)", "(2,b,b)", "(3,e,e)"}
	var d1 ep.Data = strs([]string{"1", "2", "1", "0", "3", "1", "1"})
	var d2 ep.Data = strs([]string{"a", "b", "a", "", "e", "z", "g"})
	var d3 ep.Data = strs([]string{"a", "b", "a2", "", "e", "z", "g"})

	records := ep.NewRecords(d1, d2, d3)

	sort.Sort(records)
	require.EqualValues(t, expected, records.Strings())
}

func TestRecords_sortingDescending(t *testing.T) {
	expected := []string{"(3,e,e)", "(2,b,b)", "(1,z,z)", "(1,g,g)", "(1,a,a2)", "(1,a,a)", "(0,,)"}
	var d1 ep.Data = strs([]string{"1", "2", "1", "0", "3", "1", "1"})
	var d2 ep.Data = strs([]string{"a", "b", "a", "", "e", "z", "g"})
	var d3 ep.Data = strs([]string{"a", "b", "a2", "", "e", "z", "g"})

	records := ep.NewRecords(d1, d2, d3)

	sort.Sort(sort.Reverse(records))
	require.EqualValues(t, expected, records.Strings())
}

func TestRecords_Strings(t *testing.T) {
	expected := []string{"(1,a)", "(2,b)", "(4,c)", "(0,)", "(3,e)", "(1,f)", "(1,g)"}
	var d1 ep.Data = strs([]string{"1", "2", "4", "0", "3", "1", "1"})
	var d2 ep.Data = strs([]string{"a", "b", "c", "", "e", "f", "g"})

	records := ep.NewRecords(d1, d2)
	require.EqualValues(t, expected, records.Strings())
}
