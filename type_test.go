package ep_test

import (
	"github.com/panoplyio/ep"
	"github.com/stretchr/testify/require"
	"testing"
)

var str2 = &strType2{}

type strType2 struct{}

func (s *strType2) String() string        { return s.Name() }
func (*strType2) Name() string            { return "string2" }
func (*strType2) Data(n int) ep.Data      { return &strs2{make(strs, n)} }
func (*strType2) DataEmpty(n int) ep.Data { return &strs2{make(strs, 0, n)} }

type strs2 struct {
	strs
}

func (*strs2) Type() ep.Type { return str2 }

func (vs *strs2) LessOther(thisRow int, other ep.Data, otherRow int) bool {
	data := other.(*strs2)
	return vs.strs[thisRow] < data.strs[otherRow]
}
func (vs *strs2) Append(other ep.Data) ep.Data { return append(vs.strs, other.(*strs2).strs...) }
func (vs *strs2) Copy(from ep.Data, fromRow, toRow int) {
	src := from.(*strs2)
	vs.strs[toRow] = src.strs[fromRow]
}

func TestAreEqualTypes(t *testing.T) {
	cases := []struct {
		name     string
		ts1      []ep.Type
		ts2      []ep.Type
		expected bool
	}{
		{
			name:     "same types, same order",
			ts1:      []ep.Type{str, str2},
			ts2:      []ep.Type{str, str2},
			expected: true,
		}, {
			name:     "same types, different order",
			ts1:      []ep.Type{str, str2},
			ts2:      []ep.Type{str2, str},
			expected: false,
		}, {
			name:     "different length",
			ts1:      []ep.Type{str2},
			ts2:      []ep.Type{str2, str},
			expected: false,
		}, {
			name:     "any on one of them",
			ts1:      []ep.Type{str2, ep.Any},
			ts2:      []ep.Type{str2, str},
			expected: true,
		}, {
			name:     "any on both",
			ts1:      []ep.Type{str2, ep.Any, ep.Any},
			ts2:      []ep.Type{ep.Any, str2, ep.Any},
			expected: true,
		}, {
			name:     "only any",
			ts1:      []ep.Type{ep.Any, ep.Any},
			ts2:      []ep.Type{ep.Any, str},
			expected: true,
		}, {
			name:     "only any, different length",
			ts1:      []ep.Type{ep.Any},
			ts2:      []ep.Type{ep.Any, ep.Any},
			expected: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := ep.AreEqualTypes(tc.ts1, tc.ts2)
			require.Equal(t, tc.expected, res)

			// verify order doesn't affect result
			res = ep.AreEqualTypes(tc.ts2, tc.ts1)
			require.Equal(t, tc.expected, res)
		})
	}
}
