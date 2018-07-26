package ep_test

import (
	"github.com/panoplyio/ep"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAreEqualTypes(t *testing.T) {
	cases := []struct {
		name     string
		ts1      []ep.Type
		ts2      []ep.Type
		expected bool
	}{
		{
			name:     "same types, same order",
			ts1:      []ep.Type{str, ep.Null},
			ts2:      []ep.Type{str, ep.Null},
			expected: true,
		}, {
			name:     "same types, different order",
			ts1:      []ep.Type{str, ep.Null},
			ts2:      []ep.Type{ep.Null, str},
			expected: false,
		}, {
			name:     "different length",
			ts1:      []ep.Type{ep.Null},
			ts2:      []ep.Type{ep.Null, str},
			expected: false,
		}, {
			name:     "any on one of them",
			ts1:      []ep.Type{ep.Null, ep.Any},
			ts2:      []ep.Type{ep.Null, str},
			expected: true,
		}, {
			name:     "any on both",
			ts1:      []ep.Type{ep.Null, ep.Any, ep.Any},
			ts2:      []ep.Type{ep.Any, ep.Null, ep.Any},
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
