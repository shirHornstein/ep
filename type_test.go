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
			ts1:      []ep.Type{str, integer},
			ts2:      []ep.Type{str, integer},
			expected: true,
		}, {
			name:     "same types, different order",
			ts1:      []ep.Type{str, integer},
			ts2:      []ep.Type{integer, str},
			expected: false,
		}, {
			name:     "different length",
			ts1:      []ep.Type{integer},
			ts2:      []ep.Type{integer, str},
			expected: false,
		}, {
			name:     "any on one of them",
			ts1:      []ep.Type{integer, ep.Any},
			ts2:      []ep.Type{integer, str},
			expected: true,
		}, {
			name:     "any on both",
			ts1:      []ep.Type{integer, ep.Any, ep.Any},
			ts2:      []ep.Type{ep.Any, integer, ep.Any},
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
