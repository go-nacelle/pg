package pgutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnifyLabels(t *testing.T) {
	for _, testCase := range []struct {
		name           string
		expectedLabels []string
		existingLabels []string
		valid          bool
		reconstruction []missingLabel
	}{
		{
			name:           "mismatched",
			expectedLabels: []string{"foo", "bar", "baz"},
			existingLabels: []string{"foo", "baz", "bonk"},
			valid:          false,
		},
		{
			name:           "inversions",
			expectedLabels: []string{"foo", "bar", "baz"},
			existingLabels: []string{"baz", "bar"},
			valid:          false,
		},

		{
			name:           "missing at end",
			expectedLabels: []string{"foo", "bar", "baz", "bonk"},
			existingLabels: []string{"foo", "bar"},
			valid:          true,
			reconstruction: []missingLabel{
				{Label: "baz", Prev: ptr("bar")},
				{Label: "bonk", Prev: ptr("baz")},
			},
		},
		{
			name:           "missing in middle",
			expectedLabels: []string{"foo", "bar", "baz", "bonk"},
			existingLabels: []string{"foo", "bonk"},
			valid:          true,
			reconstruction: []missingLabel{
				{Label: "bar", Prev: ptr("foo")},
				{Label: "baz", Prev: ptr("bar")},
			},
		},
		{
			name:           "missing at beginning",
			expectedLabels: []string{"foo", "bar", "baz", "bonk"},
			existingLabels: []string{"baz", "bonk"},
			valid:          true,
			reconstruction: []missingLabel{
				{Label: "foo", Next: ptr("baz")},
				{Label: "bar", Prev: ptr("foo")},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			reconstruction, valid := unifyLabels(testCase.expectedLabels, testCase.existingLabels)
			if testCase.valid {
				require.True(t, valid)
				assert.Equal(t, testCase.reconstruction, reconstruction)
			} else {
				require.False(t, valid)
			}
		})
	}
}

func ptr[S any](v S) *S {
	return &v
}
