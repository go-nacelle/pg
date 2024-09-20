package pgutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStripIdent(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "single line",
			input:    "CREATE VIEW IF NOT EXISTS v AS SELECT 1;",
			expected: "CREATE VIEW IF NOT EXISTS v AS SELECT 1;",
		},
		{
			name:     "single line with ident",
			input:    "  CREATE VIEW IF NOT EXISTS v AS SELECT 1;",
			expected: "CREATE VIEW IF NOT EXISTS v AS SELECT 1;",
		},
		{
			name:     "multi line, common indent",
			input:    "  CREATE VIEW IF NOT EXISTS v AS\n  SELECT 1;",
			expected: "CREATE VIEW IF NOT EXISTS v AS\nSELECT 1;",
		},
		{
			name:     "multi line, jagged indent",
			input:    "  CREATE VIEW IF NOT EXISTS v AS\n    SELECT *\n    FROM t;",
			expected: "CREATE VIEW IF NOT EXISTS v AS\n  SELECT *\n  FROM t;",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expected, stripIdent(testCase.input))
		})
	}
}
