package kql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuoteStringUnicodeEscapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    string
		expected string
	}{
		{"BMP", "\u65e5", `"\u65e5"`},
		{"last BMP code point", "\uffff", `"\uffff"`},
		{"first supplementary code point", "\U00010000", `"\U00010000"`},
		{"emoji", "a\U0001f642b", `"a\U0001f642b"`},
		{"last code point", "\U0010ffff", `"\U0010ffff"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, QuoteString(test.value, false))
		})
	}

	assert.Equal(t, `"\U0001f642"`, New("").AddString("\U0001f642").String())
	assert.Equal(t, `"\U0001f642"`, NewParameters().AddString("s", "\U0001f642").ToParameterCollection()["s"])
}
