package kql

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestBuilder(t *testing.T) {
	tests := []struct {
		name     string
		b        *Builder
		expected string
	}{
		{
			"Test empty",
			New(""),
			""},
		{
			"Test simple literal",
			New("").AddLiteral("foo"),
			"foo"},
		{
			"Test simple literal ctor",
			New("foo"),
			"foo"},
		{
			"Test add literal",
			New("foo").AddLiteral("bar"),
			"foobar"},
		{
			"Test add int",
			New("MyTable | where i != ").AddInt(32),
			"MyTable | where i != int(32)",
		},
		{
			"Test add long",
			New("MyTable | where i != ").AddLong(32),
			"MyTable | where i != long(32)",
		},
		{
			"Test add real",
			New("MyTable | where i != ").AddReal(32.5),
			"MyTable | where i != real(32.5)",
		},
		{
			"Test add bool",
			New("MyTable | where i != ").AddBool(true),
			"MyTable | where i != bool(true)",
		},
		{
			"Test add datetime",
			New(
				"MyTable | where i != ",
			).AddDateTime(time.Date(2019, 1, 2, 3, 4, 5, 600, time.UTC)),
			"MyTable | where i != datetime(2019-01-02T03:04:05.0000006Z)",
		},
		{
			"Test add duration",
			New(
				"MyTable | where i != ",
			).AddTimespan(1*time.Hour + 2*time.Minute + 3*time.Second + 4*time.Microsecond),
			"MyTable | where i != timespan(01:02:03.0000040)",
		},
		{
			"Test add duration with days",
			New(
				"MyTable | where i != ",
			).AddTimespan(49*time.Hour + 2*time.Minute + 3*time.Second + 4*time.Microsecond),
			"MyTable | where i != timespan(2.01:02:03.0000040)",
		},
		{
			"Test add dynamic",
			New(
				"MyTable | where i != ",
			).AddDynamic(`{"a": 3, "b": 5.4}`),
			`MyTable | where i != dynamic({"a": 3, "b": 5.4})`,
		},
		{
			"Test add guid",
			New(
				"MyTable | where i != ",
			).AddGUID(uuid.MustParse("12345678-1234-1234-1234-123456789012")),
			"MyTable | where i != guid(12345678-1234-1234-1234-123456789012)",
		},
		{
			"Test add string simple",
			New(
				"MyTable | where i != ",
			).AddString("foo"),
			"MyTable | where i != \"foo\"",
		},
		{
			"Test add string with quote",
			New(
				"MyTable | where i != ",
			).AddString("foo\"bar"),
			"MyTable | where i != \"foo\\\"bar\"",
		},
		{
			"Test add keyword",
			New(
				"MyTable | where i != ",
			).AddLiteral("(").AddKeyword("key").AddLiteral(")"),
			"MyTable | where i != (key)",
		},
		{
			"Test add identifiers",
			New("").
				AddDatabase("foo_1").AddLiteral(".").
				AddTable("_bar").AddLiteral(" | where ").
				AddColumn("_baz").AddLiteral(" == ").
				AddFunction("func_").AddLiteral("()"),
			`database("foo_1")._bar | where _baz == func_()`},
		{
			"Test add identifiers complex",
			New("").
				AddDatabase("f\"\"o").AddLiteral(".").
				AddTable("b\\a\\r").AddLiteral(" | where ").
				AddColumn("b\na\nz").AddLiteral(" == ").
				AddFunction("f_u_n\u1234c").AddLiteral("()"),
			`database("f\"\"o").["b\\a\\r"] | where ["b\na\nz"] == ["f_u_n\u1234c"]()`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := test.b.String()
			assert.Equal(t, test.expected, actual)
		})
	}
}
