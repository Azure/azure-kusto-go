package kql

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQueryParameters(t *testing.T) {
	tests := []struct {
		name     string
		b        Builder
		qp       *StatementQueryParameters
		expected string
	}{
		{"Test empty", NewStatementBuilder(""), NewStatementQueryParameters(), "\n"},
		{"Test add literal", NewStatementBuilder(""), NewStatementQueryParameters().AddLiteral("foo", "string", "bar"), "declare query_parameters(foo:string=bar);\n"},
		{"Test add identifiers",
			NewStatementBuilder("").
				AddDatabase("database").AddLiteral(".").
				AddTable("table").AddLiteral(" | where ").
				AddColumn("column").AddLiteral(" == ").
				AddFunction("function").AddLiteral("() ;"),
			NewStatementQueryParameters().
				AddLiteral("database", "string", "foo_1").
				AddLiteral("table", "string", "_bar").
				AddLiteral("column", "string", "_baz").
				AddLiteral("function", "string", "func_"),
			"declare query_parameters(database:string=foo_1, table:string=_bar, column:string=_baz, function:string=func_);\ndatabase(\"database\").table | where column == function() ;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q := test.b.Build()
			params := test.qp.ToDeclarationString()
			assert.Equal(t, test.expected, fmt.Sprintf("%s\n%s", params, q.Query()))
		})
	}
}
