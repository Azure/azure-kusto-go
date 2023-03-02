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
		{"Test add string", NewStatementBuilder(""), NewStatementQueryParameters().AddString("foo", "string", "bar"), "declare query_parameters(foo:string);\n"},
		{"Test add identifiers", // test might fail at times due to the pseudo-random nature of map that will sometimes change the order of the declaration string.
			NewStatementBuilder("database(databaseName).table(tableName) | where column == txt ;"),
			NewStatementQueryParameters().
				AddString("databaseName", "string", "foo_1").
				AddString("tableName", "string", "_bar").
				AddString("txt", "string", "txt_"),
			"declare query_parameters(databaseName:string, tableName:string, txt:string);\ndatabase(databaseName).table(tableName) | where column == txt ;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q := test.b.String()
			params := test.qp.ToDeclarationString()
			assert.Equal(t, test.expected, fmt.Sprintf("%s\n%s", params, q))
		})
	}
}
