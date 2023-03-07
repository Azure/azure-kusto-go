package kql

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestToDeclarationString(t *testing.T) {
	tests := []struct {
		name     string
		b        Builder
		qp       *StatementQueryParameters
		failFlag bool
		expected []string
	}{
		{"Test empty", NewStatementBuilder(""), NewStatementQueryParameters(), false, []string{"\n"}},
		{"Test single add", NewStatementBuilder(""), NewStatementQueryParameters().AddString("foo", "bar"), false, []string{"declare", "query_parameters(", "foo:string", ");\n"}},
		{"Test parameters standard",
			NewStatementBuilder("database(databaseName).table(tableName) | where column == txt ;"),
			NewStatementQueryParameters().
				AddString("databaseName", "foo_1").
				AddString("tableName", "_bar").
				AddString("txt", "txt_"),
			false,
			[]string{"declare", "query_parameters(", "databaseName:string", "tableName:string", "txt:string", ");\ndatabase(databaseName).table(tableName) | where column == txt ;"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q := test.b.String()
			params := test.qp.ToDeclarationString()
			actual := fmt.Sprintf("%s\n%s", params, q)
			for _, sub := range test.expected {
				require.True(t, strings.Contains(actual, sub))
			}
		})
	}
}
func TestToParameterCollection(t *testing.T) {
	dt, _ := time.Parse(time.RFC3339Nano, "2020-03-04T14:05:01.3109965Z")
	ts, _ := time.ParseDuration("1h23m45.6789s")
	guid, _ := uuid.Parse("74be27de-1e4e-49d9-b579-fe0b331d3642")

	tests := []struct {
		name     string
		b        Builder
		qp       *StatementQueryParameters
		expected map[string]string
	}{
		{"Test empty", NewStatementBuilder(""), NewStatementQueryParameters(), map[string]string{}},
		{"Test single", NewStatementBuilder(""), NewStatementQueryParameters().AddString("foo", "bar"), map[string]string{"foo": "\"bar\""}},
		{"Test complex", NewStatementBuilder(""), NewStatementQueryParameters().
			AddString("foo", "bar").
			AddInt("num", 1).
			AddDecimal("dec", decimal.RequireFromString("2.00000000000001")).
			AddDateTime("dt", dt).
			AddTimespan("span", ts).
			AddDynamic("obj", map[string]interface{}{
				"moshe": "value"}).
			AddBool("b", true).
			AddReal("rl", 0.01).
			AddLong("lg", 9223372036854775807).
			AddGUID("guid", guid), map[string]string{
			"foo":  "\"bar\"",
			"num":  "int(1)",
			"dec":  "decimal(2.00000000000001)",
			"dt":   "datetime(2020-03-04T14:05:01.3109965Z)",
			"span": "timespan(01:23:45.6789000)",
			"obj":  "dynamic({\"moshe\":\"value\"})",
			"b":    "bool(true)",
			"rl":   "real(0.01)",
			"lg":   "long(9223372036854775807)",
			"guid": "guid(74be27de-1e4e-49d9-b579-fe0b331d3642)",
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := test.qp.ToParameterCollection()
			require.Equal(t, test.expected, params)
		})
	}

}
