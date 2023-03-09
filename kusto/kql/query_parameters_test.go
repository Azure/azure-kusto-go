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

func TestQueryParameters(t *testing.T) {
	dt, _ := time.Parse(time.RFC3339Nano, "2020-03-04T14:05:01.3109965Z")
	ts, _ := time.ParseDuration("1h23m45.6789s")
	guid, _ := uuid.Parse("74be27de-1e4e-49d9-b579-fe0b331d3642")

	tests := []struct {
		name       string
		b          Builder
		qp         *StatementQueryParameters
		dsExpected []string
		pcExpected map[string]string
	}{
		{
			"Test empty",
			NewStatementBuilder(""),
			NewStatementQueryParameters(),
			[]string{"\n"},
			map[string]string{}},
		{
			"Test single add",
			NewStatementBuilder(""),
			NewStatementQueryParameters().
				AddString("foo", "bar"),
			[]string{"declare",
				"query_parameters(",
				"foo:string",
				");\n"},
			map[string]string{"foo": "\"bar\""}},
		{
			"Test standard",
			NewStatementBuilder("database(databaseName).table(tableName) | where column == txt ;"),
			NewStatementQueryParameters().
				AddString("databaseName", "foo_1").
				AddString("tableName", "_bar").
				AddString("txt", "txt_"),
			[]string{"declare",
				"query_parameters(",
				"databaseName:string",
				"tableName:string",
				"txt:string",
				");\ndatabase(databaseName).table(tableName) | where column == txt ;"},
			map[string]string{
				"databaseName": "\"foo_1\"",
				"tableName":    "\"_bar\"",
				"txt":          "\"txt_\"",
			}},
		{
			"Test complex",
			NewStatementBuilder("where vnum == num and vdec == dec and vdate == dt and vspan == span and tostring(vobj) == tostring(obj) and vb == b and vreal == rl and vstr == str and vlong == lg and vguid == guid"),
			NewStatementQueryParameters().
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
				AddGUID("guid", guid),
			[]string{"declare",
				"query_parameters(",
				"foo:string",
				"num:int",
				"dec:decimal",
				"dt:datetime",
				"span:timespan",
				"obj:dynamic",
				"b:bool",
				"rl:real",
				"lg:long",
				"guid:guid",
				");\nwhere vnum == num and vdec == dec and vdate == dt and vspan == span and tostring(vobj) == tostring(obj) and vb == b and vreal == rl and vstr == str and vlong == lg and vguid == guid"},
			map[string]string{
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
		{
			"Test unusual values",
			NewStatementBuilder("database(databaseName).table(tableName) | where column == txt ;"),
			NewStatementQueryParameters().
				AddString("databaseName", "f\"\"o").
				AddString("tableName", "b\a\r").
				AddString("txt", "f_u_n\u1234c"),
			[]string{"declare",
				"query_parameters(",
				"databaseName:string",
				"tableName:string",
				"txt:string",
				");\ndatabase(databaseName).table(tableName) | where column == txt ;"},
			map[string]string{
				"databaseName": "\"f\\\"\\\"o\"",
				"tableName":    "\"b\\a\\r\"",
				"txt":          "\"f_u_n\\u1234c\"",
			}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q := test.b.String()
			declarationString := test.qp.ToDeclarationString()
			actual := fmt.Sprintf("%s\n%s", declarationString, q)
			for _, sub := range test.dsExpected {
				require.True(t, strings.Contains(actual, sub))
			}

			params := test.qp.ToParameterCollection()
			require.Equal(t, test.pcExpected, params)
		})
	}
}
