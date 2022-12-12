package kql

import (
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestNonParameters(t *testing.T) {
	tests := []struct {
		name     string
		b        Builder
		expected string
	}{
		{"Test empty", NewBuilder(), ""},
		{"Test simple literal", NewBuilder().AddLiteral("foo"), "foo"},
		{"Test simple literal ctor", NewBuilderWithLiteral("foo"), "foo"},
		{"Test add literal", NewBuilderWithLiteral("foo").AddLiteral("bar"), "foobar"},
		{
			"Test add int",
			NewBuilderWithLiteral("MyTable | where i != ").AddValue(NewInt(32)).AddLiteral(" ;"),
			"MyTable | where i != int(32) ;",
		},
		{
			"Test add long",
			NewBuilderWithLiteral("MyTable | where i != ").AddValue(NewLong(32)).AddLiteral(" ;"),
			"MyTable | where i != long(32) ;",
		},
		{
			"Test add real",
			NewBuilderWithLiteral("MyTable | where i != ").AddValue(NewReal(32.5)).AddLiteral(" ;"),
			"MyTable | where i != real(32.5) ;",
		},
		{
			"Test add bool",
			NewBuilderWithLiteral("MyTable | where i != ").AddValue(NewBool(true)).AddLiteral(" ;"),
			"MyTable | where i != bool(true) ;",
		},
		{
			"Test add datetime",
			NewBuilderWithLiteral(
				"MyTable | where i != ",
			).AddValue(NewDateTime(time.Date(2019, 1, 2, 3, 4, 5, 600, time.UTC))).
				AddLiteral(" ;"),
			"MyTable | where i != datetime(2019-01-02T03:04:05.0000006Z) ;",
		},
		{
			"Test add duration",
			NewBuilderWithLiteral(
				"MyTable | where i != ",
			).AddValue(NewTimespan(1*time.Hour + 2*time.Minute + 3*time.Second + 4*time.Microsecond)).
				AddLiteral(" ;"),
			"MyTable | where i != timespan(01:02:03.0004000) ;",
		},
		{
			"Test add duration with days",
			NewBuilderWithLiteral(
				"MyTable | where i != ",
			).AddValue(NewTimespan(49*time.Hour + 2*time.Minute + 3*time.Second + 4*time.Microsecond)).
				AddLiteral(" ;"),
			"MyTable | where i != timespan(2.01:02:03.0004000) ;",
		},
		{
			"Test add dynamic",
			NewBuilderWithLiteral(
				"MyTable | where i != ",
			).AddValue(NewDynamic(`{"a": 3, "b": 5.4}`)).
				AddLiteral(" ;"),
			`MyTable | where i != dynamic("{\"a\": 3, \"b\": 5.4}") ;`,
		},
		{
			"Test add guid",
			NewBuilderWithLiteral(
				"MyTable | where i != ",
			).AddValue(NewGUID(uuid.MustParse("12345678-1234-1234-1234-123456789012"))).
				AddLiteral(" ;"),
			"MyTable | where i != guid(12345678-1234-1234-1234-123456789012) ;",
		},
		{
			"Test add string simple",
			NewBuilderWithLiteral(
				"MyTable | where i != ",
			).AddValue(NewString("foo")).
				AddLiteral(" ;"),
			"MyTable | where i != \"foo\" ;",
		},
		{
			"Test add string with quote",
			NewBuilderWithLiteral(
				"MyTable | where i != ",
			).AddValue(NewString("foo\"bar")).
				AddLiteral(" ;"),
			"MyTable | where i != \"foo\\\"bar\" ;",
		},
		{"Test add identifiers",
			NewBuilder().
				AddIdentifier(NewDatabase("foo_1")).
				AddLiteral(".").
				AddIdentifier(NewTable("_bar")).
				AddLiteral(" | where ").
				AddIdentifier(NewColumn("_baz")).
				AddLiteral(" == ").
				AddIdentifier(NewFunction("func_")).
				AddLiteral("() ;"),
			`database("foo_1").table("_bar") | where _baz == func_() ;`},
		{"Test add identifiers complex",
			NewBuilder().
				AddIdentifier(NewDatabase("f\"\"o")).
				AddLiteral(".").
				AddIdentifier(NewTable("b\\a\\r")).
				AddLiteral(" | where ").
				AddIdentifier(NewColumn("b\na\nz")).
				AddLiteral(" == ").
				AddIdentifier(NewFunction("f_u_n\u1234c")).
				AddLiteral("() ;"),
			`database("f\"\"o").table("b\\a\\r") | where ["b\na\nz"] == ["f_u_n\u1234c"]() ;`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := test.b.Build()
			assert.Equal(t, test.expected, actual.Query())
		})
	}
}

func TestParametersWithDefaults(t *testing.T) {
	tests := []struct {
		name           string
		b              ParameterBuilder
		expectedQuery  string
		expectedParams map[string]Parameter
	}{
		{
			name: "Test add int",
			b: NewBuilderWithLiteral(
				"MyTable | where i != MyParam ;",
			).ToParameterBuilder().
				AddParameter(NewIntParameterWithDefault("MyParam", 32)),
			expectedQuery: "declare query_parameters(MyParam:int=int(32));\nMyTable | where i != MyParam ;",
			expectedParams: map[string]Parameter{
				"MyParam": &parameter{"MyParam", types.Int, NewInt(32), true},
			},
		},
		{
			name: "Test add long",
			b: NewBuilderWithLiteral(
				"MyTable | where i != MyParam ;",
			).ToParameterBuilder().
				AddParameter(NewLongParameterWithDefault("MyParam", 32)),
			expectedQuery: "declare query_parameters(MyParam:long=long(32));\nMyTable | where i != MyParam ;",
			expectedParams: map[string]Parameter{
				"MyParam": &parameter{"MyParam", types.Long, NewLong(32), true},
			},
		},
		{
			name: "Test add real",
			b: NewBuilderWithLiteral(
				"MyTable | where i != MyParam ;",
			).ToParameterBuilder().
				AddParameter(NewRealParameterWithDefault("MyParam", 32.5)),
			expectedQuery: "declare query_parameters(MyParam:real=real(32.5));\nMyTable | where i != MyParam ;",
			expectedParams: map[string]Parameter{
				"MyParam": &parameter{"MyParam", types.Real, NewReal(32.5), true},
			},
		},
		{
			name: "Test add bool",
			b: NewBuilderWithLiteral(
				"MyTable | where i != MyParam ;",
			).ToParameterBuilder().
				AddParameter(NewBoolParameterWithDefault("MyParam", true)),
			expectedQuery: "declare query_parameters(MyParam:bool=bool(true));\nMyTable | where i != MyParam ;",
			expectedParams: map[string]Parameter{
				"MyParam": &parameter{"MyParam", types.Bool, NewBool(true), true},
			},
		},
		{
			name: "Test add datetime",
			b: NewBuilderWithLiteral(
				"MyTable | where i != MyParam ;",
			).ToParameterBuilder().
				AddParameter(NewDateTimeParameterWithDefault("MyParam", time.Date(2019, 1, 2, 3, 4, 5, 600, time.UTC))),
			expectedQuery: "declare query_parameters(MyParam:datetime=datetime(2019-01-02T03:04:05.0000006Z));\nMyTable | where i != MyParam ;",
			expectedParams: map[string]Parameter{
				"MyParam": &parameter{
					"MyParam",
					types.DateTime,
					NewDateTime(time.Date(2019, 1, 2, 3, 4, 5, 600, time.UTC)),
					true,
				},
			},
		},
		{
			name: "Test add timespan",
			b: NewBuilderWithLiteral(
				"MyTable | where i != MyParam ;",
			).ToParameterBuilder().
				AddParameter(NewTimespanParameterWithDefault("MyParam", 1*time.Hour+2*time.Minute+3*time.Second+4*time.Microsecond)),
			expectedQuery: "declare query_parameters(MyParam:timespan=timespan(01:02:03.0004000));\nMyTable | where i != MyParam ;",
			expectedParams: map[string]Parameter{
				"MyParam": &parameter{
					"MyParam",
					types.Timespan,
					NewTimespan(1*time.Hour + 2*time.Minute + 3*time.Second + 4*time.Microsecond),
					true,
				},
			},
		},
		{
			name: "Test add guid",
			b: NewBuilderWithLiteral(
				"MyTable | where i != MyParam ;",
			).ToParameterBuilder().
				AddParameter(NewGUIDParameterWithDefault("MyParam", uuid.MustParse("12345678-1234-1234-1234-123456789012"))),
			expectedQuery: "declare query_parameters(MyParam:guid=guid(12345678-1234-1234-1234-123456789012));\nMyTable | where i != MyParam ;",
			expectedParams: map[string]Parameter{
				"MyParam": &parameter{
					"MyParam",
					types.GUID,
					NewGUID(uuid.MustParse("12345678-1234-1234-1234-123456789012")),
					true,
				},
			},
		},
		{
			name: "Test add string",
			b: NewBuilderWithLiteral(
				"MyTable | where i != MyParam ;",
			).ToParameterBuilder().
				AddParameter(NewStringParameterWithDefault("MyParam", "foo")),
			expectedQuery: "declare query_parameters(MyParam:string=\"foo\");\nMyTable | where i != MyParam ;",
			expectedParams: map[string]Parameter{
				"MyParam": &parameter{"MyParam", types.String, NewString("foo"), true},
			},
		},
		{
			name: "Test add multiple parameters",
			b: NewBuilderWithLiteral("MyTable | where i != MyParam1 && b != MyParam2 ;").
				ToParameterBuilder().
				AddParameter(NewIntParameterWithDefault("MyParam1", 32)).
				AddParameter(NewStringParameterWithDefault("MyParam2", "foo")),
			expectedQuery: "declare query_parameters(MyParam1:int=int(32),MyParam2:string=\"foo\");\nMyTable | where i != MyParam1 && b != MyParam2 ;",
			expectedParams: map[string]Parameter{
				"MyParam1": &parameter{"MyParam1", types.Int, NewInt(32), true},
				"MyParam2": &parameter{"MyParam2", types.String, NewString("foo"), true},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := test.b.BuildWithValues(map[string]Value{})
			assert.NoError(t, err)
			assert.Equal(t, test.expectedQuery, actual.Query())
			assert.Equal(t, test.expectedParams, actual.Parameters())
		})
	}
}

func TestParametersWithValues(t *testing.T) {
	tests := []struct {
		name           string
		b              ParameterBuilder
		expectedValues map[string]Value
		expectedQuery  string
		expectedParams map[string]Parameter
	}{
		{
			name: "Test add int",
			b: NewBuilderWithLiteral(
				"MyTable | where i != MyParam ;",
			).ToParameterBuilder().
				AddParameter(NewIntParameter("MyParam")),
			expectedValues: map[string]Value{"MyParam": NewInt(32)},
			expectedQuery:  "declare query_parameters(MyParam:int);\nMyTable | where i != MyParam ;",
			expectedParams: map[string]Parameter{
				"MyParam": &parameter{"MyParam", types.Int, nil, false},
			},
		},
		{
			name: "Test add long",
			b: NewBuilderWithLiteral(
				"MyTable | where i != MyParam ;",
			).ToParameterBuilder().
				AddParameter(NewLongParameter("MyParam")),
			expectedValues: map[string]Value{"MyParam": NewLong(32)},
			expectedQuery:  "declare query_parameters(MyParam:long);\nMyTable | where i != MyParam ;",
			expectedParams: map[string]Parameter{
				"MyParam": &parameter{"MyParam", types.Long, nil, false},
			},
		},
		{
			name: "Test add real",
			b: NewBuilderWithLiteral(
				"MyTable | where i != MyParam ;",
			).ToParameterBuilder().
				AddParameter(NewRealParameter("MyParam")),
			expectedValues: map[string]Value{"MyParam": NewReal(32.5)},
			expectedQuery:  "declare query_parameters(MyParam:real);\nMyTable | where i != MyParam ;",
			expectedParams: map[string]Parameter{
				"MyParam": &parameter{"MyParam", types.Real, nil, false},
			},
		},
		{
			name: "Test add bool",
			b: NewBuilderWithLiteral(
				"MyTable | where i != MyParam ;",
			).ToParameterBuilder().
				AddParameter(NewBoolParameter("MyParam")),
			expectedValues: map[string]Value{"MyParam": NewBool(true)},
			expectedQuery:  "declare query_parameters(MyParam:bool);\nMyTable | where i != MyParam ;",
			expectedParams: map[string]Parameter{
				"MyParam": &parameter{"MyParam", types.Bool, nil, false},
			},
		},
		{
			name: "Test add datetime",
			b: NewBuilderWithLiteral(
				"MyTable | where i != MyParam ;",
			).ToParameterBuilder().
				AddParameter(NewDateTimeParameter("MyParam")),
			expectedValues: map[string]Value{
				"MyParam": NewDateTime(time.Date(2019, 1, 2, 3, 4, 5, 600, time.UTC)),
			},
			expectedQuery: "declare query_parameters(MyParam:datetime);\nMyTable | where i != MyParam ;",
			expectedParams: map[string]Parameter{
				"MyParam": &parameter{"MyParam", types.DateTime, nil, false},
			},
		},
		{
			name: "Test add timespan",
			b: NewBuilderWithLiteral(
				"MyTable | where i != MyParam ;",
			).ToParameterBuilder().
				AddParameter(NewTimespanParameter("MyParam")),
			expectedValues: map[string]Value{
				"MyParam": NewTimespan(
					1*time.Hour + 2*time.Minute + 3*time.Second + 4*time.Microsecond,
				),
			},
			expectedQuery: "declare query_parameters(MyParam:timespan);\nMyTable | where i != MyParam ;",
			expectedParams: map[string]Parameter{
				"MyParam": &parameter{"MyParam", types.Timespan, nil, false},
			},
		},
		{
			name: "Test add guid",
			b: NewBuilderWithLiteral(
				"MyTable | where i != MyParam ;",
			).ToParameterBuilder().
				AddParameter(NewGUIDParameter("MyParam")),
			expectedValues: map[string]Value{
				"MyParam": NewGUID(uuid.MustParse("12345678-1234-1234-1234-123456789012")),
			},
			expectedQuery: "declare query_parameters(MyParam:guid);\nMyTable | where i != MyParam ;",
			expectedParams: map[string]Parameter{
				"MyParam": &parameter{"MyParam", types.GUID, nil, false},
			},
		},
		{
			name: "Test add dynamic",
			b: NewBuilderWithLiteral(
				"MyTable | where i != MyParam ;",
			).ToParameterBuilder().
				AddParameter(NewDynamicParameter("MyParam")),
			expectedValues: map[string]Value{"MyParam": NewDynamic("foo")},
			expectedQuery:  "declare query_parameters(MyParam:dynamic);\nMyTable | where i != MyParam ;",
			expectedParams: map[string]Parameter{
				"MyParam": &parameter{"MyParam", types.Dynamic, nil, false},
			},
		},
		{
			name: "Test add string",
			b: NewBuilderWithLiteral(
				"MyTable | where i != MyParam ;",
			).ToParameterBuilder().
				AddParameter(NewStringParameter("MyParam")),
			expectedValues: map[string]Value{"MyParam": NewString("foo")},
			expectedQuery:  "declare query_parameters(MyParam:string);\nMyTable | where i != MyParam ;",
			expectedParams: map[string]Parameter{
				"MyParam": &parameter{"MyParam", types.String, nil, false},
			},
		},
		{
			name: "Test add multiple parameters",
			b: NewBuilderWithLiteral(
				"MyTable | where i != MyParam1 and i != MyParam2 ;",
			).ToParameterBuilder().
				AddParameter(NewStringParameter("MyParam1")).
				AddParameter(NewStringParameter("MyParam2")),
			expectedValues: map[string]Value{
				"MyParam1": NewString("foo"),
				"MyParam2": NewString("bar"),
			},
			expectedQuery: "declare query_parameters(MyParam1:string,MyParam2:string);\nMyTable | where i != MyParam1 and i != MyParam2 ;",
			expectedParams: map[string]Parameter{
				"MyParam1": &parameter{"MyParam1", types.String, nil, false},
				"MyParam2": &parameter{"MyParam2", types.String, nil, false},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, err := test.b.BuildWithValues(test.expectedValues)
			assert.NoError(t, err)
			assert.Equal(t, test.expectedQuery, query.Query())
			assert.Equal(t, test.expectedParams, query.Parameters())
			assert.Equal(t, test.expectedValues, query.Values())
		})
	}
}
