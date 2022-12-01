package kusto

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestParamType(t *testing.T) {
	t.Parallel()

	now := time.Now()
	uu := uuid.New()

	tests := []struct {
		desc    string
		param   ParamType
		err     bool
		wantStr string
	}{
		{
			desc: "Type not valid",
			param: ParamType{
				Type: "notValid",
			},
			err: true,
		},
		{
			desc: "Bad Default for types.Bool",
			param: ParamType{
				Type:    types.Bool,
				Default: 1,
			},
			err: true,
		},
		{
			desc: "Bad Default for types.DateTime",
			param: ParamType{
				Type:    types.DateTime,
				Default: time.Duration(1),
			},
			err: true,
		},
		{
			desc: "Bad Default for types.Dynamic",
			param: ParamType{
				Type:    types.Dynamic,
				Default: `{}`, // This is valid JSON, but Dynamic can't have a default type
			},
			err: true,
		},
		{
			desc: "Bad Default for types.GUID",
			param: ParamType{
				Type:    types.GUID,
				Default: 1,
			},
			err: true,
		},
		{
			desc: "Bad Default for types.Int",
			param: ParamType{
				Type:    types.Int,
				Default: int64(1),
			},
			err: true,
		},
		{
			desc: "Bad Default for types.Long",
			param: ParamType{
				Type:    "notValid",
				Default: 1, // Should be an int
			},
			err: true,
		},
		{
			desc: "Bad Default for types.Real",
			param: ParamType{
				Type:    types.Real,
				Default: 1,
			},
			err: true,
		},
		{
			desc: "Bad Default for types.String",
			param: ParamType{
				Type:    types.String,
				Default: 1,
			},
			err: true,
		},
		{
			desc: "Bad Default for types.Timespan",
			param: ParamType{
				Type:    types.Timespan,
				Default: 1,
			},
			err: true,
		},
		{
			desc: "Bad Default for types.Decimal",
			param: ParamType{
				Type:    types.Decimal,
				Default: "hello",
			},
			err: true,
		},
		{
			desc: "Bad Default for types.Decimal - only decimal point",
			param: ParamType{
				Type:    types.Decimal,
				Default: ".",
			},
			err: true,
		},
		{
			desc: "Bad Default for types.Decimal - nil big.Float",
			param: ParamType{
				Type:    types.Decimal,
				Default: (*big.Float)(nil),
			},
			err: true,
		},
		{
			desc: "Bad Default for types.Decimal - nil big.Int",
			param: ParamType{
				Type:    types.Decimal,
				Default: (*big.Int)(nil),
			},
			err: true,
		},
		{
			desc: "Success Default for types.Bool",
			param: ParamType{
				Type:    types.Bool,
				Default: true,
				name:    "my_value",
			},
			wantStr: "my_value:bool = true",
		},
		{
			desc: "Success Default for types.DateTime",
			param: ParamType{
				Type:    types.DateTime,
				Default: now,
				name:    "my_value",
			},
			wantStr: fmt.Sprintf("my_value:datetime = %s", now.Format(time.RFC3339Nano)),
		},
		{
			desc: "Success Default for types.Dynamic",
			param: ParamType{
				Type: types.Dynamic,
				name: "my_value",
			},
			wantStr: "my_value:dynamic",
		},
		{
			desc: "Success Default for types.GUID",
			param: ParamType{
				Type:    types.GUID,
				Default: uu,
				name:    "my_value",
			},
			wantStr: fmt.Sprintf("my_value:guid = %s", uu.String()),
		},
		{
			desc: "Success Default for types.Int",
			param: ParamType{
				Type:    types.Int,
				Default: int32(1),
				name:    "my_value",
			},
			wantStr: "my_value:int = 1",
		},
		{
			desc: "Success Default for types.Long",
			param: ParamType{
				Type:    types.Long,
				Default: int64(1),
				name:    "my_value",
			},
			wantStr: "my_value:long = 1",
		},
		{
			desc: "Success Default for types.Real",
			param: ParamType{
				Type:    types.Real,
				Default: 1.0,
				name:    "my_value",
			},
			wantStr: "my_value:real = 1.000000",
		},
		{
			desc: "Success Default for types.String",
			param: ParamType{
				Type:    types.String,
				Default: "hello",
				name:    "my_value",
			},
			wantStr: "my_value:string = \"hello\"",
		},
		/*
			{
				desc: "Success Default for types.Timespan",
				param: ParamType{
					Type:    types.Decimal,
					Default: ....,
					name: "my_value"
				},
				wantStr: "my_value:timespan = true",
			},
		*/
		{
			desc: "Success Default for types.Decimal",
			param: ParamType{
				Type:    types.Decimal,
				Default: "1.349",
				name:    "my_value",
			},
			wantStr: "my_value:decimal = 1.349",
		},
		{
			desc: "Success no decimal point for types.Decimal",
			param: ParamType{
				Type:    types.Decimal,
				Default: "1",
				name:    "my_value",
			},
			wantStr: "my_value:decimal = 1",
		},
		{
			desc: "Success elided left side for types.Decimal",
			param: ParamType{
				Type:    types.Decimal,
				Default: ".1",
				name:    "my_value",
			},
			wantStr: "my_value:decimal = .1",
		},
		{
			desc: "Success elided right side for types.Decimal",
			param: ParamType{
				Type:    types.Decimal,
				Default: "1.",
				name:    "my_value",
			},
			wantStr: "my_value:decimal = 1.",
		},
	}

	for _, test := range tests {
		err := test.param.validate()
		switch {
		case err == nil && test.err:
			t.Errorf("TestParamType(%s): got err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestParamType(%s): got err == %s, want err != nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if test.wantStr != test.param.string() {
			t.Errorf("TestParamType(%s): got %q, want %q", test.desc, test.param.string(), test.wantStr)
		}
	}
}

func TestDefinitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc    string
		with    map[string]ParamType
		err     bool
		wantStr string
	}{
		{
			desc: "name contains spaces",
			with: ParamTypes{
				"name space": ParamType{Type: types.Bool},
			},
			err: true,
		},
		{
			desc: "Param doesn't validate",
			with: ParamTypes{
				"name": ParamType{Type: "hello"},
			},
			err: true,
		},
		{
			desc:    "Success with no paramenters (returns empty string)",
			wantStr: "",
		},
		{
			desc: "Success",
			with: ParamTypes{
				"HasLicense": ParamType{Type: types.Bool, Default: false},
				"FirstName":  ParamType{Type: types.String},
			},
			wantStr: "declare query_parameters(FirstName:string, HasLicense:bool = false);",
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			params := NewDefinitions()
			var err error

			params, err = params.With(test.with)

			if test.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			assert.Equal(t, test.wantStr, params.String())

			clone := params.clone()
			_, err = clone.With(map[string]ParamType{"hellyeah": {Type: types.Bool}})
			assert.NoError(t, err)

			if _, ok := params.m["hellyeah"]; ok {
				assert.Fail(t, "clone modification modified original")
			}
		})

	}
}

func TestParameters(t *testing.T) {
	t.Parallel()

	now := time.Now()
	uu := uuid.New()

	tests := []struct {
		desc    string
		qParams Definitions
		qValues Parameters
		err     bool
		want    map[string]string
	}{
		{
			desc:    "Value key doesn't exist in Parameters",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": {Type: types.Bool}}),
			qValues: NewParameters().Must(map[string]interface{}{"key2": true}),
			err:     true,
		},
		{
			desc:    "Should be time.Time, isn't",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": {Type: types.DateTime}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": 1}),
			err:     true,
		},
		{
			desc:    "Should be uuid.UUID, isn't",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": {Type: types.GUID}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": 1}),
			err:     true,
		},
		{
			desc:    "Should be int32, isn't",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": {Type: types.Int}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": int64(1)}),
			err:     true,
		},
		{
			desc:    "Should be int64, isn't",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": {Type: types.Long}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": int32(1)}),
			err:     true,
		},
		{
			desc:    "Should be float64, isn't",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": {Type: types.Real}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": 1}),
			err:     true,
		},
		{
			desc:    "Should be string, isn't",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": {Type: types.String}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": 1}),
			err:     true,
		},
		{
			desc:    "Should be time.Duration, isn't",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": {Type: types.Timespan}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": 1}),
			err:     true,
		},
		{
			desc:    "Should be string representing decimal or *big.Float or *big.Int, isn't",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": {Type: types.Decimal}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": 1}),
			err:     true,
		},
		{
			desc:    "Success time.Time",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": {Type: types.DateTime}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": now}),
			want:    map[string]string{"key1": fmt.Sprintf("datetime(%s)", now.Format(time.RFC3339Nano))},
		},
		{
			desc:    "Success uuid.UUID",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": {Type: types.GUID}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": uu}),
			want:    map[string]string{"key1": fmt.Sprintf("guid(%s)", uu.String())},
		},
		{
			desc:    "Success int32",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": {Type: types.Int}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": int32(1)}),
			want:    map[string]string{"key1": fmt.Sprintf("int(%d)", 1)},
		},
		{
			desc:    "Success int64",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": {Type: types.Long}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": int64(1)}),
			want:    map[string]string{"key1": fmt.Sprintf("long(%d)", 1)},
		},
		{
			desc:    "Success float64",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": {Type: types.Real}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": 1.1}),
			want:    map[string]string{"key1": fmt.Sprintf("real(%f)", 1.1)},
		},
		{
			desc:    "Success string",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": {Type: types.String}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": "string"}),
			want:    map[string]string{"key1": "string"},
		},
		{
			desc:    "Success time.Duration",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": {Type: types.Timespan}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": 3 * time.Second}),
			want:    map[string]string{"key1": "timespan(00:00:03)"},
		},
		{
			desc:    "Success string representing decimal",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": {Type: types.Decimal}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": "1.3"}),
			want:    map[string]string{"key1": fmt.Sprintf("decimal(%s)", "1.3")},
		},
		{
			desc:    "Success *big.Float for decimal",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": {Type: types.Decimal}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": big.NewFloat(3.2)}),
			want:    map[string]string{"key1": fmt.Sprintf("decimal(%s)", big.NewFloat(3.2).String())},
		},
		{
			desc:    "Success *big.Int for decimal",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": {Type: types.Decimal}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": big.NewInt(5)}),
			want:    map[string]string{"key1": fmt.Sprintf("decimal(%s)", big.NewInt(5).String())},
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got, err := test.qValues.toParameters(test.qParams)
			if test.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			assert.EqualValues(t, test.want, got)
		})

	}
}

func TestStmt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc         string
		params       Definitions
		paramsZeroOk bool
		values       Parameters
		valuesZeroOk bool
		qpErr        bool
		vErr         bool
		wantStr      string
		wantValues   map[string]string
	}{
		{
			desc:         "Error: QueryParamters are empty",
			qpErr:        true,
			paramsZeroOk: true,
		},
		{
			desc:         "Error: QueryValues are empty",
			vErr:         true,
			valuesZeroOk: true,
		},

		{
			desc:   "Error: QueryValues doesn't validate",
			values: NewParameters().Must(QueryValues{"key": true}),
			vErr:   true,
		},
		{
			desc:    "Success: Just a query statement, no params or values",
			wantStr: "|query",
		},
		{
			desc: "Success: Just a query + params, no values",
			params: NewDefinitions().Must(
				ParamTypes{
					"key1": ParamType{Type: types.Bool},
					"key2": ParamType{Type: types.String, Default: "hello"},
				},
			),
			wantStr: "|query",
		},
		{
			desc: "Success: Everything",
			params: NewDefinitions().Must(
				ParamTypes{
					"key1": ParamType{Type: types.Bool},
					"key2": ParamType{Type: types.DateTime},
					"key3": ParamType{Type: types.Dynamic},
					"key4": ParamType{Type: types.GUID},
					"key5": ParamType{Type: types.Int},
					"key6": ParamType{Type: types.Long},
					"key7": ParamType{Type: types.Real},
					"key8": ParamType{Type: types.String},
					// "key9": ParamType{Type: types.Timespan},
					"key10": ParamType{Type: types.Decimal},
				},
			),
			wantStr: "|query",
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			stmt := NewStmt("|query")

			var qpErr, vErr error
			if !test.params.IsZero() || test.paramsZeroOk {
				stmt, qpErr = stmt.WithDefinitions(test.params)

				if test.qpErr {
					assert.Error(t, qpErr)
					return
				}

				assert.NoError(t, qpErr)
			}

			if !test.values.IsZero() || test.valuesZeroOk {
				stmt, vErr = stmt.WithParameters(test.values)
				if test.vErr {
					assert.Error(t, vErr)
					return
				}

				assert.NoError(t, vErr)
			}

			gotStr := stmt.String()

			wantStr := buildQueryStr(test.wantStr, test.params)

			assert.Equal(t, wantStr, gotStr)

			assert.EqualValues(t, test.wantValues, stmt.params.outM)
		})

	}
}

func buildQueryStr(query string, params Definitions) string {
	ps := params.String()
	if ps != "" {
		return params.String() + "\n" + query
	}
	return query
}

func TestNormalizeName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		desc               string
		query              string
		forceNormalization bool
		want               Stmt
	}{
		{
			desc:               "Success simple query",
			query:              "KustoLogs",
			forceNormalization: false,
			want:               NewStmt("KustoLogs"),
		},
		{
			desc:               "Success simple query",
			query:              "KustoLogs",
			forceNormalization: true,
			want:               NewStmt("[\"KustoLogs\"]"),
		},
		{
			desc:               "Success empty string",
			query:              "",
			forceNormalization: false,
			want:               NewStmt(""),
		},
		{
			desc:               "Success special Quoting",
			query:              "&",
			forceNormalization: false,
			want:               NewStmt("[\"&\"]"),
		},
	}

	for _, test := range tests {
		test := test // capture
		s := NewStmt("")
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got := s.NormalizeName(test.query, test.forceNormalization)

			assert.EqualValues(t, test.want, got)
		})

	}
}

func TestRequiresQuoting(t *testing.T) {
	t.Parallel()
	tests := []struct {
		desc  string
		query string
		want  bool
	}{
		{
			desc:  "Success simple letter",
			query: "a",
			want:  false,
		},
		{
			desc:  "Success simple digit",
			query: "8",
			want:  true,
		},
		{
			desc:  "Success simple digit",
			query: "8a",
			want:  true,
		},
		{
			desc:  "Success simple digit",
			query: "a8",
			want:  false,
		},
		{
			desc:  "Success underscore",
			query: "_",
			want:  false,
		},
		{
			desc:  "Success empty string",
			query: "",
			want:  true,
		},
		{
			desc:  "Success special",
			query: "&",
			want:  true,
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got := RequiresQuoting(test.query)

			assert.EqualValues(t, test.want, got)
		})

	}
}

func TestAddQuotedString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		desc   string
		value  string
		hidden bool
		want   string
	}{
		{
			desc:  "Success empty string",
			value: "",
			want:  "",
		},
		{
			desc:  "Success simple string",
			value: "abcd",
			want:  "\"abcd\"",
		},
		{
			desc:   "Success hidden",
			value:  "abcd",
			hidden: true,
			want:   "h\"abcd\"",
		},
		{
			desc:  "Success non latin escaping (should be escaped)",
			value: "aאbcd",
			want:  "\"a\\u5d0bcd\"",
		},
		{
			desc:  "Success case \\'",
			value: "a'bcd",
			want:  "\"a\\'bcd\"",
		},
		{
			desc:  "Success case \\\"",
			value: "a\"bcd",
			want:  "\"a\\\"bcd\"",
		},
		{
			desc:  "Success case \\\\",
			value: "a\\bcd",
			want:  "\"a\\\\bcd\"",
		},
		{
			desc:  "Success case \\0",
			value: "a\x00bcd",
			want:  "\"a\\0bcd\"",
		},
		{
			desc:  "Success case \\a",
			value: "a\abcd",
			want:  "\"a\\abcd\"",
		},
		{
			desc:  "Success case \\b",
			value: "a\bbcd",
			want:  "\"a\\bbcd\"",
		},
		{
			desc:  "Success case \\f",
			value: "a\fbcd",
			want:  "\"a\\fbcd\"",
		},
		{
			desc:  "Success case \\n",
			value: "a\nbcd",
			want:  "\"a\\nbcd\"",
		},
		{
			desc:  "Success case \\r",
			value: "a\rbcd",
			want:  "\"a\\rbcd\"",
		},
		{
			desc:  "Success case \\t",
			value: "a\tbcd",
			want:  "\"a\\tbcd\"",
		},
		{
			desc:  "Success case \\v",
			value: "a\vbcd",
			want:  "\"a\\vbcd\"",
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got := AddQuotedString(test.value, test.hidden)

			assert.EqualValues(t, test.want, got)
		})

	}
}

func TestAddInt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		desc  string
		query int
		want  Stmt
	}{
		{
			desc:  "Success simple add",
			query: 7,
			want:  NewStmt("7"),
		},
	}

	for _, test := range tests {
		test := test // capture
		s := NewStmt("")
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got := s.AddInt(test.query)

			assert.EqualValues(t, test.want, got)
		})

	}
}
func TestAddFloat(t *testing.T) {
	t.Parallel()
	tests := []struct {
		desc  string
		query float64
		want  Stmt
	}{
		{
			desc:  "Success simple add",
			query: 7.7,
			want:  NewStmt("7.700000"),
		},
		{
			desc:  "Success casting",
			query: float64(7),
			want:  NewStmt("7.000000"),
		},
	}

	for _, test := range tests {
		test := test // capture
		s := NewStmt("")
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got := s.AddFloat64(test.query)

			assert.EqualValues(t, test.want, got)
		})

	}
}
func TestAddComplex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		desc  string
		query complex64
		want  Stmt
	}{
		{
			desc:  "Success simple add",
			query: complex(10, 11),
			want:  NewStmt("(10+11i)"),
		},
	}

	for _, test := range tests {
		test := test // capture
		s := NewStmt("")
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got := s.AddComplex64(test.query)

			assert.EqualValues(t, test.want, got)
		})

	}
}
func TestAddBool(t *testing.T) {
	t.Parallel()
	tests := []struct {
		desc  string
		query bool
		want  Stmt
	}{
		{
			desc:  "Success simple true",
			query: true,
			want:  NewStmt("true"),
		},
		{
			desc:  "Success simple false",
			query: false,
			want:  NewStmt("false"),
		},
	}

	for _, test := range tests {
		test := test // capture
		s := NewStmt("")
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got := s.AddBool(test.query)

			assert.EqualValues(t, test.want, got)
		})

	}
}
