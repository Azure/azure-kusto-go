package kusto

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"

	"github.com/google/uuid"
)

func TestParamType(t *testing.T) {
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
			desc: "Bad Default for CTBool",
			param: ParamType{
				Type:    CTBool,
				Default: 1,
			},
			err: true,
		},
		{
			desc: "Bad Default for CTDateTime",
			param: ParamType{
				Type:    CTDateTime,
				Default: time.Duration(1),
			},
			err: true,
		},
		{
			desc: "Bad Default for CTDynamic",
			param: ParamType{
				Type:    CTDynamic,
				Default: `{}`, // This is valid JSON, but Dynamic can't have a default type
			},
			err: true,
		},
		{
			desc: "Bad Default for CTGUID",
			param: ParamType{
				Type:    CTGUID,
				Default: 1,
			},
			err: true,
		},
		{
			desc: "Bad Default for CTInt",
			param: ParamType{
				Type:    CTInt,
				Default: int64(1),
			},
			err: true,
		},
		{
			desc: "Bad Default for CTLong",
			param: ParamType{
				Type:    "notValid",
				Default: 1, // Should be an int
			},
			err: true,
		},
		{
			desc: "Bad Default for CTReal",
			param: ParamType{
				Type:    CTReal,
				Default: 1,
			},
			err: true,
		},
		{
			desc: "Bad Default for CTString",
			param: ParamType{
				Type:    CTString,
				Default: 1,
			},
			err: true,
		},
		{
			desc: "Bad Default for CTTimespan",
			param: ParamType{
				Type:    CTTimespan,
				Default: 1,
			},
			err: true,
		},
		{
			desc: "Bad Default for CTDecimal",
			param: ParamType{
				Type:    CTDecimal,
				Default: "hello",
			},
			err: true,
		},
		{
			desc: "Success Default for CTBool",
			param: ParamType{
				Type:    CTBool,
				Default: true,
				name:    "my_value",
			},
			wantStr: "my_value:bool = true",
		},
		{
			desc: "Success Default for CTDateTime",
			param: ParamType{
				Type:    CTDateTime,
				Default: now,
				name:    "my_value",
			},
			wantStr: fmt.Sprintf("my_value:datetime = %s", now.Format(time.RFC3339Nano)),
		},
		{
			desc: "Success Default for CTDynamic",
			param: ParamType{
				Type: CTDynamic,
				name: "my_value",
			},
			wantStr: "my_value:dynamic",
		},
		{
			desc: "Success Default for CTGUID",
			param: ParamType{
				Type:    CTGUID,
				Default: uu,
				name:    "my_value",
			},
			wantStr: fmt.Sprintf("my_value:guid = %s", uu.String()),
		},
		{
			desc: "Success Default for CTInt",
			param: ParamType{
				Type:    CTInt,
				Default: int32(1),
				name:    "my_value",
			},
			wantStr: "my_value:int = 1",
		},
		{
			desc: "Success Default for CTLong",
			param: ParamType{
				Type:    CTLong,
				Default: int64(1),
				name:    "my_value",
			},
			wantStr: "my_value:long = 1",
		},
		{
			desc: "Success Default for CTReal",
			param: ParamType{
				Type:    CTReal,
				Default: 1.0,
				name:    "my_value",
			},
			wantStr: "my_value:real = 1.000000",
		},
		{
			desc: "Success Default for CTString",
			param: ParamType{
				Type:    CTString,
				Default: "hello",
				name:    "my_value",
			},
			wantStr: "my_value:string = \"hello\"",
		},
		/*
			{
				desc: "Success Default for CTTimespan",
				param: ParamType{
					Type:    CTDecimal,
					Default: ....,
					name: "my_value"
				},
				wantStr: "my_value:timespan = true",
			},
		*/
		{
			desc: "Success Default for CTDecimal",
			param: ParamType{
				Type:    CTDecimal,
				Default: "1.349",
				name:    "my_value",
			},
			wantStr: "my_value:decimal = 1.349",
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
	tests := []struct {
		desc    string
		with    map[string]ParamType
		err     bool
		wantStr string
	}{
		{
			desc: "name contains spaces",
			with: ParamTypes{
				"name space": ParamType{Type: CTBool},
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
				"HasLicense": ParamType{Type: CTBool, Default: false},
				"FirstName":  ParamType{Type: CTString},
			},
			wantStr: "declare query_parameters(FirstName:string, HasLicense:bool = false);",
		},
	}

	for _, test := range tests {
		params := NewDefinitions()
		var err error

		params, err = params.With(test.with)

		switch {
		case err == nil && test.err:
			t.Errorf("TestParameters(%s): got err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestParameters(%s): got err == %s, want err != nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if test.wantStr != params.String() {
			t.Errorf("TestParameters(%s): got %q, want %q", test.desc, params.String(), test.wantStr)
		}

		clone := params.clone()
		clone.With(map[string]ParamType{"hellyeah": ParamType{Type: CTBool}})

		if _, ok := params.m["hellyeah"]; ok {
			t.Errorf("TestParameters(%s): clone modification modified original", test.desc)
		}
	}
}

func TestParameters(t *testing.T) {
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
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": ParamType{Type: CTBool}}),
			qValues: NewParameters().Must(map[string]interface{}{"key2": true}),
			err:     true,
		},
		{
			desc:    "Should be time.Time, isn't",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": ParamType{Type: CTDateTime}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": 1}),
			err:     true,
		},
		/*
			{
				desc: "Should be JSON marshalable, isn't",

			},
		*/
		{
			desc:    "Should be uuid.UUID, isn't",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": ParamType{Type: CTGUID}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": 1}),
			err:     true,
		},
		{
			desc:    "Should be int32, isn't",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": ParamType{Type: CTInt}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": int64(1)}),
			err:     true,
		},
		{
			desc:    "Should be int64, isn't",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": ParamType{Type: CTLong}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": int32(1)}),
			err:     true,
		},
		{
			desc:    "Should be float64, isn't",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": ParamType{Type: CTReal}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": 1}),
			err:     true,
		},
		{
			desc:    "Should be string, isn't",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": ParamType{Type: CTString}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": 1}),
			err:     true,
		},
		{
			desc:    "Should be time.Duration, isn't",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": ParamType{Type: CTTimespan}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": 1}),
			err:     true,
		},
		{
			desc:    "Should be string representing decimal or *big.Float, isn't",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": ParamType{Type: CTDecimal}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": 1}),
			err:     true,
		},
		{
			desc:    "Success time.Time",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": ParamType{Type: CTDateTime}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": now}),
			want:    map[string]string{"key1": fmt.Sprintf("datetime(%s)", now.Format(time.RFC3339Nano))},
		},
		/*
			{
				desc: "Success json marshallable",
			},
		*/
		{
			desc:    "Success uuid.UUID",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": ParamType{Type: CTGUID}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": uu}),
			want:    map[string]string{"key1": fmt.Sprintf("guid(%s)", uu.String())},
		},
		{
			desc:    "Success int32",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": ParamType{Type: CTInt}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": int32(1)}),
			want:    map[string]string{"key1": fmt.Sprintf("int(%d)", 1)},
		},
		{
			desc:    "Success int64",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": ParamType{Type: CTLong}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": int64(1)}),
			want:    map[string]string{"key1": fmt.Sprintf("long(%d)", 1)},
		},
		{
			desc:    "Success float64",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": ParamType{Type: CTReal}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": 1.1}),
			want:    map[string]string{"key1": fmt.Sprintf("real(%f)", 1.1)},
		},
		{
			desc:    "Success string",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": ParamType{Type: CTString}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": "string"}),
			want:    map[string]string{"key1": "string"},
		},
		/*
			{
				desc:    "Success time.Duration",
				qParams: NewQueryParameters().MustAdd("key1", ParamType{Type: CTTimespan}),
				qValues: QueryValues{"key1": 3 * time.Second},
				want: map[string]interface{"key1": 3 * time.Second},
			},
		*/
		{
			desc:    "Success string representing decimal",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": ParamType{Type: CTDecimal}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": "1.3"}),
			want:    map[string]string{"key1": fmt.Sprintf("decimal(%s)", "1.3")},
		},
		{
			desc:    "Success *big.Float",
			qParams: NewDefinitions().Must(map[string]ParamType{"key1": ParamType{Type: CTDecimal}}),
			qValues: NewParameters().Must(map[string]interface{}{"key1": big.NewFloat(3.2)}),
			want:    map[string]string{"key1": fmt.Sprintf("decimal(%s)", big.NewFloat(3.2).String())},
		},
	}

	for _, test := range tests {
		got, err := test.qValues.toParameters(test.qParams)
		switch {
		case err == nil && test.err:
			t.Errorf("TestQueryValues(%s): got err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestQueryValues(%s): got err == %s, want err != nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestQueryValues(%s):-want/+got:\n%s", test.desc, diff)
		}
	}
}

func TestStmt(t *testing.T) {
	stmt := NewStmt("|query")

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
					"key1": ParamType{Type: CTBool},
					"key2": ParamType{Type: CTString, Default: "hello"},
				},
			),
			wantStr: "|query",
		},
		{
			desc: "Success: Everything",
			params: NewDefinitions().Must(
				ParamTypes{
					"key1": ParamType{Type: CTBool},
					"key2": ParamType{Type: CTDateTime},
					"key3": ParamType{Type: CTDynamic},
					"key4": ParamType{Type: CTGUID},
					"key5": ParamType{Type: CTInt},
					"key6": ParamType{Type: CTLong},
					"key7": ParamType{Type: CTReal},
					"key8": ParamType{Type: CTString},
					// "key9": ParamType{Type: CTTimespan},
					"key10": ParamType{Type: CTDecimal},
				},
			),
			wantStr: "|query",
		},
	}

	for _, test := range tests {
		var qpErr, vErr error
		if !test.params.IsZero() || test.paramsZeroOk {
			stmt, qpErr = stmt.WithDefinitions(test.params)
			switch {
			case qpErr == nil && test.qpErr:
				t.Errorf("TestStmt(%s): got err == nil, want err != nil", test.desc)
				continue
			case qpErr != nil && !test.qpErr:
				t.Errorf("TestStmt(%s): got err == %s, want err != nil", test.desc, qpErr)
				continue
			case qpErr != nil:
				continue
			}
		}

		if !test.values.IsZero() || test.valuesZeroOk {
			stmt, vErr = stmt.WithParameters(test.values)
			switch {
			case vErr == nil && test.vErr:
				t.Errorf("TestStmt(%s): got err == nil, want err != nil", test.desc)
				continue
			case vErr != nil && !test.vErr:
				t.Errorf("TestStmt(%s): got err == %s, want err != nil", test.desc, vErr)
				continue
			case vErr != nil:
				continue
			}
		}

		gotStr := stmt.String()

		wantStr := buildQueryStr(test.wantStr, test.params)

		if gotStr != wantStr {
			t.Errorf("TestStmt(%s): String(): got %q, want %q", test.desc, gotStr, wantStr)
			continue
		}

		if diff := pretty.Compare(test.wantValues, stmt.params.outM); diff != "" {
			t.Errorf("TestStmt(%s): Values: -want/+got:\n%s", test.desc, diff)
		}
	}
}

func buildQueryStr(query string, params Definitions) string {
	ps := params.String()
	if ps != "" {
		return params.String() + "\n" + query
	}
	return query
}
