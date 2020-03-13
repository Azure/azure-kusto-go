package kusto

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/Azure/azure-kusto-go/kusto/data/value"

	"github.com/google/uuid"
	"github.com/kylelemons/godebug/pretty"
)

func TestStructToKustoValues(t *testing.T) {
	type testStruct struct {
		MyInt  int32 `kusto:"Int"`
		String string
		Long   int64
	}

	tests := []struct {
		desc string
		cols table.Columns
		val  *testStruct
		want value.Values
		err  bool
	}{
		{
			desc: "All fields should export",
			cols: table.Columns{
				{Name: "Int", Type: types.Int},
				{Name: "String", Type: types.String},
				{Name: "Long", Type: types.Long},
			},
			val: &testStruct{
				MyInt:  2,
				String: "hello",
				Long:   1,
			},
			want: value.Values{
				value.Int{Value: 2, Valid: true},
				value.String{Value: "hello", Valid: true},
				value.Long{Value: 1, Valid: true},
			},
		},
		{
			desc: "MyInt doesn't get exported",
			cols: table.Columns{
				{Name: "int", Type: types.Int}, // We have "int" instead of "Int"
				{Name: "String", Type: types.String},
				{Name: "Long", Type: types.Long},
			},
			val: &testStruct{
				MyInt:  2,
				String: "hello",
				Long:   1,
			},
			want: value.Values{
				value.Int{Value: 0, Valid: false},
				value.String{Value: "hello", Valid: true},
				value.Long{Value: 1, Valid: true},
			},
		},
		{
			desc: "Tagged field(MyInt) is wrong type and won't convert doesn't get exported",
			cols: table.Columns{
				{Name: "Int", Type: types.Real}, // We have types.Real instead of types.Int
				{Name: "String", Type: types.String},
				{Name: "Long", Type: types.Long},
			},
			val: &testStruct{
				MyInt:  2,
				String: "hello",
				Long:   1,
			},
			err: true,
		},
		{
			desc: "Non-tagged field(String) is wrong type and won't convert doesn't get exported",
			cols: table.Columns{
				{Name: "Int", Type: types.Int},
				{Name: "String", Type: types.Real}, // We have types.Real instead of types.String
				{Name: "Long", Type: types.Long},
			},
			val: &testStruct{
				MyInt:  2,
				String: "hello",
				Long:   1,
			},
			err: true,
		},
	}

	for _, test := range tests {
		got, err := structToKustoValues(test.cols, test.val)
		switch {
		case err == nil && test.err:
			t.Errorf("TestStructToKustoValues(%s): got err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestStructToKustoValues(%s): got err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestStructToKustoValues(%s): -wan/+got:\n%s", test.desc, diff)
		}
	}
}

func TestDefaultRow(t *testing.T) {
	columns := table.Columns{
		table.Column{Type: types.Bool},
		table.Column{Type: types.DateTime},
		table.Column{Type: types.Dynamic},
		table.Column{Type: types.GUID},
		table.Column{Type: types.Int},
		table.Column{Type: types.Long},
		table.Column{Type: types.Real},
		table.Column{Type: types.String},
		table.Column{Type: types.Timespan},
		table.Column{Type: types.Decimal},
	}
	want := value.Values{
		value.Bool{},
		value.DateTime{},
		value.Dynamic{},
		value.GUID{},
		value.Int{},
		value.Long{},
		value.Real{},
		value.String{},
		value.Timespan{},
		value.Decimal{},
	}

	got, err := defaultRow(columns)
	if err != nil {
		t.Fatalf("TestDefaultRow: got err == %s", err)
	}

	if diff := pretty.Compare(want, got); diff != "" {
		t.Fatalf("TestDefaultRow: -wan/+got:\n%s", diff)
	}
}

func TestColToValueCheck(t *testing.T) {
	matchers := []struct {
		column table.Column
		kt     value.Kusto
	}{
		{table.Column{Type: types.Bool}, value.Bool{}},
		{table.Column{Type: types.DateTime}, value.DateTime{}},
		{table.Column{Type: types.Dynamic}, value.Dynamic{}},
		{table.Column{Type: types.GUID}, value.GUID{}},
		{table.Column{Type: types.Int}, value.Int{}},
		{table.Column{Type: types.Long}, value.Long{}},
		{table.Column{Type: types.Real}, value.Real{}},
		{table.Column{Type: types.String}, value.String{}},
		{table.Column{Type: types.Timespan}, value.Timespan{}},
		{table.Column{Type: types.Decimal}, value.Decimal{}},
	}

	for _, match := range matchers {
		if err := colToValueCheck(table.Columns{match.column}, value.Values{match.kt}); err != nil {
			t.Errorf("TestColToValueCheck(%s): did not handle the correct type match, got err == %s", match.column, err)
		}

		var v value.Kusto
		if reflect.TypeOf(match.kt) != reflect.TypeOf(value.Bool{}) {
			v = value.Bool{}
		} else {
			v = value.Int{}
		}
		if err := colToValueCheck(table.Columns{match.column}, value.Values{v}); err == nil {
			t.Errorf("TestColToValueCheck(%s): did not handle the incorrect type match, got err == %s", match.column, err)
		}
	}
}

func TestConvertBool(t *testing.T) {
	var (
		val = true
		ptr = new(bool)
		ty  = value.Bool{Value: true, Valid: true}
	)
	*ptr = true

	tests := []struct {
		value interface{}
		want  value.Bool
		err   bool
	}{
		{value: 1, err: true},
		{value: val, want: value.Bool{Value: true, Valid: true}},
		{value: ptr, want: value.Bool{Value: true, Valid: true}},
		{value: ty, want: value.Bool{Value: true, Valid: true}},
	}
	for _, test := range tests {
		got, err := convertBool(reflect.ValueOf(test.value))
		switch {
		case err == nil && test.err:
			t.Errorf("TestConvertBool(%v): got err == nil, want err != nil", test.value)
			continue
		case err != nil && !test.err:
			t.Errorf("TestConvertBool(%v): got err == %s, want err != nil", test.value, err)
		case err != nil:
			continue
		}
		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestConvertBool(%v): -want/+got:\n%s", test.value, diff)
		}
	}
}

func TestConvertDateTime(t *testing.T) {
	now := time.Now()
	var (
		val = now
		ptr = new(time.Time)
		ty  = value.DateTime{Value: now, Valid: true}
	)
	*ptr = now

	tests := []struct {
		value interface{}
		want  value.DateTime
		err   bool
	}{
		{value: 1, err: true},
		{value: val, want: value.DateTime{Value: now, Valid: true}},
		{value: ptr, want: value.DateTime{Value: now, Valid: true}},
		{value: ty, want: value.DateTime{Value: now, Valid: true}},
	}
	for _, test := range tests {
		got, err := convertDateTime(reflect.ValueOf(test.value))
		switch {
		case err == nil && test.err:
			t.Errorf("TestConvertDateTime(%v): got err == nil, want err != nil", test.value)
			continue
		case err != nil && !test.err:
			t.Errorf("TestConvertDateTime(%v): got err == %s, want err != nil", test.value, err)
		case err != nil:
			continue
		}
		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestConvertDateTime(%v): -want/+got:\n%s", test.value, diff)
		}
	}
}

func TestConvertTimespan(t *testing.T) {
	var (
		val = 1 * time.Second
		ptr = new(time.Duration)
		ty  = value.Timespan{Value: 1 * time.Second, Valid: true}
	)
	*ptr = val

	tests := []struct {
		value interface{}
		want  value.Timespan
		err   bool
	}{
		{value: "hello", err: true},
		{value: val, want: value.Timespan{Value: 1 * time.Second, Valid: true}},
		{value: ptr, want: value.Timespan{Value: 1 * time.Second, Valid: true}},
		{value: ty, want: value.Timespan{Value: 1 * time.Second, Valid: true}},
	}
	for _, test := range tests {
		got, err := convertTimespan(reflect.ValueOf(test.value))
		switch {
		case err == nil && test.err:
			t.Errorf("TestConvertTimespan(%v): got err == nil, want err != nil", test.value)
			continue
		case err != nil && !test.err:
			t.Errorf("TestConvertTimespan(%v): got err == %s, want err != nil", test.value, err)
		case err != nil:
			continue
		}
		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestConvertTimespan(%v): -want/+got:\n%s", test.value, diff)
		}
	}
}

type SampleDynamic struct {
	First string
	Last  string
}

func TestConvertDynamic(t *testing.T) {
	v := SampleDynamic{"John", "Doak"}
	j, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	var (
		val = v
		ptr = &v
		ty  = value.Dynamic{Value: mustMapInter(j), Valid: true}
	)

	tests := []struct {
		value interface{}
		want  value.Dynamic
		err   bool
	}{
		{value: 1, err: true},
		{value: val, want: value.Dynamic{Value: mustMapInter(j), Valid: true}},
		{value: ptr, want: value.Dynamic{Value: mustMapInter(j), Valid: true}},
		{value: ty, want: value.Dynamic{Value: mustMapInter(j), Valid: true}},
	}
	for _, test := range tests {
		got, err := convertDynamic(reflect.ValueOf(test.value))
		switch {
		case err == nil && test.err:
			t.Errorf("TestConvertDynamic(%v): got err == nil, want err != nil", test.value)
			continue
		case err != nil && !test.err:
			t.Errorf("TestConvertDynamic(%v): got err == %s, want err != nil", test.value, err)
		case err != nil:
			continue
		}
		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestConvertDynamic(%v): -want/+got:\n%s", test.value, diff)
		}
	}
}

func TestConvertGUID(t *testing.T) {
	u := uuid.New()
	var (
		val = u
		ptr = new(uuid.UUID)
		ty  = value.GUID{Value: u, Valid: true}
	)
	*ptr = u

	tests := []struct {
		value interface{}
		want  value.GUID
		err   bool
	}{
		{value: 1, err: true},
		{value: val, want: value.GUID{Value: u, Valid: true}},
		{value: ptr, want: value.GUID{Value: u, Valid: true}},
		{value: ty, want: value.GUID{Value: u, Valid: true}},
	}
	for _, test := range tests {
		got, err := convertGUID(reflect.ValueOf(test.value))
		switch {
		case err == nil && test.err:
			t.Errorf("TestConvertGUID(%v): got err == nil, want err != nil", test.value)
			continue
		case err != nil && !test.err:
			t.Errorf("TestConvertGUID(%v): got err == %s, want err != nil", test.value, err)
		case err != nil:
			continue
		}
		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestConvertGUID(%v): -want/+got:\n%s", test.value, diff)
		}
	}
}

func TestConvertInt(t *testing.T) {
	var (
		val = int32(1)
		ptr = new(int32)
		ty  = value.Int{Value: 1, Valid: true}
	)
	*ptr = val

	tests := []struct {
		value interface{}
		want  value.Int
		err   bool
	}{
		{value: "hello", err: true},
		{value: val, want: value.Int{Value: 1, Valid: true}},
		{value: ptr, want: value.Int{Value: 1, Valid: true}},
		{value: ty, want: value.Int{Value: 1, Valid: true}},
	}
	for _, test := range tests {
		got, err := convertInt(reflect.ValueOf(test.value))
		switch {
		case err == nil && test.err:
			t.Errorf("TestConvertInt(%v): got err == nil, want err != nil", test.value)
			continue
		case err != nil && !test.err:
			t.Errorf("TestConvertInt(%v): got err == %s, want err != nil", test.value, err)
		case err != nil:
			continue
		}
		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestConvertInt(%v): -want/+got:\n%s", test.value, diff)
		}
	}
}

func TestConvertLong(t *testing.T) {
	var (
		val = int64(1)
		ptr = new(int64)
		ty  = value.Long{Value: 1, Valid: true}
	)
	*ptr = val

	tests := []struct {
		value interface{}
		want  value.Long
		err   bool
	}{
		{value: "hello", err: true},
		{value: val, want: value.Long{Value: 1, Valid: true}},
		{value: ptr, want: value.Long{Value: 1, Valid: true}},
		{value: ty, want: value.Long{Value: 1, Valid: true}},
	}
	for _, test := range tests {
		got, err := convertLong(reflect.ValueOf(test.value))
		switch {
		case err == nil && test.err:
			t.Errorf("TestConvertLong(%v): got err == nil, want err != nil", test.value)
			continue
		case err != nil && !test.err:
			t.Errorf("TestConvertLong(%v): got err == %s, want err != nil", test.value, err)
		case err != nil:
			continue
		}
		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestConvertLong(%v): -want/+got:\n%s", test.value, diff)
		}
	}
}

func TestConvertReal(t *testing.T) {
	var (
		val = float64(1.0)
		ptr = new(float64)
		ty  = value.Real{Value: 1.0, Valid: true}
	)
	*ptr = val

	tests := []struct {
		value interface{}
		want  value.Real
		err   bool
	}{
		{value: "hello", err: true},
		{value: val, want: value.Real{Value: 1.0, Valid: true}},
		{value: ptr, want: value.Real{Value: 1.0, Valid: true}},
		{value: ty, want: value.Real{Value: 1.0, Valid: true}},
	}
	for _, test := range tests {
		got, err := convertReal(reflect.ValueOf(test.value))
		switch {
		case err == nil && test.err:
			t.Errorf("TestConvertReal(%v): got err == nil, want err != nil", test.value)
			continue
		case err != nil && !test.err:
			t.Errorf("TestConvertReal(%v): got err == %s, want err != nil", test.value, err)
		case err != nil:
			continue
		}
		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestConvertReal(%v): -want/+got:\n%s", test.value, diff)
		}
	}
}

func TestConvertString(t *testing.T) {
	var (
		val = string("hello")
		ptr = new(string)
		ty  = value.String{Value: "hello", Valid: true}
	)
	*ptr = val

	tests := []struct {
		value interface{}
		want  value.String
		err   bool
	}{
		{value: 1, err: true},
		{value: val, want: value.String{Value: "hello", Valid: true}},
		{value: ptr, want: value.String{Value: "hello", Valid: true}},
		{value: ty, want: value.String{Value: "hello", Valid: true}},
	}
	for _, test := range tests {
		got, err := convertString(reflect.ValueOf(test.value))
		switch {
		case err == nil && test.err:
			t.Errorf("TestConvertString(%v): got err == nil, want err != nil", test.value)
			continue
		case err != nil && !test.err:
			t.Errorf("TestConvertString(%v): got err == %s, want err != nil", test.value, err)
		case err != nil:
			continue
		}
		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestConvertString(%v): -want/+got:\n%s", test.value, diff)
		}
	}
}

func TestConvertDecimal(t *testing.T) {
	var (
		val = string("1.3333333333")
		ptr = new(string)
		ty  = value.Decimal{Value: "1.3333333333", Valid: true}
	)
	*ptr = val

	tests := []struct {
		value interface{}
		want  value.Decimal
		err   bool
	}{
		{value: 1, err: true},
		{value: val, want: value.Decimal{Value: "1.3333333333", Valid: true}},
		{value: ptr, want: value.Decimal{Value: "1.3333333333", Valid: true}},
		{value: ty, want: value.Decimal{Value: "1.3333333333", Valid: true}},
	}
	for _, test := range tests {
		got, err := convertDecimal(reflect.ValueOf(test.value))
		switch {
		case err == nil && test.err:
			t.Errorf("TestConvertDecimal(%v): got err == nil, want err != nil", test.value)
			continue
		case err != nil && !test.err:
			t.Errorf("TestConvertDecimal(%v): got err == %s, want err != nil", test.value, err)
		case err != nil:
			continue
		}
		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestConvertDecimal (%v): -want/+got:\n%s", test.value, diff)
		}
	}
}

func mustMapInter(i interface{}) map[string]interface{} {
	if v, ok := i.(map[string]interface{}); ok {
		return v
	}

	var b []byte
	var err error
	switch v := i.(type) {
	case string:
		b = []byte(v)
	case []byte:
		b = v
	default:
		b, err = json.Marshal(i)
		if err != nil {
			panic(err)
		}
	}

	m := map[string]interface{}{}
	if err := json.Unmarshal(b, &m); err != nil {
		panic(err)
	}
	return m
}
