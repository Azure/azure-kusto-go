package kusto

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/types"

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
		cols Columns
		val  *testStruct
		want types.KustoValues
		err  bool
	}{
		{
			desc: "All fields should export",
			cols: Columns{
				{Name: "Int", Type: CTInt},
				{Name: "String", Type: CTString},
				{Name: "Long", Type: CTLong},
			},
			val: &testStruct{
				MyInt:  2,
				String: "hello",
				Long:   1,
			},
			want: types.KustoValues{
				types.Int{Value: 2, Valid: true},
				types.String{Value: "hello", Valid: true},
				types.Long{Value: 1, Valid: true},
			},
		},
		{
			desc: "MyInt doesn't get exported",
			cols: Columns{
				{Name: "int", Type: CTInt}, // We have "int" instead of "Int"
				{Name: "String", Type: CTString},
				{Name: "Long", Type: CTLong},
			},
			val: &testStruct{
				MyInt:  2,
				String: "hello",
				Long:   1,
			},
			want: types.KustoValues{
				types.Int{Value: 0, Valid: false},
				types.String{Value: "hello", Valid: true},
				types.Long{Value: 1, Valid: true},
			},
		},
		{
			desc: "Tagged field(MyInt) is wrong type and won't convert doesn't get exported",
			cols: Columns{
				{Name: "Int", Type: CTReal}, // We have CTReal instead of CTInt
				{Name: "String", Type: CTString},
				{Name: "Long", Type: CTLong},
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
			cols: Columns{
				{Name: "Int", Type: CTInt},
				{Name: "String", Type: CTReal}, // We have CTReal instead of CTString
				{Name: "Long", Type: CTLong},
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
	columns := Columns{
		Column{Type: CTBool},
		Column{Type: CTDateTime},
		Column{Type: CTDynamic},
		Column{Type: CTGUID},
		Column{Type: CTInt},
		Column{Type: CTLong},
		Column{Type: CTReal},
		Column{Type: CTString},
		Column{Type: CTTimespan},
		Column{Type: CTDecimal},
	}
	want := types.KustoValues{
		types.Bool{},
		types.DateTime{},
		types.Dynamic{},
		types.GUID{},
		types.Int{},
		types.Long{},
		types.Real{},
		types.String{},
		types.Timespan{},
		types.Decimal{},
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
		column Column
		kt     types.KustoValue
	}{
		{Column{Type: CTBool}, types.Bool{}},
		{Column{Type: CTDateTime}, types.DateTime{}},
		{Column{Type: CTDynamic}, types.Dynamic{}},
		{Column{Type: CTGUID}, types.GUID{}},
		{Column{Type: CTInt}, types.Int{}},
		{Column{Type: CTLong}, types.Long{}},
		{Column{Type: CTReal}, types.Real{}},
		{Column{Type: CTString}, types.String{}},
		{Column{Type: CTTimespan}, types.Timespan{}},
		{Column{Type: CTDecimal}, types.Decimal{}},
	}

	for _, match := range matchers {
		if err := colToValueCheck(Columns{match.column}, types.KustoValues{match.kt}); err != nil {
			t.Errorf("TestColToValueCheck(%s): did not handle the correct type match, got err == %s", match.column, err)
		}

		var v types.KustoValue
		if reflect.TypeOf(match.kt) != reflect.TypeOf(types.Bool{}) {
			v = types.Bool{}
		} else {
			v = types.Int{}
		}
		if err := colToValueCheck(Columns{match.column}, types.KustoValues{v}); err == nil {
			t.Errorf("TestColToValueCheck(%s): did not handle the incorrect type match, got err == %s", match.column, err)
		}
	}
}

func TestConvertBool(t *testing.T) {
	var (
		val = true
		ptr = new(bool)
		ty  = types.Bool{Value: true, Valid: true}
	)
	*ptr = true

	tests := []struct {
		value interface{}
		want  types.Bool
		err   bool
	}{
		{value: 1, err: true},
		{value: val, want: types.Bool{Value: true, Valid: true}},
		{value: ptr, want: types.Bool{Value: true, Valid: true}},
		{value: ty, want: types.Bool{Value: true, Valid: true}},
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
		ty  = types.DateTime{Value: now, Valid: true}
	)
	*ptr = now

	tests := []struct {
		value interface{}
		want  types.DateTime
		err   bool
	}{
		{value: 1, err: true},
		{value: val, want: types.DateTime{Value: now, Valid: true}},
		{value: ptr, want: types.DateTime{Value: now, Valid: true}},
		{value: ty, want: types.DateTime{Value: now, Valid: true}},
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
		ty  = types.Timespan{Value: 1 * time.Second, Valid: true}
	)
	*ptr = val

	tests := []struct {
		value interface{}
		want  types.Timespan
		err   bool
	}{
		{value: "hello", err: true},
		{value: val, want: types.Timespan{Value: 1 * time.Second, Valid: true}},
		{value: ptr, want: types.Timespan{Value: 1 * time.Second, Valid: true}},
		{value: ty, want: types.Timespan{Value: 1 * time.Second, Valid: true}},
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
		ty  = types.Dynamic{Value: string(j), Valid: true}
	)

	tests := []struct {
		value interface{}
		want  types.Dynamic
		err   bool
	}{
		{value: 1, err: true},
		{value: val, want: types.Dynamic{Value: string(j), Valid: true}},
		{value: ptr, want: types.Dynamic{Value: string(j), Valid: true}},
		{value: ty, want: types.Dynamic{Value: string(j), Valid: true}},
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
		ty  = types.GUID{Value: u, Valid: true}
	)
	*ptr = u

	tests := []struct {
		value interface{}
		want  types.GUID
		err   bool
	}{
		{value: 1, err: true},
		{value: val, want: types.GUID{Value: u, Valid: true}},
		{value: ptr, want: types.GUID{Value: u, Valid: true}},
		{value: ty, want: types.GUID{Value: u, Valid: true}},
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
		ty  = types.Int{Value: 1, Valid: true}
	)
	*ptr = val

	tests := []struct {
		value interface{}
		want  types.Int
		err   bool
	}{
		{value: "hello", err: true},
		{value: val, want: types.Int{Value: 1, Valid: true}},
		{value: ptr, want: types.Int{Value: 1, Valid: true}},
		{value: ty, want: types.Int{Value: 1, Valid: true}},
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
		ty  = types.Long{Value: 1, Valid: true}
	)
	*ptr = val

	tests := []struct {
		value interface{}
		want  types.Long
		err   bool
	}{
		{value: "hello", err: true},
		{value: val, want: types.Long{Value: 1, Valid: true}},
		{value: ptr, want: types.Long{Value: 1, Valid: true}},
		{value: ty, want: types.Long{Value: 1, Valid: true}},
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
		ty  = types.Real{Value: 1.0, Valid: true}
	)
	*ptr = val

	tests := []struct {
		value interface{}
		want  types.Real
		err   bool
	}{
		{value: "hello", err: true},
		{value: val, want: types.Real{Value: 1.0, Valid: true}},
		{value: ptr, want: types.Real{Value: 1.0, Valid: true}},
		{value: ty, want: types.Real{Value: 1.0, Valid: true}},
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
		ty  = types.String{Value: "hello", Valid: true}
	)
	*ptr = val

	tests := []struct {
		value interface{}
		want  types.String
		err   bool
	}{
		{value: 1, err: true},
		{value: val, want: types.String{Value: "hello", Valid: true}},
		{value: ptr, want: types.String{Value: "hello", Valid: true}},
		{value: ty, want: types.String{Value: "hello", Valid: true}},
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
		ty  = types.Decimal{Value: "1.3333333333", Valid: true}
	)
	*ptr = val

	tests := []struct {
		value interface{}
		want  types.Decimal
		err   bool
	}{
		{value: 1, err: true},
		{value: val, want: types.Decimal{Value: "1.3333333333", Valid: true}},
		{value: ptr, want: types.Decimal{Value: "1.3333333333", Valid: true}},
		{value: ty, want: types.Decimal{Value: "1.3333333333", Valid: true}},
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
