package azkustodata

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata/table"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"

	"github.com/google/uuid"
	"github.com/kylelemons/godebug/pretty"
)

func TestStructToKustoValues(t *testing.T) {
	t.Parallel()

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
				value.NewInt(2),
				value.NewString("hello"),
				value.NewLong(1),
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
				value.NewNullInt(),
				value.NewString("hello"),
				value.NewLong(1),
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
	t.Parallel()

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
		value.NewNullBool(),
		value.NewNullDateTime(),
		value.NewNullDynamic(),
		value.NewNullGUID(),
		value.NewNullInt(),
		value.NewNullLong(),
		value.NewNullReal(),
		value.NewNullString(),
		value.NewNullTimespan(),
		value.NewNullDecimal(),
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
	t.Parallel()

	matchers := []struct {
		column table.Column
		kt     value.Kusto
	}{
		{table.Column{Type: types.Bool}, value.NewNullBool()},
		{table.Column{Type: types.DateTime}, value.NewNullDateTime()},
		{table.Column{Type: types.Dynamic}, value.NewNullDynamic()},
		{table.Column{Type: types.GUID}, value.NewNullGUID()},
		{table.Column{Type: types.Int}, value.NewNullInt()},
		{table.Column{Type: types.Long}, value.NewNullLong()},
		{table.Column{Type: types.Real}, value.NewNullReal()},
		{table.Column{Type: types.String}, value.NewNullString()},
		{table.Column{Type: types.Timespan}, value.NewNullTimespan()},
		{table.Column{Type: types.Decimal}, value.NewNullDecimal()},
	}

	for _, match := range matchers {
		if err := colToValueCheck(table.Columns{match.column}, value.Values{match.kt}); err != nil {
			t.Errorf("TestColToValueCheck(%s): did not handle the correct type match, got err == %s", match.column, err)
		}

		var v value.Kusto
		if reflect.TypeOf(match.kt) != reflect.TypeOf(value.NewNullBool()) {
			v = value.NewNullBool()
		} else {
			v = value.NewNullInt()
		}
		if err := colToValueCheck(table.Columns{match.column}, value.Values{v}); err == nil {
			t.Errorf("TestColToValueCheck(%s): did not handle the incorrect type match, got err == %s", match.column, err)
		}
	}
}

func TestConvertBool(t *testing.T) {
	t.Parallel()

	var (
		val = true
		ptr = new(bool)
		ty  = value.NewBool(true)
	)
	*ptr = true

	tests := []struct {
		value interface{}
		want  value.Bool
		err   bool
	}{
		{value: 1, err: true},
		{value: val, want: *value.NewBool(true)},
		{value: ptr, want: *value.NewBool(true)},
		{value: ty, want: *value.NewBool(true)},
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
		ty  = value.NewDateTime(now)
	)
	*ptr = now

	tests := []struct {
		value interface{}
		want  value.DateTime
		err   bool
	}{
		{value: 1, err: true},
		{value: val, want: *value.NewDateTime(now)},
		{value: ptr, want: *value.NewDateTime(now)},
		{value: ty, want: *value.NewDateTime(now)},
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
	t.Parallel()

	var (
		val = 1 * time.Second
		ptr = new(time.Duration)
		ty  = value.NewTimespan(1 * time.Second)
	)
	*ptr = val

	tests := []struct {
		value interface{}
		want  value.Timespan
		err   bool
	}{
		{value: "hello", err: true},
		{value: val, want: *value.NewTimespan(1 * time.Second)},
		{value: ptr, want: *value.NewTimespan(1 * time.Second)},
		{value: ty, want: *value.NewTimespan(1 * time.Second)},
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
	t.Parallel()

	v := SampleDynamic{"John", "Doak"}
	j, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	var (
		val    = v
		ptr    = &v
		ty     = value.NewDynamic(j)
		str    = string(j)
		ptrStr = &str
		m      = mustMapInter(j)
		ptrM   = &m

		want = value.NewDynamic(j)
	)

	tests := []struct {
		value interface{}
		want  value.Dynamic
		err   bool
	}{
		{value: 1, want: *value.NewDynamic(mustMarshal(1))},
		{value: val},
		{value: ptr},
		{value: ty},
		{value: str},
		{value: ptrStr},
		{value: m},
		{value: ptrM},
		{value: []SampleDynamic{v, v}, want: *value.NewDynamic(mustMarshal([]SampleDynamic{v, v}))},
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
		if test.want.Value == nil {
			test.want = *want
		}
		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestConvertDynamic(%v): -want/+got:\n%s", test.value, diff)
			t.Errorf("TestConvertDynamic(%v): got == %s", test.value, string(got.Value))
			t.Errorf("TestConvertDynamic(%v): want == %s", test.value, string(want.Value))
		}
	}
}

func TestConvertGUID(t *testing.T) {
	t.Parallel()

	u := uuid.New()
	var (
		val = u
		ptr = new(uuid.UUID)
		ty  = value.NewGUID(u)
	)
	*ptr = u

	tests := []struct {
		value interface{}
		want  value.GUID
		err   bool
	}{
		{value: 1, err: true},
		{value: val, want: *value.NewGUID(u)},
		{value: ptr, want: *value.NewGUID(u)},
		{value: ty, want: *value.NewGUID(u)},
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
	t.Parallel()

	var (
		val = int32(1)
		ptr = new(int32)
		ty  = value.NewInt(1)
	)
	*ptr = val

	tests := []struct {
		value interface{}
		want  value.Int
		err   bool
	}{
		{value: "hello", err: true},
		{value: val, want: *value.NewInt(1)},
		{value: ptr, want: *value.NewInt(1)},
		{value: ty, want: *value.NewInt(1)},
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
	t.Parallel()

	var (
		val = int64(1)
		ptr = new(int64)
		ty  = value.NewLong(1)
	)
	*ptr = val

	tests := []struct {
		value interface{}
		want  value.Long
		err   bool
	}{
		{value: "hello", err: true},
		{value: val, want: *value.NewLong(1)},
		{value: ptr, want: *value.NewLong(1)},
		{value: ty, want: *value.NewLong(1)},
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
	t.Parallel()

	var (
		val = float64(1.0)
		ptr = new(float64)
		ty  = value.NewReal(1.0)
	)
	*ptr = val

	tests := []struct {
		value interface{}
		want  value.Real
		err   bool
	}{
		{value: "hello", err: true},
		{value: val, want: *value.NewReal(1.0)},
		{value: ptr, want: *value.NewReal(1.0)},
		{value: ty, want: *value.NewReal(1.0)},
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
	t.Parallel()

	var (
		val = string("hello")
		ptr = new(string)
		ty  = value.NewString("hello")
	)
	*ptr = val

	tests := []struct {
		value interface{}
		want  value.String
		err   bool
	}{
		{value: 1, err: true},
		{value: val, want: *value.NewString("hello")},
		{value: ptr, want: *value.NewString("hello")},
		{value: ty, want: *value.NewString("hello")},
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
	t.Parallel()

	var (
		val = string("1.3333333333")
		ptr = new(string)
		ty  = value.DecimalFromString("1.3333333333")
	)
	*ptr = val

	tests := []struct {
		value interface{}
		want  value.Decimal
		err   bool
	}{
		{value: 1, err: true},
		{value: val, want: *value.DecimalFromString("1.3333333333")},
		{value: ptr, want: *value.DecimalFromString("1.3333333333")},
		{value: ty, want: *value.DecimalFromString("1.3333333333")},
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

func mustMarshal(i interface{}) []byte {
	b, err := json.Marshal(i)
	if err != nil {
		panic(err)
	}
	return b
}
