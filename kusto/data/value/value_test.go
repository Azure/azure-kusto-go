package value

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kylelemons/godebug/pretty"
)

func TestBool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc string
		i    interface{}
		err  bool
		want Bool
	}{
		{
			desc: "value is non-nil and non-bool",
			i:    23,
			err:  true,
		},
		{
			desc: "value is nil",
			i:    nil,
			want: Bool{},
		},
		{
			desc: "value is false",
			i:    false,
			want: Bool{Valid: true},
		},
		{
			desc: "value is true",
			i:    true,
			want: Bool{Value: true, Valid: true},
		},
	}

	for _, test := range tests {
		got := Bool{}
		err := got.Unmarshal(test.i)
		switch {
		case err == nil && test.err:
			t.Errorf("TestBool(%s): err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("Testbool(%s): err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestBool(%s): -want/+got:\n%s", test.desc, diff)
		}
	}
}

func TestDateTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc string
		i    interface{}
		err  bool
		want DateTime
	}{
		{
			desc: "value is non-nil and non-string",
			i:    23,
			err:  true,
		},
		{
			desc: "value is non-RFC3339Nano",
			i:    "Mon, 02 Jan 2006 15:04:05 -0700",
			err:  true,
		},
		{
			desc: "value is nil",
			i:    nil,
			want: DateTime{},
		},
		{
			desc: "value is RFC3339Nano",
			i:    "2019-08-27T04:14:55.302919Z",
			want: DateTime{
				Value: timeMustParse(time.RFC3339Nano, "2019-08-27T04:14:55.302919Z"),
				Valid: true,
			},
		},
	}

	for _, test := range tests {
		got := DateTime{}
		err := got.Unmarshal(test.i)
		switch {
		case err == nil && test.err:
			t.Errorf("TestDateTime(%s): err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestDateTime(%s): err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestDateTime(%s): -want/+got:\n%s", test.desc, diff)
		}
	}
}

func TestDynamic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc string
		i    interface{}
		err  bool
		want Dynamic
	}{
		{
			desc: "value is nil",
			i:    nil,
			want: Dynamic{},
		},
		{
			desc: "value is string",
			i:    `{"Visualization":null}`,
			want: Dynamic{
				Value: []byte(`{"Visualization":null}`),
				Valid: true,
			},
		},
		{
			desc: "value is []byte",
			i:    []byte(`{"Visualization":null}`),
			want: Dynamic{
				Value: []byte(`{"Visualization":null}`),
				Valid: true,
			},
		},
		{
			desc: "value is map[string]interface{}",
			i:    map[string]interface{}{"Visualization": nil},
			want: Dynamic{
				Value: []byte(`{"Visualization":null}`),
				Valid: true,
			},
		},
		{
			desc: "value is a []interface{}",
			i:    []interface{}{1, "hello", 2.3},
			want: Dynamic{
				Value: []byte(`[1,"hello",2.3]`),
				Valid: true,
			},
		},
	}

	for _, test := range tests {
		got := Dynamic{}
		err := got.Unmarshal(test.i)
		switch {
		case err == nil && test.err:
			t.Errorf("TestDynamic(%s): err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestDynamic(%s): err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestDynamic(%s): -want/+got:\n%s", test.desc, diff)
			t.Errorf("want: %s", string(test.want.Value))
			t.Errorf("got: %s", string(got.Value))
		}
	}
}

func TestGUID(t *testing.T) {
	t.Parallel()

	goodUUID := uuid.New()
	tests := []struct {
		desc string
		i    interface{}
		err  bool
		want GUID
	}{
		{
			desc: "value is non-nil and non-string",
			i:    23,
			err:  true,
		},
		{
			desc: "value is a string, but not a UUID",
			i:    "hello world",
			err:  true,
		},
		{
			desc: "value is nil",
			i:    nil,
			want: GUID{},
		},
		{
			desc: "value is a UUID",
			i:    goodUUID.String(),
			want: GUID{Value: goodUUID, Valid: true},
		},
	}

	for _, test := range tests {
		got := GUID{}
		err := got.Unmarshal(test.i)
		switch {
		case err == nil && test.err:
			t.Errorf("TestGUID(%s): err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestGUID(%s): err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestGUID(%s): -want/+got:\n%s", test.desc, diff)
		}
	}
}

func TestInt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc string
		i    interface{}
		err  bool
		want Int
	}{
		{
			desc: "value is non-nil and non-int",
			i:    "hello",
			err:  true,
		},
		{
			desc: "value is json.Number that is a float",
			i:    json.Number("3.2"),
			err:  true,
		},
		{
			desc: "value is greater than int32",
			i:    math.MaxInt32 + 1,
			err:  true,
		},
		{
			desc: "value is nil",
			i:    nil,
			want: Int{},
		},
		{
			desc: "value is int",
			i:    2,
			want: Int{Value: 2, Valid: true},
		},
		{
			desc: "value is json.Number",
			i:    json.Number("23"),
			want: Int{Value: 23, Valid: true},
		},
	}

	for _, test := range tests {
		got := Int{}
		err := got.Unmarshal(test.i)
		switch {
		case err == nil && test.err:
			t.Errorf("TestInt(%s): err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestInt(%s): err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestInt(%s): -want/+got:\n%s", test.desc, diff)
		}
	}
}

func TestLong(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc string
		i    interface{}
		err  bool
		want Long
	}{
		{
			desc: "value is non-nil and non-int",
			i:    "hello",
			err:  true,
		},
		{
			desc: "value is json.Number that is a float",
			i:    json.Number("3.2"),
			err:  true,
		},
		{
			desc: "value is nil",
			i:    nil,
			want: Long{},
		},
		{
			desc: "value is int",
			i:    2,
			want: Long{Value: 2, Valid: true},
		},
		{
			desc: "value is json.Number",
			i:    json.Number("23"),
			want: Long{Value: 23, Valid: true},
		},
	}

	for _, test := range tests {
		got := Long{}
		err := got.Unmarshal(test.i)
		switch {
		case err == nil && test.err:
			t.Errorf("TestLong(%s): err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestLong(%s): err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestLong(%s): -want/+got:\n%s", test.desc, diff)
		}
	}
}

func TestReal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc string
		i    interface{}
		err  bool
		want Real
	}{
		{
			desc: "value is non-nil and non-float64",
			i:    "hello",
			err:  true,
		},
		{
			desc: "value is json.Number that is an int, which will convert to a float64",
			i:    json.Number("3"),
			want: Real{Value: 3.0, Valid: true},
		},
		{
			desc: "value is nil",
			i:    nil,
			want: Real{},
		},
		{
			desc: "value is float64",
			i:    2.3,
			want: Real{Value: 2.3, Valid: true},
		},
		{
			desc: "value is json.Number",
			i:    json.Number("23.2"),
			want: Real{Value: 23.2, Valid: true},
		},
	}

	for _, test := range tests {
		got := Real{}
		err := got.Unmarshal(test.i)
		switch {
		case err == nil && test.err:
			t.Errorf("TestReal(%s): err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestReal(%s): err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestReal(%s): -want/+got:\n%s", test.desc, diff)
		}
	}
}

func TestString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc string
		i    interface{}
		err  bool
		want String
	}{
		{
			desc: "value is non-nil and non-string",
			i:    23,
			err:  true,
		},
		{
			desc: "value is nil",
			i:    nil,
			want: String{},
		},
		{
			desc: "value is string",
			i:    "hello world",
			want: String{Value: "hello world", Valid: true},
		},
	}

	for _, test := range tests {
		got := String{}
		err := got.Unmarshal(test.i)
		switch {
		case err == nil && test.err:
			t.Errorf("TestString(%s): err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestString(%s): err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestString(%s): -want/+got:\n%s", test.desc, diff)
		}
	}
}

func TestTimespan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc string
		i    interface{}
		err  bool
		want Timespan
	}{
		{
			desc: "value is non-nil and non-string",
			i:    23,
			err:  true,
		},
		{
			desc: "value is nil",
			i:    nil,
			want: Timespan{},
		},
		{
			desc: "value is string, but doesn't represent a time",
			i:    "hello world",
			err:  true,
		},
		{
			desc: "value is string, but doesn't split right",
			i:    "00:00",
			err:  true,
		},
		{i: "00:00:00", want: Timespan{Valid: true}},
		{i: "00:00:03", want: Timespan{Value: 3 * time.Second, Valid: true}},
		{i: "00:04:03", want: Timespan{Value: 4*time.Minute + 3*time.Second, Valid: true}},
		{i: "02:04:03", want: Timespan{Value: 2*time.Hour + 4*time.Minute + 3*time.Second, Valid: true}},
		{i: "00:00:00.099", want: Timespan{Value: 99 * time.Millisecond, Valid: true}},
		{i: "02:04:03.0123", want: Timespan{Value: 2*time.Hour + 4*time.Minute + 3*time.Second + 12300*time.Microsecond, Valid: true}},
		{i: "01.00:00:00", want: Timespan{Value: 24 * time.Hour, Valid: true}},
		{i: "02.04:05:07", want: Timespan{Value: 2*24*time.Hour + 4*time.Hour + 5*time.Minute + 7*time.Second, Valid: true}},
		{i: "-01.00:00:00", want: Timespan{Value: -24 * time.Hour, Valid: true}},
		{i: "-02.04:05:07", want: Timespan{Value: time.Duration(-1) * (2*24*time.Hour + 4*time.Hour + 5*time.Minute + 7*time.Second), Valid: true}},
		{i: "00.00:00.00:00.000", want: Timespan{Valid: true}},
		{i: "02.04:05:07.789", want: Timespan{Value: 2*24*time.Hour + 4*time.Hour + 5*time.Minute + 7*time.Second + 789*time.Millisecond, Valid: true}},
		{i: "03.00:00:00.111", want: Timespan{Value: 3*24*time.Hour + 111*time.Millisecond, Valid: true}},
		{i: "03.00:00:00.111", want: Timespan{Value: 3*24*time.Hour + 111*time.Millisecond, Valid: true}},
		{i: "364.23:59:59.9999999", want: Timespan{Value: 364*day + 23*time.Hour + 59*time.Minute + 59*time.Second + 9999999*100*time.Nanosecond, Valid: true}},
	}

	for _, test := range tests {
		if test.desc == "" {
			test.desc = fmt.Sprintf("Conversion of %s", test.i)
		}

		got := Timespan{}
		err := got.Unmarshal(test.i)
		switch {
		case err == nil && test.err:
			t.Errorf("TestTimespan(%s): err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestTimespan(%s): err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if test.want != got {
			t.Errorf("TestTimespan(%s): got %v, want %v", test.desc, got, test.want)
		}

		strGot := got.Marshal()
		if test.i == nil || !got.Valid {
			if strGot != "00:00:00" {
				t.Errorf("TestTimespan(%s): Marshal(): got %v, want %v", test.desc, strGot, "00:00:00")
			}
		} else if strings.Trim(strGot, "0:.") != strings.Trim(test.i.(string), "0:.") {
			t.Errorf("TestTimespan(%s): Marshal(): got %v, want %v", test.desc, strGot, test.i)
		}
	}
}

// TestTimespanRegession35 tests that we will not have more than 7 digits of subsecond accuracy on Timespan on the
// Marshal() call.  We had 9 digits which was giving us nanosecond accuracy. Timespan is based on .Net's Timespan type
// which can only handle "tick" accuracy, where "tick" is 10 millionth of a second.
// Bug: https://github.com/Azure/azure-kusto-go/issues/35 .
func TestTimespanRegession35(t *testing.T) {
	// There are 7 digits in Timespan accuracy.  The first 3 are milliseconds, the second 3 are microseconds,
	// the last one is ticks. In a normal representation there are 9, based on that each category increment
	// is 1000 * the last one.
	ts := Timespan{
		Value: 24*time.Hour + time.Hour + time.Minute + time.Second +
			111*time.Millisecond + 222*time.Microsecond + 3*tick + 4*time.Nanosecond,
		Valid: true,
	}

	want := "01.01:01:01.1112223"

	if ts.Marshal() != want {
		t.Errorf("TestTimespanRegession35: got %s, want %s", ts.Marshal(), want)
	}
}

func TestDecimal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc string
		i    interface{}
		err  bool
		want Decimal
	}{
		{
			desc: "cannot be an string representing an integer",
			i:    "1",
			err:  true,
		},
		{
			desc: "cannot be a non string",
			i:    3.0,
			err:  true,
		},
		{i: ".1", want: Decimal{Value: ".1", Valid: true}},
		{i: "0.1", want: Decimal{Value: "0.1", Valid: true}},
		{i: "3.07", want: Decimal{Value: "3.07", Valid: true}},
	}

	for _, test := range tests {
		if test.desc == "" {
			test.desc = fmt.Sprintf("Conversion of %s", test.i)
		}

		got := Decimal{}
		err := got.Unmarshal(test.i)
		switch {
		case err == nil && test.err:
			t.Errorf("TestDecimal(%s): err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestDecimal(%s): err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if test.want != got {
			t.Errorf("TestDecimal(%s)internal string: got %v, want %v", test.desc, got, test.want)
		}
	}
}

func timeMustParse(layout string, p string) time.Time {
	t, err := time.Parse(layout, p)
	if err != nil {
		panic(err)
	}
	return t
}
