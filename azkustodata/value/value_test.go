package value

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
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
			want: *NewNullBool(),
		},
		{
			desc: "value is false",
			i:    false,
			want: *NewBool(false),
		},
		{
			desc: "value is true",
			i:    true,
			want: *NewBool(true),
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got := Bool{}
			err := got.Unmarshal(test.i)

			if test.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			assert.Equal(t, test.want, got)
		})
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
			want: *NewNullDateTime(),
		},
		{
			desc: "value is RFC3339Nano",
			i:    "2019-08-27T04:14:55.302919Z",
			want: *NewDateTime(timeMustParse(time.RFC3339Nano, "2019-08-27T04:14:55.302919Z")),
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got := DateTime{}
			err := got.Unmarshal(test.i)

			if test.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			assert.Equal(t, test.want, got)
		})
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
			want: *NewNullDynamic(),
		},
		{
			desc: "value is string",
			i:    `{"Visualization":null}`,
			want: *NewDynamic([]byte(`{"Visualization":null}`)),
		},
		{
			desc: "value is []byte",
			i:    []byte(`{"Visualization":null}`),
			want: *NewDynamic([]byte(`{"Visualization":null}`)),
		},
		{
			desc: "value is map[string]interface{}",
			i:    map[string]interface{}{"Visualization": nil},
			want: *NewDynamic([]byte(`{"Visualization":null}`)),
		},
		{
			desc: "value is a []interface{}",
			i:    []interface{}{1, "hello", 2.3},
			want: *NewDynamic([]byte(`[1,"hello",2.3]`)),
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got := Dynamic{}
			err := got.Unmarshal(test.i)

			if test.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			assert.EqualValues(t, test.want, got)
		})
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
			want: *NewNullGUID(),
		},
		{
			desc: "value is a UUID",
			i:    goodUUID.String(),
			want: *NewGUID(goodUUID),
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got := GUID{}
			err := got.Unmarshal(test.i)

			if test.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			assert.EqualValues(t, test.want, got)
		})
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
			want: *NewNullInt(),
		},
		{
			desc: "value is int",
			i:    2,
			want: *NewInt(2),
		},
		{
			desc: "value is json.Number",
			i:    json.Number("23"),
			want: *NewInt(23),
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got := Int{}
			err := got.Unmarshal(test.i)

			if test.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			assert.EqualValues(t, test.want, got)
		})
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
			want: *NewNullLong(),
		},
		{
			desc: "value is int",
			i:    2,
			want: *NewLong(2),
		},
		{
			desc: "value is json.Number",
			i:    json.Number("23"),
			want: *NewLong(23),
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got := Long{}
			err := got.Unmarshal(test.i)

			if test.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			assert.EqualValues(t, test.want, got)
		})
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
			want: *NewReal(3.0),
		},
		{
			desc: "value is nil",
			i:    nil,
			want: *NewNullReal(),
		},
		{
			desc: "value is float64",
			i:    2.3,
			want: *NewReal(2.3),
		},
		{
			desc: "value is json.Number",
			i:    json.Number("23.2"),
			want: *NewReal(23.2),
		},
		{
			desc: "value is float string",
			i:    "23.22",
			want: *NewReal(23.22),
		},
		{
			desc: "value is NaN string",
			i:    "NaN",
			want: *NewReal(math.NaN()),
		},
		{
			desc: "value is +Inf string",
			i:    "Infinity",
			want: *NewReal(math.Inf(1)),
		},
		{
			desc: "value is -Inf string",
			i:    "-Infinity",
			want: *NewReal(math.Inf(-1)),
		},
		{
			desc: "value is +Inf",
			i:    math.Inf(1),
			want: *NewReal(math.Inf(1)),
		},
		{
			desc: "value is -Inf",
			i:    math.Inf(-1),
			want: *NewReal(math.Inf(-1)),
		},
		{
			desc: "value is NaN",
			i:    math.NaN(),
			want: *NewReal(math.NaN()),
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got := Real{}
			err := got.Unmarshal(test.i)

			if test.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			if test.want.value != nil && math.IsNaN(*test.want.value) {
				assert.True(t, math.IsNaN(*got.value))
			} else {
				assert.EqualValues(t, test.want, got)
			}

		})
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
			desc: "value is empty",
			i:    "",
			want: *NewString(""),
		},
		{
			desc: "value is string",
			i:    "hello world",
			want: *NewString("hello world"),
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got := String{}
			err := got.Unmarshal(test.i)

			if test.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			assert.EqualValues(t, test.want, got)
		})
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
			want: *NewNullTimespan(),
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
		{i: "00:00:00", want: *NewTimespan(time.Duration(0))},
		{i: "00:00:03", want: *NewTimespan(3 * time.Second)},
		{i: "00:04:03", want: *NewTimespan(4*time.Minute + 3*time.Second)},
		{i: "02:04:03", want: *NewTimespan(2*time.Hour + 4*time.Minute + 3*time.Second)},
		{i: "00:00:00.099", want: *NewTimespan(99 * time.Millisecond)},
		{i: "02:04:03.0123", want: *NewTimespan(2*time.Hour + 4*time.Minute + 3*time.Second + 12300*time.Microsecond)},
		{i: "01.00:00:00", want: *NewTimespan(24 * time.Hour)},
		{i: "02.04:05:07", want: *NewTimespan(2*24*time.Hour + 4*time.Hour + 5*time.Minute + 7*time.Second)},
		{i: "-01.00:00:00", want: *NewTimespan(-24 * time.Hour)},
		{i: "-02.04:05:07", want: *NewTimespan(time.Duration(-1) * (2*24*time.Hour + 4*time.Hour + 5*time.Minute + 7*time.Second))},
		{i: "00.00:00.00:00.000", want: *NewTimespan(time.Duration(0))},
		{i: "02.04:05:07.789", want: *NewTimespan(2*24*time.Hour + 4*time.Hour + 5*time.Minute + 7*time.Second + 789*time.Millisecond)},
		{i: "03.00:00:00.111", want: *NewTimespan(3*24*time.Hour + 111*time.Millisecond)},
		{i: "03.00:00:00.111", want: *NewTimespan(3*24*time.Hour + 111*time.Millisecond)},
		{i: "364.23:59:59.9999999", want: *NewTimespan(364*day + 23*time.Hour + 59*time.Minute + 59*time.Second + 9999999*100*time.Nanosecond)},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got := Timespan{}
			err := got.Unmarshal(test.i)

			if test.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			assert.EqualValues(t, test.want, got)

			strGot := got.Marshal()

			if test.i == nil || got.value == nil {
				assert.Equal(t, "00:00:00", strGot)
				return
			}
			assert.EqualValues(t, removeLeadingZeros(test.i.(string)), removeLeadingZeros(strGot))
		})
	}
}

func removeLeadingZeros(s string) string {
	if len(s) == 0 {
		return s
	}
	if string(s[0]) == "-" {
		return string(s[0]) + strings.Trim(s[1:], "0:.")
	}
	return strings.Trim(s, "0:.")
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
			desc: "cannot be a non string",
			i:    3.0,
			err:  true,
		},
		{desc: "Conversion of '1',", i: "1", want: *DecimalFromString("1")},
		{desc: "Conversion of '.1',", i: ".1", want: *DecimalFromString(".1")},
		{desc: "Conversion of '1.',", i: "1.", want: *DecimalFromString("1.")},
		{desc: "Conversion of '0.1',", i: "0.1", want: *DecimalFromString("0.1")},
		{desc: "Conversion of '3.07',", i: "3.07", want: *DecimalFromString("3.07")},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got := Decimal{}
			err := got.Unmarshal(test.i)

			if test.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.EqualValues(t, test.want, got)
		})
	}
}

func timeMustParse(layout string, p string) time.Time {
	t, err := time.Parse(layout, p)
	if err != nil {
		panic(err)
	}
	return t
}
