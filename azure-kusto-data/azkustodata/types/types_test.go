package types

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kylelemons/godebug/pretty"
)

func TestBool(t *testing.T) {
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
	tests := []struct {
		desc string
		i    interface{}
		err  bool
		want Dynamic
	}{
		{
			desc: "value is non-nil and non-string",
			i:    23,
			err:  true,
		},
		{
			desc: "value is string, but is not valid JSON",
			i:    "{\"Visualization\":null", // Missing closing }
			err:  true,
		},
		{
			desc: "value is nil",
			i:    nil,
			want: Dynamic{},
		},
		{
			desc: "value is string",
			i:    "{\"Visualization\":null}",
			want: Dynamic{Value: "{\"Visualization\":null}", Valid: true},
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
		}
	}
}

func TestGUID(t *testing.T) {
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

func timeMustParse(layout string, p string) time.Time {
	t, err := time.Parse(layout, p)
	if err != nil {
		panic(err)
	}
	return t
}
