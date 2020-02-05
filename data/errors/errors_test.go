package errors

import (
	"errors"
	"io"
	"log"
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

type anErrorType string

func (e *anErrorType) Error() string {
	return string(*e)
}

func TestE(t *testing.T) {
	wrappedErr := anErrorType("wrappedError")
	err := E(OpMgmt, KLimitsExceeded, &wrappedErr)

	if !errors.Is(err, err) {
		t.Errorf("TestE: errors.Is() returned false for *Error")
	}

	got, ok := err.(*Error)
	if !ok {
		t.Fatalf("TestE: returned error did not have underlying type *Error, was %T", err)
	}
	if got.Op != OpMgmt {
		t.Errorf("TestE: got Op == %v, want Op == %v", got.Op, OpMgmt)
	}
	if got.Kind != KLimitsExceeded {
		t.Errorf("TestE: got Kind == %v, want Kind == %v", got.Kind, KLimitsExceeded)
	}

	if diff := pretty.Compare(wrappedErr, got.Err); diff != "" {
		t.Errorf("TestE: internal error: -want/+got:\n%s", diff)
	}
}

func TestW(t *testing.T) {
	inner := E(OpMgmt, KLimitsExceeded, io.EOF)
	outer := W(inner, ES(OpMgmt, KClientArgs, "Client supplied bad arguments"))

	if !errors.Is(outer, io.EOF) {
		t.Errorf("TestW: errors.Is(outer, io.EOF): got false, want true")
	}

	var err = new(Error)
	if !errors.As(outer, &err) {
		t.Errorf("TestW: errors.As(outer, &Error{}): got false, want true")
	}
	if diff := pretty.Compare(outer, err); diff != "" {
		t.Errorf("TestW: errors.As(outer, &Error{}): -want/+got:\n%s", diff)
	}
}

func TestOneToErr(t *testing.T) {
	tests := []struct {
		desc  string
		input map[string]interface{}
		want  *Error
	}{
		{
			desc: "Input is nil",
		},
		{
			desc: "Missing OneApiErrors key",
			input: map[string]interface{}{
				"blah": "string",
			},
		},
		{
			desc: "OneApiErrors key has a non []interface{} value",
			input: map[string]interface{}{
				"OneApiErrors": []string{"not a []interface{}"},
			},
		},
		{
			desc: "OneApiErrors has entries that are not map[string]interface{}",
			input: map[string]interface{}{
				"OneApiErrors": []interface{}{
					"string1",
					"string2",
				},
			},
		},
		{
			desc: "Two layer error",
			input: map[string]interface{}{
				"OneApiErrors": []interface{}{
					map[string]interface{}{
						"error": map[string]interface{}{
							"message": "Top level error",
							"code":    "notAValidCode",
						},
					},
					map[string]interface{}{
						"error": map[string]interface{}{
							"message": "Request was too large",
							"code":    "LimitsExceeded",
						},
					},
				},
			},
			want: &Error{
				Op:  OpQuery,
				Err: errors.New("Top level error"),
				inner: &Error{
					Op:   OpQuery,
					Kind: KLimitsExceeded,
					Err:  errors.New("Request was too large;See https://docs.microsoft.com/en-us/azure/kusto/concepts/querylimits"),
				},
			},
		},
	}

	for _, test := range tests {
		got := OneToErr(test.input, OpQuery)
		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestOneToErr(%s): -want/+got:\n%s", test.desc, diff)
			log.Printf("%#+v", test.want)
			log.Printf("%#+v", got)
		}
	}
}
