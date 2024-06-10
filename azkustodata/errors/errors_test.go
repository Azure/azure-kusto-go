package errors

import (
	"errors"
	"fmt"
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
	got := E(OpMgmt, KLimitsExceeded, &wrappedErr)

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

func TestRetry(t *testing.T) {
	tests := []struct {
		desc string
		err  error
		want bool
	}{
		{desc: "KOther", err: &Error{Kind: KOther}, want: false},
		{desc: "KIO", err: &Error{Kind: KIO}, want: false},
		{desc: "KInternal", err: &Error{Kind: KInternal}, want: false},
		{desc: "KDBNotExist", err: &Error{Kind: KDBNotExist}, want: false},
		{desc: "KLimitsExceeded", err: &Error{Kind: KLimitsExceeded}, want: false},
		{desc: "KClientArgs", err: &Error{Kind: KClientArgs}, want: false},
		{desc: "KLocalFileSystem", err: &Error{Kind: KLocalFileSystem}, want: false},
		{desc: "KTimeout", err: &Error{Kind: KTimeout}, want: true},
		{
			desc: "standard error",
			err:  fmt.Errorf("blah"),
			want: false,
		},
		{
			desc: "permanent was set",
			err:  &Error{permanent: true},
			want: false,
		},
		{
			desc: "http no variable for @permanent",
			err: &Error{
				Kind:       KHTTPError,
				restErrMsg: []byte(`{"error": {"@notPermanent": true}}`),
			},
			want: true,
		},
		{
			desc: "http @permanent set to false",
			err: &Error{
				Kind:       KHTTPError,
				restErrMsg: []byte(`{"error": {"@permanent": false}}`),
			},
			want: true,
		},
		{
			desc: "http @permanent set to true",
			err: &Error{
				Kind:       KHTTPError,
				restErrMsg: []byte(`{"error": {"@permanent": true}}`),
			},
			want: false,
		},
		{
			desc: "inner error can't be retried",
			err: &Error{
				Kind:  KTimeout,
				inner: &Error{Kind: KInternal},
			},
			want: false,
		},
		{
			desc: "inner error can be retried",
			err: &Error{
				Kind:  KTimeout,
				inner: &Error{Kind: KTimeout},
			},
			want: true,
		},
	}

	for _, test := range tests {
		got := Retry(test.err)

		if got != test.want {
			t.Errorf("Test(%s): got %v, want %v", test.desc, got, test.want)
		}
	}
}
