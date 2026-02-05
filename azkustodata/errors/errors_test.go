package errors

import (
	"errors"
	"fmt"
	"io"
	"log"
	"testing"
	"time"

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
		// CombinedError tests - these verify the Unwrap() []error fix works correctly
		{
			desc: "CombinedError with 3 plain errors (no *Error) - not retryable",
			err: CombineErrors(
				fmt.Errorf("error 1"),
				fmt.Errorf("error 2"),
				fmt.Errorf("error 3"),
			),
			want: false,
		},
		{
			desc: "CombinedError with 3 errors, last is retryable *Error",
			err: CombineErrors(
				fmt.Errorf("error 1"),
				fmt.Errorf("error 2"),
				&Error{Kind: KTimeout},
			),
			want: true,
		},
		{
			desc: "CombinedError with 5 errors, one retryable *Error among them",
			err: CombineErrors(
				fmt.Errorf("error 1"),
				fmt.Errorf("error 2"),
				fmt.Errorf("error 3"),
				&Error{Kind: KHTTPError, restErrMsg: []byte(`{"error": {"@permanent": false}}`)},
				fmt.Errorf("error 5"),
			),
			want: true,
		},
		{
			desc: "CombinedError with permanent *Error - not retryable",
			err: CombineErrors(
				fmt.Errorf("error 1"),
				&Error{Kind: KTimeout, permanent: true},
				fmt.Errorf("error 3"),
			),
			want: false,
		},
	}

	for _, test := range tests {
		got := Retry(test.err)

		if got != test.want {
			t.Errorf("Test(%s): got %v, want %v", test.desc, got, test.want)
		}
	}
}

// TestCombinedErrorUnwrapNoInfiniteLoop verifies that errors.As on CombinedError
// with multiple errors does not cause an infinite loop (the original bug).
func TestCombinedErrorUnwrapNoInfiniteLoop(t *testing.T) {
	// Create a CombinedError with 3+ errors
	combined := CombineErrors(
		fmt.Errorf("error 1"),
		fmt.Errorf("error 2"),
		fmt.Errorf("error 3"),
		fmt.Errorf("error 4"),
	)

	// This would hang forever with the old buggy Unwrap() implementation
	// that returned 'c' (itself) when len(c.Errors) >= 2
	done := make(chan bool, 1)
	go func() {
		var target *Error
		errors.As(combined, &target) // Should NOT infinite loop
		done <- true
	}()

	select {
	case <-done:
		// Success - errors.As completed without hanging
	case <-time.After(1 * time.Second):
		t.Fatal("errors.As on CombinedError caused infinite loop (timeout after 1 second)")
	}
}

// TestCombinedErrorAsFindsNestedError verifies errors.As can find *Error
// inside a CombinedError with multiple errors.
func TestCombinedErrorAsFindsNestedError(t *testing.T) {
	expectedErr := &Error{Op: OpQuery, Kind: KTimeout, Err: fmt.Errorf("timeout occurred")}

	combined := CombineErrors(
		fmt.Errorf("error 1"),
		fmt.Errorf("error 2"),
		expectedErr,
		fmt.Errorf("error 4"),
	)

	var target *Error
	if !errors.As(combined, &target) {
		t.Fatal("errors.As failed to find *Error inside CombinedError")
	}

	if target.Op != OpQuery {
		t.Errorf("got Op=%v, want Op=%v", target.Op, OpQuery)
	}
	if target.Kind != KTimeout {
		t.Errorf("got Kind=%v, want Kind=%v", target.Kind, KTimeout)
	}
}
