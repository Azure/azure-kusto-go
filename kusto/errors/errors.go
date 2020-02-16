/*
Package errors provides the error package for Kusto. It wraps all errors for Kusto. No error should be
generated that doesn't come from this package. This borrows heavily fron the Upspin errors paper written
by Rob Pike. See: https://commandcenter.blogspot.com/2017/12/error-handling-in-upspin.html
Key differences are that we support wrapped errors and the 1.13 Unwrap/Is/As additions to the go stdlib
errors package and this is tailored for Kusto and not Upspin.

Usage is simply to pass an Op, a Kind, and either a standard error to be wrapped or string that will become
a string error.  See examples included in the file for more details.
*/
package errors

import (
	"errors"
	"fmt"
	"log"
	"runtime"
	"strings"
)

// Separator is the string used to separate nested errors. By
// default, to make errors easier on the eye, nested errors are
// indented on a new line. A server may instead choose to keep each
// error on a single line by modifying the separator string, perhaps
// to ":: ".
var Separator = ":\n\t"

// Op field denotes the operation being performed.
type Op uint16

//go:generate stringer -type Op
const (
	OpUnknown  Op = 0 // OpUnknown indicates that the operation that caused the problem is unknown.
	OpQuery    Op = 1 // OpQuery indicates that a Query() call is being made.
	OpMgmt     Op = 2 // OpMgmt indicates that a Mgmt() call is being made.
	OpServConn Op = 3 // OpServConn indicates that the client is attempting to connect to the service.
)

// Kind field classifies the error as one of a set of standard conditions.
type Kind uint16

//go:generate stringer -type Kind
const (
	KOther          Kind = 0 // Other indicates the error kind was not defined.
	KIO             Kind = 1 // External I/O error such as network failure.
	KInternal       Kind = 2 // Internal error or inconsistency at the server.
	KDBNotExist     Kind = 3 // Database does not exist.
	KTimeout        Kind = 4 // The request timed out.
	KLimitsExceeded Kind = 5 // The request was too large.
	KClientArgs     Kind = 6 // The client supplied some type of arg(s) that were invalid.\
	KClientInternal Kind = 7 // Internal error at the client.
	KHTTPError      Kind = 8 // The HTTP client gave some type of error. This wraps the http library error types.
)

// Error is a core error for the Kusto package.
type Error struct {
	// Op is the operations that the client was trying to perform.
	Op Op
	// Kind is the error code we identify the error as.
	Kind Kind
	// Err is the wrapped internal error message. This may be of any error
	// type and may also wrap errors.
	Err error

	inner *Error
}

func (e *Error) isZero() bool {
	return e == nil || (e.Op == OpUnknown && e.Kind == KOther && e.Err == nil)
}

// Unwrap implements "interface {Unwrap() error}" as defined internaly by the go stdlib errors package.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	if e.inner == nil {
		return e.Err
	}
	return e.inner
}

// pad appends str to the buffer if the buffer already has some kusto.
func pad(b *strings.Builder, str string) {
	if b.Len() == 0 {
		return
	}
	b.WriteString(str)
}

func (e *Error) Error() string {
	b := new(strings.Builder)
	if e.Op != OpUnknown {
		pad(b, ": ")
		b.WriteString(fmt.Sprintf("Op(%s)", e.Op.String()))
	}
	if e.Kind != KOther {
		pad(b, ": ")
		b.WriteString(fmt.Sprintf("Kind(%s)", e.Kind.String()))
	}

	if e.Err != nil {
		pad(b, ": ")
		b.WriteString(e.Err.Error())
	}
	var inner = e.inner
	for {
		if inner == nil {
			break
		}
		pad(b, Separator)
		b.WriteString(inner.Err.Error())
		inner = inner.inner
	}

	if b.Len() == 0 {
		return "no error"
	}
	return b.String()
}

// E constructs an Error. You may pass in an Op, Kind and error.  This will strip an *Error if you
// pass if of its Kind and Op and put it in here. It will wrap a non-*Error implementation of error.
// If you want to wrap the *Error in an *Error, use W(). If you pass a nil error, it panics.
func E(o Op, k Kind, err error) error {
	if err == nil {
		panic("cannot pass a nil error")
	}
	return e(o, k, err)
}

// ES constructs an Error. You may pass in an Op, Kind, string and args to the string (like fmt.Sprintf).
// If the result of strings.TrimSpace(s+args) == "", it panics.
func ES(o Op, k Kind, s string, args ...interface{}) error {
	str := fmt.Sprintf(s, args...)
	if strings.TrimSpace(str) == "" {
		panic("errors.ES() cannot have an empty string error")
	}
	return e(k, o, str)
}

// e constructs an Error. You may pass in an Op, Kind, string or error.  This will strip an *Error if you
// pass if of its Kind and Op and put it in here. It will wrap a non-*Error implementation of error.
// If you want to wrap the *Error in an *Error, use W().
func e(args ...interface{}) error {
	if len(args) == 0 {
		panic("call to errors.E with no arguments")
	}
	e := &Error{}

	for _, arg := range args {
		switch arg := arg.(type) {
		case Op:
			e.Op = arg
		case string:
			e.Err = errors.New(arg)
		case Kind:
			e.Kind = arg
		case *Error:
			// Make a copy
			copy := *arg
			e.Err = copy.Err
		case error:
			e.Err = arg
		default:
			if err, ok := arg.(error); ok {
				e.Err = err
			} else {
				_, file, line, _ := runtime.Caller(1)
				log.Printf("errors.E: bad call from %s:%d: %v", file, line, args)
				return fmt.Errorf("unknown type %T, value %v in error call", arg, arg)
			}
		}
	}

	return e
}

// W wraps error outer around inner. Both must be of type *Error or this will panic.
func W(inner error, outer error) error {
	o, ok := outer.(*Error)
	if !ok {
		panic("W() got an outer error that was not of type *Error")
	}
	i, ok := inner.(*Error)
	if !ok {
		panic("W() got an inner error that was not of type *Error")
	}

	o.inner = i
	return o
}

// OneToErr translates what we think is a Kusto OneApiError into an Error. If we don't recognize it, we return nil.
// This tries to wrap the internal errors, but because the errors we see don't conform to OneAPIError, I'm not sure
// what is going on.  We shouldn't get a list of errors, but we do.  We should get embedded. So I'm taking the guess
// that these are supposed to be wrapped errors.
func OneToErr(m map[string]interface{}, op Op) *Error {
	if m == nil {
		return nil
	}

	if _, ok := m["OneApiErrors"]; ok {
		var topErr *Error
		if oneErrors, ok := m["OneApiErrors"].([]interface{}); ok {
			var bottomErr *Error
			for _, oneErr := range oneErrors {
				if errMap, ok := oneErr.(map[string]interface{}); ok {
					e := oneToErr(errMap, bottomErr, op)
					if e == nil {
						continue
					}
					if topErr == nil {
						topErr = e
						bottomErr = e
						continue
					}
					bottomErr = e
				}
			}
			return topErr
		}
	}
	return nil
}

func oneToErr(m map[string]interface{}, err *Error, op Op) *Error {
	errJSON, ok := m["error"]
	if !ok {
		return nil
	}
	errMap, ok := errJSON.(map[string]interface{})
	if !ok {
		return nil
	}

	var msg string
	msgInter, ok := errMap["message"]
	if !ok {
		return nil
	}

	if msg, ok = msgInter.(string); !ok {
		return nil
	}

	var code string

	codeInter, ok := errMap["code"]
	if ok {
		codeStr, ok := codeInter.(string)
		if ok {
			code = codeStr
		}
	}

	var kind Kind
	switch code {
	case "LimitsExceeded":
		kind = KLimitsExceeded
		msg = msg + ";See https://docs.microsoft.com/en-us/azure/kusto/concepts/querylimits"
	}

	if err == nil {
		return ES(op, kind, msg).(*Error)
	}

	W(ES(op, kind, msg), err)

	return err
}
