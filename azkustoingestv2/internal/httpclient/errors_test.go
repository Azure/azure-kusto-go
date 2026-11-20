// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package httpclient

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

func TestParseErrorResponseHTTPStatusAndPermanence(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		permanent bool
		code      int
	}{
		{"throttled-json", 429, `{"error":{"code":"TooManyRequests","message":"Try later"}}`, false, 0},
		{"throttled-text", 429, "Too many requests", false, 0},
		{"timeout-json", 408, `{"error":{"code":"Timeout"}}`, false, 0},
		{"timeout-text", 408, "Request timed out", false, 0},
		{"server-error-json", 500, `{"error":{"code":"InternalError"}}`, false, 0},
		{"server-error-text", 500, "Internal Server Error", false, 0},
		{"gateway-json", 502, `{"error":{"code":"BadGateway"}}`, false, 0},
		{"unavailable-json", 503, `{"error":{"code":"Unavailable"}}`, false, 0},
		{"gateway-timeout", 504, "Gateway Timeout", false, 0},
		{"preserve-service-code", 429, `{"error":{"code":"Busy","@failureCode":"12345"}}`, false, 12345},
		{"explicit-permanent-throttle", 429, `{"error":{"code":"Rejected","@permanent":true}}`, true, 0},
		{"explicit-permanent-server-error", 503, `{"error":{"code":"Rejected","@permanent":true}}`, true, 0},
		{"explicit-transient-client-error", 400, `{"error":{"code":"Retry","@permanent":false}}`, false, 0},
		{"bad-request-default", 400, `{"error":{"code":"BadRequest"}}`, true, 0},
		{"unauthorized-default", 401, "Unauthorized", true, 0},
		{"forbidden-default", 403, `{"error":{"code":"Forbidden"}}`, true, 0},
		{"not-found-preserves-override", 404, `{"error":{"code":"NotFound","@permanent":true}}`, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ParseErrorResponse(&http.Response{
				StatusCode: tc.status,
				Body:       io.NopCloser(strings.NewReader(tc.body)),
			})
			var base *ingestoptions.IngestError
			switch e := err.(type) {
			case *ingestoptions.IngestError:
				base = e
			case *ingestoptions.IngestRequestError:
				base = &e.IngestError
			case *ingestoptions.IngestServiceError:
				base = &e.IngestError
			default:
				t.Fatalf("unexpected error type: %T", err)
			}
			if base.HTTPStatusCode != tc.status || base.FailureCode != tc.code || base.IsPermanent != tc.permanent {
				t.Fatalf("got HTTP=%d, service code=%d, permanent=%t; want %d, %d, %t",
					base.HTTPStatusCode, base.FailureCode, base.IsPermanent, tc.status, tc.code, tc.permanent)
			}
		})
	}
}

func TestParseErrorResponseReadFailurePreservesHTTPStatus(t *testing.T) {
	want := errors.New("response body failed")
	err := ParseErrorResponse(&http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Body:       io.NopCloser(iotest.ErrReader(want)),
	})
	var ingestErr *ingestoptions.IngestError
	if !errors.As(err, &ingestErr) || !errors.Is(err, want) ||
		ingestErr.HTTPStatusCode != http.StatusServiceUnavailable || ingestErr.IsPermanent {
		t.Fatalf("response read failure lost status or cause: %v", err)
	}
}
