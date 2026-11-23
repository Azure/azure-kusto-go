// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package httpclient

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

// OneAPIError represents a Kusto OneAPI-style error response.
type OneAPIError struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	Type          string `json:"@type"`
	DetailMessage string `json:"@message"`
	FailureCode   string `json:"@failureCode"`
	IsPermanent   *bool  `json:"@permanent"`
}

// oneAPIErrorEnvelope wraps the error field in the response.
type oneAPIErrorEnvelope struct {
	Error *OneAPIError `json:"error"`
}

// ParseErrorResponse parses a non-success HTTP response into an IngestError and closes its body.
// It attempts to parse OneAPI-style error JSON; if that fails, it uses the raw body.
func ParseErrorResponse(resp *http.Response) error {
	if resp == nil {
		return ingestoptions.NewIngestError("nil response", nil, true)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 8192))
	if err != nil {
		ingestErr := ingestoptions.NewIngestError(
			fmt.Sprintf("HTTP %d: failed to read error body", resp.StatusCode),
			err, isPermanentHTTPStatus(resp.StatusCode),
		)
		ingestErr.HTTPStatusCode = resp.StatusCode
		return ingestErr
	}

	// Try to parse as OneAPI error
	var envelope oneAPIErrorEnvelope
	if json.Unmarshal(body, &envelope) == nil && envelope.Error != nil {
		return oneAPIToIngestError(resp.StatusCode, envelope.Error)
	}

	// Fallback: raw body
	ingestErr := ingestoptions.NewIngestError(
		fmt.Sprintf("HTTP %d: %s", resp.StatusCode, string(body)),
		nil, isPermanentHTTPStatus(resp.StatusCode),
	)
	ingestErr.HTTPStatusCode = resp.StatusCode
	return ingestErr
}

// oneAPIToIngestError converts a OneAPI error into the appropriate IngestError type.
func oneAPIToIngestError(statusCode int, e *OneAPIError) error {
	isPermanent := isPermanentHTTPStatus(statusCode)
	if e.IsPermanent != nil {
		isPermanent = *e.IsPermanent
	}
	// HTTP 404 overrides to non-permanent
	if statusCode == http.StatusNotFound {
		isPermanent = false
	}

	message := e.Message
	if e.DetailMessage != "" {
		message = e.DetailMessage
	}
	if message == "" {
		message = fmt.Sprintf("HTTP %d: %s", statusCode, e.Code)
	}

	base := ingestoptions.IngestError{
		Message:        message,
		HTTPStatusCode: statusCode,
		IsPermanent:    isPermanent,
		FailureCode:    parseFailureCode(e.FailureCode),
		FailureSubCode: failureSubCode(statusCode),
	}
	if isPermanent {
		return &ingestoptions.IngestRequestError{
			IngestError: base,
			ErrorCode:   e.Code,
			ErrorReason: e.Type,
		}
	}

	return &ingestoptions.IngestServiceError{IngestError: base}
}

func isPermanentHTTPStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusNotFound, http.StatusRequestTimeout, http.StatusTooManyRequests:
		return false
	default:
		return statusCode < http.StatusInternalServerError || statusCode >= 600
	}
}

// parseFailureCode converts the string @failureCode to int.
func parseFailureCode(s string) int {
	if s == "" {
		return 0
	}
	var code int
	if _, err := fmt.Sscanf(s, "%d", &code); err != nil {
		return 0
	}
	return code
}

// failureSubCode returns a sub-code string for certain HTTP status codes.
func failureSubCode(statusCode int) string {
	if statusCode == http.StatusNotFound {
		return "NETWORK_ERROR"
	}
	return ""
}
