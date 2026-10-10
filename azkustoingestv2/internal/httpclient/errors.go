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
	Code           string `json:"code"`
	Message        string `json:"message"`
	Type           string `json:"@type"`
	DetailMessage  string `json:"@message"`
	FailureCode    string `json:"@failureCode"`
	IsPermanent    *bool  `json:"@permanent"`
}

// oneAPIErrorEnvelope wraps the error field in the response.
type oneAPIErrorEnvelope struct {
	Error *OneAPIError `json:"error"`
}

// ParseErrorResponse parses a non-success HTTP response into an IngestError.
// It attempts to parse OneAPI-style error JSON; if that fails, it uses the raw body.
func ParseErrorResponse(resp *http.Response) error {
	if resp == nil {
		return ingestoptions.NewIngestError("nil response", nil, true)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 8192))
	if err != nil {
		return ingestoptions.NewIngestError(
			fmt.Sprintf("HTTP %d: failed to read error body", resp.StatusCode),
			err, true,
		)
	}

	// Try to parse as OneAPI error
	var envelope oneAPIErrorEnvelope
	if json.Unmarshal(body, &envelope) == nil && envelope.Error != nil {
		return oneAPIToIngestError(resp.StatusCode, envelope.Error)
	}

	// Fallback: raw body
	isPermanent := resp.StatusCode != http.StatusNotFound
	return ingestoptions.NewIngestError(
		fmt.Sprintf("HTTP %d: %s", resp.StatusCode, string(body)),
		nil, isPermanent,
	)
}

// oneAPIToIngestError converts a OneAPI error into the appropriate IngestError type.
func oneAPIToIngestError(statusCode int, e *OneAPIError) error {
	// Determine permanence: @permanent defaults to true when absent
	isPermanent := true
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

	if isPermanent {
		return &ingestoptions.IngestRequestError{
			IngestError: ingestoptions.IngestError{
				Message:        message,
				IsPermanent:    true,
				FailureCode:    parseFailureCode(e.FailureCode),
				FailureSubCode: failureSubCode(statusCode),
			},
			ErrorCode:   e.Code,
			ErrorReason: e.Type,
		}
	}

	return ingestoptions.NewIngestServiceError(message, nil, parseFailureCode(e.FailureCode), failureSubCode(statusCode))
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
