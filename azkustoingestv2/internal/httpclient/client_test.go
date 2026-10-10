// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package httpclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

func TestBaseClient_HeadersSet(t *testing.T) {
	var capturedHeaders http.Header

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	details := ingestoptions.NewClientDetails("myApp", "myUser", "1.0.0")
	client := NewBaseClient(
		func(ctx context.Context) (string, error) { return "test-token", nil },
		details,
		WithHTTPClient(server.Client()),
	)

	_, err := client.Do(context.Background(), "GET", server.URL, nil, "")
	if err != nil {
		t.Fatal(err)
	}

	// Verify required headers
	assertHeader(t, capturedHeaders, "Authorization", "Bearer test-token")
	assertHeader(t, capturedHeaders, ingestoptions.HeaderMSApp, "myApp")
	assertHeader(t, capturedHeaders, ingestoptions.HeaderMSUser, "myUser")
	assertHeader(t, capturedHeaders, ingestoptions.HeaderMSVersion, ingestoptions.KustoAPIVersion)
	assertHeader(t, capturedHeaders, ingestoptions.HeaderConnection, "keep-alive")
	assertHeader(t, capturedHeaders, ingestoptions.HeaderAccept, "application/json")
	assertHeader(t, capturedHeaders, ingestoptions.HeaderContentType, "application/json")

	// Client version should contain "1.0.0"
	cv := capturedHeaders.Get(ingestoptions.HeaderMSClientVersion)
	if !strings.Contains(cv, "1.0.0") {
		t.Errorf("expected client version to contain '1.0.0', got %q", cv)
	}

	// Client request ID should start with "KIC.execute;"
	crid := capturedHeaders.Get(ingestoptions.HeaderMSClientRequestID)
	if !strings.HasPrefix(crid, "KIC.execute;") {
		t.Errorf("expected client request ID to start with 'KIC.execute;', got %q", crid)
	}
}

func TestBaseClient_S2SHeaders(t *testing.T) {
	var capturedHeaders http.Header

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewBaseClient(
		nil,
		nil,
		WithHTTPClient(server.Client()),
		WithS2STokenProvider(func(ctx context.Context) (*ingestoptions.S2SToken, error) {
			return &ingestoptions.S2SToken{Scheme: "Bearer", Token: "s2s-token"}, nil
		}),
		WithS2SFabricAccessContext("fabric-context"),
	)

	_, err := client.Do(context.Background(), "GET", server.URL, nil, "")
	if err != nil {
		t.Fatal(err)
	}

	assertHeader(t, capturedHeaders, ingestoptions.HeaderMSS2SActorAuthorization, "Bearer s2s-token")
	assertHeader(t, capturedHeaders, ingestoptions.HeaderMSFabricS2SAccessContext, "fabric-context")
}

func TestBaseClient_DoJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]string
		json.Unmarshal(body, &req)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"echo": req["message"],
		})
	}))
	defer server.Close()

	client := NewBaseClient(nil, nil, WithHTTPClient(server.Client()))

	var result map[string]string
	_, err := client.DoJSON(context.Background(), "POST", server.URL,
		map[string]string{"message": "hello"},
		&result,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result["echo"] != "hello" {
		t.Errorf("expected echo='hello', got %q", result["echo"])
	}
}

func TestParseErrorResponse_OneAPI(t *testing.T) {
	errorBody := `{"error":{"code":"BadRequest","message":"test error","@type":"Kusto.DataNode","@permanent":true,"@failureCode":"400"}}`

	resp := &http.Response{
		StatusCode: 400,
		Body:       io.NopCloser(strings.NewReader(errorBody)),
	}

	err := ParseErrorResponse(resp)
	if err == nil {
		t.Fatal("expected error")
	}

	reqErr, ok := err.(*ingestoptions.IngestRequestError)
	if !ok {
		t.Fatalf("expected IngestRequestError, got %T", err)
	}
	if !reqErr.IsPermanent {
		t.Error("expected permanent error")
	}
	if reqErr.FailureCode != 400 {
		t.Errorf("expected failure code 400, got %d", reqErr.FailureCode)
	}
}

func TestParseErrorResponse_404(t *testing.T) {
	errorBody := `{"error":{"code":"NotFound","message":"not found","@permanent":true}}`

	resp := &http.Response{
		StatusCode: 404,
		Body:       io.NopCloser(strings.NewReader(errorBody)),
	}

	err := ParseErrorResponse(resp)
	if err == nil {
		t.Fatal("expected error")
	}

	// 404 should override to non-permanent → IngestServiceError
	svcErr, ok := err.(*ingestoptions.IngestServiceError)
	if !ok {
		t.Fatalf("expected IngestServiceError for 404, got %T", err)
	}
	if svcErr.IsPermanent {
		t.Error("404 should be non-permanent")
	}
	if svcErr.FailureSubCode != "NETWORK_ERROR" {
		t.Errorf("expected NETWORK_ERROR sub-code, got %q", svcErr.FailureSubCode)
	}
}

func TestParseErrorResponse_NonJSON(t *testing.T) {
	resp := &http.Response{
		StatusCode: 500,
		Body:       io.NopCloser(strings.NewReader("Internal Server Error")),
	}

	err := ParseErrorResponse(resp)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should contain status code 500, got %q", err.Error())
	}
}

func assertHeader(t *testing.T, headers http.Header, key, expected string) {
	t.Helper()
	got := headers.Get(key)
	if got != expected {
		t.Errorf("header %q: expected %q, got %q", key, expected, got)
	}
}
