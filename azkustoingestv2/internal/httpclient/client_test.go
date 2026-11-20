// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package httpclient

import (
	"context"
	"encoding/json"
	"errors"
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

func TestBaseClient_RequestHeadersArePerRequest(t *testing.T) {
	requests := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client := NewBaseClient(
		func(context.Context) (string, error) { return "test-token", nil },
		ingestoptions.NewClientDetails("myApp", "myUser", "1.0.0"),
		WithHTTPClient(server.Client()),
		WithS2STokenProvider(func(context.Context) (*ingestoptions.S2SToken, error) {
			return &ingestoptions.S2SToken{Scheme: "Bearer", Token: "s2s-token"}, nil
		}),
		WithS2SFabricAccessContext("fabric-context"),
	)
	response, err := client.DoWithHeaders(context.Background(), "POST", server.URL,
		strings.NewReader("payload"), "application/octet-stream",
		http.Header{"Content-Encoding": []string{"gzip"}})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	headers := <-requests
	assertHeader(t, headers, "Content-Encoding", "gzip")
	assertHeader(t, headers, "Authorization", "Bearer test-token")
	assertHeader(t, headers, ingestoptions.HeaderMSApp, "myApp")
	assertHeader(t, headers, ingestoptions.HeaderMSS2SActorAuthorization, "Bearer s2s-token")
	assertHeader(t, headers, ingestoptions.HeaderMSFabricS2SAccessContext, "fabric-context")

	response, err = client.Do(context.Background(), "GET", server.URL, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	assertHeader(t, <-requests, "Content-Encoding", "")
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

type endpointTransport func(*http.Request) (*http.Response, error)

type endpointRequestBody struct {
	io.Reader
	closed bool
}

func (b *endpointRequestBody) Close() error {
	b.closed = true
	return nil
}

func (f endpointTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestBaseClientValidatesEndpointBeforeAuthentication(t *testing.T) {
	for _, tc := range []struct {
		name    string
		url     string
		skip    bool
		wantErr bool
	}{
		{"trusted", "https://cluster.kusto.windows.net", false, false},
		{"untrusted", "https://custom.example", false, true},
		{"localhost-lookalike", "https://localhost.example", false, true},
		{"explicit-bypass", "https://custom.example", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var tokenCalled, s2sCalled, transportCalled bool
			client := NewBaseClient(
				func(context.Context) (string, error) {
					tokenCalled = true
					return "token", nil
				}, nil,
				WithSkipSecurityChecks(tc.skip),
				WithS2STokenProvider(func(context.Context) (*ingestoptions.S2SToken, error) {
					s2sCalled = true
					return &ingestoptions.S2SToken{Scheme: "Bearer", Token: "actor-token"}, nil
				}),
				WithHTTPClient(&http.Client{Transport: endpointTransport(func(req *http.Request) (*http.Response, error) {
					transportCalled = true
					assertHeader(t, req.Header, "Authorization", "Bearer token")
					return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: req}, nil
				})}),
			)
			body := &endpointRequestBody{Reader: strings.NewReader("data")}
			response, err := client.Do(context.Background(), http.MethodPost, tc.url, body, "")
			if response != nil {
				response.Body.Close()
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("unexpected request error: %v", err)
			}
			if tc.wantErr {
				if !body.closed {
					t.Error("rejected request body was not closed")
				}
				var clientErr *ingestoptions.IngestClientError
				if !errors.As(err, &clientErr) || !clientErr.IsPermanent {
					t.Fatalf("expected permanent client error, got %v", err)
				}
			}
			if tokenCalled != !tc.wantErr || s2sCalled != !tc.wantErr || transportCalled != !tc.wantErr {
				t.Fatalf("unexpected authentication/transport calls: token=%v, s2s=%v, transport=%v",
					tokenCalled, s2sCalled, transportCalled)
			}
		})
	}
}
