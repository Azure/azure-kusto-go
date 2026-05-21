// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/google/uuid"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

// TokenProvider returns a bearer token for authentication.
type TokenProvider func(ctx context.Context) (string, error)

// S2STokenProvider returns an S2S token for Fabric Private Link authentication.
type S2STokenProvider func(ctx context.Context) (*ingestoptions.S2SToken, error)

// BaseClient is a shared HTTP client for Kusto REST API calls.
// It handles authentication, tracing headers, timeouts, and JSON serialization.
type BaseClient struct {
	httpClient    *http.Client
	tokenProvider TokenProvider
	clientDetails *ingestoptions.ClientDetails

	s2sTokenProvider         S2STokenProvider
	s2sFabricPLAccessContext string
}

// Option configures a BaseClient.
type Option func(*BaseClient)

// WithHTTPClient sets a custom http.Client.
func WithHTTPClient(c *http.Client) Option {
	return func(b *BaseClient) { b.httpClient = c }
}

// WithS2STokenProvider sets the S2S token provider for Fabric Private Link.
func WithS2STokenProvider(p S2STokenProvider) Option {
	return func(b *BaseClient) { b.s2sTokenProvider = p }
}

// WithS2SFabricAccessContext sets the Fabric Private Link access context.
func WithS2SFabricAccessContext(ctx string) Option {
	return func(b *BaseClient) { b.s2sFabricPLAccessContext = ctx }
}

// NewBaseClient creates a new BaseClient.
func NewBaseClient(
	tokenProvider TokenProvider,
	clientDetails *ingestoptions.ClientDetails,
	opts ...Option,
) *BaseClient {
	c := &BaseClient{
		httpClient: &http.Client{
			Timeout: ingestoptions.KustoAPIRequestTimeout,
		},
		tokenProvider: tokenProvider,
		clientDetails: clientDetails,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.clientDetails == nil {
		c.clientDetails = ingestoptions.NewClientDetails("", "", "")
	}
	return c
}

// Do executes an HTTP request with authentication and tracing headers.
func (c *BaseClient) Do(ctx context.Context, method, url string, body io.Reader, contentType string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.setHeaders(req, contentType)

	if err := c.setAuth(ctx, req); err != nil {
		return nil, err
	}

	return c.httpClient.Do(req)
}

// DoJSON executes a POST with JSON body and parses the JSON response into result.
func (c *BaseClient) DoJSON(ctx context.Context, method, url string, requestBody any, result any) (*http.Response, error) {
	var body io.Reader
	if requestBody != nil {
		data, err := json.Marshal(requestBody)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		body = bytes.NewReader(data)
	}

	resp, err := c.Do(ctx, method, url, body, "application/json")
	if err != nil {
		return nil, err
	}

	if result != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		defer resp.Body.Close()
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return resp, fmt.Errorf("failed to read response body: %w", err)
		}
		if len(respBody) > 0 {
			if err := json.Unmarshal(respBody, result); err != nil {
				return resp, fmt.Errorf("failed to unmarshal response: %w", err)
			}
		}
	}

	return resp, nil
}

// setHeaders applies standard Kusto API headers to the request.
func (c *BaseClient) setHeaders(req *http.Request, contentType string) {
	if contentType == "" {
		contentType = "application/json"
	}
	req.Header.Set(ingestoptions.HeaderContentType, contentType)
	req.Header.Set(ingestoptions.HeaderAccept, "application/json")
	req.Header.Set(ingestoptions.HeaderMSVersion, ingestoptions.KustoAPIVersion)
	req.Header.Set(ingestoptions.HeaderConnection, "keep-alive")
	req.Header.Set(ingestoptions.HeaderMSClientRequestID, fmt.Sprintf("KIC.execute;%s", uuid.New().String()))

	if c.clientDetails != nil {
		req.Header.Set(ingestoptions.HeaderMSApp, c.clientDetails.EffectiveApplicationForTracing())
		req.Header.Set(ingestoptions.HeaderMSUser, c.clientDetails.EffectiveUserNameForTracing())
		req.Header.Set(ingestoptions.HeaderMSClientVersion, c.clientDetails.ClientHeader())
	}

	// S2S headers for Fabric Private Link
	if c.s2sFabricPLAccessContext != "" {
		req.Header.Set(ingestoptions.HeaderMSFabricS2SAccessContext, c.s2sFabricPLAccessContext)
	}
}

// setAuth sets the Authorization header and optional S2S token headers.
func (c *BaseClient) setAuth(ctx context.Context, req *http.Request) error {
	if c.tokenProvider != nil {
		token, err := c.tokenProvider(ctx)
		if err != nil {
			return fmt.Errorf("failed to get auth token: %w", err)
		}
		if token != "" {
			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		}
	}

	if c.s2sTokenProvider != nil {
		s2sToken, err := c.s2sTokenProvider(ctx)
		if err != nil {
			return fmt.Errorf("failed to get S2S token: %w", err)
		}
		if s2sToken != nil {
			req.Header.Set(ingestoptions.HeaderMSS2SActorAuthorization, s2sToken.ToHeaderValue())
		}
	}

	return nil
}

// HTTPClient returns the underlying http.Client.
func (c *BaseClient) HTTPClient() *http.Client {
	return c.httpClient
}
