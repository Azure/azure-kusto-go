package azkustodata

import (
	"context"
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNoRedirects(t *testing.T) {
	t.Parallel()
	for _, code := range []int{301, 302, 307, 308} {
		for _, test := range []struct {
			name           string
			failAtMetadata bool
		}{
			{name: "cloud", failAtMetadata: true},
			{name: "client"},
		} {
			t.Run(fmt.Sprintf("%s/%d", test.name, code), func(t *testing.T) {
				t.Parallel()
				var redirectedRequests atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/redirected" {
						redirectedRequests.Add(1)
						w.WriteHeader(http.StatusOK)
						return
					}
					if r.URL.Path == metadataPath && !test.failAtMetadata {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					w.Header().Set("Location", "/redirected")
					w.WriteHeader(code)
				}))
				t.Cleanup(server.Close)
				t.Cleanup(func() { cloudInfoCache.Delete(server.URL) })

				// Exercise the SDK's default HTTP client and its redirect policy.
				client, err := New(NewConnectionStringBuilder(server.URL))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, client.Close()) })

				_, err = client.Query(context.Background(), "db", kql.New("table"))
				require.Error(t, err)
				if test.failAtMetadata {
					assert.Contains(t, err.Error(), fmt.Sprintf("%d", code))
					assert.Contains(t, err.Error(), "metadata")
				} else {
					var httpError *errors.HttpError
					require.ErrorAs(t, err, &httpError)
					assert.Equal(t, code, httpError.StatusCode)
				}
				assert.Zero(t, redirectedRequests.Load())
			})
		}
	}
}

func TestHeaders(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                              string
		kcsbApplication, kcsbUser         string
		propApplication, propUser         string
		expectedApplication, expectedUser string
	}{
		{
			name: "TestDefault",
		},
		{
			name:                "TestKcsb",
			kcsbApplication:     "kcsbApplication",
			kcsbUser:            "kcsbUser",
			expectedApplication: "kcsbApplication",
			expectedUser:        "kcsbUser",
		},
		{
			name:                "TestProp",
			propApplication:     "propApplication",
			propUser:            "propUser",
			expectedApplication: "propApplication",
			expectedUser:        "propUser",
		},
		{
			name:                "TestKcsbProp",
			kcsbApplication:     "kcsbApplication",
			kcsbUser:            "kcsbUser",
			propApplication:     "propApplication",
			propUser:            "propUser",
			expectedApplication: "propApplication",
			expectedUser:        "propUser",
		},
	}
	for _, tt := range tests {
		tt := tt // Capture
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			kcsb := NewConnectionStringBuilder("https://test.kusto.windows.net")

			if tt.kcsbApplication != "" {
				kcsb.ApplicationForTracing = tt.kcsbApplication
			}
			if tt.kcsbUser != "" {
				kcsb.UserForTracing = tt.kcsbUser
			}

			queryOptions := make([]QueryOption, 0)
			queryOptions = append(queryOptions, Application(tt.propApplication))
			queryOptions = append(queryOptions, User(tt.propUser))

			opts, err := setQueryOptions(context.Background(), errors.OpQuery, kql.New("test"), queryCall, queryOptions...)
			require.NoError(t, err)

			client, err := New(kcsb)
			require.NoError(t, err)

			headers := client.conn.(*Conn).getHeaders(*opts.requestProperties)

			if tt.expectedApplication != "" {
				assert.Equal(t, tt.expectedApplication, headers.Get("x-ms-app"))
			} else {
				assert.Greater(t, len(headers.Get("x-ms-app")), 0)
			}
			if tt.expectedUser != "" {
				assert.Equal(t, tt.expectedUser, headers.Get("x-ms-user"))
			} else {
				assert.Greater(t, len(headers.Get("x-ms-user")), 0)
			}
			assert.True(t, strings.HasPrefix(headers.Get("x-ms-client-version"), "Kusto.Go.Client:"))
		})
	}
}

func TestSchemas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		url     string
		hasAuth bool
		err     error
	}{
		{
			name:    "TestNoAuthHttps",
			url:     "https://test.kusto.windows.net",
			hasAuth: false,
		},
		{
			name:    "TestNoAuthHttp",
			url:     "http://test.kusto.windows.net",
			hasAuth: false,
		},
		{
			name:    "TestAuthHttps",
			url:     "https://test.kusto.windows.net",
			hasAuth: true,
		},
		{
			name:    "TestAuthHttp",
			url:     "http://test.kusto.windows.net",
			hasAuth: true,
			err:     errors.ES(errors.OpServConn, errors.KClientArgs, "cannot use token provider with http endpoint, as it would send the token in clear text").SetNoRetry(),
		},
		{
			name: "TestEmptyUrl",
			url:  "",
			err:  errors.ES(errors.OpQuery, errors.KClientArgs, "endpoint cannot be empty"),
		},
	}
	for _, tt := range tests {
		tt := tt // Capture
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var kcsb = &ConnectionStringBuilder{}
			if tt.url != "" {
				kcsb = NewConnectionStringBuilder(tt.url)
			}
			if tt.hasAuth {
				kcsb = kcsb.WithApplicationToken("1", "1")
			}
			client, err := New(kcsb)
			if tt.err != nil {
				assert.Equal(t, tt.err, err)
				return
			}
			assert.NoError(t, err)

			_, err = client.Query(context.Background(), "test", kql.New("test"))
			assert.Contains(t, err.Error(), "no such host")
		})
	}
}

func TestSetConnectorDetails(t *testing.T) {
	t.Parallel()
	tests := []struct {
		testName                          string
		name, version                     string
		sendUser                          bool
		overrideUser, appName, appVersion string
		additionalFields                  []StringPair
		expectedApp, expectedUser         string
		appPrefix                         bool
		expectAnyUser                     bool
	}{
		{
			testName: "TestNameAndVersion",
			name:     "testName", version: "testVersion",
			expectedApp:  "Kusto.testName:{testVersion}|App.",
			appPrefix:    true,
			expectedUser: "[none]",
		},
		{
			testName: "TestNameAndVersionAndUser",
			name:     "testName", version: "testVersion", sendUser: true,
			expectedApp:   "Kusto.testName:{testVersion}|App.",
			appPrefix:     true,
			expectAnyUser: true,
		},
		{
			testName: "TestAll",
			name:     "testName", version: "testVersion", sendUser: true, overrideUser: "testUser", appName: "testApp", appVersion: "testAppVersion", additionalFields: []StringPair{{"testKey", "testValue"}},
			expectedApp:  "Kusto.testName:{testVersion}|App.{testApp}:{testAppVersion}|testKey:{testValue}",
			expectedUser: "testUser",
		},
	}
	for _, tt := range tests {
		tt := tt // Capture
		t.Run(tt.testName, func(t *testing.T) {
			t.Parallel()
			kcsb := NewConnectionStringBuilder("https://test.kusto.windows.net")
			kcsb.SetConnectorDetails(tt.name, tt.version, tt.appName, tt.appVersion, tt.sendUser, tt.overrideUser, tt.additionalFields...)

			if tt.appPrefix {
				assert.True(t, strings.HasPrefix(kcsb.ApplicationForTracing, tt.expectedApp))
			} else {
				assert.Equal(t, tt.expectedApp, kcsb.ApplicationForTracing)
			}

			if tt.expectAnyUser {
				assert.Greater(t, len(kcsb.UserForTracing), 0)
			} else {
				assert.Equal(t, tt.expectedUser, kcsb.UserForTracing)
			}
		})
	}
}

func TestTimeout(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(1997, 3, 9, 6, 14, 6, 3, time.UTC)
	nower = func() time.Time {
		return fixedTime
	}

	newContextWithTimeout := func(duration time.Duration) context.Context {
		ctx, _ := context.WithDeadline(context.Background(), fixedTime.Add(duration))
		return ctx
	}

	tests := []struct {
		name                  string
		ctx                   context.Context
		callType              int
		queryOptions          []QueryOption
		expectedServerTimeout time.Duration
	}{
		{
			name:                  "TestDefaultQuery",
			ctx:                   context.Background(),
			callType:              queryCall,
			expectedServerTimeout: defaultQueryTimeout,
		},
		{
			name:                  "TestDefaultMgmt",
			ctx:                   context.Background(),
			callType:              mgmtCall,
			expectedServerTimeout: defaultMgmtTimeout,
		},
		{
			name:                  "OptionSet",
			ctx:                   context.Background(),
			callType:              queryCall,
			queryOptions:          []QueryOption{ServerTimeout(15 * time.Second)},
			expectedServerTimeout: 15 * time.Second,
		},
		{
			name:                  "Context",
			ctx:                   newContextWithTimeout(15 * time.Second),
			callType:              queryCall,
			expectedServerTimeout: 15 * time.Second,
		},
		{
			name:                  "ContextAndOption",
			ctx:                   newContextWithTimeout(15 * time.Second),
			queryOptions:          []QueryOption{ServerTimeout(250 * time.Second)},
			callType:              queryCall,
			expectedServerTimeout: 250 * time.Second,
		},
	}
	for _, tt := range tests {
		tt := tt // Capture
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts, err := setQueryOptions(tt.ctx, errors.OpUnknown, kql.New("test"), tt.callType, tt.queryOptions...)
			require.NoError(t, err)

			require.Equal(t, value.TimespanString(tt.expectedServerTimeout), opts.requestProperties.Options[ServerTimeoutValue])
		})
	}
}

type recordingTransport struct {
	mu          sync.Mutex
	metadata    string
	metaStatus  int
	authHeaders []string
	paths       []string
}

func (r *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.paths = append(r.paths, req.URL.Path)
	if a := req.Header.Get("Authorization"); a != "" {
		r.authHeaders = append(r.authHeaders, a)
	}
	r.mu.Unlock()

	if req.URL.Path == metadataPath {
		status := r.metaStatus
		if status == 0 {
			status = http.StatusOK
		}
		return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: http.Header{}, Body: io.NopCloser(strings.NewReader(r.metadata)), Request: req}, nil
	}
	return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: http.Header{}, Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
}

type testStreamFormat struct{}

func (testStreamFormat) CamelCase() string                      { return "csv" }
func (testStreamFormat) KnownOrDefault() DataFormatForStreaming { return testStreamFormat{} }

const untrustedMetadata = `{"AzureAD":{"LoginEndpoint":"https://login.microsoftonline.com","LoginMfaRequired":false,"KustoClientAppId":"x","KustoClientRedirectUri":"x","KustoServiceResourceId":"https://untrusted.example.com","FirstPartyAuthorityUrl":"x"}}`

// forgetMetadata drops cached auth metadata for the given URIs after the test, so repeated runs (-count=N) start clean.
func forgetMetadata(t *testing.T, uris ...string) {
	t.Cleanup(func() {
		for _, u := range uris {
			cloudInfoCache.Delete(u)
		}
	})
}

func newRecordingClient(t *testing.T, endpoint string, rt *recordingTransport) *Client {
	forgetMetadata(t, endpoint)
	kcsb := NewConnectionStringBuilder(endpoint).WithApplicationToken("app", "secret-token")
	client, err := New(kcsb, WithHttpClient(&http.Client{Transport: rt}))
	require.NoError(t, err)
	return client
}

func TestUntrustedEndpointNeverReceivesToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		endpoint   string
		metaStatus int
		call       func(c *Client) error
	}{
		{
			name:     "Query",
			endpoint: "https://attacker-query.example.com:443",
			call: func(c *Client) error {
				_, err := c.Query(context.Background(), "db", kql.New("print 1"))
				return err
			},
		},
		{
			name:     "Mgmt",
			endpoint: "https://attacker-mgmt.example.com",
			call: func(c *Client) error {
				_, err := c.Mgmt(context.Background(), "db", kql.New(".show tables"))
				return err
			},
		},
		{
			name:     "StreamIngest",
			endpoint: "https://attacker-stream.example.com",
			call: func(c *Client) error {
				return c.conn.(*Conn).StreamIngest(context.Background(), "db", "t", strings.NewReader("a"), testStreamFormat{}, "", "", false)
			},
		},
		{
			name:       "MetadataFailure",
			endpoint:   "https://attacker-metafail.example.com",
			metaStatus: http.StatusInternalServerError,
			call: func(c *Client) error {
				_, err := c.Query(context.Background(), "db", kql.New("print 1"))
				return err
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rt := &recordingTransport{metadata: untrustedMetadata, metaStatus: tt.metaStatus}
			client := newRecordingClient(t, tt.endpoint, rt)

			err := tt.call(client)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "could not validate endpoint")
			assert.Empty(t, rt.authHeaders, "token must not be sent to an untrusted endpoint")
			assert.Equal(t, []string{metadataPath}, rt.paths)
		})
	}
}

func TestTrustedEndpointWithPortIsAccepted(t *testing.T) {
	t.Parallel()
	rt := &recordingTransport{}
	client := newRecordingClient(t, "https://trustedport.westus.kusto.windows.net:443", rt)

	err := client.conn.(*Conn).StreamIngest(context.Background(), "db", "t", strings.NewReader("a"), testStreamFormat{}, "", "", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"Bearer secret-token"}, rt.authHeaders)
}

type countingCredential struct {
	calls atomic.Int32
}

func (c *countingCredential) GetToken(_ context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error) {
	c.calls.Add(1)
	return azcore.AccessToken{Token: "cred-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func TestUntrustedEndpointNeverAcquiresToken(t *testing.T) {
	t.Parallel()
	rt := &recordingTransport{metadata: untrustedMetadata}
	cred := &countingCredential{}
	forgetMetadata(t, "https://attacker-cred.example.com")
	kcsb := NewConnectionStringBuilder("https://attacker-cred.example.com").WithTokenCredential(cred)
	client, err := New(kcsb, WithHttpClient(&http.Client{Transport: rt}))
	require.NoError(t, err)

	_, err = client.Query(context.Background(), "db", kql.New("print 1"))
	require.Error(t, err)
	assert.Equal(t, int32(0), cred.calls.Load(), "token must not be acquired for an untrusted endpoint")
	assert.Empty(t, rt.authHeaders)
}

func TestTokenProviderRejectsUntrustedMetadataSource(t *testing.T) {
	t.Parallel()
	// Metadata for every host claims the public cloud, so only the hostname decides trust.
	rt := &recordingTransport{metadata: untrustedMetadata}
	httpClient := &http.Client{Transport: rt}
	cred := &countingCredential{}

	// Token provider reads its audience from an untrusted host, while the Conn targets a trusted host.
	forgetMetadata(t, "https://attacker-tp.example.com", "https://mismatch.westus.kusto.windows.net")
	kcsb := NewConnectionStringBuilder("https://attacker-tp.example.com").WithTokenCredential(cred)
	client, err := New(kcsb, WithHttpClient(httpClient))
	require.NoError(t, err)
	conn, err := NewConn("https://mismatch.westus.kusto.windows.net", client.Auth(), httpClient, client.ClientDetails())
	require.NoError(t, err)

	err = conn.StreamIngest(context.Background(), "db", "t", strings.NewReader("a"), testStreamFormat{}, "", "", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not trusted")
	assert.False(t, errors.Retry(err), "an untrusted metadata source must not be retried: %v", err)
	assert.Equal(t, int32(0), cred.calls.Load())
	assert.Empty(t, rt.authHeaders)
}

type closeRecorder struct {
	io.Reader
	closed atomic.Bool
}

func (c *closeRecorder) Close() error {
	c.closed.Store(true)
	return nil
}

func TestStreamIngestClosesPayloadWhenEndpointRejected(t *testing.T) {
	t.Parallel()
	rt := &recordingTransport{metadata: untrustedMetadata}
	client := newRecordingClient(t, "https://attacker-close.example.com", rt)
	payload := &closeRecorder{Reader: strings.NewReader("a")}

	err := client.conn.(*Conn).StreamIngest(context.Background(), "db", "t", payload, testStreamFormat{}, "", "", false)
	require.Error(t, err)
	assert.True(t, payload.closed.Load(), "payload must be closed so a streaming compressor is not blocked")
}

type blockingTransport struct{}

func (blockingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	<-req.Context().Done()
	return nil, req.Context().Err()
}

func TestEndpointValidationHonorsContext(t *testing.T) {
	t.Parallel()
	forgetMetadata(t, "https://blocking-meta.example.com")
	kcsb := NewConnectionStringBuilder("https://blocking-meta.example.com").WithApplicationToken("app", "secret-token")
	client, err := New(kcsb, WithHttpClient(&http.Client{Transport: blockingTransport{}}))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- client.conn.(*Conn).StreamIngest(ctx, "db", "t", strings.NewReader("a"), testStreamFormat{}, "", "", false)
	}()

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("endpoint validation did not stop when the context was cancelled")
	}
}

func TestEndpointValidationRetryClassification(t *testing.T) {
	t.Parallel()

	transient := &recordingTransport{metadata: untrustedMetadata, metaStatus: http.StatusServiceUnavailable}
	_, err := newRecordingClient(t, "https://retry-transient.example.com", transient).Query(context.Background(), "db", kql.New("print 1"))
	require.Error(t, err)
	assert.True(t, errors.Retry(err), "a metadata fetch failure may be transient and must stay retryable: %v", err)
	assert.Empty(t, transient.authHeaders)

	untrusted := &recordingTransport{metadata: untrustedMetadata}
	_, err = newRecordingClient(t, "https://retry-untrusted.example.com", untrusted).Query(context.Background(), "db", kql.New("print 1"))
	require.Error(t, err)
	assert.False(t, errors.Retry(err), "an untrusted endpoint must not be retried: %v", err)
	assert.Empty(t, untrusted.authHeaders)

	err = newRecordingClient(t, "https://retry-untrusted-stream.example.com", untrusted).conn.(*Conn).StreamIngest(context.Background(), "db", "t", strings.NewReader("a"), testStreamFormat{}, "", "", false)
	require.Error(t, err)
	assert.False(t, errors.Retry(err), "streaming must keep the no-retry classification of an untrusted endpoint: %v", err)
	assert.Empty(t, untrusted.authHeaders)
}

// hostBlockingTransport blocks metadata requests to one host until the request context ends; other hosts get an empty
// (public cloud default) metadata response.
type hostBlockingTransport struct{ blockHost string }

func (b hostBlockingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Hostname() == b.blockHost {
		<-req.Context().Done()
		return nil, req.Context().Err()
	}
	return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: http.Header{}, Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
}

func TestTokenProviderMetadataHonorsContext(t *testing.T) {
	t.Parallel()
	const tokenSource = "https://ingest-ctxtp.westus.kusto.windows.net"
	const target = "https://ctxtp.westus.kusto.windows.net"
	forgetMetadata(t, tokenSource, target)
	httpClient := &http.Client{Transport: hostBlockingTransport{blockHost: "ingest-ctxtp.westus.kusto.windows.net"}}

	// As with streaming from an ingest client: the Conn targets the engine while the token comes from the DM URL.
	client, err := New(NewConnectionStringBuilder(tokenSource).WithTokenCredential(&countingCredential{}), WithHttpClient(httpClient))
	require.NoError(t, err)
	conn, err := NewConn(target, client.Auth(), httpClient, client.ClientDetails())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- conn.StreamIngest(ctx, "db", "t", strings.NewReader("a"), testStreamFormat{}, "", "", false)
	}()

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("token provider metadata fetch did not stop when the context was cancelled")
	}
}
