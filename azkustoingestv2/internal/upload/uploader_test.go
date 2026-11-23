// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package upload

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/config"
)

type cachedUploadConfig struct {
	data *config.CachedConfigurationData
}

func (c cachedUploadConfig) GetConfiguration(context.Context) (*config.CachedConfigurationData, error) {
	return c.data, nil
}
func (cachedUploadConfig) RefreshInterval() time.Duration { return time.Hour }
func (cachedUploadConfig) Close()                         {}

type seekableUploadReader struct{ *bytes.Reader }

func (seekableUploadReader) Close() error { return nil }

type uploadRequest struct {
	method  string
	path    string
	query   url.Values
	headers http.Header
	body    []byte
}

func recordUpload(t *testing.T, requests chan<- uploadRequest, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Errorf("read upload body: %v", err)
	}
	requests <- uploadRequest{r.Method, r.URL.Path, r.URL.Query(), r.Header.Clone(), body}
	w.Header().Set("ETag", `"uploaded"`)
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	switch r.URL.Query().Get("action") {
	case "append":
		w.WriteHeader(http.StatusAccepted)
	case "flush":
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusCreated)
	}
}

func newTestUploader(t *testing.T, method ingestoptions.UploadMethod, paths []string, options ...ManagedUploaderOption) *ManagedUploader {
	t.Helper()
	settings := &config.ContainerSettings{}
	for _, path := range paths {
		if method == ingestoptions.UploadMethodLake {
			settings.LakeFolders = append(settings.LakeFolders, config.ContainerInfo{Path: path})
		} else {
			settings.Containers = append(settings.Containers, config.ContainerInfo{Path: path})
		}
	}
	cache := cachedUploadConfig{config.NewCachedConfigurationData(&config.ConfigurationResponse{
		ContainerSettings: settings,
	})}
	opts := []ManagedUploaderOption{
		WithUploaderConfigCache(cache),
		WithUploaderRetryPolicy(ingestoptions.NewSimpleRetryPolicy(0, 2)),
	}
	u, err := NewManagedUploader(append(opts, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	u.clientOptions.Retry.MaxRetries = -1
	return u
}

func TestUploadManyCounts(t *testing.T) {
	for _, tc := range []struct {
		name      string
		failures  []bool
		successes int
		failed    int
	}{
		{"empty", nil, 0, 0},
		{"all-success", []bool{false, false}, 2, 0},
		{"mixed", []bool{false, true, false}, 2, 1},
		{"all-failure", []bool{true, true}, 0, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := make(chan uploadRequest, 16)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				recordUpload(t, requests, w, r)
			}))
			defer server.Close()
			uploader := newTestUploader(t, ingestoptions.UploadMethodStorage, []string{server.URL + "/container"},
				WithUploaderMaxConcurrency(2))
			wantErr := errors.New("source read failed")
			sources := make([]ingestoptions.LocalSource, len(tc.failures))
			for i, fail := range tc.failures {
				var reader io.Reader = strings.NewReader("data")
				if fail {
					reader = iotest.ErrReader(wantErr)
				}
				source := ingestoptions.NewStreamSource(io.NopCloser(reader), ingestoptions.FormatParquet)
				t.Cleanup(func() { _ = source.Close() })
				sources[i] = source
			}
			results, err := uploader.UploadMany(context.Background(), sources)
			if err != nil {
				t.Fatal(err)
			}
			if results.SuccessCount != tc.successes || results.FailureCount != tc.failed {
				t.Fatalf("got counts success=%d failure=%d, want success=%d failure=%d",
					results.SuccessCount, results.FailureCount, tc.successes, tc.failed)
			}
			if len(results.Results) != len(sources) {
				t.Fatalf("got %d results for %d sources", len(results.Results), len(sources))
			}
			for i, result := range results.Results {
				if tc.failures[i] {
					if result.BlobSource != nil || !errors.Is(result.Error, wantErr) {
						t.Errorf("failure at index %d was not preserved: %+v", i, result)
					}
				} else if result.Error != nil || result.BlobSource == nil || result.BlobSource.SourceID() != sources[i].SourceID() {
					t.Errorf("success at index %d was not preserved: %+v", i, result)
				}
			}
		})
	}
}

func TestUploadMethodServerPreferenceCaseInsensitive(t *testing.T) {
	for _, tc := range []struct {
		preference string
		method     ingestoptions.UploadMethod
		want       ingestoptions.UploadMethod
	}{
		{"Lake", ingestoptions.UploadMethodDefault, ingestoptions.UploadMethodLake},
		{"lake", ingestoptions.UploadMethodDefault, ingestoptions.UploadMethodLake},
		{"LAKE", ingestoptions.UploadMethodDefault, ingestoptions.UploadMethodLake},
		{"lAkE", ingestoptions.UploadMethodDefault, ingestoptions.UploadMethodLake},
		{"Storage", ingestoptions.UploadMethodDefault, ingestoptions.UploadMethodStorage},
		{"", ingestoptions.UploadMethodDefault, ingestoptions.UploadMethodStorage},
		{"lake", ingestoptions.UploadMethodStorage, ingestoptions.UploadMethodStorage},
		{"Storage", ingestoptions.UploadMethodLake, ingestoptions.UploadMethodLake},
	} {
		t.Run(fmt.Sprintf("%s/%s", tc.preference, tc.method), func(t *testing.T) {
			cache := cachedUploadConfig{config.NewCachedConfigurationData(&config.ConfigurationResponse{
				ContainerSettings: &config.ContainerSettings{
					Containers:            []config.ContainerInfo{{Path: "https://storage.example/container"}},
					LakeFolders:           []config.ContainerInfo{{Path: "https://lake.example/workspace/folder"}},
					PreferredUploadMethod: tc.preference,
				},
			})}
			uploader, err := NewManagedUploader(WithUploaderConfigCache(cache))
			if err != nil {
				t.Fatal(err)
			}
			containers, err := uploader.doSelectContainers(context.Background(), tc.method)
			if err != nil {
				t.Fatal(err)
			}
			selected := containers.Next()
			if selected == nil || selected.UploadMethod != tc.want {
				t.Fatalf("selected %+v, want upload method %s", selected, tc.want)
			}
		})
	}
}

func TestUploadDispatchAndCompression(t *testing.T) {
	for _, method := range []ingestoptions.UploadMethod{ingestoptions.UploadMethodStorage, ingestoptions.UploadMethodLake} {
		for _, format := range []ingestoptions.DataFormat{ingestoptions.FormatCSV, ingestoptions.FormatParquet} {
			t.Run(fmt.Sprintf("%s/%s", method, format), func(t *testing.T) {
				requests := make(chan uploadRequest, 16)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					recordUpload(t, requests, w, r)
				}))
				defer server.Close()
				payload := []byte("name,age\nAlice,30\n")
				source := ingestoptions.NewStreamSource(seekableUploadReader{bytes.NewReader(payload)}, format)
				u := newTestUploader(t, method, []string{server.URL + "/workspace/folder?sig=upload-sas"})
				blob, err := u.Upload(context.Background(), source)
				if err != nil {
					t.Fatal(err)
				}
				if blob.SourceID() != source.SourceID() || blob.Format() != format {
					t.Fatalf("source metadata was not preserved: %+v", blob)
				}
				wantCompression := ingestoptions.CompressionNone
				if source.ShouldCompress() {
					wantCompression = ingestoptions.CompressionGZip
				}
				if blob.CompressionType() != wantCompression || blob.Size() != 0 {
					t.Fatalf("unexpected compression or raw size: %+v", blob)
				}
				var uploaded []byte
				var created, flushed bool
				for len(requests) > 0 {
					req := <-requests
					if req.query.Get("sig") != "upload-sas" {
						t.Errorf("SAS was not preserved: %v", req.query)
					}
					if !strings.HasPrefix(req.path, "/workspace/folder/") {
						t.Errorf("unexpected upload path: %s", req.path)
					}
					switch method {
					case ingestoptions.UploadMethodStorage:
						if req.method != http.MethodPut || req.query.Get("action") != "" {
							t.Errorf("not a Blob API request: %+v", req)
						}
						if req.query.Get("comp") == "block" || req.query.Get("comp") == "" {
							uploaded = append(uploaded, req.body...)
						}
					case ingestoptions.UploadMethodLake:
						created = created || req.method == http.MethodPut && req.query.Get("resource") == "file"
						flushed = flushed || req.method == http.MethodPatch && req.query.Get("action") == "flush"
						if req.query.Get("action") == "append" {
							uploaded = append(uploaded, req.body...)
						}
					}
				}
				if method == ingestoptions.UploadMethodLake && (!created || !flushed) {
					t.Fatalf("Data Lake file was not created and flushed: create=%t flush=%t", created, flushed)
				}
				if source.ShouldCompress() {
					reader, err := gzip.NewReader(bytes.NewReader(uploaded))
					if err != nil {
						t.Fatal(err)
					}
					uploaded, err = io.ReadAll(reader)
					_ = reader.Close()
					if err != nil {
						t.Fatal(err)
					}
				}
				if !bytes.Equal(uploaded, payload) {
					t.Errorf("uploaded %q, want %q", uploaded, payload)
				}
			})
		}
	}
}

type uploadCredential struct {
	scopes chan []string
}

func (c uploadCredential) GetToken(_ context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	c.scopes <- options.Scopes
	return azcore.AccessToken{Token: "storage-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func TestOneLakeAuthentication(t *testing.T) {
	for _, mode := range []string{"credential", "sas", "anonymous"} {
		t.Run(mode, func(t *testing.T) {
			requests := make(chan uploadRequest, 16)
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				recordUpload(t, requests, w, r)
			}))
			defer server.Close()
			scopes := make(chan []string, 16)
			var tokenCalls atomic.Int32
			provider := func(context.Context) (*ingestoptions.S2SToken, error) {
				return &ingestoptions.S2SToken{Scheme: "Bearer", Token: fmt.Sprintf("actor-%d", tokenCalls.Add(1))}, nil
			}
			opts := []ManagedUploaderOption{
				WithUploaderHTTPClient(server.Client()),
				WithUploaderFabricPrivateLink(provider, "workspace-context"),
			}
			target := server.URL + "/workspace/lakehouse/Files/Ingestions"
			if mode != "anonymous" {
				target += "?sig=upload-sas"
			}
			if mode == "credential" {
				opts = append(opts, WithUploaderTokenCredential(uploadCredential{scopes}))
			}
			u := newTestUploader(t, ingestoptions.UploadMethodLake, []string{target}, opts...)
			source := ingestoptions.NewStreamSource(io.NopCloser(strings.NewReader("data")), ingestoptions.FormatParquet)
			if _, err := u.Upload(context.Background(), source); err != nil {
				t.Fatal(err)
			}
			count := len(requests)
			if count < 3 || int(tokenCalls.Load()) != count {
				t.Fatalf("S2S provider was not called per request: calls=%d requests=%d", tokenCalls.Load(), count)
			}
			seenTokens := make(map[string]bool)
			for len(requests) > 0 {
				req := <-requests
				token := req.headers.Get(ingestoptions.HeaderMSS2SActorAuthorization)
				if !strings.HasPrefix(token, "Bearer actor-") || seenTokens[token] {
					t.Errorf("missing or reused S2S token: %q", token)
				}
				seenTokens[token] = true
				if req.headers.Get(ingestoptions.HeaderMSFabricS2SAccessContext) != "workspace-context" {
					t.Error("missing access context")
				}
				if mode == "credential" {
					if req.headers.Get("Authorization") != "Bearer storage-token" || req.query.Has("sig") {
						t.Error("TokenCredential did not take precedence over SAS")
					}
				} else if req.headers.Get("Authorization") != "" {
					t.Error("unexpected bearer authentication")
				}
				if mode == "sas" && req.query.Get("sig") != "upload-sas" {
					t.Error("SAS missing from OneLake request")
				}
			}
			if mode == "credential" {
				if len(scopes) == 0 || strings.Join(<-scopes, ",") != "https://storage.azure.com/.default" {
					t.Error("unexpected storage token scope")
				}
			}
		})
	}
}

func TestUploadRetryRewindsAndRecompresses(t *testing.T) {
	requests := make(chan uploadRequest, 16)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/first/") {
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		recordUpload(t, requests, w, r)
	}))
	defer server.Close()
	payload := "name\nAlice\n"
	u := newTestUploader(t, ingestoptions.UploadMethodStorage, []string{server.URL + "/first", server.URL + "/second"},
		WithUploaderMaxDataSize(int64(len(payload))))
	reader, err := os.CreateTemp(t.TempDir(), "retry-*.csv")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	if _, err := reader.WriteString("skip" + payload); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Seek(4, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	source := ingestoptions.NewStreamSource(reader, ingestoptions.FormatCSV)
	blob, err := u.Upload(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(blob.BlobPath(), "/second/") {
		t.Fatalf("did not cycle upload container: %s", blob.BlobPath())
	}
	for len(requests) > 0 {
		req := <-requests
		if req.query.Get("comp") == "block" || req.query.Get("comp") == "" {
			reader, err := gzip.NewReader(bytes.NewReader(req.body))
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(reader)
			_ = reader.Close()
			if err != nil || string(body) != payload {
				t.Fatalf("retry uploaded incomplete data: %q, %v", body, err)
			}
			return
		}
	}
	t.Fatal("retry did not transfer data")
}

func TestUploadFailuresAreNotReportedAsSuccess(t *testing.T) {
	for _, method := range []ingestoptions.UploadMethod{ingestoptions.UploadMethodStorage, ingestoptions.UploadMethodLake} {
		t.Run(method.String(), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusForbidden)
			}))
			defer server.Close()
			u := newTestUploader(t, method, []string{server.URL + "/container"})
			source := ingestoptions.NewStreamSource(io.NopCloser(strings.NewReader("data")), ingestoptions.FormatCSV)
			blob, err := u.Upload(context.Background(), source)
			var responseError *azcore.ResponseError
			if blob != nil || !errors.As(err, &responseError) || responseError.StatusCode != http.StatusForbidden {
				t.Fatalf("expected preserved upload error, got blob=%v err=%v", blob, err)
			}
			if calls.Load() != 1 {
				t.Errorf("permanent failure was retried %d times", calls.Load())
			}
		})
	}
}

func TestOneLakeTokenProviderFailure(t *testing.T) {
	want := errors.New("S2S unavailable")
	for _, nilToken := range []bool{false, true} {
		t.Run(fmt.Sprint(nilToken), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusCreated)
			}))
			defer server.Close()
			u := newTestUploader(t, ingestoptions.UploadMethodLake, []string{server.URL + "/workspace/folder"},
				WithUploaderFabricPrivateLink(func(context.Context) (*ingestoptions.S2SToken, error) {
					if nilToken {
						return nil, nil
					}
					return nil, want
				}, "workspace-context"))
			source := ingestoptions.NewStreamSource(io.NopCloser(strings.NewReader("data")), ingestoptions.FormatParquet)
			blob, err := u.Upload(context.Background(), source)
			if blob != nil || err == nil || !nilToken && !errors.Is(err, want) || calls.Load() != 0 {
				t.Fatalf("invalid S2S token was not rejected: blob=%v err=%v requests=%d", blob, err, calls.Load())
			}
		})
	}
}

func TestUploadNonSeekableFailureDoesNotRetryConsumedData(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	u := newTestUploader(t, ingestoptions.UploadMethodStorage, []string{server.URL + "/first", server.URL + "/second"})
	source := ingestoptions.NewStreamSource(io.NopCloser(strings.NewReader("data")), ingestoptions.FormatCSV)
	blob, err := u.Upload(context.Background(), source)
	var responseError *azcore.ResponseError
	if blob != nil || !errors.As(err, &responseError) || !strings.Contains(err.Error(), "non-seekable") || calls.Load() != 1 {
		t.Fatalf("unsafe stream retry: blob=%v err=%v requests=%d", blob, err, calls.Load())
	}
}

func TestOneLakeS2STokenRefreshedOnSDKRetry(t *testing.T) {
	requests := make(chan string, 16)
	var calls, tokenCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Header.Get(ingestoptions.HeaderMSS2SActorAuthorization)
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		switch r.URL.Query().Get("action") {
		case "append":
			w.WriteHeader(http.StatusAccepted)
		case "flush":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusCreated)
		}
	}))
	defer server.Close()
	u := newTestUploader(t, ingestoptions.UploadMethodLake, []string{server.URL + "/workspace/folder"},
		WithUploaderFabricPrivateLink(func(context.Context) (*ingestoptions.S2SToken, error) {
			return &ingestoptions.S2SToken{Scheme: "Bearer", Token: fmt.Sprint(tokenCalls.Add(1))}, nil
		}, "workspace-context"))
	u.clientOptions.Retry = policy.RetryOptions{MaxRetries: 1, RetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond}
	source := ingestoptions.NewStreamSource(io.NopCloser(strings.NewReader("data")), ingestoptions.FormatParquet)
	if _, err := u.Upload(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 4 || tokenCalls.Load() != 4 {
		t.Fatalf("unexpected requests or provider calls: %d / %d", len(requests), tokenCalls.Load())
	}
	if first, second := <-requests, <-requests; first == second {
		t.Errorf("retry reused S2S token %q", first)
	}
}

type uploadTransportFunc func(*http.Request) (*http.Response, error)

func (f uploadTransportFunc) Do(req *http.Request) (*http.Response, error) { return f(req) }

func TestUploadCancellation(t *testing.T) {
	for _, method := range []ingestoptions.UploadMethod{ingestoptions.UploadMethodStorage, ingestoptions.UploadMethodLake} {
		t.Run(method.String(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			u := newTestUploader(t, method, []string{"https://example.test/workspace/folder"})
			u.clientOptions.Transport = uploadTransportFunc(func(req *http.Request) (*http.Response, error) {
				deadline, ok := req.Context().Deadline()
				if !ok || time.Until(deadline) > ingestoptions.BlobUploadTimeout {
					t.Error("upload timeout was not applied")
				}
				cancel()
				return nil, req.Context().Err()
			})
			source := ingestoptions.NewStreamSource(io.NopCloser(strings.NewReader("data")), ingestoptions.FormatCSV)
			blob, err := u.Upload(ctx, source)
			if blob != nil || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation was not propagated: blob=%v err=%v", blob, err)
			}
		})
	}
}

func TestUploadReadFailure(t *testing.T) {
	want := errors.New("source read failed")
	for _, method := range []ingestoptions.UploadMethod{ingestoptions.UploadMethodStorage, ingestoptions.UploadMethodLake} {
		t.Run(method.String(), func(t *testing.T) {
			requests := make(chan uploadRequest, 16)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				recordUpload(t, requests, w, r)
			}))
			defer server.Close()
			u := newTestUploader(t, method, []string{server.URL + "/workspace/folder"})
			source := ingestoptions.NewStreamSource(io.NopCloser(iotest.ErrReader(want)), ingestoptions.FormatCSV)
			blob, err := u.Upload(context.Background(), source)
			if blob != nil || !errors.Is(err, want) {
				t.Fatalf("source failure was not preserved: blob=%v err=%v", blob, err)
			}
		})
	}
}
