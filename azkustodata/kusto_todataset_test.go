package azkustodata

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/stretchr/testify/require"
)

// toDatasetServer serves body as a v2 response. When stall is set, the response is left open after body
// has been sent, and disconnected is closed once the client goes away.
func toDatasetServer(t *testing.T, body string, stall bool) (srv *httptest.Server, disconnected chan struct{}) {
	t.Helper()
	done := make(chan struct{})
	disconnected = make(chan struct{})
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/v2/rest/query") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
		w.(http.Flusher).Flush()
		if !stall {
			return
		}
		select {
		case <-r.Context().Done():
			close(disconnected)
		case <-done:
		}
	}))
	t.Cleanup(func() { close(done); srv.Close() })
	return srv, disconnected
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

type toDatasetCall func(t *testing.T, c *Client) (query.Dataset, error)

var toDatasetCalls = map[string]toDatasetCall{
	"IterativeQuery.ToDataset": func(t *testing.T, c *Client) (query.Dataset, error) {
		ds, err := c.IterativeQuery(t.Context(), "db", kql.New("print 1"))
		require.NoError(t, err)
		return ds.ToDataset()
	},
	"Query": func(t *testing.T, c *Client) (query.Dataset, error) {
		return c.Query(t.Context(), "db", kql.New("print 1"))
	},
}

// TestToDatasetReleasesRequest checks that converting an iterative dataset releases the request context and
// closes the response body exactly once, both on success and on an early conversion error with a stalled stream.
func TestToDatasetReleasesRequest(t *testing.T) {
	full, err := os.ReadFile("query/v2/testData/twoTables.json")
	require.NoError(t, err)

	for name, call := range toDatasetCalls {
		call := call
		t.Run(name+"/success", func(t *testing.T) {
			srv, _ := toDatasetServer(t, string(full), false)
			tt := &trackingTransport{base: http.DefaultTransport}
			client, err := New(NewConnectionStringBuilder(srv.URL), WithHttpClient(&http.Client{Transport: tt}))
			require.NoError(t, err)
			defer client.Close()

			ds, err := call(t, client)
			require.NoError(t, err)
			// Cancellation must not cut the stream short: both primary tables arrive complete.
			require.Len(t, ds.Tables(), 4)
			primaryRows := 0
			for _, tb := range ds.Tables() {
				if tb.Kind() == "PrimaryResult" {
					require.Len(t, tb.Rows(), 3)
					primaryRows++
				}
			}
			require.Equal(t, 2, primaryRows)

			body := tt.body.Load()
			require.NotNil(t, body)
			waitClosed(t, body.closed, "response body close")
			time.Sleep(100 * time.Millisecond)
			require.EqualValues(t, 1, body.closeCount.Load(), "body must be closed exactly once")
			require.False(t, body.closedMidRead.Load())
		})

		t.Run(name+"/early-error-stalled", func(t *testing.T) {
			// An unexpected frame inside a primary table makes conversion fail while the stream then stalls.
			bad := stalledV2Frames + `,{"FrameType":"DataSetHeader","Version":"v2.0"}` + "\n"
			srv, disconnected := toDatasetServer(t, bad, true)
			tt := &trackingTransport{base: http.DefaultTransport}
			client, err := New(NewConnectionStringBuilder(srv.URL), WithHttpClient(&http.Client{Transport: tt}))
			require.NoError(t, err)
			defer client.Close()

			_, err = call(t, client)
			require.Error(t, err)

			body := tt.body.Load()
			require.NotNil(t, body)
			waitClosed(t, body.closed, "response body close without an explicit dataset Close")
			waitClosed(t, disconnected, "request cancellation to reach the server")
			time.Sleep(100 * time.Millisecond)
			require.EqualValues(t, 1, body.closeCount.Load(), "body must be closed exactly once")
			require.False(t, body.closedMidRead.Load(), "body was closed while a read was still in flight")
		})
	}
}

// TestIterativeQueryClosesBodyOnInvalidResponse checks that the response body is closed when the dataset
// cannot be created (no read goroutine exists to do it).
func TestIterativeQueryClosesBodyOnInvalidResponse(t *testing.T) {
	srv, _ := toDatasetServer(t, "this is not json", false)
	tt := &trackingTransport{base: http.DefaultTransport}
	client, err := New(NewConnectionStringBuilder(srv.URL), WithHttpClient(&http.Client{Transport: tt}))
	require.NoError(t, err)
	defer client.Close()

	_, err = client.IterativeQuery(t.Context(), "db", kql.New("print 1"))
	require.Error(t, err)

	body := tt.body.Load()
	require.NotNil(t, body)
	waitClosed(t, body.closed, "response body close")
	require.EqualValues(t, 1, body.closeCount.Load())
}
