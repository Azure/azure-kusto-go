package azkustodata

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/stretchr/testify/require"
)

// lateBodyTransport answers every request at once and keeps its body unread, so the
// test can read it after Do has returned. That is what net/http's transport can do:
// Client.Do may return (early response, error, cancelled context) while the
// connection's write loop is still sending the request body.
type lateBodyTransport struct {
	mu     sync.Mutex
	bodies []io.ReadCloser
}

func (l *lateBodyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Path == metadataPath {
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: http.Header{}, Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
	}
	l.mu.Lock()
	l.bodies = append(l.bodies, req.Body)
	l.mu.Unlock()
	return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: http.Header{}, Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
}

// A request body must stay intact after Do returns. With the body in a pooled
// buffer, the next request reset and refilled that same buffer, so an earlier body
// still being sent carried another query's text (and -race reported a data race).
func TestRequestBodySurvivesLateRead(t *testing.T) {
	t.Parallel()
	const endpoint = "https://latebody.westus.kusto.windows.net"
	forgetMetadata(t, endpoint)
	rt := &lateBodyTransport{}
	kcsb := NewConnectionStringBuilder(endpoint).WithApplicationToken("app", "secret-token")
	client, err := New(kcsb, WithHttpClient(&http.Client{Transport: rt}))
	require.NoError(t, err)
	conn := client.conn.(*Conn)

	const n = 20
	for i := 0; i < n; i++ {
		_, _, _, body, _ := conn.doRequest(context.Background(), execMgmt, "db", kql.New("").AddUnsafe(fmt.Sprintf("print request_%d", i)), requestProperties{})
		if body != nil {
			_ = body.Close()
		}
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()
	require.Len(t, rt.bodies, n)
	for i, b := range rt.bodies {
		got, err := io.ReadAll(b)
		require.NoError(t, err)
		require.Contains(t, string(got), fmt.Sprintf("print request_%d", i), "body of request %d was overwritten after Do returned", i)
	}
}
