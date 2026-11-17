package azkustodata

import (
	"compress/flate"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/stretchr/testify/require"
)

const stalledV2Frames = `[{"FrameType":"DataSetHeader","IsProgressive":false,"Version":"v2.0","IsFragmented":true,"ErrorReportingPlacement":"EndOfTable"}
,{"FrameType":"DataTable","TableId":0,"TableKind":"QueryProperties","TableName":"@ExtendedProperties","Columns":[],"Rows":[]}
,{"FrameType":"TableHeader","TableId":1,"TableKind":"PrimaryResult","TableName":"PrimaryResult","Columns":[{"ColumnName":"a","ColumnType":"int"}]}
`

// TestIterativeQueryCloseStalledCompressedRead checks that closing an iterative dataset whose (optionally compressed)
// response stalls mid-stream returns nil and lets the read goroutine finish.
func TestIterativeQueryCloseStalledCompressedRead(t *testing.T) {
	for _, enc := range []string{"identity", "gzip", "deflate"} {
		enc := enc
		t.Run(enc, func(t *testing.T) {
			done := make(chan struct{})
			disconnected := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, "/v2/rest/query") {
					// Auth metadata lookup: an empty 404 selects the default cloud info.
					w.WriteHeader(http.StatusNotFound)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if enc != "identity" {
					w.Header().Set("Content-Encoding", enc)
				}
				w.WriteHeader(http.StatusOK)

				var cw io.WriteCloser
				var flusher interface{ Flush() error }
				if enc == "identity" {
					cw, flusher = nopWriteCloser{w}, nopFlusher{}
				} else if enc == "gzip" {
					g := gzip.NewWriter(w)
					cw, flusher = g, g
				} else {
					f, err := flate.NewWriter(w, flate.DefaultCompression)
					if err != nil {
						return
					}
					cw, flusher = f, f
				}
				_, _ = io.Copy(cw, strings.NewReader(stalledV2Frames))
				_ = flusher.Flush()
				w.(http.Flusher).Flush()

				// Stall: send no more data until the client goes away.
				select {
				case <-r.Context().Done():
					close(disconnected)
				case <-done:
				}
			}))
			defer srv.Close()
			defer close(done)

			client, err := New(NewConnectionStringBuilder(srv.URL))
			require.NoError(t, err)
			defer client.Close()

			ds, err := client.IterativeQuery(t.Context(), "db", kql.New("print 1"))
			require.NoError(t, err)

			// Receiving the primary table proves the stream was consumed up to the stall, so the read
			// goroutine is (about to be) blocked on the response body when Close runs.
			select {
			case tr := <-ds.Tables():
				require.NotNil(t, tr)
			case <-time.After(2 * time.Second):
				t.Fatal("primary table was not delivered before the stall")
			}
			time.Sleep(100 * time.Millisecond)

			start := time.Now()
			require.NoError(t, ds.Close())

			select {
			case <-drain(ds.Tables()):
			case <-time.After(2 * time.Second):
				t.Fatal("dataset read goroutine did not terminate after Close")
			}
			// The response body must be released too, so the server sees the connection go away.
			select {
			case <-disconnected:
			case <-time.After(2 * time.Second):
				t.Fatal("response body was not closed after Close")
			}
			require.Less(t, time.Since(start), 4*time.Second)
		})
	}
}

// drain consumes the channel until it is closed and signals completion.
func drain[T any](ch <-chan T) <-chan struct{} {
	out := make(chan struct{})
	go func() {
		defer close(out)
		for range ch {
		}
	}()
	return out
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

type nopFlusher struct{}

func (nopFlusher) Flush() error { return nil }
