package conn

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/properties"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeStreamService struct {
	serv     *http.Server
	listener net.Listener
	port     int

	req *http.Request
	out []byte
}

func newFakeStreamService() *fakeStreamService {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}

	fs := &fakeStreamService{
		serv: &http.Server{
			ReadTimeout:    10 * time.Second,
			WriteTimeout:   10 * time.Second,
			MaxHeaderBytes: 1 << 20,
		},
		listener: listener,
		port:     listener.Addr().(*net.TCPAddr).Port,
	}
	fs.serv.Handler = http.HandlerFunc(fs.handleStream)

	return fs
}

func (f *fakeStreamService) handleStream(res http.ResponseWriter, r *http.Request) {
	f.req = r

	zr, err := gzip.NewReader(r.Body)
	if err != nil {
		log.Fatal(err)
	}

	buf := &bytes.Buffer{}
	if _, err := io.Copy(buf, zr); err != nil {
		log.Fatal(err)
	}

	if err := zr.Close(); err != nil {
		log.Fatal(err)
	}
	f.out = buf.Bytes()

	if strings.Contains(r.URL.Path, "httpError") {

		data := []byte(`{"error":{"code":"BadRequest","message":"Bad Request"}}`)
		res.Header().Set("Content-Type", "application/json")

		if strings.Contains(r.URL.Path, "gzip") {
			res.Header().Set("Content-Encoding", "gzip")
			res.WriteHeader(400)

			zw := gzip.NewWriter(res)
			if _, err := zw.Write(data); err != nil {
				log.Fatal(err)
			}
			if err := zw.Close(); err != nil {
				log.Fatal(err)
			}
		} else {
			res.WriteHeader(400)
			_, err := res.Write(data)
			if err != nil {
				log.Fatal(err)
			}
		}

		return
	}

	res.WriteHeader(200)
}

func (f *fakeStreamService) start() error {
	return f.serv.Serve(f.listener)
}

type fakeContent struct {
	Name string
	ID   int32
}

func TestStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc        string
		payload     fakeContent
		mappingName string
		err         error
		httpError   bool
		gzip        bool
	}{
		{
			desc:        "AVRO without mappingName",
			payload:     fakeContent{Name: "Doak", ID: 25},
			mappingName: "",
		},
		{
			desc:        "JSON without mappingName",
			payload:     fakeContent{Name: "Doak", ID: 25},
			mappingName: "",
		},
		{
			desc:        "Success",
			payload:     fakeContent{Name: "Doak", ID: 25},
			mappingName: "jsonMap",
		},
		{
			desc:        "HTTP error",
			payload:     fakeContent{Name: "Doak", ID: 25},
			mappingName: "jsonMap",
			err:         fmt.Errorf("streaming ingest issue(400 Bad Request):\n{\"error\":{\"code\":\"BadRequest\",\"message\":\"Bad Request\"}}"),
			httpError:   true,
			gzip:        false,
		},
		{
			desc:        "HTTP error gzipped",
			payload:     fakeContent{Name: "Doak", ID: 25},
			mappingName: "jsonMap",
			err:         fmt.Errorf("streaming ingest issue(400 Bad Request):\n{\"error\":{\"code\":\"BadRequest\",\"message\":\"Bad Request\"}}"),
			httpError:   true,
			gzip:        true,
		},
	}
	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			server := newFakeStreamService()
			go func() {
				err := server.start()
				if err != nil {
					t.Errorf("failed to start server: %v", err)
					return
				}
			}()
			time.Sleep(10 * time.Millisecond)

			fmt.Println(server.port)
			conn, err := newWithoutValidation(fmt.Sprintf("http://127.0.0.1:%d", server.port), kusto.Authorization{})
			if err != nil {
				panic(err)
			}
			conn.inTest = true

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			b, err := json.Marshal(test.payload)
			require.NoError(t, err)

			var payload bytes.Buffer
			zw := gzip.NewWriter(&payload)
			_, err = zw.Write(b)
			require.NoError(t, err)

			err = zw.Close()
			require.NoError(t, err)

			db := "database"
			if test.httpError {
				db = "httpError"
			}
			if test.gzip {
				db += ".gzip"
			}

			err = conn.StreamIngest(ctx, db, "table", &payload, properties.JSON, test.mappingName, "")

			if test.err != nil {
				assert.Equal(t, test.err, err.(*errors.Error).Err)
				return
			} else {
				assert.NoError(t, err)
			}

			assert.EqualValues(t, "application/json; charset=utf-8", server.req.Header.Get("Content-Type"))
			assert.EqualValues(t, "Json", server.req.URL.Query().Get("streamFormat"))
			assert.EqualValues(t, test.mappingName, server.req.URL.Query().Get("mappingName"))

			assert.True(t, strings.HasPrefix(server.req.Header.Get("x-ms-client-request-id"), "KGC.execute;"))

			_, err = uuid.Parse(strings.TrimPrefix(server.req.Header.Get("x-ms-client-request-id"), "KGC.execute;"))
			assert.NoError(t, err)

			got := fakeContent{}
			err = json.Unmarshal(server.out, &got)
			assert.NoError(t, err)

			assert.EqualValues(t, test.payload, got)
		})
	}
}
