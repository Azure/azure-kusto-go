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
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/properties"
	"github.com/google/uuid"
	"github.com/kylelemons/godebug/pretty"
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

func (f *fakeStreamService) handleStream(_ http.ResponseWriter, r *http.Request) {
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
		err         bool
	}{
		{
			desc:        "AVRO without mappingName",
			payload:     fakeContent{Name: "Doak", ID: 25},
			mappingName: "",
			err:         true,
		},
		{
			desc:        "JSON without mappingName",
			payload:     fakeContent{Name: "Doak", ID: 25},
			mappingName: "",
			err:         true,
		},
		{
			desc:        "Success",
			payload:     fakeContent{Name: "Doak", ID: 25},
			mappingName: "jsonMap",
		},
	}

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

	for _, test := range tests {
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			b, err := json.Marshal(test.payload)
			if err != nil {
				panic(err)
			}

			var payload bytes.Buffer
			zw := gzip.NewWriter(&payload)
			_, err = zw.Write(b)
			if err != nil {
				panic(err)
			}

			if err := zw.Close(); err != nil {
				panic(err)
			}

			err = conn.Write(ctx, "database", "table", &payload, properties.JSON, test.mappingName, "")

			switch {
			case err == nil && test.err:
				t.Errorf("TestStream(%s): got err == nil, want err != nil", test.desc)
				return
			case err != nil && !test.err:
				t.Errorf("TestStream(%s): got err == %s, want err == nil", test.desc, err)
				return
			case err != nil:
				return
			}

			switch {
			case server.req.Header.Get("Content-Type") != "application/json; charset=utf-8":
				t.Fatalf("TestStream(%s): Content-Type: got %s, want %s", test.desc, server.req.Header.Get("Content-Type"), "application/json; charset=utf-8")
			case server.req.URL.Query().Get("streamFormat") != "Json":
				t.Fatalf("TestStream(%s): Query Variable(streamFormat): got %s, want json", test.desc, server.req.URL.Query().Get("streamFormat"))
			case server.req.URL.Query().Get("mappingName") != test.mappingName:
				t.Fatalf("TestStream(%s): Query Variable(mappingName): got %s, want %s", test.desc, server.req.URL.Query().Get("mappingName"), test.mappingName)
			}

			if !strings.HasPrefix(server.req.Header.Get("x-ms-client-request-id"), "KGC.execute;") {
				t.Fatalf("TestStream(%s): x-ms-client-request-id(%s): was not expected format", test.desc, server.req.Header.Get("x-ms-client-request-id"))
			}
			uuid.MustParse(strings.TrimPrefix(server.req.Header.Get("x-ms-client-request-id"), "KGC.execute;"))

			got := fakeContent{}
			if err := json.Unmarshal(server.out, &got); err != nil {
				t.Fatalf("TestStream(%s): could not unmarshal data sent to server: %s", test.desc, err)
			}

			if diff := pretty.Compare(test.payload, got); diff != "" {
				t.Fatalf("TestStream(%s) -want/+got:\n%s", test.desc, diff)
			}
		}()
	}
}
