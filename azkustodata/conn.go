package azkustodata

// conn.go holds the connection to the Kusto server and provides methods to do queries
// and receive Kusto frames back.

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata/errors"

	"github.com/Azure/go-autorest/autorest"
	"github.com/google/uuid"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

// conn provides connectivity to a Kusto instance.
type conn struct {
	endpoint                       string
	auth                           autorest.Authorizer
	endMgmt, endQuery, streamQuery *url.URL
	reqHeaders                     http.Header
	headersPool                    chan http.Header
	client                         *http.Client
}

// newConn returns a new conn object.
func newConn(endpoint string, auth Authorization, timeout time.Duration) (*conn, error) {
	headers := http.Header{}
	headers.Add("Accept", "application/json")
	headers.Add("Accept-Encoding", "gzip,deflate")
	headers.Add("x-ms-client-version", "Kusto.Go.Client: "+version)

	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, errors.E(errors.OpServConn, errors.KClientArgs, fmt.Errorf("could not parse the endpoint(%s): %s", endpoint, err))
	}

	c := &conn{
		auth:        auth.Authorizer,
		endMgmt:     &url.URL{Scheme: u.Scheme, Host: u.Host, Path: "/v1/rest/mgmt"},
		endQuery:    &url.URL{Scheme: u.Scheme, Host: u.Host, Path: "/v2/rest/query"},
		streamQuery: &url.URL{Scheme: u.Scheme, Host: u.Host, Path: "/v1/rest/ingest/"},
		reqHeaders:  headers,
		headersPool: make(chan http.Header, 100),
		client: &http.Client{
			Timeout: timeout,
		},
	}

	// Fills a pool of headers to alleviate header copying timing at request time.
	// These are automatically renewed by spun off goroutines when a header is pulled.
	// TODO(jdoak): Decide if a sync.Pool would be better. In 1.13 they aren't triggering GC nearly as much.
	for i := 0; i < 100; i++ {
		c.headersPool <- copyHeaders(headers)
	}

	return c, nil
}

type queryMsg struct {
	DB         string            `json:"db"`
	CSL        string            `json:"csl"`
	Properties requestProperties `json:"properties,omitempty"`
}

// query makes a query for the purpose of extracting data from Kusto. Context can be used to set
// a timeout or cancel the query. Queries cannot take longer than 5 minutes.
func (c *conn) query(ctx context.Context, db, query string, options ...QueryOption) (chan frame, error) {
	opt := &queryOptions{
		requestProperties: &requestProperties{
			Options:    map[string]interface{}{},
			Parameters: map[string]interface{}{},
		},
	}

	for _, o := range options {
		o(opt)
	}

	return c.execute(ctx, execQuery, db, query, "", *opt.requestProperties)
}

// mgmt is used to do management queries to Kusto.
func (c *conn) mgmt(ctx context.Context, db, query string, options ...QueryOption) (chan frame, error) {
	opt := &queryOptions{
		requestProperties: &requestProperties{
			Options:    map[string]interface{}{},
			Parameters: map[string]interface{}{},
		},
	}

	for _, o := range options {
		o(opt)
	}

	return c.execute(ctx, execMgmt, db, query, "", *opt.requestProperties)
}

const (
	execUnknown = 0
	execQuery   = 1
	execMgmt    = 2
)

func newKustoRequest(ctx context.Context, c conn, op errors.Op, db string, query string, properties requestProperties) (*http.Request, error) {
	headers := <-c.headersPool
	// TODO (daniel): ask John why this is done async
	go func() {
		c.headersPool <- copyHeaders(c.reqHeaders)
	}()

	var endpoint *url.URL
	buff := bufferPool.Get().(*bytes.Buffer)
	buff.Reset()
	defer bufferPool.Put(buff)

	headers.Add("Content-Type", "application/json; charset=utf-8")
	headers.Add("x-ms-client-request-id", "KGC.execute;"+uuid.New().String())

	var err error
	err = json.NewEncoder(buff).Encode(
		queryMsg{
			DB:         db,
			CSL:        query,
			Properties: properties,
		},
	)
	if err != nil {
		return nil, errors.E(op, errors.KClientInternal, fmt.Errorf("could not JSON marshal the Query message: %s", err))
	}

	if op == errors.OpQuery {
		endpoint = c.endQuery
	} else {
		endpoint = c.endMgmt
	}

	req := &http.Request{
		Method: http.MethodPost,
		URL:    endpoint,
		Header: headers,
		Body:   ioutil.NopCloser(buff),
	}

	if c.auth != nil {
		prep := c.auth.WithAuthorization()
		req, err = prep(autorest.CreatePreparer()).Prepare(req)
	}

	if err != nil {
		return nil, errors.E(op, errors.KClientInternal, err)
	}

	req.WithContext(ctx)

	return req, nil
}

func bodyFromResponse(resp http.Response, op errors.Op) (io.Reader, error) {
	var err error
	body := resp.Body
	switch enc := strings.ToLower(resp.Header.Get("Content-Encoding")); enc {
	case "":
		// Do nothing
	case "gzip":
		body, err = gzip.NewReader(resp.Body)
		if err != nil {
			return nil, errors.E(op, errors.KClientInternal, fmt.Errorf("gzip reader error: %s", err))
		}
	case "deflate":
		body = flate.NewReader(resp.Body)
	default:
		return nil, errors.ES(op, errors.KClientInternal, "Content-Encoding was unrecognized: %s", enc)
	}

	return body, nil
}

func (c *conn) execute(ctx context.Context, execType int, db, query string, payload string, properties requestProperties) (chan frame, error) {
	var op errors.Op
	if execType == execQuery {
		op = errors.OpQuery
	} else if execType == execMgmt {
		op = errors.OpMgmt
	}

	kReq, err := newKustoRequest(ctx, *c, op, db, query, properties)

	resp, err := c.client.Do(kReq)

	if err != nil {
		// TODO(jdoak): We need a http error unwrap function that pulls out an *errors.Error.
		return nil, errors.E(op, errors.KHTTPError, err)
	}

	if resp.StatusCode != 200 {
		// TODO(jdoak): We need to make this more verbose to be compliant with API guidelines.
		return nil, errors.E(op, errors.KHTTPError, fmt.Errorf("received non 200 (OK) response from server, got: %s", resp.Status))
	}

	body, err := bodyFromResponse(*resp, op)
	dec := newDecoder(body, op)

	switch op {
	case execQuery:
		ch := dec.decodeV2(ctx)
		return ch, nil
	case execMgmt:
		ch := dec.decodeV1(ctx)
		return ch, nil
	default:
		panic(errors.E(op, errors.KOther, fmt.Errorf("Unexpected op type")))
	}
}

func copyHeaders(header http.Header) http.Header {
	headers := make(http.Header, len(header))
	for k, v := range header {
		headers[k] = v
	}
	return headers
}
