package kusto

// conn.go holds the connection to the Kusto server and provides methods to do queries
// and receive Kusto frames back.

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"

	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/internal/frames"
	v1 "github.com/Azure/azure-kusto-go/kusto/internal/frames/v1"
	v2 "github.com/Azure/azure-kusto-go/kusto/internal/frames/v2"
	"github.com/Azure/azure-kusto-go/kusto/internal/version"

	"github.com/Azure/go-autorest/autorest"
	"github.com/google/uuid"
)

var validURL = regexp.MustCompile(`https://([a-zA-Z0-9_-]{1,}\.){1,2}.*`)

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
func newConn(endpoint string, auth Authorization) (*conn, error) {
	if !validURL.MatchString(endpoint) {
		return nil, errors.ES(errors.OpServConn, errors.KClientArgs, "endpoint is not valid(%s), should be https://<cluster name>.*", endpoint).SetNoRetry()
	}

	headers := http.Header{}
	headers.Add("Accept", "application/json")
	headers.Add("Accept-Encoding", "gzip,deflate")
	headers.Add("x-ms-client-version", "Kusto.Go.Client: "+version.Kusto)

	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, errors.ES(errors.OpServConn, errors.KClientArgs, "could not parse the endpoint(%s): %s", endpoint, err).SetNoRetry()
	}

	c := &conn{
		auth:        auth.Authorizer,
		endMgmt:     &url.URL{Scheme: "https", Host: u.Hostname(), Path: "/v1/rest/mgmt"},
		endQuery:    &url.URL{Scheme: "https", Host: u.Hostname(), Path: "/v2/rest/query"},
		streamQuery: &url.URL{Scheme: "https", Host: u.Hostname(), Path: "/v1/rest/ingest/"},
		reqHeaders:  headers,
		headersPool: make(chan http.Header, 100),
		client:      &http.Client{},
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

var writeRE = regexp.MustCompile(`(\.set|\.append|\.set-or-append|\.set-or-replace)`)

// query makes a query for the purpose of extracting data from Kusto. Context can be used to set
// a timeout or cancel the query. Queries cannot take longer than 5 minutes.
func (c *conn) query(ctx context.Context, db string, query Stmt, options ...QueryOption) (execResp, error) {
	params, err := query.params.toParameters(query.defs)
	if err != nil {
		return execResp{}, errors.ES(errors.OpQuery, errors.KClientArgs, "QueryValues in the the Stmt were incorrect: %s", err).SetNoRetry()
	}

	// Match our server deadline to our context.Deadline. This should be set from withing kusto.Query() to always have a value.
	deadline, ok := ctx.Deadline()
	if ok {
		options = append(
			options,
			serverTimeout(deadline.Sub(nower())),
		)
	}

	opt := &queryOptions{
		requestProperties: &requestProperties{
			Options: map[string]interface{}{
				"results_progressive_enabled": true, // We want progressive frames by default.
			},
			Parameters: params,
		},
	}

	for _, o := range options {
		if err := o(opt); err != nil {
			return execResp{}, err
		}
	}

	if strings.HasPrefix(strings.TrimSpace(query.String()), ".") {
		return execResp{}, errors.ES(errors.OpQuery, errors.KClientArgs, "a Stmt to Query() cannot begin with a period(.), only Mgmt() calls can do that").SetNoRetry()
	}

	return c.execute(ctx, execQuery, db, query, "", *opt.requestProperties)
}

// mgmt is used to do management queries to Kusto.
func (c *conn) mgmt(ctx context.Context, db string, query Stmt, options ...QueryOption) (execResp, error) {
	// Match our server deadline to our context.Deadline. This should be set from withing kusto.Mgmt() to always have a value.
	deadline, ok := ctx.Deadline()
	if ok {
		options = append(
			options,
			serverTimeout(deadline.Sub(nower())),
		)
	}

	opt := &queryOptions{
		requestProperties: &requestProperties{
			Options:    map[string]interface{}{},
			Parameters: map[string]string{},
		},
	}

	for _, o := range options {
		if err := o(opt); err != nil {
			return execResp{}, err
		}
	}

	if writeRE.MatchString(query.String()) {
		if !opt.canWrite {
			return execResp{}, errors.ES(
				errors.OpQuery,
				errors.KClientArgs,
				"Mgmt() attempted to do a write operation. "+
					"This requires the AllowWrite() QueryOption to be passed. "+
					"Please see documentation on that option before use",
			).SetNoRetry()
		}
	}

	return c.execute(ctx, execMgmt, db, query, "", *opt.requestProperties)
}

const (
	execUnknown = 0
	execQuery   = 1
	execMgmt    = 2
)

type execResp struct {
	reqHeader  http.Header
	respHeader http.Header
	frameCh    chan frames.Frame
}

func (c *conn) execute(ctx context.Context, execType int, db string, query Stmt, payload string, properties requestProperties) (execResp, error) {
	headers := <-c.headersPool
	go func() {
		c.headersPool <- copyHeaders(c.reqHeaders)
	}()

	var op errors.Op
	if execType == execQuery {
		op = errors.OpQuery
	} else if execType == execMgmt {
		op = errors.OpMgmt
	}

	var endpoint *url.URL
	buff := bufferPool.Get().(*bytes.Buffer)
	buff.Reset()
	defer bufferPool.Put(buff)

	switch execType {
	case execQuery, execMgmt:
		headers.Add("Content-Type", "application/json; charset=utf-8")
		headers.Add("x-ms-client-request-id", "KGC.execute;"+uuid.New().String())

		var err error
		err = json.NewEncoder(buff).Encode(
			queryMsg{
				DB:         db,
				CSL:        query.String(),
				Properties: properties,
			},
		)
		if err != nil {
			return execResp{}, errors.E(op, errors.KInternal, fmt.Errorf("could not JSON marshal the Query message: %w", err))
		}
		if execType == execQuery {
			endpoint = c.endQuery
		} else {
			endpoint = c.endMgmt
		}
	default:
		return execResp{}, errors.ES(op, errors.KInternal, "internal error: did not understand the type of execType: %d", execType)
	}

	req := &http.Request{
		Method: http.MethodPost,
		URL:    endpoint,
		Header: headers,
		Body:   ioutil.NopCloser(buff),
	}

	var err error
	prep := c.auth.WithAuthorization()
	req, err = prep(autorest.CreatePreparer()).Prepare(req)
	if err != nil {
		return execResp{}, errors.E(op, errors.KInternal, err)
	}

	resp, err := c.client.Do(req.WithContext(ctx))
	if err != nil {
		// TODO(jdoak): We need a http error unwrap function that pulls out an *errors.Error.
		return execResp{}, errors.E(op, errors.KHTTPError, err)
	}

	if resp.StatusCode != 200 {
		return execResp{}, errors.HTTP(op, resp, "error from Kusto endpoint")
	}

	body := resp.Body
	switch enc := strings.ToLower(resp.Header.Get("Content-Encoding")); enc {
	case "":
		// Do nothing
	case "gzip":
		body, err = gzip.NewReader(resp.Body)
		if err != nil {
			return execResp{}, errors.E(op, errors.KInternal, fmt.Errorf("gzip reader error: %w", err))
		}
	case "deflate":
		body = flate.NewReader(resp.Body)
	default:
		return execResp{}, errors.ES(op, errors.KInternal, "Content-Encoding was unrecognized: %s", enc)
	}

	var dec frames.Decoder
	switch execType {
	case execMgmt:
		dec = &v1.Decoder{}
	case execQuery:
		dec = &v2.Decoder{}
	default:
		return execResp{}, errors.ES(op, errors.KInternal, "unknown execution type was %v", execType).SetNoRetry()
	}

	frameCh := dec.Decode(ctx, body, op)

	return execResp{reqHeader: headers, respHeader: resp.Header, frameCh: frameCh}, nil
}

func copyHeaders(header http.Header) http.Header {
	headers := make(http.Header, len(header))
	for k, v := range header {
		headers[k] = v
	}
	return headers
}
