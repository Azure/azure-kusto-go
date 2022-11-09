// Package conn holds a streaming ingest connetion.
package conn

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
	"sync"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/properties"
	"github.com/Azure/azure-kusto-go/kusto/internal/response"
	"github.com/Azure/azure-kusto-go/kusto/internal/version"
	"github.com/Azure/go-autorest/autorest"
	"github.com/google/uuid"
)

var validURL = regexp.MustCompile(`https://([a-zA-Z0-9_-]+\.){1,2}.*\??`)

// BuffPool provides a pool of *bytes.Buffer objects.
var BuffPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

// Conn provides connectivity to the Kusto streaming ingestion service.
type Conn struct {
	auth        kusto.Authorization
	baseURL     *url.URL
	reqHeaders  http.Header
	headersPool chan http.Header
	client      *http.Client
	done        chan struct{}

	inTest bool
}

// New returns a new Conn object.
func New(endpoint string, auth kusto.Authorization, client *http.Client) (*Conn, error) {
	if !validURL.MatchString(endpoint) {
		return nil, errors.ES(
			errors.OpServConn,
			errors.KClientArgs,
			"endpoint is not valid(%s) for Kusto streaming ingestion", endpoint,
		).SetNoRetry()
	}
	if err := auth.Validate(endpoint); err != nil {
		return nil, err
	}

	return newWithoutValidation(endpoint, auth, client)
}

func newWithoutValidation(endpoint string, auth kusto.Authorization, client *http.Client) (*Conn, error) {
	headers := http.Header{}
	headers.Add("Accept", "application/json")
	headers.Add("Accept-Encoding", "gzip,deflate")
	headers.Add("x-ms-client-version", "Kusto.Go.Client: "+version.Kusto)
	headers.Add("Connection", "Keep-Alive")

	// TODO(daniel/jdoak): Get rid of this Replace stuff. I mean, its just hacky.
	u, err := url.Parse(strings.Replace(endpoint, "ingest-", "", 1))
	if err != nil {
		return nil, errors.E(
			errors.OpServConn,
			errors.KClientArgs,
			fmt.Errorf("could not parse the endpoint(%s): %s", endpoint, err),
		).SetNoRetry()
	}

	c := &Conn{
		auth:        auth,
		baseURL:     &url.URL{Scheme: u.Scheme, Host: u.Host, Path: "/v1/rest/ingest/"},
		reqHeaders:  headers,
		headersPool: make(chan http.Header, 100),
		client:      client,
		done:        make(chan struct{}),
	}

	// Fills a pool of headers to alleviate header copying timing at request time.
	// These are automatically renewed by spun off goroutines when a header is pulled.
	// TODO(jdoak): Decide if a sync.Pool would be better. In 1.13 they aren't triggering GC nearly as much.
	for i := 0; i < 100; i++ {
		c.headersPool <- copyHeaders(headers)
	}

	return c, nil
}

var writeOp = errors.OpIngestStream

// StreamIngest ingests into database "db", table "table" what is stored in "payload" which should be encoded in "format" and
// have a server side data mapping reference named "mappingName".  "mappingName" can be nil.
func (c *Conn) StreamIngest(ctx context.Context, db, table string, payload io.Reader, format properties.DataFormat, mappingName string, clientRequestId string) error {
	defer func() {
		if buf, ok := payload.(*bytes.Buffer); ok {
			buf.Reset()
			BuffPool.Put(buf)
		}
	}()

	switch {
	case format == properties.DFUnknown:
		format = properties.CSV
	}

	headers := <-c.headersPool
	go func() {
		header := copyHeaders(c.reqHeaders)
		select {
		case <-c.done:
			return
		case c.headersPool <- header:
			return
		}
	}()

	if clientRequestId != "" {
		headers.Add("x-ms-client-request-id", clientRequestId)
	} else {
		headers.Add("x-ms-client-request-id", "KGC.execute;"+uuid.New().String())
	}

	headers.Add("Content-Type", "application/json; charset=utf-8")
	headers.Add("Content-Encoding", "gzip")

	u, _ := url.Parse(c.baseURL.String()) // Safe copy of a known good URL object
	u.Path = path.Join(u.Path, db, table)

	qv := url.Values{}
	if mappingName != "" {
		qv.Add("mappingName", mappingName)
	}
	qv.Add("streamFormat", format.CamelCase())
	u.RawQuery = qv.Encode()

	var closeablePayload io.ReadCloser
	var ok bool
	if closeablePayload, ok = payload.(io.ReadCloser); !ok {
		closeablePayload = io.NopCloser(payload)
	}

	req := &http.Request{
		Method: http.MethodPost,
		URL:    u,
		Header: headers,
		Body:   closeablePayload,
	}

	if !c.inTest {
		var err error
		prep := c.auth.Authorizer.WithAuthorization()
		req, err = prep(autorest.CreatePreparer()).Prepare(req)
		if err != nil {
			return errors.E(writeOp, errors.KInternal, err)
		}
	}

	resp, err := c.client.Do(req.WithContext(ctx))
	if err != nil {
		return errors.E(writeOp, errors.KHTTPError, err)
	}

	if resp.StatusCode != 200 {
		body, err := response.TranslateBody(resp, writeOp)
		if err != nil {
			return err
		}
		return errors.HTTP(writeOp, resp.Status, resp.StatusCode, body, "streaming ingest issue")
	}
	return nil
}

func copyHeaders(header http.Header) http.Header {
	headers := make(http.Header, len(header))
	for k, v := range header {
		headers[k] = v
	}
	return headers
}

func (c *Conn) Close() error {
	select {
	case <-c.done:
		return nil
	default:
		close(c.done)
		return nil
	}
}
