package kusto

// conn.go holds the connection to the Kusto server and provides methods to do queries
// and receive Kusto frames back.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	"github.com/Azure/azure-kusto-go/kusto/internal/response"
	"github.com/Azure/azure-kusto-go/kusto/internal/version"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"

	"github.com/Azure/go-autorest/autorest"
	"github.com/google/uuid"
)

var validURL = regexp.MustCompile(`https://([a-zA-Z0-9_-]+\.){1,2}.*`)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

// conn provides connectivity to a Kusto instance.
type conn struct {
	endpoint                       string
	auth                           autorest.Authorizer
	tokenCred                      azcore.TokenCredential
	endMgmt, endQuery, streamQuery *url.URL
	client                         *http.Client
}

// newConn returns a new conn object with an injected http.Client
func newConn(endpoint string, auth Authorization, client *http.Client) (*conn, error) {
	c := &conn{}
	if !validURL.MatchString(endpoint) {
		return nil, errors.ES(errors.OpServConn, errors.KClientArgs, "endpoint is not valid(%s), should be https://<cluster name>.*", endpoint).SetNoRetry()
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, errors.ES(errors.OpServConn, errors.KClientArgs, "could not parse the endpoint(%s): %s", endpoint, err).SetNoRetry()
	}

	c.endMgmt = &url.URL{Scheme: "https", Host: u.Host, Path: "/v1/rest/mgmt"}
	c.endQuery = &url.URL{Scheme: "https", Host: u.Host, Path: "/v2/rest/query"}
	c.streamQuery = &url.URL{Scheme: "https", Host: u.Host, Path: "/v1/rest/ingest/"}
	c.client = client

	if auth.TokenCredential != nil {
		c.tokenCred = auth.TokenCredential
	} else {
		c.auth = auth.Authorizer
	}

	return c, nil
}

type queryMsg struct {
	DB         string            `json:"db"`
	CSL        string            `json:"csl"`
	Properties requestProperties `json:"properties,omitempty"`
}

var writeRE = regexp.MustCompile(`(\.set|\.append|\.set-or-append|\.set-or-replace)`)

type connOptions struct {
	queryOptions *queryOptions
	mgmtOptions  *mgmtOptions
}

// query makes a query for the purpose of extracting data from Kusto. Context can be used to set
// a timeout or cancel the query. Queries cannot take longer than 5 minutes.
func (c *conn) query(ctx context.Context, db string, query Stmt, options *queryOptions) (execResp, error) {
	if strings.HasPrefix(strings.TrimSpace(query.String()), ".") {
		return execResp{}, errors.ES(errors.OpQuery, errors.KClientArgs, "a Stmt to Query() cannot begin with a period(.), only Mgmt() calls can do that").SetNoRetry()
	}

	return c.execute(ctx, execQuery, db, query, *options.requestProperties)
}

// mgmt is used to do management queries to Kusto.
func (c *conn) mgmt(ctx context.Context, db string, query Stmt, options *mgmtOptions) (execResp, error) {
	if writeRE.MatchString(query.String()) {
		if !options.canWrite {
			return execResp{}, errors.ES(
				errors.OpQuery,
				errors.KClientArgs,
				"Mgmt() attempted to do a write operation. "+
					"This requires the AllowWrite() QueryOption to be passed. "+
					"Please see documentation on that option before use",
			).SetNoRetry()
		}
	}

	return c.execute(ctx, execMgmt, db, query, *options.requestProperties)
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

func (c *conn) execute(ctx context.Context, execType int, db string, query Stmt, properties requestProperties) (execResp, error) {
	var op errors.Op
	if execType == execQuery {
		op = errors.OpQuery
	} else if execType == execMgmt {
		op = errors.OpMgmt
	}

	header := http.Header{}
	header.Add("Accept", "application/json")
	header.Add("Accept-Encoding", "gzip")
	header.Add("x-ms-client-version", "Kusto.Go.Client: "+version.Kusto)
	header.Add("Content-Type", "application/json; charset=utf-8")
	header.Add("x-ms-client-request-id", "KGC.execute;"+uuid.New().String())

	var endpoint *url.URL
	buff := bufferPool.Get().(*bytes.Buffer)
	buff.Reset()
	defer bufferPool.Put(buff)

	// Test
	token, err2 := c.tokenCred.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{"https://otelkusto1.southeastasia.dev.kusto.windows.net/.default"}})
	fmt.Println("Here is the token :", token.Token)
	if err2 != nil {
		fmt.Println("Error while getting token", err2)
	}

	accessToken := token.Token
	//tokenExpiry := token.ExpiresOn

	header.Add("Authorization", fmt.Sprintf("Bearer %s", accessToken))

	//********
	switch execType {
	case execQuery, execMgmt:
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
		Header: header,
		Body:   ioutil.NopCloser(buff),
	}

	var err error
	if c.auth != nil {

		prep := c.auth.WithAuthorization()
		req, err = prep(autorest.CreatePreparer()).Prepare(req)
		if err != nil {
			return execResp{}, errors.E(op, errors.KInternal, err)
		}
	}
	resp, err := c.client.Do(req.WithContext(ctx))
	if err != nil {
		// TODO(jdoak): We need a http error unwrap function that pulls out an *errors.Error.
		return execResp{}, errors.E(op, errors.KHTTPError, fmt.Errorf("with query %q: %w", query.String(), err))
	}

	body, err := response.TranslateBody(resp, op)
	if err != nil {
		return execResp{}, err
	}

	if resp.StatusCode != http.StatusOK {
		return execResp{}, errors.HTTP(op, resp.Status, resp.StatusCode, body, fmt.Sprintf("error from Kusto endpoint for query %q: ", query.String()))
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

	return execResp{reqHeader: header, respHeader: resp.Header, frameCh: frameCh}, nil
}

func (c *conn) Close() error {
	if closer, ok := c.auth.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}
