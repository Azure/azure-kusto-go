package kusto

// Conn.go holds the connection to the Kusto server and provides methods to do queries
// and receive Kusto frames back.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"

	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/internal/frames"
	v1 "github.com/Azure/azure-kusto-go/kusto/internal/frames/v1"
	v2 "github.com/Azure/azure-kusto-go/kusto/internal/frames/v2"
	"github.com/Azure/azure-kusto-go/kusto/internal/response"
	truestedEndpoints "github.com/Azure/azure-kusto-go/kusto/trustedendpoints"
	"github.com/google/uuid"
)

var validURL = regexp.MustCompile(`https://([a-zA-Z0-9_-]+\.){1,2}.*`)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

// Conn provides connectivity to a Kusto instance.
type Conn struct {
	endpoint                           string
	auth                               Authorization
	endMgmt, endQuery, endStreamIngest *url.URL
	client                             *http.Client
	endpointValidated                  atomic.Bool
	clientDetails                      *ClientDetails
}

// NewConn returns a new Conn object with an injected http.Client
func NewConn(endpoint string, auth Authorization, client *http.Client, clientDetails *ClientDetails) (*Conn, error) {
	if !validURL.MatchString(endpoint) {
		return nil, errors.ES(errors.OpServConn, errors.KClientArgs, "endpoint is not valid(%s), should be https://<cluster name>.*", endpoint).SetNoRetry()
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, errors.ES(errors.OpServConn, errors.KClientArgs, "could not parse the endpoint(%s): %s", endpoint, err).SetNoRetry()
	}
	if !strings.HasPrefix(u.Path, "/") {
		u.Path = "/" + u.Path
	}

	c := &Conn{
		auth:            auth,
		endMgmt:         u.JoinPath("/v1/rest/mgmt"),
		endQuery:        u.JoinPath("/v2/rest/query"),
		endStreamIngest: u.JoinPath("/v1/rest/ingest"),
		client:          client,
		clientDetails:   clientDetails,
	}

	return c, nil
}

type queryMsg struct {
	DB         string            `json:"db"`
	CSL        string            `json:"csl"`
	Properties requestProperties `json:"properties,omitempty"`
}

type connOptions struct {
	queryOptions *queryOptions
}

// query makes a query for the purpose of extracting data from Kusto. Context can be used to set
// a timeout or cancel the query. Queries cannot take longer than 5 minutes.
func (c *Conn) query(ctx context.Context, db string, query Statement, options *queryOptions) (execResp, error) {
	if strings.HasPrefix(strings.TrimSpace(query.String()), ".") {
		return execResp{}, errors.ES(errors.OpQuery, errors.KClientArgs, "a Stmt to Query() cannot begin with a period(.), only Mgmt() calls can do that").SetNoRetry()
	}

	return c.execute(ctx, execQuery, db, query, *options.requestProperties)
}

// mgmt is used to do management queries to Kusto.
func (c *Conn) mgmt(ctx context.Context, db string, query Statement, options *queryOptions) (execResp, error) {
	return c.execute(ctx, execMgmt, db, query, *options.requestProperties)
}

func (c *Conn) queryToJson(ctx context.Context, db string, query Statement, options *queryOptions) (string, error) {
	_, _, _, body, e := c.doRequest(ctx, execQuery, db, query, *options.requestProperties)
	if e != nil {
		return "", e
	}

	defer body.Close()
	all, e := io.ReadAll(body)
	return string(all), e
}

const (
	execQuery = 1
	execMgmt  = 2
)

type execResp struct {
	reqHeader  http.Header
	respHeader http.Header
	frameCh    chan frames.Frame
}

func (c *Conn) execute(ctx context.Context, execType int, db string, query Statement, properties requestProperties) (execResp, error) {
	logger := zerolog.Ctx(ctx).With().
		Str("function", "execute").
		Str("db", db).
		Str("query", query.String()).
		Str("clientRequestID", properties.ClientRequestID).
		Str("User", properties.User).
		Str("Application", properties.Application).
		Logger()

	logger.Info().Msg("starting execution")
	op, reqHeader, respHeader, body, e := c.doRequest(ctx, execType, db, query, properties)
	if e != nil {
		logger.Error().Err(e).Msg("request failed")
		return execResp{}, e
	}

	logger.Info().Msg("request done")

	var dec frames.Decoder
	var decName = ""
	switch execType {
	case execMgmt:
		dec = &v1.Decoder{}
		decName = "v1"
	case execQuery:
		dec = &v2.Decoder{}
		decName = "v2"
	default:
		return execResp{}, errors.ES(op, errors.KInternal, "unknown execution type was %v", execType).SetNoRetry()
	}

	logger.Info().Str("decoder", decName).Msg("decoder created")

	frameCh := dec.Decode(ctx, body, op)

	logger.Info().Msg("decoder started")

	return execResp{reqHeader: reqHeader, respHeader: respHeader, frameCh: frameCh}, nil
}

func (c *Conn) doRequest(ctx context.Context, execType int, db string, query Statement, properties requestProperties) (errors.Op, http.Header, http.Header,
	io.ReadCloser, error) {

	logger := zerolog.Ctx(ctx).With().
		Str("function", "doRequest").
		Str("db", db).
		Str("query", query.String()).
		Str("execType", fmt.Sprintf("%v", execType)).
		Logger()

	logger.Info().Msg("starting doRequest")

	logger.Info().Msg("validating endpoint")

	err := c.validateEndpoint()

	logger.Info().Msg("validated endpoint")

	var op errors.Op
	if execType == execQuery {
		op = errors.OpQuery
	} else if execType == execMgmt {
		op = errors.OpMgmt
	}

	logger.Info().Str("op", op.String()).Msg("setting op")

	var endpoint *url.URL

	buff := bufferPool.Get().(*bytes.Buffer)
	buff.Reset()
	defer bufferPool.Put(buff)

	logger.Info().Msg("got buffer from pool")

	switch execType {
	case execQuery, execMgmt:
		var err error
		var csl string
		if query.SupportsInlineParameters() || properties.QueryParameters.Count() == 0 {
			csl = query.String()
		} else {
			csl = fmt.Sprintf("%s\n%s", properties.QueryParameters.ToDeclarationString(), query.String())
		}

		logger.Info().Str("query", csl).Msg("got query")

		err = json.NewEncoder(buff).Encode(
			queryMsg{
				DB:         db,
				CSL:        csl,
				Properties: properties,
			},
		)
		logger.Info().Msg("encoded query")

		if err != nil {
			logger.Error().Err(err).Msg("could not JSON marshal the Query message")
			return 0, nil, nil, nil, errors.E(op, errors.KInternal, fmt.Errorf("could not JSON marshal the Query message: %w", err))
		}
		if execType == execQuery {
			endpoint = c.endQuery
		} else {
			endpoint = c.endMgmt
		}

	default:
		return 0, nil, nil, nil, errors.ES(op, errors.KInternal, "internal error: did not understand the type of execType: %d", execType)
	}

	headers := c.getHeaders(properties)
	logger.Info().Dict("headers", zerolog.Dict().Fields(headers)).Msg("got headers")
	responseHeaders, closer, err := c.doRequestImpl(ctx, op, endpoint, io.NopCloser(buff), headers, fmt.Sprintf("With query: %s", query.String()))
	return op, headers, responseHeaders, closer, err
}

func (c *Conn) doRequestImpl(
	ctx context.Context,
	op errors.Op,
	endpoint *url.URL,
	buff io.ReadCloser,
	headers http.Header,
	errorContext string) (http.Header, io.ReadCloser, error) {

	logger := zerolog.Ctx(ctx).With().
		Str("function", "doRequestImpl").
		Str("endpoint", endpoint.String()).
		Str("errorContext", errorContext).
		Logger()

	// Replace non-ascii chars in headers with '?'
	for _, values := range headers {
		var builder strings.Builder
		for i := range values {
			for _, char := range values[i] {
				if char > unicode.MaxASCII {
					builder.WriteRune('?')
				} else {
					builder.WriteRune(char)
				}
			}
			values[i] = builder.String()
		}
	}

	logger.Info().Dict("headers", zerolog.Dict().Fields(headers)).Msg("got headers")

	if c.auth.TokenProvider != nil && c.auth.TokenProvider.AuthorizationRequired() {
		c.auth.TokenProvider.SetHttp(c.client)
		token, tokenType, tkerr := c.auth.TokenProvider.AcquireToken(ctx)
		if tkerr != nil {
			return nil, nil, errors.ES(op, errors.KInternal, "Error while getting token : %s", tkerr)
		}
		headers.Add("Authorization", fmt.Sprintf("%s %s", tokenType, token))
	}

	logger.Info().Msg("added authorization header")

	req := &http.Request{
		Method: http.MethodPost,
		URL:    endpoint,
		Header: headers,
		Body:   buff,
	}

	logger.Info().Msg("created request")

	resp, err := c.client.Do(req.WithContext(ctx))
	logger.Info().Msg("sent request")

	if err != nil {
		logger.Error().Err(err).Msg("error sending request")
		// TODO(jdoak): We need a http error unwrap function that pulls out an *errors.Error.
		return nil, nil, errors.E(op, errors.KHTTPError, fmt.Errorf("%v, %w", errorContext, err))
	}

	logger.Info().Msg("got response")
	body, err := response.TranslateBody(resp, op, logger)
	if err != nil {
		logger.Error().Err(err).Msg("error translating response body")
		return nil, nil, err
	}

	if resp.StatusCode != http.StatusOK {
		logger.Error().Msg("response status code not OK")
		return nil, nil, errors.HTTP(op, resp.Status, resp.StatusCode, body, fmt.Sprintf("error from Kusto endpoint, %v", errorContext))
	}
	return resp.Header, body, nil
}

func (c *Conn) validateEndpoint() error {
	if !c.endpointValidated.Load() {
		var err error
		if cloud, err := GetMetadata(c.endpoint, c.client); err == nil {
			err = truestedEndpoints.Instance.ValidateTrustedEndpoint(c.endpoint, cloud.LoginEndpoint)
			if err == nil {
				c.endpointValidated.Store(true)
			}
		}

		return err
	}

	return nil
}

const ClientRequestIdHeader = "x-ms-client-request-id"
const ApplicationHeader = "x-ms-app"
const UserHeader = "x-ms-user"
const ClientVersionHeader = "x-ms-client-version"

func (c *Conn) getHeaders(properties requestProperties) http.Header {
	header := http.Header{}
	header.Add("Accept", "application/json")
	header.Add("Accept-Encoding", "gzip, deflate")
	header.Add("Content-Type", "application/json; charset=utf-8")
	header.Add("Connection", "Keep-Alive")
	header.Add("x-ms-version", "2019-02-13")

	if properties.ClientRequestID != "" {
		header.Add(ClientRequestIdHeader, properties.ClientRequestID)
	} else {
		header.Add(ClientRequestIdHeader, "KGC.execute;"+uuid.New().String())
	}

	if properties.Application != "" {
		header.Add(ApplicationHeader, properties.Application)
	} else {
		header.Add(ApplicationHeader, c.clientDetails.ApplicationForTracing())
	}

	if properties.User != "" {
		header.Add(UserHeader, properties.User)
	} else {
		header.Add(UserHeader, c.clientDetails.UserNameForTracing())
	}

	header.Add(ClientVersionHeader, c.clientDetails.ClientVersionForTracing())
	return header
}

func (c *Conn) Close() error {
	c.client.CloseIdleConnections()
	return nil
}
