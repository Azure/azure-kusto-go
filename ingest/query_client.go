package ingest

import (
	"context"
	"io"
	"net/http"

	"github.com/Azure/azure-kusto-go/data"
)

type QueryClient interface {
	io.Closer
	Auth() data.Authorization
	Endpoint() string
	Query(ctx context.Context, db string, query data.Statement, options ...data.QueryOption) (*data.RowIterator, error)
	Mgmt(ctx context.Context, db string, query data.Statement, options ...data.MgmtOption) (*data.RowIterator, error)
	HttpClient() *http.Client
	ClientDetails() *data.ClientDetails
}
