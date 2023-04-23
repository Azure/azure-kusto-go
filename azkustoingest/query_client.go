package azkustoingest

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata"
	"io"
	"net/http"
)

type QueryClient interface {
	io.Closer
	Auth() azkustodata.Authorization
	Endpoint() string
	Query(ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.QueryOption) (*azkustodata.RowIterator, error)
	Mgmt(ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.MgmtOption) (*azkustodata.RowIterator, error)
	HttpClient() *http.Client
	ClientDetails() *azkustodata.ClientDetails
}
