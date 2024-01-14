package azkustoingest

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata"
	v1 "github.com/Azure/azure-kusto-go/azkustodata/query/v1"
	v2 "github.com/Azure/azure-kusto-go/azkustodata/query/v2"
	"io"
	"net/http"
)

type QueryClient interface {
	io.Closer
	Auth() azkustodata.Authorization
	Endpoint() string
	Query(ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.QueryOption) (v2.FullDataset, error)
	Mgmt(ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.QueryOption) (v1.Dataset, error)
	IterativeQuery(ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.QueryOption) (v2.IterativeDataset, error)
	HttpClient() *http.Client
	ClientDetails() *azkustodata.ClientDetails
}
