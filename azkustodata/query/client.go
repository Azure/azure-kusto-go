package query

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/query/v1"
	"github.com/Azure/azure-kusto-go/azkustodata/query/v2"
)

type KustoClient interface {
	Mgmt(ctx context.Context, db string, kqlQuery azkustodata.Statement, options ...azkustodata.QueryOption) (v1.Dataset, error)
	Query(ctx context.Context, db string, kqlQuery azkustodata.Statement, options ...azkustodata.QueryOption) (v2.FullDataset, error)
	IterativeQuery(ctx context.Context, db string, kqlQuery azkustodata.Statement, options ...azkustodata.QueryOption) (v2.IterativeDataset, error)
	QueryToJson(ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.QueryOption) (string, error)
	Close() error
}
