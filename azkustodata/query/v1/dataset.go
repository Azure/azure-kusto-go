package v1

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/query/common"
	"github.com/google/uuid"
)

type TableIndex struct {
	Ordinal    int64
	Kind       string
	Name       string
	Id         string
	PrettyName string
}

type QueryStatus struct {
	TimeStamp         string
	Severity          int64
	SeverityName      string
	StatusCode        int64
	StatusDescription string
	Count             int64
	RequestId         string
	ActivityId        string
	SubActivityId     uuid.UUID
	ClientActivityId  uuid.UUID
}

type QueryInformation struct {
	Value map[string]interface{}
}

type dataset struct {
	query.Dataset
	results []query.Table
	status  QueryStatus
	info    QueryInformation
}

func NewDataset(ctx context.Context, op errors.Op, v1 V1) (query.Dataset, error) {
	d := &dataset{
		Dataset: common.NewDataset(ctx, op),
	}

	if v1.Exceptions != nil {
		return nil, errors.ES(d.Op(), errors.KInternal, "kusto query failed: %v", v1.Exceptions)
	}

	if len(v1.Tables) == 0 {
		return nil, errors.ES(d.Op(), errors.KInternal, "kusto query failed: no tables returned")
	}

	return d, nil
}
