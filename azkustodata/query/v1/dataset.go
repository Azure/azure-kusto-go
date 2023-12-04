package v1

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/query/common"
	"github.com/google/uuid"
	"io"
)

type TableIndexRow struct {
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
	index   []TableIndexRow
	status  []QueryStatus
	info    []QueryInformation
}

func NewDatasetFromReader(ctx context.Context, op errors.Op, reader io.ReadCloser) (Dataset, error) {
	defer reader.Close()
	v1, err := decodeV1(reader)
	if err != nil {
		return nil, err
	}

	return NewDataset(ctx, op, v1)
}

func NewDataset(ctx context.Context, op errors.Op, v1 V1) (Dataset, error) {
	d := &dataset{
		Dataset: common.NewDataset(ctx, op),
	}

	if v1.Exceptions != nil {
		return nil, errors.ES(d.Op(), errors.KInternal, "kusto query failed: %v", v1.Exceptions)
	}

	if len(v1.Tables) == 0 {
		return nil, errors.ES(d.Op(), errors.KInternal, "kusto query failed: no tables returned")
	}

	// index is always the last table
	lastTable := &v1.Tables[len(v1.Tables)-1]

	index, err := parseTable[TableIndexRow](lastTable, d, nil)
	if err != nil {
		return nil, err
	}

	for _, r := range index {
		if r.Kind == "QueryStatus" {
			queryStatus, err := parseTable[QueryStatus](lastTable, d, &r)
			if err != nil {
				return nil, err
			}
			d.status = queryStatus
		}
		if r.Kind == "QueryInformation" {
			queryInfo, err := parseTable[QueryInformation](lastTable, d, &r)
			if err != nil {
				return nil, err
			}
			d.info = queryInfo
		}
		if r.Kind == "QueryResult" {
			table, err := parseTable[query.Table](lastTable, d, &r)
			if err != nil {
				return nil, err
			}
			d.results = append(d.results, table...)
		}
	}

	return d, nil
}

func parseTable[T any](table *RawTable, d *dataset, index *TableIndexRow) ([]T, error) {
	fullTable, err := NewFullTable(d, table, index)
	if err != nil {
		return nil, err
	}

	indexRows, errs := fullTable.Consume()
	if errs != nil {
		return nil, errors.GetCombinedError(errs...)
	}

	rows, err := query.ToStructs[T](indexRows)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (d *dataset) Results() []query.Table {
	return d.results
}

func (d *dataset) Index() []TableIndexRow {
	return d.index
}

func (d *dataset) Status() []QueryStatus {
	return d.status
}

func (d *dataset) Info() []QueryInformation {
	return d.info
}

type Dataset interface {
	query.Dataset
	Results() []query.Table
	Index() []TableIndexRow
	Status() []QueryStatus
	Info() []QueryInformation
}
