package v1

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/google/uuid"
	"io"
	"time"
)

type TableIndexRow struct {
	Ordinal    int64
	Kind       string
	Name       string
	Id         string
	PrettyName string
}

var primaryResultIndexRow = &TableIndexRow{
	Ordinal:    0,
	Kind:       "QueryResult",
	Name:       "PrimaryResult",
	Id:         "00000000-0000-0000-0000-000000000000",
	PrettyName: "",
}

type QueryStatus struct {
	Timestamp         time.Time
	Severity          int32
	SeverityName      string
	StatusCode        int32
	StatusDescription string
	Count             int32
	RequestId         uuid.UUID
	ActivityId        uuid.UUID
	SubActivityId     uuid.UUID
	ClientActivityId  string
}

type QueryProperties struct {
	Value string
}

type dataset struct {
	query.Dataset
	results []query.FullTable
	index   []TableIndexRow
	status  []QueryStatus
	info    []QueryProperties
}

func NewDatasetFromReader(ctx context.Context, op errors.Op, reader io.ReadCloser) (Dataset, error) {
	defer reader.Close()
	v1, err := decodeV1(reader)
	if err != nil {
		return nil, err
	}

	return NewDataset(ctx, op, *v1)
}

func NewDataset(ctx context.Context, op errors.Op, v1 V1) (Dataset, error) {
	d := &dataset{
		Dataset: query.NewDataset(ctx, op),
	}

	if len(v1.Tables) == 0 {
		return nil, errors.ES(d.Op(), errors.KInternal, "kusto query failed: no tables returned")
	}

	// Special case - if there is only one table, it is the primary result
	if len(v1.Tables) == 1 {
		if v1.Exceptions != nil {
			return nil, errors.ES(d.Op(), errors.KInternal, "exceptions: %v", v1.Exceptions)
		}

		table, err := NewDataTable(d, &v1.Tables[0], primaryResultIndexRow)
		if err != nil {
			return nil, err
		}

		d.results = append(d.results, table)

		return d, err
	}

	// index is always the last table
	lastTable := &v1.Tables[len(v1.Tables)-1]

	index, err := parseTable[TableIndexRow](lastTable, d, nil)
	if err != nil {
		return nil, err
	}

	d.index = index

	for i, r := range index {
		if r.Kind == "QueryStatus" {
			queryStatus, err := parseTable[QueryStatus](&v1.Tables[i], d, &r)
			if err != nil {
				return nil, err
			}
			d.status = queryStatus
		} else if r.Kind == "QueryProperties" {
			queryInfo, err := parseTable[QueryProperties](&v1.Tables[i], d, &r)
			if err != nil {
				return nil, err
			}
			d.info = queryInfo
		} else if r.Kind == "QueryResult" {
			table, err := NewDataTable(d, &v1.Tables[i], &r)
			if err != nil {
				return nil, err
			}

			d.results = append(d.results, table)
		}
	}

	err = nil

	if v1.Exceptions != nil {
		err = errors.ES(d.Op(), errors.KInternal, "exceptions: %v", v1.Exceptions)
	}

	return d, err
}

func parseTable[T any](table *RawTable, d *dataset, index *TableIndexRow) ([]T, error) {
	fullTable, err := NewDataTable(d, table, index)
	if err != nil {
		return nil, err
	}

	indexRows := fullTable.Rows()

	rows, err := query.ToStructs[T](indexRows)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (d *dataset) Tables() []query.FullTable {
	return d.results
}

func (d *dataset) Index() []TableIndexRow {
	return d.index
}

func (d *dataset) Status() []QueryStatus {
	return d.status
}

func (d *dataset) Info() []QueryProperties {
	return d.info
}

type Dataset interface {
	query.Dataset
	query.FullDataset
	Index() []TableIndexRow
	Status() []QueryStatus
	Info() []QueryProperties
}
