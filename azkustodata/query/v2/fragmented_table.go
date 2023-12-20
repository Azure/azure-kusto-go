package v2

import (
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"strconv"
)

// fragmentedTable represents a table that is fragmented - the rows are received from the service in chunks.
// It is used by the full dataset, as it needs to store the rows until the dataset is returned.
type fragmentedTable struct {
	query.Table
	rows   []query.Row
	errors []error
}

func (f *fragmentedTable) RowCount() int {
	return len(f.rows)
}

func (f *fragmentedTable) addRawRows(rows RawRows) {
	for _, r := range rows {
		if r.Errors != nil {
			for _, e := range r.Errors {
				f.errors = append(f.errors, &e)
			}
			continue
		}

		row, err := parseRow(r.Row, f)
		if err != nil {
			f.errors = append(f.errors, err)
		}
		f.rows = append(f.rows, row)
	}
}

func (f *fragmentedTable) close(errors []OneApiError) {
	for _, e := range errors {
		f.errors = append(f.errors, &e)
	}
}

func (d *fullDataset) newTableFromHeader(th *TableHeader) (table, error) {
	columns := make([]query.Column, len(th.Columns))
	err := parseColumns(th, columns, d.Op())
	if err != nil {
		return nil, err
	}

	return &fragmentedTable{Table: query.NewDataTable(d, int64(th.TableId), strconv.Itoa(th.TableId), th.TableName, th.TableKind, columns, make([]query.Row, 0), make([]error, 0))}, nil
}
