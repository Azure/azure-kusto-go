package query

// dataTable is a basic implementation of Table, to be used by specific implementations.
// It contains all the rows and errors for the table.
type dataTable struct {
	baseTable
	rows   []Row
	errors []error
}

func NewDataTable(ds Dataset, ordinal int64, id string, name string, kind string, columns []Column, rows []Row, errors []error) Table {
	t := &dataTable{
		baseTable: *NewTable(ds, ordinal, id, name, kind, columns).(*baseTable),
		rows:      rows,
		errors:    errors,
	}

	for _, r := range rows {
		if rr, ok := r.(*row); ok {
			rr.table = t
		}
	}

	return t
}

func (t *dataTable) GetAllRows() ([]Row, []error) {
	errs := t.errors
	if t.errors != nil && len(t.errors) == 0 {
		errs = nil
	}
	return t.rows, errs
}
