package query

type dataTable struct {
	baseTable
	rows   []Row
	errors []error
}

func NewDataTable(ds Dataset, ordinal int64, id string, name string, kind string, columns []Column, rows []Row, errors []error) Table {
	t := &dataTable{
		baseTable: baseTable{
			dataSet: ds,
			ordinal: ordinal,
			id:      id,
			name:    name,
			kind:    kind,
			columns: columns,
		},
		rows:   rows,
		errors: errors,
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
