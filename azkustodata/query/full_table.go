package query

type fullTable struct {
	baseTable
	rows   []Row
	errors []error
}

func NewFullTable(ds Dataset, ordinal int64, id string, name string, kind string, columns []Column, rows []Row, errors []error) (Table, error) {
	t := &fullTable{
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

	return t, nil
}

func (t *fullTable) Consume() ([]Row, []error) {
	return t.rows, nil
}
