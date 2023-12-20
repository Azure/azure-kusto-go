package query

type Table interface {
	BaseTable
	GetAllRows() ([]Row, []error)
}

type TableResult interface {
	Table() IterativeTable
	Err() error
}
