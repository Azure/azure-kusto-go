package query

type Table interface {
	BaseTable
	Consume() ([]Row, []error)
}
type TableResult interface {
	Table() StreamingTable
	Err() error
}
