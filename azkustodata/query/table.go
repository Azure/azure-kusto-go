package query

import (
	"github.com/Azure/azure-kusto-go/azkustodata/query/common"
)

type Table interface {
	common.BaseTable
	Consume() ([]Row, []error)
}
type TableResult interface {
	Table() StreamingTable
	Err() error
}
