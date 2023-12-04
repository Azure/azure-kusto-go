package query

import "github.com/Azure/azure-kusto-go/azkustodata/types"

type Column interface {
	Ordinal() int
	Name() string
	Type() types.Column
}
