package query

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
)

type Dataset interface {
	Context() context.Context
	Op() errors.Op
}
