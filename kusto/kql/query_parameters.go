package kql

import (
	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"strings"
	"sync"
	"time"
)

type StatementQueryParameters struct {
	parameters map[string]Value
}

var buildPool = sync.Pool{
	New: func() interface{} {
		return new(strings.Builder)
	},
}

func NewStatementQueryParameters() *StatementQueryParameters {
	return &StatementQueryParameters{parameters: make(map[string]Value)}
}
func (q *StatementQueryParameters) addBase(key string, value Value) *StatementQueryParameters {
	if RequiresQuoting(key) {
		panic("Invalid parameter values. make sure to adhere to KQL entity name conventions and escaping rules.")
	}
	q.parameters[key] = value
	return q
}

func (q *StatementQueryParameters) AddBool(key string, value bool) *StatementQueryParameters {
	return q.addBase(key, newValue(value, types.Bool))
}

func (q *StatementQueryParameters) AddDateTime(key string, value time.Time) *StatementQueryParameters {
	return q.addBase(key, newValue(value, types.DateTime))
}

func (q *StatementQueryParameters) AddDynamic(key string, value interface{}) *StatementQueryParameters {
	return q.addBase(key, newValue(value, types.Dynamic))
}

func (q *StatementQueryParameters) AddGUID(key string, value uuid.UUID) *StatementQueryParameters {
	return q.addBase(key, newValue(value, types.GUID))
}

func (q *StatementQueryParameters) AddInt(key string, value int32) *StatementQueryParameters {
	return q.addBase(key, newValue(value, types.Int))
}

func (q *StatementQueryParameters) AddLong(key string, value int64) *StatementQueryParameters {
	return q.addBase(key, newValue(value, types.Long))
}

func (q *StatementQueryParameters) AddReal(key string, value float64) *StatementQueryParameters {
	return q.addBase(key, newValue(value, types.Real))
}

func (q *StatementQueryParameters) AddString(key string, value string) *StatementQueryParameters {
	return q.addBase(key, newValue(value, types.String))
}

func (q *StatementQueryParameters) AddTimespan(key string, value time.Duration) *StatementQueryParameters {
	return q.addBase(key, newValue(value, types.Timespan))
}

func (q *StatementQueryParameters) AddDecimal(key string, value decimal.Decimal) *StatementQueryParameters {
	return q.addBase(key, newValue(value, types.Decimal))
}

// note - due to the psuedo-random nature of maps, the declaration string might be ordered differently for different runs.
// might crash the test in those times.
func (q *StatementQueryParameters) ToDeclarationString() string {
	const (
		declare   = "declare query_parameters("
		closeStmt = ");"
	)

	if len(q.parameters) == 0 {
		return ""
	}

	build := buildPool.Get().(*strings.Builder)
	build.Reset()
	defer buildPool.Put(build)

	build.WriteString(declare)
	comma := len(q.parameters)
	for key, paramVals := range q.parameters {
		build.WriteString(key)
		build.WriteString(":")
		build.WriteString(string(paramVals.Type()))
		if comma > 1 {
			build.WriteString(", ")
		}
		comma--
	}
	build.WriteString(closeStmt)
	return build.String()
}
func (q *StatementQueryParameters) ToParameterCollection() map[string]string {
	var parameters = make(map[string]string)
	for key, paramVals := range q.parameters {
		parameters[key] = paramVals.String()
	}
	return parameters
}
