package kql

import (
	"fmt"
	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"time"
)

type Value interface {
	fmt.Stringer
	Value() interface{}
	Type() types.Column
}

type kqlValue struct {
	value     interface{}
	kustoType types.Column
}

func (v *kqlValue) Value() interface{} {
	return v.value
}

func (v *kqlValue) Type() types.Column {
	return v.kustoType
}

func (v *kqlValue) String() string {
	value := v.value
	switch v.kustoType {
	case types.String:
		return QuoteString(value.(string), false)
	case types.DateTime:
		value = FormatDatetime(value.(time.Time))
	case types.Timespan:
		value = FormatTimespan(value.(time.Duration))
	case types.Dynamic:
		value = QuoteString(value.(string), false)
	}

	return fmt.Sprintf("%v(%v)", v.kustoType, value)
}

func newValue(value interface{}, kustoType types.Column) Value {
	return &kqlValue{
		value:     value,
		kustoType: kustoType,
	}
}

func (b *statementBuilder) AddBool(value bool) Builder {
	return b.addBase(newValue(value, types.Bool))
}

func (b *statementBuilder) AddDateTime(value time.Time) Builder {
	return b.addBase(newValue(value, types.DateTime))
}

func (b *statementBuilder) AddDynamic(value string) Builder {
	return b.addBase(newValue(value, types.Dynamic))
}

func (b *statementBuilder) AddGUID(value uuid.UUID) Builder {
	return b.addBase(newValue(value, types.GUID))
}

func (b *statementBuilder) AddInt(value int32) Builder {
	return b.addBase(newValue(value, types.Int))
}

func (b *statementBuilder) AddLong(value int64) Builder {
	return b.addBase(newValue(value, types.Long))
}

func (b *statementBuilder) AddReal(value float64) Builder {
	return b.addBase(newValue(value, types.Real))
}

func (b *statementBuilder) AddString(value string) Builder {
	return b.addBase(newValue(value, types.String))
}

func (b *statementBuilder) AddTimespan(value time.Duration) Builder {
	return b.addBase(newValue(value, types.Timespan))
}

func (b *statementBuilder) AddDecimal(value decimal.Decimal) Builder {
	return b.addBase(newValue(value, types.Decimal))
}

func (q *StatementQueryParameters) AddBool(key string, paramType string, value bool) *StatementQueryParameters {
	return q.addBase(key, newValue(paramType, types.String), newValue(value, types.Bool))
}

func (q *StatementQueryParameters) AddDateTime(key string, paramType string, value time.Time) *StatementQueryParameters {
	return q.addBase(key, newValue(paramType, types.String), newValue(value, types.DateTime))
}

func (q *StatementQueryParameters) AddDynamic(key string, paramType string, value string) *StatementQueryParameters {
	return q.addBase(key, newValue(paramType, types.String), newValue(value, types.Dynamic))
}

func (q *StatementQueryParameters) AddGUID(key string, paramType string, value uuid.UUID) *StatementQueryParameters {
	return q.addBase(key, newValue(paramType, types.String), newValue(value, types.GUID))
}

func (q *StatementQueryParameters) AddInt(key string, paramType string, value int32) *StatementQueryParameters {
	return q.addBase(key, newValue(paramType, types.String), newValue(value, types.Int))
}

func (q *StatementQueryParameters) AddLong(key string, paramType string, value int64) *StatementQueryParameters {
	return q.addBase(key, newValue(paramType, types.String), newValue(value, types.Long))
}

func (q *StatementQueryParameters) AddReal(key string, paramType string, value float64) *StatementQueryParameters {
	return q.addBase(key, newValue(paramType, types.String), newValue(value, types.Real))
}

func (q *StatementQueryParameters) AddString(key string, paramType string, value string) *StatementQueryParameters {
	return q.addBase(key, newValue(paramType, types.String), newValue(value, types.String))
}

func (q *StatementQueryParameters) AddTimespan(key string, paramType string, value time.Duration) *StatementQueryParameters {
	return q.addBase(key, newValue(paramType, types.String), newValue(value, types.Timespan))
}

func (q *StatementQueryParameters) AddDecimal(key string, paramType string, value decimal.Decimal) *StatementQueryParameters {
	return q.addBase(key, newValue(paramType, types.String), newValue(value, types.Decimal))
}
