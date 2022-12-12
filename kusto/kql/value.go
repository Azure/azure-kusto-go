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

func NewBool(value bool) Value {
	return newValue(value, types.Bool)
}

func NewDateTime(value time.Time) Value {
	return newValue(value, types.DateTime)
}

func NewDynamic(value string) Value {
	return newValue(value, types.Dynamic)
}

func NewGUID(value uuid.UUID) Value {
	return newValue(value, types.GUID)
}

func NewInt(value int32) Value {
	return newValue(value, types.Int)
}

func NewLong(value int64) Value {
	return newValue(value, types.Long)
}

func NewReal(value float64) Value {
	return newValue(value, types.Real)
}

func NewString(value string) Value {
	return newValue(value, types.String)
}

func NewTimespan(value time.Duration) Value {
	return newValue(value, types.Timespan)
}

func NewDecimal(value decimal.Decimal) Value {
	return newValue(value, types.Decimal)
}
