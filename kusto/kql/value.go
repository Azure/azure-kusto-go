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

func (b *builder) AddBool(value bool) Builder {
	return b.addBase(newValue(value, types.Bool))
}

func (b *builder) AddDateTime(value time.Time) Builder {
	return b.addBase(newValue(value, types.DateTime))
}

func (b *builder) AddDynamic(value string) Builder {
	return b.addBase(newValue(value, types.Dynamic))
}

func (b *builder) AddGUID(value uuid.UUID) Builder {
	return b.addBase(newValue(value, types.GUID))
}

func (b *builder) AddInt(value int32) Builder {
	return b.addBase(newValue(value, types.Int))
}

func (b *builder) AddLong(value int64) Builder {
	return b.addBase(newValue(value, types.Long))
}

func (b *builder) AddReal(value float64) Builder {
	return b.addBase(newValue(value, types.Real))
}

func (b *builder) AddString(value string) Builder {
	return b.addBase(newValue(value, types.String))
}

func (b *builder) AddTimespan(value time.Duration) Builder {
	return b.addBase(newValue(value, types.Timespan))
}

func (b *builder) AddDecimal(value decimal.Decimal) Builder {
	return b.addBase(newValue(value, types.Decimal))
}
