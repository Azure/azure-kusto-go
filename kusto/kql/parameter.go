package kql

import (
	"fmt"
	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"time"
)

type Parameter interface {
	fmt.Stringer
	Name() string
	Type() types.Column
	HasDefaultValue() bool
	DefaultValue() Value
}

type parameter struct {
	name          string
	kustoType     types.Column
	defaultValue  Value
	hasDefaultVal bool
}

func newParameter(name string, kustoType types.Column) Parameter {
	return &parameter{name: name, kustoType: kustoType}
}

func newParameterWithDefault(name string, defaultValue Value) Parameter {
	return &parameter{name: name, kustoType: defaultValue.Type(), defaultValue: defaultValue, hasDefaultVal: true}
}

func NewIntParameter(name string) Parameter {
	return newParameter(name, types.Int)
}

func NewIntParameterWithDefault(name string, defaultValue int32) Parameter {
	return newParameterWithDefault(name, NewInt(defaultValue))
}

func NewLongParameter(name string) Parameter {
	return newParameter(name, types.Long)
}

func NewLongParameterWithDefault(name string, defaultValue int64) Parameter {
	return newParameterWithDefault(name, NewLong(defaultValue))
}

func NewRealParameter(name string) Parameter {
	return newParameter(name, types.Real)
}

func NewRealParameterWithDefault(name string, defaultValue float64) Parameter {
	return newParameterWithDefault(name, NewReal(defaultValue))
}

func NewDecimalParameter(name string) Parameter {
	return newParameter(name, types.Decimal)
}

func NewDecimalParameterWithDefault(name string, defaultValue decimal.Decimal) Parameter {
	return newParameterWithDefault(name, NewDecimal(defaultValue))
}

func NewStringParameter(name string) Parameter {
	return newParameter(name, types.String)
}

func NewStringParameterWithDefault(name string, defaultValue string) Parameter {
	return newParameterWithDefault(name, NewString(defaultValue))
}

func NewBoolParameter(name string) Parameter {
	return newParameter(name, types.Bool)
}

func NewBoolParameterWithDefault(name string, defaultValue bool) Parameter {
	return newParameterWithDefault(name, NewBool(defaultValue))
}

func NewGUIDParameter(name string) Parameter {
	return newParameter(name, types.GUID)
}

func NewGUIDParameterWithDefault(name string, defaultValue uuid.UUID) Parameter {
	return newParameterWithDefault(name, NewGUID(defaultValue))
}

func NewDateTimeParameter(name string) Parameter {
	return newParameter(name, types.DateTime)
}

func NewDateTimeParameterWithDefault(name string, defaultValue time.Time) Parameter {
	return newParameterWithDefault(name, NewDateTime(defaultValue))
}

func NewTimespanParameter(name string) Parameter {
	return newParameter(name, types.Timespan)
}

func NewTimespanParameterWithDefault(name string, defaultValue time.Duration) Parameter {
	return newParameterWithDefault(name, NewTimespan(defaultValue))
}

func NewDynamicParameter(name string) Parameter {
	return newParameter(name, types.Dynamic)
}

func (p *parameter) String() string {
	if p.hasDefaultVal {
		return fmt.Sprintf("@%s:%s=%v", p.name, p.kustoType, p.defaultValue)
	}
	return fmt.Sprintf("@%s:%s", p.name, p.kustoType)
}

func (p *parameter) Name() string {
	return p.name
}

func (p *parameter) Type() types.Column {
	return p.kustoType
}

func (p *parameter) HasDefaultValue() bool {
	return p.hasDefaultVal
}

func (p *parameter) DefaultValue() Value {
	return p.defaultValue
}
