package kql

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"strings"
	"time"
)

// stringConstant is an internal type that cannot be created outside the package.  The only two ways to build
// a stringConstant is to pass a string constant or use a local function to build the stringConstant.
// This allows us to enforce the use of constants or strings built with injection protection.
type stringConstant string

// String implements fmt.Stringer.
func (s stringConstant) String() string {
	return string(s)
}

type Builder struct {
	builder strings.Builder
}

func New(value stringConstant) *Builder {
	return (&Builder{
		builder: strings.Builder{},
	}).AddLiteral(value)
}

func FromBuilder(builder *Builder) *Builder {
	return New(stringConstant(builder.String()))
}

// String implements fmt.Stringer.
func (b *Builder) String() string {
	return b.builder.String()
}
func (b *Builder) addBase(value fmt.Stringer) *Builder {
	b.builder.WriteString(value.String())
	return b
}

// AddUnsafe enables unsafe actions on a Builder - adds a string as is, no validation checking or escaping.
// This turns off safety features that could allow a service client to compromise your data store.
// USE AT YOUR OWN RISK!
func (b *Builder) AddUnsafe(value string) *Builder {
	b.builder.WriteString(value)
	return b
}

func (b *Builder) AddLiteral(value stringConstant) *Builder {
	return b.addBase(value)
}

func (b *Builder) AddBool(v bool) *Builder {
	return b.addBase(value.NewBool(v))
}

func (b *Builder) AddDateTime(v time.Time) *Builder {
	return b.addBase(&value.DateTime{Value: v, Valid: true})
}

func (b *Builder) AddDynamic(v interface{}) *Builder {
	marshal, err := json.Marshal(v)
	if err != nil {
		return b.addBase(&value.Dynamic{Value: nil, Valid: false})
	}

	return b.addBase(&value.Dynamic{Value: marshal, Valid: true})
}

func (b *Builder) AddGUID(v uuid.UUID) *Builder {
	return b.addBase(&value.GUID{Value: v, Valid: true})
}

func (b *Builder) AddInt(v int32) *Builder {
	return b.addBase(newValue(v, types.Int))
}

func (b *Builder) AddLong(v int64) *Builder {
	return b.addBase(newValue(v, types.Long))
}

func (b *Builder) AddReal(v float64) *Builder {
	return b.addBase(newValue(v, types.Real))
}

func (b *Builder) AddString(v string) *Builder {
	return b.addBase(newValue(v, types.String))
}

func (b *Builder) AddTimespan(v time.Duration) *Builder {
	return b.addBase(newValue(v, types.Timespan))
}

func (b *Builder) AddDecimal(v decimal.Decimal) *Builder {
	return b.addBase(newValue(v, types.Decimal))
}

func (b *Builder) GetParameters() (map[string]string, error) {
	return nil, errors.New("this option does not support Parameters")
}
func (b *Builder) SupportsInlineParameters() bool {
	return false
}

// Reset resets the stringBuilder
func (b *Builder) Reset() {
	b.builder.Reset()
}
