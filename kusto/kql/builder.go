package kql

import (
	"errors"
	"fmt"
	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"strings"
	"time"
)

type Builder interface {
	AddBool(value bool) Builder
	AddDateTime(value time.Time) Builder
	AddDynamic(value interface{}) Builder
	AddGUID(value uuid.UUID) Builder
	AddInt(value int32) Builder
	AddLong(value int64) Builder
	AddReal(value float64) Builder
	AddString(value string) Builder
	AddTimespan(value time.Duration) Builder
	AddDecimal(value decimal.Decimal) Builder
	AddLiteral(value stringConstant) Builder

	AddDatabase(database string) Builder
	AddTable(table string) Builder
	AddColumn(column string) Builder
	AddFunction(function string) Builder

	String() string
	GetParameters() (map[string]string, error)
	SupportsInlineParameters() bool
}

// stringConstant is an internal type that cannot be created outside the package.  The only two ways to build
// a stringConstant is to pass a string constant or use a local function to build the stringConstant.
// This allows us to enforce the use of constants or strings built with injection protection.
type stringConstant string

// String implements fmt.Stringer.
func (s stringConstant) String() string {
	return string(s)
}

type statementBuilder struct {
	builder strings.Builder
}

func NewStatementBuilder(value stringConstant) Builder {
	return (&statementBuilder{
		builder: strings.Builder{},
	}).AddLiteral(value)
}

// String implements fmt.Stringer.
func (b *statementBuilder) String() string {
	return b.builder.String()
}
func (b *statementBuilder) addBase(value fmt.Stringer) Builder {
	b.builder.WriteString(value.String())
	return b
}

func (b *statementBuilder) AddLiteral(value stringConstant) Builder {
	return b.addBase(value)
}

func (b *statementBuilder) AddBool(value bool) Builder {
	return b.addBase(newValue(value, types.Bool))
}

func (b *statementBuilder) AddDateTime(value time.Time) Builder {
	return b.addBase(newValue(value, types.DateTime))
}

func (b *statementBuilder) AddDynamic(value interface{}) Builder {
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

func (b *statementBuilder) GetParameters() (map[string]string, error) {
	return nil, errors.New("this option does not support Parameters")
}
func (b *statementBuilder) SupportsInlineParameters() bool {
	return false
}
