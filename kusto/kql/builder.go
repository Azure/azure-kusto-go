package kql

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"strings"
	"time"
)

type Builder interface {
	AddBool(value bool) Builder
	AddDateTime(value time.Time) Builder
	AddDynamic(value string) Builder
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
	SupportsParameters() bool
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

// String implements fmt.Stringer.
func (b *statementBuilder) String() string {
	return b.builder.String()
}

func NewStatementBuilder(value stringConstant) Builder {
	return (&statementBuilder{
		builder: strings.Builder{},
	}).AddLiteral(value)
}

func (b *statementBuilder) AddLiteral(value stringConstant) Builder {
	return b.addBase(value)
}

func (b *statementBuilder) addBase(value fmt.Stringer) Builder {
	b.builder.WriteString(value.String())
	return b
}

func (b *statementBuilder) GetParameters() (map[string]string, error) {
	return nil, errors.New("this option does not support Parameters")
}
func (b *statementBuilder) SupportsParameters() bool {
	return false
}
