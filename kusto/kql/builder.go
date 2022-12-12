package kql

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"strings"
	"time"
)

const (
	declare   = "declare query_parameters("
	closeStmt = ");\n"
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

	Build() Query
}

// stringConstant is an internal type that cannot be created outside the package.  The only two ways to build
// a stringConstant is to pass a string constant or use a local function to build the stringConstant.
// This allows us to enforce the use of constants or strings built with injection protection.
type stringConstant string

// String implements fmt.Stringer.
func (s stringConstant) String() string {
	return string(s)
}

type builder struct {
	builder strings.Builder
}

func NewBuilder(value stringConstant) Builder {
	return (&builder{
		builder: strings.Builder{},
	}).AddLiteral(value)
}

func (b *builder) AddLiteral(value stringConstant) Builder {
	return b.addBase(value)
}

func (b *builder) addBase(value fmt.Stringer) Builder {
	b.builder.WriteString(value.String())
	return b
}

func (b *builder) addString(value string) Builder {
	b.builder.WriteString(value)
	return b
}

func (b *builder) Build() Query {
	return &query{
		query: b.builder.String(),
	}
}
