package kql

import (
	"fmt"
	"strings"
)

const (
	declare   = "declare query_parameters("
	closeStmt = ");\n"
)

// stringConstant is an internal type that cannot be created outside the package.  The only two ways to build
// a stringConstant is to pass a string constant or use a local function to build the stringConstant.
// This allows us to enforce the use of constants or strings built with injection protection.
type stringConstant string

// String implements fmt.Stringer.
func (s stringConstant) String() string {
	return string(s)
}

type builderBase interface {
	AddLiteral(value stringConstant) Builder
	AddValue(value Value) Builder
	AddIdentifier(value Identifier) Builder
	AddHiddenString(value string) Builder
}

type Builder interface {
	builderBase
	ToParameterBuilder() ParameterBuilder
	Build() ReadyQuery
}

type ParameterBuilder interface {
	builderBase
	AddParameter(value Parameter) ParameterBuilder
	BuildPrepared() PreparedQuery
	BuildWithValues(values map[string]Value) (ReadyQuery, error)
}

type kqlBuilder struct {
	builder    strings.Builder
	parameters map[string]Parameter
}

func NewBuilder() Builder {
	return &kqlBuilder{
		builder:    strings.Builder{},
		parameters: make(map[string]Parameter),
	}
}

func NewBuilderWithLiteral(value stringConstant) Builder {
	return NewBuilder().AddLiteral(value)
}

func (k *kqlBuilder) addBase(value fmt.Stringer) Builder {
	k.builder.WriteString(value.String())
	return k
}

func (k *kqlBuilder) addString(value string) Builder {
	k.builder.WriteString(value)
	return k
}

func (k *kqlBuilder) AddLiteral(value stringConstant) Builder {
	return k.addBase(value)
}

func (k *kqlBuilder) AddValue(value Value) Builder {
	return k.addBase(value)
}

func (k *kqlBuilder) AddIdentifier(value Identifier) Builder {
	return k.addBase(value)
}

func (k *kqlBuilder) AddHiddenString(value string) Builder {
	return k.addString(QuoteString(value, true))
}

func (k *kqlBuilder) AddParameter(value Parameter) ParameterBuilder {
	k.parameters[value.Name()] = value
	return k
}

func (k *kqlBuilder) Build() ReadyQuery {
	if len(k.parameters) != 0 {
		panic("got to Build() with parameters, should be unreachable")
	}

	return &query{
		query: k.builder.String(),
	}
}

func (k *kqlBuilder) ToParameterBuilder() ParameterBuilder {
	return k
}

func (k *kqlBuilder) BuildPrepared() PreparedQuery {
	if len(k.parameters) == 0 {
		panic("got to BuildWithValues() with no parameters, should be unreachable")
	}

	preStmt := strings.Builder{}

	preStmt.WriteString(declare)

	counter := 0

	for _, p := range k.parameters {
		preStmt.WriteString(p.Name())
		preStmt.WriteString(":")
		preStmt.WriteString(string(p.Type()))
		if p.HasDefaultValue() {
			preStmt.WriteString("=")
			preStmt.WriteString(p.DefaultValue().String())
		}
		if counter < len(k.parameters)-1 {
			preStmt.WriteString(",")
		}

		counter++
	}

	preStmt.WriteString(closeStmt)
	preStmt.WriteString(k.builder.String())

	return &query{
		query:      preStmt.String(),
		parameters: k.parameters,
	}
}

func (k *kqlBuilder) BuildWithValues(values map[string]Value) (ReadyQuery, error) {
	return k.BuildPrepared().ToReadyQuery(values)
}
