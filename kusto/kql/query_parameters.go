package kql

import (
	"fmt"
	"strings"
	"sync"
)

type ParamVals struct {
	paramType string
	value     string
}

type StatementQueryParameters struct {
	parameters map[string]ParamVals
}

var buildPool = sync.Pool{
	New: func() interface{} {
		return new(strings.Builder)
	},
}

func NewStatementQueryParameters() *StatementQueryParameters {
	return &StatementQueryParameters{parameters: make(map[string]ParamVals)}
}
func (q *StatementQueryParameters) AddLiteral(key string, paramType string, value stringConstant) *StatementQueryParameters {
	return q.addBase(key, paramType, value)
}
func (q *StatementQueryParameters) addBase(key string, paramType string, value fmt.Stringer) *StatementQueryParameters {
	q.parameters[key] = ParamVals{paramType, value.String()}
	return q
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

	for key, paramVals := range q.parameters {
		build.WriteString(key)
		build.WriteString(":")
		build.WriteString(paramVals.paramType)
		build.WriteString(", ")
	}
	build.WriteString(closeStmt)
	var cleaner = build.String()
	var decStr = cleaner[:len(cleaner)-len(closeStmt)-2] + cleaner[len(cleaner)-len(closeStmt):]
	return decStr
}
func (q *StatementQueryParameters) ToParameterCollection() map[string]string {
	var parameters = make(map[string]string)
	for key, paramVals := range q.parameters {
		parameters[key] = paramVals.value
	}
	return parameters
}
