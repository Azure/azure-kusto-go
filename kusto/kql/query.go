package kql

import (
	"fmt"
)

type queryBase interface {
	Query() string
	Parameters() map[string]Parameter
}

type PreparedQuery interface {
	queryBase
	ToReadyQuery(values map[string]Value) (ReadyQuery, error)
}

type ReadyQuery interface {
	queryBase
	Values() map[string]Value
}

type query struct {
	query      string
	parameters map[string]Parameter
	values     map[string]Value
}

func (q *query) ToReadyQuery(values map[string]Value) (ReadyQuery, error) {
	q.values = values

	for k, p := range q.parameters {

		if v, ok := q.values[k]; ok {
			if p.Type() != v.Type() {
				return nil, fmt.Errorf("error: parameter type and value type do not match")
			}
		} else {
			if !p.HasDefaultValue() {
				return nil, fmt.Errorf("error: parameter %s does not have a value", k)
			}
		}
	}

	return q, nil
}

func (q *query) Query() string {
	return q.query
}

func (q *query) Parameters() map[string]Parameter {
	return q.parameters
}

func (q *query) Values() map[string]Value {
	return q.values
}
