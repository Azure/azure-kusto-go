package kql

type Query interface {
	Query() string
}

type query struct {
	query string
}

func (q *query) Query() string {
	return q.query
}
