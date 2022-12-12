package kql

import "fmt"

type Identifier struct {
	name             string
	wrappingFunction string
}

func newIdentifier(name string) Identifier {
	return Identifier{name: name}
}

func NewDatabase(database string) Identifier {
	return Identifier{wrappingFunction: "database", name: database}
}

func NewTable(table string) Identifier {
	return Identifier{wrappingFunction: "table", name: table}
}

func NewColumn(column string) Identifier {
	return Identifier{name: column}
}

func NewFunction(function string) Identifier {
	return Identifier{name: function}
}

func (i Identifier) String() string {
	return i.NormalizeName()
}

// NormalizeName normalizes a string in order to be used safely in the engine - given "query" will produce [\"query\"].
func (i Identifier) NormalizeName() string {
	if i.name == "" {
		return i.name
	}

	if i.wrappingFunction != "" {
		return fmt.Sprintf("%s(%s)", i.wrappingFunction, QuoteString(i.name, false))
	}

	if !RequiresQuoting(i.name) {
		return i.name
	}

	return "[" + QuoteString(i.name, false) + "]"
}
