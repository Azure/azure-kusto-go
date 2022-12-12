package kql

import "fmt"

type Identifier struct {
	name             string
	wrappingFunction string
}

func (b *builder) AddDatabase(database string) Builder {
	return b.addBase(Identifier{wrappingFunction: "database", name: database})
}

func (b *builder) AddTable(table string) Builder {
	return b.addBase(Identifier{name: table})
}

func (b *builder) AddColumn(column string) Builder {
	return b.addBase(Identifier{name: column})
}

func (b *builder) AddFunction(function string) Builder {
	return b.addBase(Identifier{name: function})
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
