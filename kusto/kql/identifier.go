package kql

import "fmt"

func (b *statementBuilder) AddDatabase(database string) Builder {
	return b.addBase(stringConstant(fmt.Sprintf("%s(%s)", "database", QuoteString(database, false))))
}

func (b *statementBuilder) AddTable(table string) Builder {
	return b.addBase(stringConstant(NormalizeName(table)))
}

func (b *statementBuilder) AddColumn(column string) Builder {
	return b.addBase(stringConstant(NormalizeName(column)))
}

func (b *statementBuilder) AddFunction(function string) Builder {
	return b.addBase(stringConstant(NormalizeName(function)))
}

// NormalizeName normalizes a string in order to be used safely in the engine - given "query" will produce [\"query\"].
func NormalizeName(name string) string {
	if name == "" {
		return name
	}

	if !RequiresQuoting(name) {
		return name
	}

	return "[" + QuoteString(name, false) + "]"
}
