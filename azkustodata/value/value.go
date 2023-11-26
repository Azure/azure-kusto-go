/*
Package value holds Kusto data value representations. All types provide a Kusto that
stores the native value and Valid which indicates if the value was set or was null.

# Kusto Value

A value.Kusto can hold types that represent Kusto Scalar types that define column data.
We represent that with an interface:

	type Kusto interface

This interface can hold the following values:

	value.Bool
	value.Int
	value.Long
	value.Real
	value.Decimal
	value.String
	value.Dynamic
	value.DateTime
	value.Timespan

Each type defined above has at minimum two fields:

	.Value - The type specific value
	.Valid - True if the value was non-null in the Kusto table

Each provides at minimum the following two methods:

	.String() - Returns the string representation of the value.
	.Unmarshal() - Unmarshals the value into a standard Go type.

The Unmarshal() is for internal use, it should not be needed by an end user. Use .Value or table.Row.ToStruct() instead.
*/
package value

import (
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"reflect"
)

// Kusto represents a Kusto value.
type Kusto interface {
	fmt.Stringer
	isKustoVal()
	Convert(v reflect.Value) error
	GetValue() interface{}
	GetType() types.Column
	Unmarshal(interface{}) error
}

func Default(t types.Column) Kusto {
	switch t {
	case types.Bool:
		return NewNullBool()
	case types.Int:
		return NewNullInt()
	case types.Long:
		return NewNullLong()
	case types.Real:
		return NewNullReal()
	case types.Decimal:
		return NewNullDecimal()
	case types.String:
		return NewNullString()
	case types.Dynamic:
		return NewNullDynamic()
	case types.DateTime:
		return NewNullDateTime()
	case types.Timespan:
		return NewNullTimespan()
	case types.GUID:
		return NewNullGUID()
	default:
		return nil
	}
}

// Values is a list of Kusto values, usually an ordered row.
type Values []Kusto
