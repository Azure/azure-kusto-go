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

type pointerValue[T any] struct {
	value *T
}

func newPointerValue[T any](v *T) pointerValue[T] {
	return pointerValue[T]{value: v}
}

func (p *pointerValue[T]) String() string {
	if p.value == nil {
		return ""
	}
	return fmt.Sprintf("%v", *p.value)
}

func (p *pointerValue[T]) GetValue() interface{} {
	if p.value == nil {
		return nil
	}
	return p.value
}

func (p *pointerValue[T]) Value() *T {
	return p.value
}

func (p *pointerValue[T]) Unmarshal(i interface{}) error {
	if i == nil {
		p.value = nil
		return nil
	}

	v, ok := i.(T)
	if !ok {
		return fmt.Errorf("column with type '%T' had value that was %T", p, i)
	}

	p.value = &v
	return nil
}

func TryConvert[T any](holder interface{}, p *pointerValue[T], v reflect.Value, kind *reflect.Kind) bool {
	t := v.Type()

	if kind != nil && t.Kind() == *kind {
		if p.value != nil {
			v.Set(reflect.ValueOf(*p.value))
		}
		return true
	}

	if t.ConvertibleTo(reflect.TypeOf(p.value)) {
		if p.value != nil {
			v.Set(reflect.ValueOf(p.value))
		}
		return true
	}

	newT := new(T)
	if t.ConvertibleTo(reflect.TypeOf(newT)) {
		if p.value != nil {
			b := newT
			*b = *p.value
			v.Set(reflect.ValueOf(b))
		}
		return true
	}

	if t.ConvertibleTo(reflect.TypeOf(holder)) {
		v.Set(reflect.ValueOf(holder))
		return true
	}

	if t.ConvertibleTo(reflect.TypeOf(&holder)) {
		v.Set(reflect.ValueOf(&holder))
		return true
	}

	return false
}

// Kusto represents a Kusto value.
type Kusto interface {
	fmt.Stringer
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
