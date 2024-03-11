package value

import (
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"reflect"
)

// Bool represents a Kusto boolean type. Bool implements Kusto.
type Bool struct {
	pointerValue[bool]
}

func NewBool(v bool) *Bool {
	return &Bool{newPointerValue[bool](&v)}
}

func NewNullBool() *Bool {
	return &Bool{newPointerValue[bool](nil)}
}

// Convert Bool into reflect value.
func (bo *Bool) Convert(v reflect.Value) error {
	return Convert[bool](*bo, &bo.pointerValue, v)
}

// GetType returns the type of the value.
func (bo *Bool) GetType() types.Column {
	return types.Bool
}
