package value

import (
	"fmt"
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
	kind := reflect.Bool
	if !TryConvert[bool](bo, &bo.pointerValue, v, &kind) {
		return fmt.Errorf("column with type 'bool' had value that was %T", v)
	}

	return nil
}

// GetType returns the type of the value.
func (bo *Bool) GetType() types.Column {
	return types.Bool
}
