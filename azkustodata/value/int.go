package value

import (
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"math"
	"reflect"
)

// Int represents a Kusto boolean type. Bool implements Kusto.
type Int struct {
	pointerValue[int32]
}

func NewInt(v int32) *Int {
	return &Int{newPointerValue[int32](&v)}
}

func NewNullInt() *Int {
	return &Int{newPointerValue[int32](nil)}
}

// Convert Int into reflect value.
func (in *Int) Convert(v reflect.Value) error {
	kind := reflect.Int32
	if TryConvert[int32](in, &in.pointerValue, v, &kind) {
		return nil
	}

	if v.Type().Kind() == reflect.Int {
		if in.value != nil {
			v.SetInt(int64(*in.value))
		}
		return nil
	}

	return fmt.Errorf("column with type 'int' had value that was %T", v)
}

// GetType returns the type of the value.
func (in *Int) GetType() types.Column {
	return types.Int
}

func (in *Int) Unmarshal(i interface{}) error {
	if i == nil {
		in.value = nil
		return nil
	}

	var myInt int64

	switch v := i.(type) {
	case json.Number:
		var err error
		myInt, err = v.Int64()
		if err != nil {
			return fmt.Errorf("Column with type 'int' had value json.Number that had error on .Int64(): %s", err)
		}
	case float64:
		if v != math.Trunc(v) {
			return fmt.Errorf("Column with type 'int' had value float64(%v) that did not represent a whole number", v)
		}
		myInt = int64(v)
	case int:
		myInt = int64(v)
	default:
		return fmt.Errorf("Column with type 'int' had value that was not a json.Number or int, was %T", i)
	}

	if myInt > math.MaxInt32 {
		return fmt.Errorf("Column with type 'int' had value that was greater than an int32 can hold, was %d", myInt)
	}
	val := int32(myInt)
	in.value = &val
	return nil
}
