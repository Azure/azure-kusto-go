package kusto

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/types"

	"github.com/google/uuid"
)

// structToKustoValues takes a *struct and encodes to types.KustoValues. At least one column must get set.
func structToKustoValues(cols Columns, p interface{}) (types.KustoValues, error) {
	t := reflect.TypeOf(p).Elem()
	v := reflect.ValueOf(p).Elem()

	m := newColumnMap(cols)

	row, err := defaultRow(cols)
	if err != nil {
		return nil, err
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if tag := field.Tag.Get("kusto"); strings.TrimSpace(tag) != "" {
			colData, ok := m[tag]
			if !ok {
				log.Printf("did not find tag %q in our columnMap", tag)
				continue
			}
			if err := fieldConvert(colData, v.Field(i), row); err != nil {
				return nil, err
			}
		} else {
			colData, ok := m[field.Name]
			if !ok {
				continue
			}

			if err := fieldConvert(colData, v.Field(i), row); err != nil {
				return nil, err
			}
		}
	}

	return row, nil
}

// fieldConvert will attempt to take the value held in v and convert it to the appropriate types.KustoValue
// that is described in colData in the correct location in row.
func fieldConvert(colData columnData, v reflect.Value, row types.KustoValues) error {
	switch colData.column.Type {
	case CTBool:
		c, err := convertBool(v)
		if err != nil {
			return err
		}
		row[colData.position] = c
	case CTDateTime:
		c, err := convertDateTime(v)
		if err != nil {
			return err
		}
		row[colData.position] = c
	case CTDynamic:
		c, err := convertDynamic(v)
		if err != nil {
			return err
		}
		row[colData.position] = c
	case CTGUID:
		c, err := convertGUID(v)
		if err != nil {
			return err
		}
		row[colData.position] = c
	case CTInt:
		c, err := convertInt(v)
		if err != nil {
			return err
		}
		row[colData.position] = c
	case CTLong:
		c, err := convertLong(v)
		if err != nil {
			return err
		}
		row[colData.position] = c
	case CTReal:
		c, err := convertReal(v)
		if err != nil {
			return err
		}
		row[colData.position] = c
	case CTString:
		c, err := convertString(v)
		if err != nil {
			return err
		}
		row[colData.position] = c
	case CTTimespan:
		c, err := convertTimespan(v)
		if err != nil {
			return err
		}
		row[colData.position] = c
	case CTDecimal:
		c, err := convertDecimal(v)
		if err != nil {
			return err
		}
		row[colData.position] = c
	default:
		return fmt.Errorf("column[%d] was for a column type that we don't understand(%s)", colData.position, colData.column.Type)
	}
	return nil
}

// defaultRow creates a complete row of KustoValues set to types outlined with cols. Useful for having
// default values for fields that are not set.
func defaultRow(cols Columns) (types.KustoValues, error) {
	var row = make(types.KustoValues, len(cols))
	for i, col := range cols {
		switch col.Type {
		case CTBool:
			row[i] = types.Bool{}
		case CTDateTime:
			row[i] = types.DateTime{}
		case CTDynamic:
			row[i] = types.Dynamic{}
		case CTGUID:
			row[i] = types.GUID{}
		case CTInt:
			row[i] = types.Int{}
		case CTLong:
			row[i] = types.Long{}
		case CTReal:
			row[i] = types.Real{}
		case CTString:
			row[i] = types.String{}
		case CTTimespan:
			row[i] = types.Timespan{}
		case CTDecimal:
			row[i] = types.Decimal{}
		default:
			return nil, fmt.Errorf("column[%d] was for a column type that we don't understand(%s)", i, col.Type)
		}
	}
	return row, nil
}

func colToValueCheck(cols Columns, values types.KustoValues) error {
	if len(cols) != len(values) {
		return fmt.Errorf("the length of columns(%d) is not the same as the length of the row(%d)", len(cols), len(values))
	}

	for i, v := range values {
		col := cols[i]

		switch col.Type {
		case CTBool:
			if reflect.TypeOf(v) != reflect.TypeOf(types.Bool{}) {
				return fmt.Errorf("value[%d] was expected to be of a types.Bool, was %T", i, v)
			}
		case CTDateTime:
			if reflect.TypeOf(v) != reflect.TypeOf(types.DateTime{}) {
				return fmt.Errorf("value[%d] was expected to be of a types.DateTime, was %T", i, v)
			}
		case CTDynamic:
			if reflect.TypeOf(v) != reflect.TypeOf(types.Dynamic{}) {
				return fmt.Errorf("value[%d] was expected to be of a types.Dynamic, was %T", i, v)
			}
		case CTGUID:
			if reflect.TypeOf(v) != reflect.TypeOf(types.GUID{}) {
				return fmt.Errorf("value[%d] was expected to be of a types.GUID, was %T", i, v)
			}
		case CTInt:
			if reflect.TypeOf(v) != reflect.TypeOf(types.Int{}) {
				return fmt.Errorf("value[%d] was expected to be of a types.Int, was %T", i, v)
			}
		case CTLong:
			if reflect.TypeOf(v) != reflect.TypeOf(types.Long{}) {
				return fmt.Errorf("value[%d] was expected to be of a types.Long, was %T", i, v)
			}
		case CTReal:
			if reflect.TypeOf(v) != reflect.TypeOf(types.Real{}) {
				return fmt.Errorf("value[%d] was expected to be of a types.Real, was %T", i, v)
			}
		case CTString:
			if reflect.TypeOf(v) != reflect.TypeOf(types.String{}) {
				return fmt.Errorf("value[%d] was expected to be of a types.String, was %T", i, v)
			}
		case CTTimespan:
			if reflect.TypeOf(v) != reflect.TypeOf(types.Timespan{}) {
				return fmt.Errorf("value[%d] was expected to be of a types.Timespan, was %T", i, v)
			}
		case CTDecimal:
			if reflect.TypeOf(v) != reflect.TypeOf(types.Decimal{}) {
				return fmt.Errorf("value[%d] was expected to be of a types.Decimal, was %T", i, v)
			}
		default:
			return fmt.Errorf("value[%d] was for a column type that MockRow doesn't understand(%s)", i, col.Type)
		}
	}
	return nil
}

func convertBool(v reflect.Value) (types.Bool, error) {
	t := v.Type()

	// If it is a pointer, dereference it.
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	// Was a types.Bool{}, so return it.
	if t == reflect.TypeOf(types.Bool{}) {
		return v.Interface().(types.Bool), nil
	}

	// Was a Bool, so return its value.
	if t == reflect.TypeOf(true) {
		return types.Bool{Value: v.Interface().(bool), Valid: true}, nil
	}

	return types.Bool{}, fmt.Errorf("value was expected to be either a types.Bool, *bool or bool, was %T", v.Interface())
}

func convertDateTime(v reflect.Value) (types.DateTime, error) {
	t := v.Type()

	// If it is a pointer, dereference it.
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	// Was a types.DateTime{}, so return it.
	if t == reflect.TypeOf(types.DateTime{}) {
		return v.Interface().(types.DateTime), nil
	}

	// Was a time.Time, so return its value.
	if t == reflect.TypeOf(time.Time{}) {
		return types.DateTime{Value: v.Interface().(time.Time), Valid: true}, nil
	}

	return types.DateTime{}, fmt.Errorf("value was expected to be either a types.DateTime, *time.Time or time.Time, was %T", v.Interface())
}

func convertTimespan(v reflect.Value) (types.Timespan, error) {
	t := v.Type()

	// If it is a pointer, dereference it.
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	// Was a types.Timespan{}, so return it.
	if t == reflect.TypeOf(types.Timespan{}) {
		return v.Interface().(types.Timespan), nil
	}

	// Was a time.Duration, so return its value.
	if t == reflect.TypeOf(time.Second) {
		return types.Timespan{Value: v.Interface().(time.Duration), Valid: true}, nil
	}

	return types.Timespan{}, fmt.Errorf("value was expected to be either a types.Timespan, *time.Duration or time.Duration, was %T", v.Interface())
}

func convertDynamic(v reflect.Value) (types.Dynamic, error) {
	t := v.Type()
	pointer := false

	// If it is a pointer, dereference it.
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
		pointer = true
	}

	// Was a types.Dynamic{}, so return it.
	if t == reflect.TypeOf(types.Dynamic{}) {
		return v.Interface().(types.Dynamic), nil
	}

	// Was a map[string]interface{}, so convert and return it.
	if t == reflect.TypeOf(map[string]interface{}{}) {
		b, err := json.Marshal(v.Interface())
		if err != nil {
			return types.Dynamic{}, fmt.Errorf("the map[string]interface{} representing a types.Dynamic could not be JSON encoded: %s", err)
		}
		return types.Dynamic{Value: string(b), Valid: true}, nil
	}

	if t.Kind() == reflect.Struct {
		b, err := json.Marshal(v.Interface())
		if err != nil {
			if pointer {
				return types.Dynamic{}, fmt.Errorf("the type *%T used in a types.Dynamic could not be JSON encoded: %s", v.Interface(), err)
			}
			return types.Dynamic{}, fmt.Errorf("the type %T used in a types.Dynamic could not be JSON encoded: %s", v.Interface(), err)
		}
		return types.Dynamic{Value: string(b), Valid: true}, nil
	}

	return types.Dynamic{}, fmt.Errorf("value was expected to be either a types.Dynamic, *map[string]interface{}, map[string]interface{}, *struct, or struct, was %T", v.Interface())
}

func convertGUID(v reflect.Value) (types.GUID, error) {
	t := v.Type()

	// If it is a pointer, dereference it.
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	// Was a types.GUID{}, so return it.
	if t == reflect.TypeOf(types.GUID{}) {
		return v.Interface().(types.GUID), nil
	}

	// Was a uuid.UUID, so return its value.
	if t == reflect.TypeOf(uuid.UUID{}) {
		return types.GUID{Value: v.Interface().(uuid.UUID), Valid: true}, nil
	}

	return types.GUID{}, fmt.Errorf("value was expected to be either a types.BUID, *uuid.UUID or uuid.UUID, was %T", v.Interface())
}

func convertInt(v reflect.Value) (types.Int, error) {
	t := v.Type()

	// If it is a pointer, dereference it.
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	// Was a types.Int{}, so return it.
	if t == reflect.TypeOf(types.Int{}) {
		return v.Interface().(types.Int), nil
	}

	// Was a int32, so return its value.
	if t == reflect.TypeOf(int32(1)) {
		return types.Int{Value: v.Interface().(int32), Valid: true}, nil
	}

	return types.Int{}, fmt.Errorf("value was expected to be either a types.Int, *int32 or int32, was %T", v.Interface())
}

func convertLong(v reflect.Value) (types.Long, error) {
	t := v.Type()

	// If it is a pointer, dereference it.
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	// Was a types.Long{}, so return it.
	if t == reflect.TypeOf(types.Long{}) {
		return v.Interface().(types.Long), nil
	}

	// Was a int64, so return its value.
	if t == reflect.TypeOf(int64(1)) {
		return types.Long{Value: v.Interface().(int64), Valid: true}, nil
	}

	return types.Long{}, fmt.Errorf("value was expected to be either a types.Long, *int64 or int64, was %T", v.Interface())
}

func convertReal(v reflect.Value) (types.Real, error) {
	t := v.Type()

	// If it is a pointer, dereference it.
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	// Was a types.Real{}, so return it.
	if t == reflect.TypeOf(types.Real{}) {
		return v.Interface().(types.Real), nil
	}

	// Was a float64, so return its value.
	if t == reflect.TypeOf(float64(1.0)) {
		return types.Real{Value: v.Interface().(float64), Valid: true}, nil
	}

	return types.Real{}, fmt.Errorf("value was expected to be either a types.Real, *float64 or float64, was %T", v.Interface())
}

func convertString(v reflect.Value) (types.String, error) {
	t := v.Type()

	// If it is a pointer, dereference it.
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	// Was a types.String{}, so return it.
	if t == reflect.TypeOf(types.String{}) {
		return v.Interface().(types.String), nil
	}

	// Was a string, so return its value.
	if t == reflect.TypeOf("") {
		return types.String{Value: v.Interface().(string), Valid: true}, nil
	}

	return types.String{}, fmt.Errorf("value was expected to be either a types.String, *string or string, was %T", v.Interface())
}

func convertDecimal(v reflect.Value) (types.Decimal, error) {
	t := v.Type()

	// If it is a pointer, dereference it.
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	// Was a types.Decimal{}, so return it.
	if t == reflect.TypeOf(types.Decimal{}) {
		return v.Interface().(types.Decimal), nil
	}

	// Was a string, so return its value.
	if t == reflect.TypeOf("") {
		return types.Decimal{Value: v.Interface().(string), Valid: true}, nil
	}

	return types.Decimal{}, fmt.Errorf("value was expected to be either a types.Decimal, *string or string, was %T", v.Interface())
}
