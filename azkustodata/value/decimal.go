package value

import (
	"fmt"
	"github.com/shopspring/decimal"
	"math/big"
	"reflect"
)

// Decimal represents a Kusto decimal type.  Decimal implements Kusto.
type Decimal struct {
	Value decimal.NullDecimal
}

func (*Decimal) isKustoVal() {}

// String implements fmt.Stringer.
func (d *Decimal) String() string {
	if !d.Value.Valid {
		return ""
	}
	return d.Value.Decimal.String()
}

// ParseFloat provides builtin support for Go's *big.Float conversion where that type meets your needs.
func (d *Decimal) ParseFloat(base int, prec uint, mode big.RoundingMode) (f *big.Float, b int, err error) {
	if !d.Value.Valid {
		return nil, 0, fmt.Errorf("Decimal was not valid")
	}
	return big.ParseFloat(d.Value.Decimal.String(), base, prec, mode)
}

// Unmarshal unmarshals i into Decimal. i must be a string representing a decimal type or nil.
func (d *Decimal) Unmarshal(i interface{}) error {
	if i == nil {
		d.Value = decimal.NullDecimal{}
	}

	v, ok := i.(string)
	if !ok {
		return fmt.Errorf("Column with type 'decimal' had type %T", i)
	}

	dec, err := decimal.NewFromString(v)
	if err != nil {
		return fmt.Errorf("Column with type 'decimal' had value %s which did not parse: %s", v, err)
	}

	d.Value = decimal.NullDecimal{Decimal: dec, Valid: true}

	return nil
}

// Convert Decimal into reflect value.
func (d *Decimal) Convert(v reflect.Value) error {
	t := v.Type()
	switch {
	case t.Kind() == reflect.String:
		if d.Value.Valid {
			v.Set(reflect.ValueOf(d.String()))
		}
		return nil
	case t.ConvertibleTo(reflect.TypeOf(new(string))):
		if d.Value.Valid {
			i := d.Value.Decimal.String()
			v.Set(reflect.ValueOf(&i))
		}
		return nil
	case t.ConvertibleTo(reflect.TypeOf(decimal.NullDecimal{})):
		v.Set(reflect.ValueOf(d.Value))
		return nil
	case t.ConvertibleTo(reflect.TypeOf(&decimal.NullDecimal{})):
		v.Set(reflect.ValueOf(&d.Value))
		return nil
	case t.ConvertibleTo(reflect.TypeOf(decimal.Decimal{})):
		if d.Value.Valid {
			v.Set(reflect.ValueOf(d.Value.Decimal))
		}
		return nil
	case t.ConvertibleTo(reflect.TypeOf(&decimal.Decimal{})):
		if d.Value.Valid {
			v.Set(reflect.ValueOf(&d.Value.Decimal))
		}
		return nil
	case t.ConvertibleTo(reflect.TypeOf(Decimal{})):
		v.Set(reflect.ValueOf(*d))
		return nil
	case t.ConvertibleTo(reflect.TypeOf(&Decimal{})):
		v.Set(reflect.ValueOf(d))
		return nil
	}
	return fmt.Errorf("Column was type Kusto.Decimal, receiver had base Kind %s ", t.Kind())
}

// GetValue returns the value of the type.
func (d *Decimal) GetValue() interface{} {
	if !d.Value.Valid {
		return nil
	}
	return d.Value.Decimal
}
