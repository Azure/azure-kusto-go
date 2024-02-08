package value

import (
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/shopspring/decimal"
	"math/big"
	"reflect"
)

// Decimal represents a Kusto decimal type.  Decimal implements Kusto.
type Decimal struct {
	pointerValue[decimal.Decimal]
}

func NewDecimal(v decimal.Decimal) *Decimal {
	return &Decimal{newPointerValue[decimal.Decimal](&v)}
}

func NewNullDecimal() *Decimal {
	return &Decimal{newPointerValue[decimal.Decimal](nil)}
}

func DecimalFromFloat(f float64) *Decimal {
	return NewDecimal(decimal.NewFromFloat(f))
}

func DecimalFromString(s string) *Decimal {
	dec, err := decimal.NewFromString(s)
	if err != nil {
		return NewNullDecimal()
	}
	return NewDecimal(dec)
}

func (*Decimal) isKustoVal() {}

// ParseFloat provides builtin support for Go's *big.Float conversion where that type meets your needs.
func (d *Decimal) ParseFloat(base int, prec uint, mode big.RoundingMode) (f *big.Float, b int, err error) {
	if d.value == nil {
		return nil, 0, fmt.Errorf("Decimal was not valid")
	}
	return big.ParseFloat(d.value.String(), base, prec, mode)
}

// Unmarshal unmarshals i into Decimal. i must be a string representing a decimal type or nil.
func (d *Decimal) Unmarshal(i interface{}) error {
	if i == nil {
		d.value = nil
		return nil
	}

	v, ok := i.(string)
	if !ok {
		return fmt.Errorf("column with type 'decimal' had type %T", i)
	}

	dec, err := decimal.NewFromString(v)
	if err != nil {
		return fmt.Errorf("Column with type 'decimal' had value %s which did not parse: %s", v, err)
	}

	d.value = &dec

	return nil
}

// Convert Decimal into reflect value.
func (d *Decimal) Convert(v reflect.Value) error {
	if !TryConvert[decimal.Decimal](d, &d.pointerValue, v, nil) {
		return fmt.Errorf("column with type 'decimal' had value that was %T", v)
	}

	return nil
}

// GetValue returns the value of the type.
func (d *Decimal) GetValue() interface{} {
	if d.value == nil {
		return nil
	}
	return d.value
}

// GetType returns the type of the value.
func (d *Decimal) GetType() types.Column {
	return types.Decimal
}
