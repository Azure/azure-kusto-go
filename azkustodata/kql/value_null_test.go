package kql

import (
	"testing"

	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"github.com/stretchr/testify/require"
)

func TestNullValueLiterals(t *testing.T) {
	tests := []struct {
		value value.Kusto
		want  string
	}{
		{value.NewNullBool(), "bool(null)"},
		{value.NewNullDateTime(), "datetime(null)"},
		{value.NewNullDecimal(), "decimal(null)"},
		{value.NewNullDynamic(), "dynamic(null)"},
		{value.NewNullGUID(), "guid(null)"},
		{value.NewNullInt(), "int(null)"},
		{value.NewNullLong(), "long(null)"},
		{value.NewNullReal(), "real(null)"},
		{value.NewNullTimespan(), "timespan(null)"},
	}
	for _, test := range tests {
		t.Run(test.want, func(t *testing.T) {
			params := NewParameters().AddValue("p", test.value).ToParameterCollection()
			require.Equal(t, map[string]string{"p": test.want}, params)
			require.Equal(t, test.want, New("").AddValue(test.value).String())
		})
	}
}
