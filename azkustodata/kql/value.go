package kql

import (
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"time"
)

func QuoteValue(v value.Kusto) string {
	val := v.GetValue()
	t := v.GetType()
	if val == nil {
		return fmt.Sprintf("%v(null)", t)
	}

	switch t {
	case types.String:
		return QuoteString(v.String(), false)
	case types.DateTime:
		val = FormatDatetime(val.(time.Time))
	case types.Timespan:
		val = FormatTimespan(val.(time.Duration))
	case types.Dynamic:
		val = string(val.([]byte))
	}

	return fmt.Sprintf("%v(%v)", t, val)
}
