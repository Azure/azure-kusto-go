package unmarshal

import (
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/table"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"

	"github.com/google/uuid"
	"github.com/kylelemons/godebug/pretty"
)

func TestUnmarshalRows(t *testing.T) {
	t.Parallel()

	aUUID := uuid.New()
	dt, err := time.Parse(time.RFC3339Nano, "2019-08-27T04:14:55.302919Z")
	if err != nil {
		panic(err)
	}

	tests := []struct {
		columnType types.Column
		value      interface{}
		result     value.Kusto
	}{
		{types.Bool, nil, value.NewNullBool()},
		{types.Bool, true, value.NewBool(true)},
		{types.DateTime, nil, value.NewNullDateTime()},
		{types.DateTime, "2019-08-27T04:14:55.302919Z", value.NewDateTime(dt)},
		{types.Decimal, nil, value.NewNullDecimal()},
		{types.Decimal, "3.2", value.DecimalFromString("3.2")},
		{types.Dynamic, nil, value.NewNullDynamic()},
		{types.Dynamic, `{"key":"value"}`, value.NewDynamic([]byte(`{"key":"value"}`))},
		{types.GUID, nil, value.NewNullGUID()},
		{types.GUID, aUUID.String(), value.NewGUID(aUUID)},
		{types.Int, nil, value.NewNullInt()},
		{types.Int, 1, value.NewInt(1)},
		{types.Long, nil, value.NewNullLong()},
		{types.Long, 1, value.NewLong(1)},
		{types.Real, nil, value.NewNullReal()},
		{types.Real, 1.2, value.NewReal(1.2)},
		{types.String, nil, value.NewNullString()},
		{types.String, "John Doak", value.NewString("John Doak")},
		{types.Timespan, nil, value.NewNullTimespan()},
		{types.Timespan, "00:00:00.099", value.NewTimespan(99 * time.Millisecond)},
	}

	for _, test := range tests {
		rows, _, err := Rows(table.Columns{table.Column{Name: "store", Type: test.columnType}}, []interface{}{[]interface{}{test.value}}, errors.OpUnknown)
		if err != nil {
			t.Errorf("TestUnmarshalRows(%v): got err == %s, want err == nil", test.value, err)
			continue
		}

		if diff := pretty.Compare(test.result, rows[0][0]); diff != "" {
			t.Errorf("TestUnmarshalRows(%v): -want/+got:\n%s", test.value, diff)
		}
	}
}
