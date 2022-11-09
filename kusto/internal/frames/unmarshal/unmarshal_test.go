package unmarshal

import (
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/Azure/azure-kusto-go/kusto/data/value"

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
		{types.Bool, nil, value.Bool{}},
		{types.Bool, true, value.Bool{Value: true, Valid: true}},
		{types.DateTime, nil, value.DateTime{}},
		{types.DateTime, "2019-08-27T04:14:55.302919Z", value.DateTime{Value: dt, Valid: true}},
		{types.Decimal, nil, value.Decimal{}},
		{types.Decimal, "3.2", value.Decimal{Value: "3.2", Valid: true}},
		{types.Dynamic, nil, value.Dynamic{}},
		{types.Dynamic, `{"key":"value"}`, value.Dynamic{Value: []byte(`{"key":"value"}`), Valid: true}},
		{types.GUID, nil, value.GUID{}},
		{types.GUID, aUUID.String(), value.GUID{Value: aUUID, Valid: true}},
		{types.Int, nil, value.Int{}},
		{types.Int, 1, value.Int{Value: 1, Valid: true}},
		{types.Long, nil, value.Long{}},
		{types.Long, 1, value.Long{Value: 1, Valid: true}},
		{types.Real, nil, value.Real{}},
		{types.Real, 1.2, value.Real{Value: 1.2, Valid: true}},
		{types.String, nil, value.String{}},
		{types.String, "John Doak", value.String{Value: "John Doak", Valid: true}},
		{types.Timespan, nil, value.Timespan{}},
		{types.Timespan, "00:00:00.099", value.Timespan{Value: 99 * time.Millisecond, Valid: true}},
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
