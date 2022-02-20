package unmarshal

import (
	"testing"
	"time"

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
		{types.Bool, nil, value.Bool{false, false}},
		{types.Bool, true, value.Bool{true, true}},
		{types.DateTime, nil, value.DateTime{time.Time{}, false}},
		{types.DateTime, "2019-08-27T04:14:55.302919Z", value.DateTime{dt, true}},
		{types.Decimal, nil, value.Decimal{"", false}},
		{types.Decimal, "3.2", value.Decimal{"3.2", true}},
		{types.Dynamic, nil, value.Dynamic{nil, false}},
		{types.Dynamic, `{"key":"value"}`, value.Dynamic{[]byte(`{"key":"value"}`), true}},
		{types.GUID, nil, value.GUID{uuid.UUID{}, false}},
		{types.GUID, aUUID.String(), value.GUID{aUUID, true}},
		{types.Int, nil, value.Int{0, false}},
		{types.Int, 1, value.Int{1, true}},
		{types.Long, nil, value.Long{0, false}},
		{types.Long, 1, value.Long{1, true}},
		{types.Real, nil, value.Real{0.0, false}},
		{types.Real, 1.2, value.Real{1.2, true}},
		{types.String, nil, value.String{"", false}},
		{types.String, "John Doak", value.String{"John Doak", true}},
		{types.Timespan, nil, value.Timespan{0, false}},
		{types.Timespan, "00:00:00.099", value.Timespan{99 * time.Millisecond, true}},
	}

	for _, test := range tests {
		rows, err := Rows(table.Columns{table.Column{Name: "store", Type: test.columnType}}, [][]interface{}{{test.value}})
		if err != nil {
			t.Errorf("TestUnmarshalRows(%v): got err == %s, want err == nil", test.value, err)
			continue
		}

		if diff := pretty.Compare(test.result, rows[0][0]); diff != "" {
			t.Errorf("TestUnmarshalRows(%v): -want/+got:\n%s", test.value, diff)
		}
	}
}
