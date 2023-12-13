package v2

import (
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewFullTable_WithValidDataTable(t *testing.T) {
	t.Parallel()
	dt := &DataTable{
		TableId:   1,
		TableName: "AllDataTypes",
		TableKind: "PrimaryResult",
		Columns: []FrameColumn{
			{ColumnName: "vnum", ColumnType: "int"},
			{ColumnName: "vdec", ColumnType: "decimal"},
			{ColumnName: "vdate", ColumnType: "datetime"},
			{ColumnName: "vspan", ColumnType: "timespan"},
			{ColumnName: "vobj", ColumnType: "dynamic"},
			{ColumnName: "vb", ColumnType: "bool"},
			{ColumnName: "vreal", ColumnType: "real"},
			{ColumnName: "vstr", ColumnType: "string"},
			{ColumnName: "vlong", ColumnType: "long"},
			{ColumnName: "vguid", ColumnType: "guid"},
		},
		Rows: [][]interface{}{{1, "1.1", "2019-03-02T05:40:02Z", "13:14:20", "{\"a\": 3}", true, 1.1, "test", 1, "00000000-0000-0000-0000-000000000000"}},
	}

	table, err := NewDataTable(nil, dt)

	assert.NoError(t, err)
	assert.Equal(t, dt.TableId, 1)
	assert.Equal(t, dt.TableName, "AllDataTypes")
	assert.Equal(t, dt.TableKind, "PrimaryResult")

	rows, errs := table.GetAllRows()
	assert.Nil(t, errs)

	assert.Lenf(t, rows, 1, "expected 1 row, got %d", len(rows))
	row := rows[0]
	assert.Equal(t, int32(1), row.ValueByColumn(table.ColumnByName("vnum")).GetValue().(int32))
	assert.Equal(t, decimal.RequireFromString("1.1"), row.ValueByColumn(table.ColumnByName("vdec")).GetValue().(decimal.Decimal))
	assert.Equal(t, time.Date(2019, 3, 2, 5, 40, 2, 0, time.UTC), row.ValueByColumn(table.ColumnByName("vdate")).GetValue().(time.Time))
	duration, err := time.ParseDuration("13h14m20s")
	assert.NoError(t, err)
	assert.Equal(t, duration, row.ValueByColumn(table.ColumnByName("vspan")).GetValue().(time.Duration))
	assert.Equal(t, []byte("{\"a\": 3}"), row.ValueByColumn(table.ColumnByName("vobj")).GetValue().([]byte))
	assert.Equal(t, true, row.ValueByColumn(table.ColumnByName("vb")).GetValue().(bool))
	assert.Equal(t, 1.1, row.ValueByColumn(table.ColumnByName("vreal")).GetValue().(float64))
	assert.Equal(t, "test", row.ValueByColumn(table.ColumnByName("vstr")).GetValue().(string))
	assert.Equal(t, int64(1), row.ValueByColumn(table.ColumnByName("vlong")).GetValue().(int64))
	assert.Equal(t, uuid.UUID{}, row.ValueByColumn(table.ColumnByName("vguid")).GetValue().(uuid.UUID))
}

func TestNewFullTable_WithInvalidColumnType(t *testing.T) {
	t.Parallel()
	dt := &DataTable{
		TableId:   1,
		TableName: "TestTable",
		Columns:   []FrameColumn{{ColumnName: "TestColumn", ColumnType: "invalid"}},
		Rows:      [][]interface{}{{"TestValue"}},
	}

	_, err := NewDataTable(nil, dt)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not valid")
}

func TestNewFullTable_WithInvalidRowValue(t *testing.T) {
	t.Parallel()
	dt := &DataTable{
		TableId:   1,
		TableName: "TestTable",
		Columns:   []FrameColumn{{ColumnName: "TestColumn", ColumnType: "int"}},
		Rows:      [][]interface{}{{"TestValue"}},
	}

	_, err := NewDataTable(nil, dt)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unable to unmarshal")
}
