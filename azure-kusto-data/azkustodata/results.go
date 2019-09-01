package azkustodata

type KustoTableColumn struct {
	ColumnName string `json:"ColumnName"`
	ColumnType string `json:"ColumnType"`
}

type KustoTable interface {
	GetColumns() []KustoTableColumn
	GetRows() [][]interface{}
}

type KustoResults interface {
	GetTables() ([]KustoTable, error)
	GetPrimaryResults() ([]KustoTable, error)
}

type KustoResponseTableV1 struct {
	FrameType     string             `json:"FrameType"`
	IsProgressive bool               `json:"IsProgressive,omitempty"`
	Version       string             `json:"Version,omitempty"`
	TableID       int                `json:"TableId,omitempty"`
	TableKind     string             `json:"TableKind,omitempty"`
	TableName     string             `json:"TableName,omitempty"`
	Columns       []KustoTableColumn `json:"Columns,omitempty"`
	Rows          [][]interface{}    `json:"Rows,omitempty"`
	HasErrors     bool               `json:"HasErrors,omitempty"`
	Cancelled     bool               `json:"Cancelled,omitempty"`
}

type KustoResponseTableV2 struct {
	FrameType     string             `json:"FrameType"`
	IsProgressive bool               `json:"IsProgressive,omitempty"`
	Version       string             `json:"Version,omitempty"`
	TableID       int                `json:"TableId,omitempty"`
	TableKind     string             `json:"TableKind,omitempty"`
	TableName     string             `json:"TableName,omitempty"`
	Columns       []KustoTableColumn `json:"Columns,omitempty"`
	Rows          [][]interface{}    `json:"Rows,omitempty"`
	HasErrors     bool               `json:"HasErrors,omitempty"`
	Cancelled     bool               `json:"Cancelled,omitempty"`
}

func (krt1 KustoResponseTableV1) GetColumns() ([]KustoTableColumn) {
	return krt1.Columns
}

func (krt1 KustoResponseTableV1) GetRows() ([][]interface{}) {
	return krt1.Rows
}

func (krt2 KustoResponseTableV2) GetColumns() ([]KustoTableColumn) {
	return krt2.Columns
}

func (krt2 KustoResponseTableV2) GetRows() ([][]interface{}) {
	return krt2.Rows
}

type KustoResponseDataSetV1 struct {
	Tables []KustoResponseTableV1
}

type KustoResponseDataSetV2 struct {
	Tables []KustoResponseTableV2
}

func (krdsv1 KustoResponseDataSetV1) GetTables() ([]KustoTable) {
	tables := make([]KustoTable, len(krdsv1.Tables))
	for i, v := range krdsv1.Tables {
		tables[i] = v
	}

	return tables
}

func (krdsv2 KustoResponseDataSetV2) GetTables() ([]KustoTable) {
	tables := make([]KustoTable, len(krdsv2.Tables))
	for i, v := range krdsv2.Tables {
		tables[i] = v
	}

	return tables
}

func (krdsv1 KustoResponseDataSetV1) GetPrimaryResults() ([]KustoTable) {
	tables := make([]KustoTable, 0)

	for _, v := range krdsv1.Tables {
		if v.TableKind == "PrimaryResult" {
			tables = append(tables, v)
		}
	}

	return tables
}

func (krdsv2 KustoResponseDataSetV2) GetPrimaryResults() ([]KustoTable) {
	tables := make([]KustoTable, 0)

	for _, v := range krdsv2.Tables {
		if v.TableKind == "PrimaryResult" {
			tables = append(tables, v)
		}
	}

	return tables
}
