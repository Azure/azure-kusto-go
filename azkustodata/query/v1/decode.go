package v1

import (
	"encoding/json"
	"io"
)

type RowOrErrors struct {
	Row    []interface{}
	Errors []string
}

func (r *RowOrErrors) UnmarshalJSON(data []byte) error {
	var row []interface{}
	var errors struct {
		Exceptions []string `json:"Exceptions"`
	}

	var err error

	if err = json.Unmarshal(data, &row); err != nil {
		if err = json.Unmarshal(data, &errors); err != nil {
			return err
		}
		r.Errors = errors.Exceptions
		return nil
	}
	r.Row = row
	return nil
}

type RawColumn struct {
	ColumnName string `json:"ColumnName"`
	DataType   string `json:"DataType"`
	ColumnType string `json:"ColumnType"`
}

type RawTable struct {
	TableName string        `json:"TableName"`
	Columns   []RawColumn   `json:"Columns"`
	Rows      []RowOrErrors `json:"Rows"`
}

type V1 struct {
	Tables     []RawTable `json:"Tables"`
	Exceptions []string   `json:"Exceptions"`
}

func decodeV1(data io.ReadCloser) (V1, error) {
	var v1 V1
	dec := json.NewDecoder(data)
	dec.UseNumber()
	err := dec.Decode(&v1)
	return v1, err
}
