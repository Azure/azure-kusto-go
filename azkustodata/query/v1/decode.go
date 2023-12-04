package v1

import (
	"encoding/json"
	"io"
)

type V1RowOrErrors struct {
	Row    []interface{}
	Errors []string
}

func (r *V1RowOrErrors) UnmarshalJSON(data []byte) error {
	var row []interface{}
	var errors []string
	var err error

	if err = json.Unmarshal(data, &row); err != nil {
		if err = json.Unmarshal(data, &errors); err != nil {
			return err
		}
		r.Errors = errors
		return nil
	}
	r.Row = row
	return nil
}

type V1 struct {
	Tables []struct {
		TableName string `json:"TableName"`
		Columns   []struct {
			ColumnName string `json:"ColumnName"`
			DataType   string `json:"DataType"`
			ColumnType string `json:"ColumnType"`
		} `json:"Columns"`
		Rows V1RowOrErrors `json:"Rows"`
	} `json:"Tables"`
	Exceptions []string `json:"Exceptions"`
}

func decodeV1(data io.ReadCloser) (V1, error) {
	var v1 V1
	dec := json.NewDecoder(data)
	dec.UseNumber()
	err := dec.Decode(&v1)
	return v1, err
}
