package v1

import (
	"bufio"
	"encoding/json"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
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

func decodeV1(data io.ReadCloser) (*V1, error) {
	var v1 V1
	br := bufio.NewReader(data)
	peek, err := br.Peek(1)
	if err != nil {
		return nil, err
	}
	if peek[0] != '{' {
		all, err := io.ReadAll(br)
		if err != nil {
			return nil, err
		}
		return nil, errors.ES(errors.OpUnknown, errors.KInternal, "Got error: %v", string(all))
	}

	dec := json.NewDecoder(br)
	dec.UseNumber()
	err = dec.Decode(&v1)
	if err != nil {
		return nil, err
	}

	return &v1, nil
}
