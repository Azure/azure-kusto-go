package v2

import (
	"bytes"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"github.com/goccy/go-json"
)

func (t *TableFragment) UnmarshalJSON(b []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()

	for {
		tok, err := decoder.Token()
		if err != nil {
			return err
		}
		if tok == json.Token("Rows") {
			break
		}
	}

	rows, err := decodeRows(b, decoder, t.Columns, t.PreviousIndex)
	if err != nil {
		return err
	}

	t.Rows = rows
	return nil
}

func (q *DataTable) UnmarshalJSON(b []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()

	err := decodeHeader(decoder, &q.Header, DataTableFrameType)
	if err != nil {
		return err
	}

	err = assertToken(decoder, json.Token("Rows"))
	if err != nil {
		return err
	}

	rows, err := decodeRows(b, decoder, q.Header.Columns, 0)
	if err != nil {
		return err
	}

	q.Rows = rows
	return nil
}

func (t *TableHeader) UnmarshalJSON(b []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()

	err2 := decodeHeader(decoder, t, TableHeaderFrameType)
	if err2 != nil {
		return err2
	}

	return nil
}

func decodeHeader(decoder *json.Decoder, t *TableHeader, frameType FrameType) error {
	err := assertToken(decoder, json.Delim('{'))
	if err != nil {
		return err
	}

	err = assertStringProperty(decoder, "FrameType", string(frameType))
	if err != nil {
		return err
	}

	t.TableId, err = getIntProperty(decoder, "TableId")
	if err != nil {
		return err
	}

	t.TableKind, err = getStringProperty(decoder, "TableKind")
	if err != nil {
		return err
	}

	t.TableName, err = getStringProperty(decoder, "TableName")
	if err != nil {
		return err
	}

	err = assertToken(decoder, json.Token("Columns"))
	if err != nil {
		return err
	}

	t.Columns, err = decodeColumns(decoder)
	if err != nil {
		return err
	}
	return nil
}

func decodeColumns(decoder *json.Decoder) ([]query.Column, error) {
	cols := make([]query.Column, 0)

	err := assertToken(decoder, json.Delim('['))
	if err != nil {
		return nil, err
	}

	for i := 0; decoder.More(); i++ {
		col := FrameColumn{}
		err := decoder.Decode(&col)
		if err != nil {
			return nil, err
		}
		col.ColumnIndex = i
		col.ColumnType = string(types.NormalizeColumn(col.ColumnType))
		if col.ColumnType == "" {
			return nil, errors.ES(errors.OpTableAccess, errors.KClientArgs, "column[%d] is of type %s, which is not valid", i, col.ColumnType)
		}
		cols = append(cols, col)
	}

	if err := assertToken(decoder, json.Delim(']')); err != nil {
		return nil, err
	}

	return cols, nil
}

func decodeRows(b []byte, decoder *json.Decoder, cols []query.Column, startIndex int) ([]query.Row, error) {
	var rows = make([]query.Row, 0, 1000)

	columnsByName := make(map[string]query.Column, len(cols))
	for _, c := range cols {
		columnsByName[c.Name()] = c
	}

	err := assertToken(decoder, json.Delim('['))
	if err != nil {
		return nil, err
	}

	for i := startIndex; decoder.More(); i++ {
		values := make([]value.Kusto, 0, len(cols))
		err := decodeRow(b, decoder, func(field int, t json.Token) error {
			kusto := value.Default(cols[field].Type())
			err := kusto.Unmarshal(t)
			if err != nil {
				return err
			}
			values = append(values, kusto)
			return nil
		})
		if err != nil {
			return nil, err
		}

		row := query.NewRowFromParts(cols, func(name string) query.Column { return columnsByName[name] }, i, values)
		rows = append(rows, row)
	}

	if err := assertToken(decoder, json.Delim(']')); err != nil {
		return nil, err
	}
	return rows, nil
}

func decodeRow(
	buffer []byte,
	decoder *json.Decoder,
	onField func(field int, t json.Token) error) error {
	for {
		t, err := decoder.Token()
		if err != nil {
			return err
		}

		// end of outer array
		if t != json.Delim('[') {
			break
		}

		field := 0

		for ; decoder.More(); field++ {

			t, err = decoder.Token()
			if err != nil {
				return err
			}

			// If it's a nested object, just make it into a byte array
			if t == json.Delim('[') || t == json.Delim('{') {
				initialOffset := decoder.InputOffset() - 1
				for decoder.More() {
					_, err := decoder.Token()
					if err != nil {
						return err
					}
				}
				_, err := decoder.Token()
				if err != nil {
					return err
				}

				finalOffset := decoder.InputOffset()

				err = onField(field, json.Token(buffer[initialOffset:finalOffset]))
				if err != nil {
					return err
				}
				continue
			}

			err := onField(field, t)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
