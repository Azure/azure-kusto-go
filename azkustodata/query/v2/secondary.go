package v2

import (
	"bytes"
	"encoding/json"
	"github.com/google/uuid"
	"time"
)

// This file handles the parsing of the known secondary tables in v2 datasets.

// QueryProperties represents the query properties table, which arrives before the first result.
type QueryProperties struct {
	TableId int
	Key     string
	Value   map[string]interface{}
}

// QueryCompletionInformation represents the query completion information table, which arrives after the last result.
type QueryCompletionInformation struct {
	Timestamp        time.Time
	ClientRequestId  string
	ActivityId       uuid.UUID
	SubActivityId    uuid.UUID
	ParentActivityId uuid.UUID
	Level            int
	LevelName        string
	StatusCode       int
	StatusCodeName   string
	EventType        int
	EventTypeName    string
	Payload          string
}

const QueryPropertiesKind = "QueryProperties"
const QueryCompletionInformationKind = "QueryCompletionInformation"

func unmarhsalRow(b []byte, onField func(field int, t json.Token)) error {
	decoder := json.NewDecoder(bytes.NewReader(b))
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

			onField(field, t)
		}
	}

	return nil
}

// UnmarshalJSON implements the json.Unmarshaler interface for QueryProperties.
func (q *QueryProperties) UnmarshalJSON(b []byte) error {
	return unmarhsalRow(b, func(field int, t json.Token) {
		switch field {
		case 0:
			q.TableId = int(t.(float64))
		case 1:
			q.Key = t.(string)
		case 2:
			q.Value = t.(map[string]interface{})
		}
	})
}

// UnmarshalJSON implements the json.Unmarshaler interface for QueryCompletionInformation.
func (q *QueryCompletionInformation) UnmarshalJSON(b []byte) error {
	return unmarhsalRow(b, func(field int, t json.Token) {
		switch field {
		case 0:
			q.Timestamp, _ = time.Parse(time.RFC3339Nano, t.(string))
		case 1:
			q.ClientRequestId = t.(string)
		case 2:
			q.ActivityId = uuid.MustParse(t.(string))
		case 3:
			q.SubActivityId = uuid.MustParse(t.(string))
		case 4:
			q.ParentActivityId = uuid.MustParse(t.(string))
		case 5:
			q.Level = int(t.(float64))
		case 6:
			q.LevelName = t.(string)
		case 7:
			q.StatusCode = int(t.(float64))
		case 8:
			q.StatusCodeName = t.(string)
		case 9:
			q.EventType = int(t.(float64))
		case 10:
			q.EventTypeName = t.(string)
		case 11:
			q.Payload = t.(string)
		}
	})
}
