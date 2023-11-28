package query

import (
	"github.com/google/uuid"
	"time"
)

type QueryProperties struct {
	TableId int
	Key     string
	Value   interface{}
}

type QueryCompletionInformation struct {
	Timestamp        time.Time
	ClientRequestId  uuid.UUID
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
