package status

import (
	"testing"

	"github.com/google/uuid"
)

func TestRecordMapConversion(t *testing.T) {
	sourceRec := NewRecord(uuid.New(), "/mnt/somewhere/file", "database", "table", uuid.New(), uuid.New())
	sourceRec.Status = PartiallySucceeded
	sourceRec.Details = "bla bla"
	sourceRec.ErrorCode = 3

	props := sourceRec.ToMap()

	targetRec := IngestionStatusRecord{}
	targetRec.FromMap(props)

	if *sourceRec != targetRec {
		t.Errorf("conversion to map then back resulted in diffrenent record values\nSource Rec: %+v\n Target Rec %+v", *sourceRec, targetRec)
	}
}
