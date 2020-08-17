package ingest

import (
	"testing"

	"github.com/google/uuid"
)

func TestRecordMapConversion(t *testing.T) {
	sourceRec := newStatusRecord()
	sourceRec.Status = PartiallySucceeded
	sourceRec.IngestionSourceID = uuid.New()
	sourceRec.IngestionSourcePath = "/mnt/somewhere/file"
	sourceRec.Database = "database"
	sourceRec.Table = "table"
	sourceRec.OperationID = uuid.New()
	sourceRec.ActivityID = uuid.New()
	sourceRec.Details = "bla bla"
	sourceRec.ErrorCode = 3

	props := sourceRec.ToMap()

	targetRec := StatusRecord{}
	targetRec.FromMap(props)

	if sourceRec != targetRec {
		t.Errorf("conversion to map then back resulted in diffrenent record values\nSource Rec: %+v\n Target Rec %+v", sourceRec, targetRec)
	}
}
