package kusto

import (
	"context"
	goErr "errors"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"github.com/Azure/azure-kusto-go/kusto/data/value"
	"github.com/Azure/azure-kusto-go/kusto/internal/frames"
	v1 "github.com/Azure/azure-kusto-go/kusto/internal/frames/v1"
	v2 "github.com/Azure/azure-kusto-go/kusto/internal/frames/v2"
	"github.com/stretchr/testify/assert"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func iterateRowsWithErrors(iter *RowIterator) (table.Rows, []*errors.Error, error) {
	// Pulls the frames from the downstream RowIterator.
	got := table.Rows{}
	var inlineErrors []*errors.Error
	err := iter.Do2(func(r *table.Row, inlineError *errors.Error) error {
		if r != nil {
			got = append(got, r)
		} else {
			inlineErrors = append(inlineErrors, inlineError)
		}
		return nil
	})
	return got, inlineErrors, err
}

func assertValues(t *testing.T, wantErr error, gotErr error, want table.Rows, got table.Rows, wantInlineErrors []*errors.Error,
	gotInlineErrors []*errors.Error) {
	if wantErr != nil {
		assert.Error(t, gotErr)
		assert.EqualValues(t, wantErr, gotErr)
		return
	} else {
		assert.NoError(t, gotErr)
	}

	assert.Equal(t, want, got)
	assert.Equal(t, wantInlineErrors, gotInlineErrors)
}

func TestNonProgressive(t *testing.T) {
	t.Parallel()

	nowish := time.Now().UTC()

	tests := []struct {
		desc         string
		ctx          context.Context
		stream       []frames.Frame
		err          error
		want         table.Rows
		nonPrimary   map[frames.TableKind]v2.DataTable
		inlineErrors []*errors.Error
	}{
		{
			desc:   "No completion frame error",
			ctx:    context.Background(),
			stream: []frames.Frame{},
			err:    errors.ES(errors.OpUnknown, errors.KInternal, "non-progressive stream did not have DataSetCompletion frame"),
		},
		{
			desc: "Cancelled context",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			}(),
			stream: []frames.Frame{},
			err:    goErr.New("context canceled"),
		},
		{
			desc:   "No DataSetCompletion Frame",
			ctx:    context.Background(),
			stream: []frames.Frame{v2.DataTable{TableKind: frames.PrimaryResult}},
			err:    errors.ES(errors.OpUnknown, errors.KInternal, "non-progressive stream did not have DataSetCompletion frame"),
		},
		{
			desc: "Frame after DataSetCompletion",
			ctx:  context.Background(),
			stream: []frames.Frame{
				v2.DataTable{TableKind: frames.PrimaryResult},
				v2.DataSetCompletion{},
				v2.DataTable{TableKind: frames.PrimaryResult},
			},
			err: errors.ES(errors.OpUnknown, errors.KInternal, "saw a DataSetCompletion frame, then received a v2.DataTable frame"),
		},
		{
			desc: "The expected frame set",
			ctx:  context.Background(),
			stream: []frames.Frame{
				v2.DataTable{
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.QueryProperties,
					TableName: frames.ExtendedProperties,
					Columns: table.Columns{
						{Name: "TableId", Type: "int"},
						{Name: "Key", Type: "string"},
						{Name: "Value", Type: "dynamic"},
					},
					KustoRows: []value.Values{
						{
							value.Int{Value: 1, Valid: true},
							value.String{Value: "Visualization", Valid: true},
							value.Dynamic{Value: []byte(`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null}`), Valid: true},
						},
					},
				},
				v2.DataTable{
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.PrimaryResult,
					TableName: frames.PrimaryResult,
					Columns: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					KustoRows: []value.Values{
						{
							value.DateTime{Value: nowish, Valid: true},
							value.String{Value: "Doak", Valid: true},
							value.Long{Value: 10, Valid: true},
						},
						{
							value.DateTime{Value: nowish, Valid: true},
							value.String{Value: "Dubovski", Valid: true},
							value.Long{Value: 0, Valid: false},
						},
					},
				},
				v2.DataSetCompletion{},
			},
			want: table.Rows{
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.DateTime{Value: nowish, Valid: true},
						value.String{Value: "Doak", Valid: true},
						value.Long{Value: 10, Valid: true},
					},
					Op: errors.OpQuery,
				},
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.DateTime{Value: nowish, Valid: true},
						value.String{Value: "Dubovski", Valid: true},
						value.Long{Value: 0, Valid: false},
					},
					Op: errors.OpQuery,
				},
			},
			nonPrimary: map[frames.TableKind]v2.DataTable{
				frames.QueryProperties: {
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.QueryProperties,
					TableName: frames.ExtendedProperties,
					Columns: table.Columns{
						{Name: "TableId", Type: "int"},
						{Name: "Key", Type: "string"},
						{Name: "Value", Type: "dynamic"},
					},
					KustoRows: []value.Values{
						{
							value.Int{Value: 1, Valid: true},
							value.String{Value: "Visualization", Valid: true},
							value.Dynamic{Value: []byte(`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null}`), Valid: true},
						},
					},
				},
			},
		},
		{
			desc: "The expected frame set with inline errors",
			ctx:  context.Background(),
			stream: []frames.Frame{
				v2.DataTable{
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.QueryProperties,
					TableName: frames.ExtendedProperties,
					Columns: table.Columns{
						{Name: "TableId", Type: "int"},
						{Name: "Key", Type: "string"},
						{Name: "Value", Type: "dynamic"},
					},
					KustoRows: []value.Values{
						{
							value.Int{Value: 1, Valid: true},
							value.String{Value: "Visualization", Valid: true},
							value.Dynamic{Value: []byte(`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null}`), Valid: true},
						},
					},
				},
				v2.DataTable{
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.PrimaryResult,
					TableName: frames.PrimaryResult,
					Columns: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					KustoRows: []value.Values{
						{
							value.DateTime{Value: nowish, Valid: true},
							value.String{Value: "Doak", Valid: true},
							value.Long{Value: 10, Valid: true},
						},
						{
							value.DateTime{Value: nowish, Valid: true},
							value.String{Value: "Dubovski", Valid: true},
							value.Long{Value: 0, Valid: false},
						},
					},
					RowErrors: []errors.Error{
						*errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Request is invalid and cannot be executed.;See https://docs.microsoft."+
							"com/en-us/azure/kusto/concepts/querylimits"),
						*errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Some other error"),
					},
				},
				v2.DataSetCompletion{},
			},
			want: table.Rows{
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.DateTime{Value: nowish, Valid: true},
						value.String{Value: "Doak", Valid: true},
						value.Long{Value: 10, Valid: true},
					},
					Op: errors.OpQuery,
				},
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.DateTime{Value: nowish, Valid: true},
						value.String{Value: "Dubovski", Valid: true},
						value.Long{Value: 0, Valid: false},
					},
					Op: errors.OpQuery,
				},
			},
			nonPrimary: map[frames.TableKind]v2.DataTable{
				frames.QueryProperties: {
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.QueryProperties,
					TableName: frames.ExtendedProperties,
					Columns: table.Columns{
						{Name: "TableId", Type: "int"},
						{Name: "Key", Type: "string"},
						{Name: "Value", Type: "dynamic"},
					},
					KustoRows: []value.Values{
						{
							value.Int{Value: 1, Valid: true},
							value.String{Value: "Visualization", Valid: true},
							value.Dynamic{Value: []byte(`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null}`), Valid: true},
						},
					},
				},
			},
			inlineErrors: []*errors.Error{
				errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Request is invalid and cannot be executed.;See https://docs.microsoft."+
					"com/en-us/azure/kusto/concepts/querylimits"),
				errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Some other error"),
			},
		},
	}

	for _, test := range tests {
		test := test // Capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			wg := sync.WaitGroup{}
			// Sends the frames like the upstream provider.
			toSM := make(chan frames.Frame)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer close(toSM)
				for _, fr := range test.stream {
					toSM <- fr
				}
			}()

			ctx, cancel := context.WithCancel(context.Background())
			iter, gotColumns := newRowIterator(ctx, cancel, execResp{}, v2.DataSetHeader{}, errors.OpQuery)

			sm := nonProgressiveSM{
				iter: iter,
				in:   toSM,
				ctx:  test.ctx,
				wg:   &sync.WaitGroup{},
			}

			runSM(&sm)
			<-gotColumns

			got, inlineErrors, err := iterateRowsWithErrors(sm.iter)
			if err != nil {
				return
			}

			wg.Wait()

			assertValues(t, test.err, err, test.want, got, test.inlineErrors, inlineErrors)
		})
	}
}

func TestProgressive(t *testing.T) {
	t.Parallel()

	// TODO(jdoak/daniel): There are other edge cases worth testing for, like TableHeader/Fragments with nonPrimary tables.

	nowish := time.Now().UTC()

	tests := []struct {
		desc         string
		ctx          context.Context
		stream       []frames.Frame
		err          error
		want         table.Rows
		nonPrimary   map[frames.TableKind]v2.DataTable
		inlineErrors []*errors.Error
	}{
		{
			desc:   "No completion frame error",
			ctx:    context.Background(),
			stream: []frames.Frame{},
			err:    errors.ES(errors.OpUnknown, errors.KInternal, "received a table stream that did not finish before our input channel, this is usually a return size or time limit"),
		},
		{
			desc: "Cancelled context",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			}(),
			stream: []frames.Frame{},
			err:    goErr.New("context canceled"),
		},
		{
			desc:   "No frames.DataSetCompletion Frame",
			ctx:    context.Background(),
			stream: []frames.Frame{v2.DataTable{TableKind: frames.QueryProperties}},
			err:    errors.ES(errors.OpUnknown, errors.KInternal, "received a table stream that did not finish before our input channel, this is usually a return size or time limit"),
		},
		{
			desc: "dataTable was PrimaryResult",
			ctx:  context.Background(),
			stream: []frames.Frame{
				v2.DataTable{TableKind: frames.PrimaryResult},
				v2.DataSetCompletion{},
			},
			err: errors.ES(errors.OpUnknown, errors.KInternal, "progressive stream had dataTable with Kind == PrimaryResult"),
		},
		{
			desc: "Frame after frames.DataSetCompletion",
			ctx:  context.Background(),
			stream: []frames.Frame{
				v2.DataTable{TableKind: frames.QueryProperties},
				v2.DataSetCompletion{},
				v2.DataTable{TableKind: frames.QueryProperties},
			},
			err: errors.ES(errors.OpUnknown, errors.KInternal, "received a dataSetCompletion frame and then a v2.DataTable frame"),
		},
		{
			desc: "TableFragment with no TableHeader",
			ctx:  context.Background(),
			stream: []frames.Frame{
				v2.DataTable{TableKind: frames.QueryProperties},
				v2.TableFragment{},
			},
			err: errors.ES(errors.OpUnknown, errors.KInternal, "received a TableFragment without a tableHeader"),
		},
		{
			desc: "Had a Primary DataTable",
			err:  errors.ES(errors.OpUnknown, errors.KInternal, "progressive stream had dataTable with Kind == PrimaryResult"),
			ctx:  context.Background(),
			stream: []frames.Frame{
				v2.DataTable{
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.QueryProperties,
					TableName: frames.ExtendedProperties,
					Columns: table.Columns{
						{Name: "TableId", Type: "int"},
						{Name: "Key", Type: "string"},
						{Name: "Value", Type: "dynamic"},
					},
					KustoRows: []value.Values{
						{
							value.Int{Value: 1, Valid: true},
							value.String{Value: "Visualization", Valid: true},
							value.Dynamic{Value: []byte(`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null}`), Valid: true},
						},
					},
				},
				v2.DataTable{
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.PrimaryResult,
					TableName: frames.PrimaryResult,
					Columns: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					KustoRows: []value.Values{
						{
							value.DateTime{Value: nowish, Valid: true},
							value.String{Value: "Doak", Valid: true},
							value.Long{Value: 10, Valid: true},
						},
						{
							value.DateTime{Value: nowish, Valid: true},
							value.String{Value: "Dubovski", Valid: true},
							value.Long{Value: 0, Valid: false},
						},
					},
				},
				v2.DataSetCompletion{},
			},
			want: table.Rows{
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.DateTime{Value: nowish, Valid: true},
						value.String{Value: "Doak", Valid: true},
						value.Long{Value: 10, Valid: true},
					},
					Op: errors.OpQuery,
				},
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.DateTime{Value: nowish, Valid: true},
						value.String{Value: "Dubovski", Valid: true},
						value.Long{Value: 0, Valid: false},
					},
					Op: errors.OpQuery,
				},
			},
			nonPrimary: map[frames.TableKind]v2.DataTable{
				frames.QueryProperties: {
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.QueryProperties,
					TableName: frames.ExtendedProperties,
					Columns: table.Columns{
						{Name: "TableId", Type: "int"},
						{Name: "Key", Type: "string"},
						{Name: "Value", Type: "dynamic"},
					},
					KustoRows: []value.Values{
						{
							value.Int{Value: 1, Valid: true},
							value.String{Value: "Visualization", Valid: true},
							value.Dynamic{Value: []byte(`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null}`), Valid: true},
						},
					},
				},
			},
		},
		{
			desc: "Expected Result",
			ctx:  context.Background(),
			stream: []frames.Frame{
				v2.TableHeader{
					Base:      v2.Base{FrameType: frames.TypeTableHeader},
					TableKind: frames.PrimaryResult,
					Columns: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
				},
				v2.TableFragment{
					KustoRows: []value.Values{
						{
							value.DateTime{Value: nowish, Valid: true},
							value.String{Value: "Doak", Valid: true},
							value.Long{Value: 10, Valid: true},
						},
					},
				},
				v2.TableFragment{
					KustoRows: []value.Values{
						{
							value.DateTime{Value: nowish, Valid: true},
							value.String{Value: "Dubovski", Valid: true},
							value.Long{Value: 0, Valid: false},
						},
						{
							value.DateTime{Value: nowish, Valid: true},
							value.String{Value: "Evcpwtlj", Valid: true},
							value.Long{Value: 1, Valid: true},
						},
					},
					TableFragmentType: "DataReplace",
				},
				v2.TableCompletion{},
				v2.DataTable{
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.QueryProperties,
					TableName: frames.ExtendedProperties,
					Columns: table.Columns{
						{Name: "TableId", Type: "int"},
						{Name: "Key", Type: "string"},
						{Name: "Value", Type: "dynamic"},
					},
					KustoRows: []value.Values{
						{
							value.Int{Value: 1, Valid: true},
							value.String{Value: "Visualization", Valid: true},
							value.Dynamic{Value: []byte(`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null}`), Valid: true},
						},
					},
				},
				v2.DataSetCompletion{},
			},
			want: table.Rows{
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.DateTime{Value: nowish, Valid: true},
						value.String{Value: "Doak", Valid: true},
						value.Long{Value: 10, Valid: true},
					},
					Op: errors.OpQuery,
				},
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.DateTime{Value: nowish, Valid: true},
						value.String{Value: "Dubovski", Valid: true},
						value.Long{Value: 0, Valid: false},
					},
					Replace: true,
					Op:      errors.OpQuery,
				},
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.DateTime{Value: nowish, Valid: true},
						value.String{Value: "Evcpwtlj", Valid: true},
						value.Long{Value: 1, Valid: true},
					},
					Op: errors.OpQuery,
				},
			},
			nonPrimary: map[frames.TableKind]v2.DataTable{
				frames.QueryProperties: {
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.QueryProperties,
					TableName: frames.ExtendedProperties,
					Columns: table.Columns{
						{Name: "TableId", Type: "int"},
						{Name: "Key", Type: "string"},
						{Name: "Value", Type: "dynamic"},
					},
					KustoRows: []value.Values{
						{
							value.Int{Value: 1, Valid: true},
							value.String{Value: "Visualization", Valid: true},
							value.Dynamic{Value: []byte(`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null}`), Valid: true},
						},
					},
				},
			},
		},
		{
			desc: "Expected Result with inline errors",
			ctx:  context.Background(),
			stream: []frames.Frame{
				v2.TableHeader{
					Base:      v2.Base{FrameType: frames.TypeTableHeader},
					TableKind: frames.PrimaryResult,
					Columns: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
				},
				v2.TableFragment{
					KustoRows: []value.Values{
						{
							value.DateTime{Value: nowish, Valid: true},
							value.String{Value: "Doak", Valid: true},
							value.Long{Value: 10, Valid: true},
						},
					},
					RowErrors: []errors.Error{
						*errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Request is invalid and cannot be executed.;See https://docs.microsoft."+
							"com/en-us/azure/kusto/concepts/querylimits"),
						*errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Some error"),
					},
				},
				v2.TableFragment{
					KustoRows: []value.Values{
						{
							value.DateTime{Value: nowish, Valid: true},
							value.String{Value: "Dubovski", Valid: true},
							value.Long{Value: 0, Valid: false},
						},
						{
							value.DateTime{Value: nowish, Valid: true},
							value.String{Value: "Evcpwtlj", Valid: true},
							value.Long{Value: 1, Valid: true},
						},
					},
					RowErrors: []errors.Error{
						*errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Some other error"),
					},
					TableFragmentType: "DataReplace",
				},
				v2.TableCompletion{},
				v2.DataTable{
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.QueryProperties,
					TableName: frames.ExtendedProperties,
					Columns: table.Columns{
						{Name: "TableId", Type: "int"},
						{Name: "Key", Type: "string"},
						{Name: "Value", Type: "dynamic"},
					},
					KustoRows: []value.Values{
						{
							value.Int{Value: 1, Valid: true},
							value.String{Value: "Visualization", Valid: true},
							value.Dynamic{Value: []byte(`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null}`), Valid: true},
						},
					},
				},
				v2.DataSetCompletion{},
			},
			want: table.Rows{
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.DateTime{Value: nowish, Valid: true},
						value.String{Value: "Doak", Valid: true},
						value.Long{Value: 10, Valid: true},
					},
					Op: errors.OpQuery,
				},
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.DateTime{Value: nowish, Valid: true},
						value.String{Value: "Dubovski", Valid: true},
						value.Long{Value: 0, Valid: false},
					},
					Replace: true,
					Op:      errors.OpQuery,
				},
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.DateTime{Value: nowish, Valid: true},
						value.String{Value: "Evcpwtlj", Valid: true},
						value.Long{Value: 1, Valid: true},
					},
					Op: errors.OpQuery,
				},
			},
			nonPrimary: map[frames.TableKind]v2.DataTable{
				frames.QueryProperties: {
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.QueryProperties,
					TableName: frames.ExtendedProperties,
					Columns: table.Columns{
						{Name: "TableId", Type: "int"},
						{Name: "Key", Type: "string"},
						{Name: "Value", Type: "dynamic"},
					},
					KustoRows: []value.Values{
						{
							value.Int{Value: 1, Valid: true},
							value.String{Value: "Visualization", Valid: true},
							value.Dynamic{Value: []byte(`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null}`), Valid: true},
						},
					},
				},
			},
			inlineErrors: []*errors.Error{
				errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Request is invalid and cannot be executed.;See https://docs.microsoft."+
					"com/en-us/azure/kusto/concepts/querylimits"),
				errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Some error"),
				errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Some other error"),
			},
		},
	}

	// TODO(jdoak): This could use some cleanup. Rarely are their reasons to have WaitGroup and channel canceling.
	// That was there to prevent "test" from being used in the goroutine still when an error had occured. But I think
	// we can do better.
	for _, test := range tests {
		test := test // Capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			// Sends the frames like the upstream provider.
			toSM := make(chan frames.Frame)
			sendCtx, sendCancel := context.WithCancel(context.Background())
			sendDone := make(chan struct{})
			defer func() {
				sendCancel()
				<-sendDone
			}()
			go func() {
				defer close(sendDone)
				defer close(toSM)
				for _, fr := range test.stream {
					select {
					case <-sendCtx.Done():
						return
					case toSM <- fr:
					}
				}
			}()

			ctx, cancel := context.WithCancel(context.Background())
			iter, gotColumns := newRowIterator(ctx, cancel, execResp{}, v2.DataSetHeader{}, errors.OpQuery)

			sm := progressiveSM{
				iter: iter,
				in:   toSM,
				ctx:  test.ctx,
				wg:   &sync.WaitGroup{},
			}

			runSM(&sm)
			<-gotColumns

			got, inlineErrors, err := iterateRowsWithErrors(sm.iter)

			assertValues(t, test.err, err, test.want, got, test.inlineErrors, inlineErrors)
		})
	}
}

func TestV1SM(t *testing.T) {
	t.Parallel()

	nowish := time.Now().UTC()

	tests := []struct {
		desc         string
		ctx          context.Context
		stream       []frames.Frame
		err          error
		want         table.Rows
		inlineErrors []*errors.Error
	}{
		{
			desc:   "No DataTable frame error",
			ctx:    context.Background(),
			stream: []frames.Frame{},
			err:    errors.ES(errors.OpUnknown, errors.KInternal, "received a table stream that did not finish before our input channel, this is usually a return size or time limit"),
		},
		{
			desc: "Cancelled context",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			}(),
			stream: []frames.Frame{},
			err:    goErr.New("context canceled"),
		},
		{
			desc: "Single Table",
			ctx:  context.Background(),
			stream: []frames.Frame{
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Timestamp", ColumnType: "datetime"},
						{ColumnName: "Name", ColumnType: "string"},
						{ColumnName: "ID", ColumnType: "long"},
					},
					KustoRows: []value.Values{
						{
							value.DateTime{Value: nowish, Valid: true},
							value.String{Value: "Doak", Valid: true},
							value.Long{Value: 10, Valid: true},
						},
					},
				},
			},
			want: table.Rows{
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.DateTime{Value: nowish, Valid: true},
						value.String{Value: "Doak", Valid: true},
						value.Long{Value: 10, Valid: true},
					},
					Op: errors.OpQuery,
				},
			},
		},
		{
			desc: "Primary And QueryProperties",
			ctx:  context.Background(),
			stream: []frames.Frame{
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Timestamp", ColumnType: "datetime"},
						{ColumnName: "Name", ColumnType: "string"},
						{ColumnName: "ID", ColumnType: "long"},
					},
					KustoRows: []value.Values{
						{
							value.DateTime{Value: nowish, Valid: true},
							value.String{Value: "Doak", Valid: true},
							value.Long{Value: 10, Valid: true},
						},
					},
				},
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Value", ColumnType: "string"},
					},
					KustoRows: []value.Values{
						{
							value.String{Value: "{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\"}", Valid: true},
						},
					},
				},
			},
			want: table.Rows{
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.DateTime{Value: nowish, Valid: true},
						value.String{Value: "Doak", Valid: true},
						value.Long{Value: 10, Valid: true},
					},
					Op: errors.OpQuery,
				},
			},
		},
		{
			desc: "Primary With TableOfContents",
			ctx:  context.Background(),
			stream: []frames.Frame{
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Timestamp", ColumnType: "datetime"},
						{ColumnName: "Name", ColumnType: "string"},
						{ColumnName: "ID", ColumnType: "long"},
					},
					KustoRows: []value.Values{
						{
							value.DateTime{Value: nowish, Valid: true},
							value.String{Value: "Doak", Valid: true},
							value.Long{Value: 10, Valid: true},
						},
					},
				},
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Value", ColumnType: "string"},
					},
					KustoRows: []value.Values{
						{
							value.String{Value: "{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\"}", Valid: true},
						},
					},
				},
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Ordinal", ColumnType: "long"},
						{ColumnName: "Kind", ColumnType: "string"},
						{ColumnName: "Name", ColumnType: "string"},
						{ColumnName: "Id", ColumnType: "string"},
						{ColumnName: "PrettyName", ColumnType: "string"},
					},
					KustoRows: []value.Values{
						{
							value.Long{Value: 0, Valid: true},
							value.String{Value: "QueryResult", Valid: true},
							value.String{Value: "PrimaryResult", Valid: true},
							value.String{Value: "07dd9603-3e06-4c62-986b-dfc3d586b05a", Valid: true},
							value.String{Value: "", Valid: true},
						},
						{
							value.Long{Value: 1, Valid: true},
							value.String{Value: "QueryProperties", Valid: true},
							value.String{Value: "@ExtendedProperties", Valid: true},
							value.String{Value: "309c015e-5693-4b66-92e7-4a4f98c3155b", Valid: true},
							value.String{Value: "", Valid: true},
						},
					},
				},
			},
			want: table.Rows{
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.DateTime{Value: nowish, Valid: true},
						value.String{Value: "Doak", Valid: true},
						value.Long{Value: 10, Valid: true},
					},
					Op: errors.OpQuery,
				},
			},
		},
		{
			desc: "Multiple Primaries",
			ctx:  context.Background(),
			stream: []frames.Frame{
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Timestamp", ColumnType: "datetime"},
						{ColumnName: "Name", ColumnType: "string"},
						{ColumnName: "ID", ColumnType: "long"},
					},
					KustoRows: []value.Values{
						{
							value.DateTime{Value: nowish, Valid: true},
							value.String{Value: "Doak", Valid: true},
							value.Long{Value: 10, Valid: true},
						},
					},
				},
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Value", ColumnType: "string"},
					},
					KustoRows: []value.Values{
						{
							value.String{Value: "{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\"}", Valid: true},
						},
					},
				},
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Timestamp", ColumnType: "datetime"},
						{ColumnName: "Name", ColumnType: "string"},
						{ColumnName: "ID", ColumnType: "long"},
					},
					KustoRows: []value.Values{
						{
							value.DateTime{Value: nowish, Valid: true},
							value.String{Value: "DD", Valid: true},
							value.Long{Value: 101, Valid: true},
						},
					},
				},
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Ordinal", ColumnType: "long"},
						{ColumnName: "Kind", ColumnType: "string"},
						{ColumnName: "Name", ColumnType: "string"},
						{ColumnName: "Id", ColumnType: "string"},
						{ColumnName: "PrettyName", ColumnType: "string"},
					},
					KustoRows: []value.Values{
						{
							value.Long{Value: 0, Valid: true},
							value.String{Value: "QueryResult", Valid: true},
							value.String{Value: "PrimaryResult", Valid: true},
							value.String{Value: "07dd9603-3e06-4c62-986b-dfc3d586b05a", Valid: true},
							value.String{Value: "", Valid: true},
						},
						{
							value.Long{Value: 1, Valid: true},
							value.String{Value: "QueryProperties", Valid: true},
							value.String{Value: "@ExtendedProperties", Valid: true},
							value.String{Value: "309c015e-5693-4b66-92e7-4a4f98c3155b", Valid: true},
							value.String{Value: "", Valid: true},
						},
						{
							value.Long{Value: 2, Valid: true},
							value.String{Value: "QueryResult", Valid: true},
							value.String{Value: "PrimaryResult", Valid: true},
							value.String{Value: "07dd9603-3e06-4c62-986b-dfc3d586b05a", Valid: true},
							value.String{Value: "", Valid: true},
						},
					},
				},
			},
			want: table.Rows{
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.DateTime{Value: nowish, Valid: true},
						value.String{Value: "Doak", Valid: true},
						value.Long{Value: 10, Valid: true},
					},
					Op: errors.OpQuery,
				},
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.DateTime{Value: nowish, Valid: true},
						value.String{Value: "DD", Valid: true},
						value.Long{Value: 101, Valid: true},
					},
					Op: errors.OpQuery,
				},
			},
		},
		{
			desc: "Multiple Primaries with errors",
			ctx:  context.Background(),
			stream: []frames.Frame{
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Timestamp", ColumnType: "datetime"},
						{ColumnName: "Name", ColumnType: "string"},
						{ColumnName: "ID", ColumnType: "long"},
					},
					KustoRows: []value.Values{
						{
							value.DateTime{Value: nowish, Valid: true},
							value.String{Value: "Doak", Valid: true},
							value.Long{Value: 10, Valid: true},
						},
					},
					RowErrors: []errors.Error{
						*errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Request is invalid and cannot be executed.;See https://docs.microsoft."+
							"com/en-us/azure/kusto/concepts/querylimits"),
						*errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Some error"),
					},
				},
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Value", ColumnType: "string"},
					},
					KustoRows: []value.Values{
						{
							value.String{Value: "{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\"}", Valid: true},
						},
					},
				},
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Timestamp", ColumnType: "datetime"},
						{ColumnName: "Name", ColumnType: "string"},
						{ColumnName: "ID", ColumnType: "long"},
					},
					KustoRows: []value.Values{
						{
							value.DateTime{Value: nowish, Valid: true},
							value.String{Value: "DD", Valid: true},
							value.Long{Value: 101, Valid: true},
						},
					},
					RowErrors: []errors.Error{
						*errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Some other error"),
					},
				},
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Ordinal", ColumnType: "long"},
						{ColumnName: "Kind", ColumnType: "string"},
						{ColumnName: "Name", ColumnType: "string"},
						{ColumnName: "Id", ColumnType: "string"},
						{ColumnName: "PrettyName", ColumnType: "string"},
					},
					KustoRows: []value.Values{
						{
							value.Long{Value: 0, Valid: true},
							value.String{Value: "QueryResult", Valid: true},
							value.String{Value: "PrimaryResult", Valid: true},
							value.String{Value: "07dd9603-3e06-4c62-986b-dfc3d586b05a", Valid: true},
							value.String{Value: "", Valid: true},
						},
						{
							value.Long{Value: 1, Valid: true},
							value.String{Value: "QueryProperties", Valid: true},
							value.String{Value: "@ExtendedProperties", Valid: true},
							value.String{Value: "309c015e-5693-4b66-92e7-4a4f98c3155b", Valid: true},
							value.String{Value: "", Valid: true},
						},
						{
							value.Long{Value: 2, Valid: true},
							value.String{Value: "QueryResult", Valid: true},
							value.String{Value: "PrimaryResult", Valid: true},
							value.String{Value: "07dd9603-3e06-4c62-986b-dfc3d586b05a", Valid: true},
							value.String{Value: "", Valid: true},
						},
					},
				},
			},
			want: table.Rows{
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.DateTime{Value: nowish, Valid: true},
						value.String{Value: "Doak", Valid: true},
						value.Long{Value: 10, Valid: true},
					},
					Op: errors.OpQuery,
				},
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.DateTime{Value: nowish, Valid: true},
						value.String{Value: "DD", Valid: true},
						value.Long{Value: 101, Valid: true},
					},
					Op: errors.OpQuery,
				},
			},
			inlineErrors: []*errors.Error{
				errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Request is invalid and cannot be executed.;See https://docs.microsoft."+
					"com/en-us/azure/kusto/concepts/querylimits"),
				errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Some error"),
				errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Some other error"),
			},
		},
	}

	for _, test := range tests {
		test := test // Capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			// Sends the frames like the upstream provider.
			toSM := make(chan frames.Frame)

			sendCtx, sendCancel := context.WithCancel(context.Background())
			sendDone := make(chan struct{})
			defer func() {
				sendCancel()
				<-sendDone
			}()
			go func() {
				defer close(sendDone)
				defer close(toSM)
				for _, fr := range test.stream {
					select {
					case <-sendCtx.Done():
						return
					case toSM <- fr:
					}
				}
			}()
			ctx, cancel := context.WithCancel(context.Background())
			iter, gotColumns := newRowIterator(ctx, cancel, execResp{}, v2.DataSetHeader{}, errors.OpQuery)

			sm := v1SM{
				iter: iter,
				in:   toSM,
				ctx:  test.ctx,
				wg:   &sync.WaitGroup{},
			}

			runSM(&sm)
			<-gotColumns

			got, inlineErrors, err := iterateRowsWithErrors(sm.iter)

			assertValues(t, test.err, err, test.want, got, test.inlineErrors, inlineErrors)
		})
	}
}
