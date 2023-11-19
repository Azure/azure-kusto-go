package azkustodata

import (
	"context"
	goErr "errors"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/internal/frames"
	v1 "github.com/Azure/azure-kusto-go/azkustodata/internal/frames/v1"
	v2 "github.com/Azure/azure-kusto-go/azkustodata/internal/frames/v2"
	"github.com/Azure/azure-kusto-go/azkustodata/table"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func iterateRowsWithErrors(iter *RowIterator) (table.Rows, []*errors.Error, error) {
	// Pulls the frames from the downstream RowIterator.
	var got table.Rows = nil
	var inlineErrors []*errors.Error
	err := iter.DoOnRowOrError(func(r *table.Row, inlineError *errors.Error) error {
		if r != nil {
			if got == nil {
				got = table.Rows{}
			}
			got = append(got, r)
		} else {
			inlineErrors = append(inlineErrors, inlineError)
		}
		return nil
	})
	return got, inlineErrors, err
}

func iterateRows(iter *RowIterator) (table.Rows, error) {
	// Pulls the frames from the downstream RowIterator.
	var got table.Rows = nil
	err := iter.Do(func(r *table.Row) error {
		if r != nil {
			if got == nil {
				got = table.Rows{}
			}
			got = append(got, r)
		}
		return nil
	})
	return got, err
}
func assertValues(t *testing.T, wantErr error, gotErr error, want table.Rows, got table.Rows, wantInlineErrors []*errors.Error,
	gotInlineErrors []*errors.Error) {
	if wantErr != nil {
		assert.Error(t, gotErr)
		assert.EqualValues(t, wantErr, gotErr, "wantErr: %v, gotErr: %v", wantErr, gotErr)
	} else {
		assert.NoError(t, gotErr)
	}

	assert.Equal(t, want, got)
	assert.Equal(t, wantInlineErrors, gotInlineErrors)
}

func checkNonPrimary(t *testing.T, want map[frames.TableKind]v2.DataTable, iter *RowIterator) {
	if want != nil {
		assert.EqualValues(t, want, iter.nonPrimary)

		extendedManual, err := iter.GetNonPrimary(frames.QueryProperties, frames.ExtendedProperties)
		assert.NoError(t, err)
		assert.EqualValues(t, want[frames.QueryProperties], extendedManual)
		extended, err := iter.GetExtendedProperties()
		assert.NoError(t, err)
		assert.EqualValues(t, want[frames.QueryProperties], extended)

		completionManual, err := iter.GetNonPrimary(frames.QueryCompletionInformation, frames.QueryCompletionInformation)
		assert.NoError(t, err)
		assert.EqualValues(t, want[frames.QueryCompletionInformation], completionManual)
		completion, err := iter.GetQueryCompletionInformation()
		assert.NoError(t, err)
		assert.EqualValues(t, want[frames.QueryCompletionInformation], completion)
	}
}

func streamStateMachine(stream []frames.Frame, createSM func(iter *RowIterator, toSM chan frames.Frame) stateMachine, recv func(iter *RowIterator)) {
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
		for _, fr := range stream {
			select {
			case <-sendCtx.Done():
				return
			case toSM <- fr:
			}
		}
	}()
	iterCtx, cancel := context.WithCancel(context.Background())
	iter, gotColumns := newRowIterator(iterCtx, cancel, execResp{}, v2.DataSetHeader{}, errors.OpQuery)

	sm := createSM(iter, toSM)

	runSM(sm)
	<-gotColumns

	recv(iter)
}

func TestNonProgressive(t *testing.T) {
	t.Parallel()

	nowish := time.Now().UTC()

	tests := []struct {
		desc                    string
		ctx                     func() context.Context
		stream                  []frames.Frame
		err                     error
		want                    table.Rows
		wantWithoutInlineErrors table.Rows
		nonPrimary              map[frames.TableKind]v2.DataTable
		inlineErrors            []*errors.Error
	}{
		{
			desc:   "No completion frame error",
			stream: []frames.Frame{},
			err:    errors.ES(errors.OpUnknown, errors.KInternal, "non-progressive stream did not have DataSetCompletion frame"),
		},
		{
			desc: "Cancelled context",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			stream: []frames.Frame{},
			err:    goErr.New("context canceled"),
		},
		{
			desc:   "No DataSetCompletion Frame",
			stream: []frames.Frame{v2.DataTable{TableKind: frames.PrimaryResult}},
			err:    errors.ES(errors.OpUnknown, errors.KInternal, "non-progressive stream did not have DataSetCompletion frame"),
		},
		{
			desc: "Frame after DataSetCompletion",
			stream: []frames.Frame{
				v2.DataTable{TableKind: frames.PrimaryResult},
				v2.DataSetCompletion{},
				v2.DataTable{TableKind: frames.PrimaryResult},
			},
			err: errors.ES(errors.OpUnknown, errors.KInternal, "saw a DataSetCompletion frame, then received a v2.DataTable frame"),
		},
		{
			desc: "The expected frame set",
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
							value.NewInt(1),
							value.NewString("Visualization"),
							value.NewDynamic([]byte(`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null}`)),
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
							value.NewDateTime(nowish),
							value.NewString("Doak"),
							value.NewLong(10),
						},
						{
							value.NewDateTime(nowish),
							value.NewString("Dubovski"),
							value.NewNullLong(),
						},
					},
				},
				v2.DataTable{
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.QueryCompletionInformation,
					TableName: frames.QueryCompletionInformation,
					Columns: []table.Column{
						{
							Name: "Timestamp",
							Type: "datetime",
						},
						{
							Name: "ClientRequestId",
							Type: "string",
						},
						{
							Name: "ActivityId",
							Type: "guid",
						},
					},
					KustoRows: []value.Values{
						{
							value.NewDateTime(nowish),
							value.NewString("KPC.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3"),
							value.NewGUID(uuid.MustParse("011e7e1b-3c8f-4e91-a04b-0fa5f7be6100")),
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
						value.NewDateTime(nowish),
						value.NewString("Doak"),
						value.NewLong(10),
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
						value.NewDateTime(nowish),
						value.NewString("Dubovski"),
						value.NewNullLong(),
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
							value.NewInt(1),
							value.NewString("Visualization"),
							value.NewDynamic([]byte(`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null}`)),
						},
					},
				},
				frames.QueryCompletionInformation: {
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.QueryCompletionInformation,
					TableName: frames.QueryCompletionInformation,
					Columns: []table.Column{
						{
							Name: "Timestamp",
							Type: "datetime",
						},
						{
							Name: "ClientRequestId",
							Type: "string",
						},
						{
							Name: "ActivityId",
							Type: "guid",
						},
					},
					KustoRows: []value.Values{
						{
							value.NewDateTime(nowish),
							value.NewString("KPC.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3"),
							value.NewGUID(uuid.MustParse("011e7e1b-3c8f-4e91-a04b-0fa5f7be6100")),
						},
					},
				},
			},
		},
		{
			desc: "The expected frame set with inline errors",
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
							value.NewInt(1),
							value.NewString("Visualization"),
							value.NewDynamic([]byte(`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null}`)),
						},
					},
					RowErrors: []errors.Error{
						*errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Request is invalid and cannot be executed.;See https://docs.microsoft."+
							"com/en-us/azure/kusto/concepts/querylimits"),
						*errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Some other error"),
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
							value.NewDateTime(nowish),
							value.NewString("Doak"),
							value.NewLong(10),
						},
						{
							value.NewDateTime(nowish),
							value.NewString("Dubovski"),
							value.NewNullLong(),
						},
					},
					RowErrors: []errors.Error{
						*errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Request is invalid and cannot be executed.;See https://docs.microsoft."+
							"com/en-us/azure/kusto/concepts/querylimits"),
						*errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Some other error"),
					},
				},
				v2.DataTable{
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.QueryCompletionInformation,
					TableName: frames.QueryCompletionInformation,
					Columns: []table.Column{
						{
							Name: "Timestamp",
							Type: "datetime",
						},
						{
							Name: "ClientRequestId",
							Type: "string",
						},
						{
							Name: "ActivityId",
							Type: "guid",
						},
					},
					KustoRows: []value.Values{
						{
							value.NewDateTime(nowish),
							value.NewString("KPC.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3"),
							value.NewGUID(uuid.MustParse("011e7e1b-3c8f-4e91-a04b-0fa5f7be6100")),
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
						value.NewDateTime(nowish),
						value.NewString("Doak"),
						value.NewLong(10),
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
						value.NewDateTime(nowish),
						value.NewString("Dubovski"),
						value.NewNullLong(),
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
							value.NewInt(1),
							value.NewString("Visualization"),
							value.NewDynamic([]byte(`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null}`)),
						},
					},
					RowErrors: []errors.Error{
						*errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Request is invalid and cannot be executed.;See https://docs.microsoft."+
							"com/en-us/azure/kusto/concepts/querylimits"),
						*errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Some other error"),
					},
				},
				frames.QueryCompletionInformation: {
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.QueryCompletionInformation,
					TableName: frames.QueryCompletionInformation,
					Columns: []table.Column{
						{
							Name: "Timestamp",
							Type: "datetime",
						},
						{
							Name: "ClientRequestId",
							Type: "string",
						},
						{
							Name: "ActivityId",
							Type: "guid",
						},
					},
					KustoRows: []value.Values{
						{
							value.NewDateTime(nowish),
							value.NewString("KPC.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3"),
							value.NewGUID(uuid.MustParse("011e7e1b-3c8f-4e91-a04b-0fa5f7be6100")),
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

			if test.ctx == nil {
				test.ctx = func() context.Context {
					return context.Background()
				}
			}

			ctx := test.ctx()

			createSm := func(iter *RowIterator, toSM chan frames.Frame) stateMachine {
				return &nonProgressiveSM{
					iter: iter,
					in:   toSM,
					ctx:  ctx,
					wg:   &sync.WaitGroup{},
				}
			}

			streamStateMachine(test.stream, createSm, func(iter *RowIterator) {
				got, inlineErrors, err := iterateRowsWithErrors(iter)

				assertValues(t, test.err, err, test.want, got, test.inlineErrors, inlineErrors)

				checkNonPrimary(t, test.nonPrimary, iter)
			})

			streamStateMachine(test.stream, createSm, func(iter *RowIterator) {
				got, err := iterateRows(iter)

				testErr := test.err
				if testErr == nil && test.inlineErrors != nil && len(test.inlineErrors) > 0 {
					testErr = test.inlineErrors[0]
				}

				want := test.want
				if test.wantWithoutInlineErrors != nil {
					want = test.wantWithoutInlineErrors
				}

				assertValues(t, testErr, err, want, got, nil, nil)

				checkNonPrimary(t, test.nonPrimary, iter)
			})

		})
	}
}

func TestProgressive(t *testing.T) {
	t.Parallel()

	// TODO(jdoak/daniel): There are other edge cases worth testing for, like TableHeader/Fragments with nonPrimary tables.

	nowish := time.Now().UTC()

	tests := []struct {
		desc                    string
		ctx                     func() context.Context
		stream                  []frames.Frame
		err                     error
		want                    table.Rows
		wantWithoutInlineErrors table.Rows
		nonPrimary              map[frames.TableKind]v2.DataTable
		inlineErrors            []*errors.Error
	}{
		{
			desc:   "No completion frame error",
			stream: []frames.Frame{},
			err:    errors.ES(errors.OpUnknown, errors.KInternal, "received a table stream that did not finish before our input channel, this is usually a return size or time limit"),
		},
		{
			desc: "Cancelled context",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			stream: []frames.Frame{},
			err:    goErr.New("context canceled"),
		},
		{
			desc:   "No frames.DataSetCompletion Frame",
			stream: []frames.Frame{v2.DataTable{TableKind: frames.QueryProperties}},
			err:    errors.ES(errors.OpUnknown, errors.KInternal, "received a table stream that did not finish before our input channel, this is usually a return size or time limit"),
		},
		{
			desc: "dataTable was PrimaryResult",
			stream: []frames.Frame{
				v2.DataTable{TableKind: frames.PrimaryResult},
				v2.DataSetCompletion{},
			},
			err: errors.ES(errors.OpUnknown, errors.KInternal, "progressive stream had dataTable with Kind == PrimaryResult"),
		},
		{
			desc: "Frame after frames.DataSetCompletion",
			stream: []frames.Frame{
				v2.DataTable{TableKind: frames.QueryProperties},
				v2.DataSetCompletion{},
				v2.DataTable{TableKind: frames.QueryProperties},
			},
			err: errors.ES(errors.OpUnknown, errors.KInternal, "received a dataSetCompletion frame and then a v2.DataTable frame"),
		},
		{
			desc: "TableFragment with no TableHeader",
			stream: []frames.Frame{
				v2.DataTable{TableKind: frames.QueryProperties},
				v2.TableFragment{},
			},
			err: errors.ES(errors.OpUnknown, errors.KInternal, "received a TableFragment without a tableHeader"),
		},
		{
			desc: "Had a Primary DataTable",
			err:  errors.ES(errors.OpUnknown, errors.KInternal, "progressive stream had dataTable with Kind == PrimaryResult"),
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
							value.NewInt(1),
							value.NewString("Visualization"),
							value.NewDynamic([]byte(`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null}`)),
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
							value.NewDateTime(nowish),
							value.NewString("Doak"),
							value.NewLong(10),
						},
						{
							value.NewDateTime(nowish),
							value.NewString("Dubovski"),
							value.NewNullLong(),
						},
					},
				},
				v2.DataSetCompletion{},
			},
		},
		{
			desc: "Expected Result",
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
							value.NewDateTime(nowish),
							value.NewString("Doak"),
							value.NewLong(10),
						},
					},
				},
				v2.TableFragment{
					KustoRows: []value.Values{
						{
							value.NewDateTime(nowish),
							value.NewString("Dubovski"),
							value.NewNullLong(),
						},
						{
							value.NewDateTime(nowish),
							value.NewString("Evcpwtlj"),
							value.NewLong(1),
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
							value.NewInt(1),
							value.NewString("Visualization"),
							value.NewDynamic([]byte(`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null}`)),
						},
					},
				},
				v2.DataTable{
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.QueryCompletionInformation,
					TableName: frames.QueryCompletionInformation,
					Columns: []table.Column{
						{
							Name: "Timestamp",
							Type: "datetime",
						},
						{
							Name: "ClientRequestId",
							Type: "string",
						},
						{
							Name: "ActivityId",
							Type: "guid",
						},
					},
					KustoRows: []value.Values{
						{
							value.NewDateTime(nowish),
							value.NewString("KPC.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3"),
							value.NewGUID(uuid.MustParse("011e7e1b-3c8f-4e91-a04b-0fa5f7be6100")),
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
						value.NewDateTime(nowish),
						value.NewString("Doak"),
						value.NewLong(10),
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
						value.NewDateTime(nowish),
						value.NewString("Dubovski"),
						value.NewNullLong(),
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
						value.NewDateTime(nowish),
						value.NewString("Evcpwtlj"),
						value.NewLong(1),
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
							value.NewInt(1),
							value.NewString("Visualization"),
							value.NewDynamic([]byte(`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null}`)),
						},
					},
				},
				frames.QueryCompletionInformation: {
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.QueryCompletionInformation,
					TableName: frames.QueryCompletionInformation,
					Columns: []table.Column{
						{
							Name: "Timestamp",
							Type: "datetime",
						},
						{
							Name: "ClientRequestId",
							Type: "string",
						},
						{
							Name: "ActivityId",
							Type: "guid",
						},
					},
					KustoRows: []value.Values{
						{
							value.NewDateTime(nowish),
							value.NewString("KPC.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3"),
							value.NewGUID(uuid.MustParse("011e7e1b-3c8f-4e91-a04b-0fa5f7be6100")),
						},
					},
				},
			},
		},
		{
			desc: "Expected Result with inline errors",
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
							value.NewDateTime(nowish),
							value.NewString("Doak"),
							value.NewLong(10),
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
							value.NewDateTime(nowish),
							value.NewString("Dubovski"),
							value.NewNullLong(),
						},
						{
							value.NewDateTime(nowish),
							value.NewString("Evcpwtlj"),
							value.NewLong(1),
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
							value.NewInt(1),
							value.NewString("Visualization"),
							value.NewDynamic([]byte(`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null}`)),
						},
					},
				},
				v2.DataTable{
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.QueryCompletionInformation,
					TableName: frames.QueryCompletionInformation,
					Columns: []table.Column{
						{
							Name: "Timestamp",
							Type: "datetime",
						},
						{
							Name: "ClientRequestId",
							Type: "string",
						},
						{
							Name: "ActivityId",
							Type: "guid",
						},
					},
					KustoRows: []value.Values{
						{
							value.NewDateTime(nowish),
							value.NewString("KPC.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3"),
							value.NewGUID(uuid.MustParse("011e7e1b-3c8f-4e91-a04b-0fa5f7be6100")),
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
						value.NewDateTime(nowish),
						value.NewString("Doak"),
						value.NewLong(10),
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
						value.NewDateTime(nowish),
						value.NewString("Dubovski"),
						value.NewNullLong(),
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
						value.NewDateTime(nowish),
						value.NewString("Evcpwtlj"),
						value.NewLong(1),
					},
					Op: errors.OpQuery,
				},
			},
			wantWithoutInlineErrors: table.Rows{
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.NewDateTime(nowish),
						value.NewString("Doak"),
						value.NewLong(10),
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
							value.NewInt(1),
							value.NewString("Visualization"),
							value.NewDynamic([]byte(`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null}`)),
						},
					},
				},
				frames.QueryCompletionInformation: {
					Base:      v2.Base{FrameType: frames.TypeDataTable},
					TableKind: frames.QueryCompletionInformation,
					TableName: frames.QueryCompletionInformation,
					Columns: []table.Column{
						{
							Name: "Timestamp",
							Type: "datetime",
						},
						{
							Name: "ClientRequestId",
							Type: "string",
						},
						{
							Name: "ActivityId",
							Type: "guid",
						},
					},
					KustoRows: []value.Values{
						{
							value.NewDateTime(nowish),
							value.NewString("KPC.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3"),
							value.NewGUID(uuid.MustParse("011e7e1b-3c8f-4e91-a04b-0fa5f7be6100")),
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
			if test.ctx == nil {
				test.ctx = func() context.Context {
					return context.Background()
				}
			}

			ctx := test.ctx()

			createSm := func(iter *RowIterator, toSM chan frames.Frame) stateMachine {
				return &progressiveSM{
					iter: iter,
					in:   toSM,
					ctx:  ctx,
					wg:   &sync.WaitGroup{},
				}
			}

			streamStateMachine(test.stream, createSm, func(iter *RowIterator) {
				got, inlineErrors, err := iterateRowsWithErrors(iter)

				assertValues(t, test.err, err, test.want, got, test.inlineErrors, inlineErrors)

				checkNonPrimary(t, test.nonPrimary, iter)
			})

			streamStateMachine(test.stream, createSm, func(iter *RowIterator) {
				got, err := iterateRows(iter)

				testErr := test.err
				if testErr == nil && test.inlineErrors != nil && len(test.inlineErrors) > 0 {
					testErr = test.inlineErrors[0]
				}

				want := test.want
				if test.wantWithoutInlineErrors != nil {
					want = test.wantWithoutInlineErrors
				}

				assertValues(t, testErr, err, want, got, nil, nil)

				checkNonPrimary(t, test.nonPrimary, iter)
			})
		})
	}
}

func TestV1SM(t *testing.T) {
	t.Parallel()

	nowish := time.Now().UTC()

	tests := []struct {
		desc                    string
		ctx                     func() context.Context
		stream                  []frames.Frame
		err                     error
		want                    table.Rows
		wantWithoutInlineErrors table.Rows
		inlineErrors            []*errors.Error
	}{
		{
			desc:   "No DataTable frame error",
			stream: []frames.Frame{},
			err:    errors.ES(errors.OpUnknown, errors.KInternal, "received a table stream that did not finish before our input channel, this is usually a return size or time limit"),
		},
		{
			desc: "Cancelled context",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			stream: []frames.Frame{},
			err:    goErr.New("context canceled"),
		},
		{
			desc: "Single Table",
			stream: []frames.Frame{
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Timestamp", ColumnType: "datetime"},
						{ColumnName: "Name", ColumnType: "string"},
						{ColumnName: "ID", ColumnType: "long"},
					},
					KustoRows: []value.Values{
						{
							value.NewDateTime(nowish),
							value.NewString("Doak"),
							value.NewLong(10),
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
						value.NewDateTime(nowish),
						value.NewString("Doak"),
						value.NewLong(10),
					},
					Op: errors.OpQuery,
				},
			},
		},
		{
			desc: "Primary And QueryProperties",
			stream: []frames.Frame{
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Timestamp", ColumnType: "datetime"},
						{ColumnName: "Name", ColumnType: "string"},
						{ColumnName: "ID", ColumnType: "long"},
					},
					KustoRows: []value.Values{
						{
							value.NewDateTime(nowish),
							value.NewString("Doak"),
							value.NewLong(10),
						},
					},
				},
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Value", ColumnType: "string"},
					},
					KustoRows: []value.Values{
						{
							value.NewString("{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\"}"),
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
						value.NewDateTime(nowish),
						value.NewString("Doak"),
						value.NewLong(10),
					},
					Op: errors.OpQuery,
				},
			},
		},
		{
			desc: "Primary With TableOfContents",
			stream: []frames.Frame{
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Timestamp", ColumnType: "datetime"},
						{ColumnName: "Name", ColumnType: "string"},
						{ColumnName: "ID", ColumnType: "long"},
					},
					KustoRows: []value.Values{
						{
							value.NewDateTime(nowish),
							value.NewString("Doak"),
							value.NewLong(10),
						},
					},
				},
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Value", ColumnType: "string"},
					},
					KustoRows: []value.Values{
						{
							value.NewString("{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\"}"),
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
							value.NewLong(0),
							value.NewString("QueryResult"),
							value.NewString("PrimaryResult"),
							value.NewString("07dd9603-3e06-4c62-986b-dfc3d586b05a"),
							value.NewString(""),
						},
						{
							value.NewLong(1),
							value.NewString("QueryProperties"),
							value.NewString("@ExtendedProperties"),
							value.NewString("309c015e-5693-4b66-92e7-4a4f98c3155b"),
							value.NewString(""),
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
						value.NewDateTime(nowish),
						value.NewString("Doak"),
						value.NewLong(10),
					},
					Op: errors.OpQuery,
				},
			},
		},
		{
			desc: "Multiple Primaries",
			stream: []frames.Frame{
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Timestamp", ColumnType: "datetime"},
						{ColumnName: "Name", ColumnType: "string"},
						{ColumnName: "ID", ColumnType: "long"},
					},
					KustoRows: []value.Values{
						{
							value.NewDateTime(nowish),
							value.NewString("Doak"),
							value.NewLong(10),
						},
					},
				},
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Value", ColumnType: "string"},
					},
					KustoRows: []value.Values{
						{
							value.NewString("{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\"}"),
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
							value.NewDateTime(nowish),
							value.NewString("DD"),
							value.NewLong(101),
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
							value.NewLong(0),
							value.NewString("QueryResult"),
							value.NewString("PrimaryResult"),
							value.NewString("07dd9603-3e06-4c62-986b-dfc3d586b05a"),
							value.NewString(""),
						},
						{
							value.NewLong(1),
							value.NewString("QueryProperties"),
							value.NewString("@ExtendedProperties"),
							value.NewString("309c015e-5693-4b66-92e7-4a4f98c3155b"),
							value.NewString(""),
						},
						{
							value.NewLong(2),
							value.NewString("QueryResult"),
							value.NewString("PrimaryResult"),
							value.NewString("07dd9603-3e06-4c62-986b-dfc3d586b05a"),
							value.NewString(""),
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
						value.NewDateTime(nowish),
						value.NewString("Doak"),
						value.NewLong(10),
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
						value.NewDateTime(nowish),
						value.NewString("DD"),
						value.NewLong(101),
					},
					Op: errors.OpQuery,
				},
			},
		},
		{
			desc: "Multiple Primaries with errors",
			stream: []frames.Frame{
				v1.DataTable{
					DataTypes: v1.DataTypes{
						{ColumnName: "Timestamp", ColumnType: "datetime"},
						{ColumnName: "Name", ColumnType: "string"},
						{ColumnName: "ID", ColumnType: "long"},
					},
					KustoRows: []value.Values{
						{
							value.NewDateTime(nowish),
							value.NewString("Doak"),
							value.NewLong(10),
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
							value.NewString("{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\"}"),
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
							value.NewDateTime(nowish),
							value.NewString("DD"),
							value.NewLong(101),
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
							value.NewLong(0),
							value.NewString("QueryResult"),
							value.NewString("PrimaryResult"),
							value.NewString("07dd9603-3e06-4c62-986b-dfc3d586b05a"),
							value.NewString(""),
						},
						{
							value.NewLong(1),
							value.NewString("QueryProperties"),
							value.NewString("@ExtendedProperties"),
							value.NewString("309c015e-5693-4b66-92e7-4a4f98c3155b"),
							value.NewString(""),
						},
						{
							value.NewLong(2),
							value.NewString("QueryResult"),
							value.NewString("PrimaryResult"),
							value.NewString("07dd9603-3e06-4c62-986b-dfc3d586b05a"),
							value.NewString(""),
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
						value.NewDateTime(nowish),
						value.NewString("Doak"),
						value.NewLong(10),
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
						value.NewDateTime(nowish),
						value.NewString("DD"),
						value.NewLong(101),
					},
					Op: errors.OpQuery,
				},
			},
			wantWithoutInlineErrors: table.Rows{
				&table.Row{
					ColumnTypes: table.Columns{
						{Name: "Timestamp", Type: "datetime"},
						{Name: "Name", Type: "string"},
						{Name: "ID", Type: "long"},
					},
					Values: value.Values{
						value.NewDateTime(nowish),
						value.NewString("Doak"),
						value.NewLong(10),
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

			if test.ctx == nil {
				test.ctx = func() context.Context {
					return context.Background()
				}
			}

			ctx := test.ctx()

			createSm := func(iter *RowIterator, toSM chan frames.Frame) stateMachine {
				return &v1SM{
					iter: iter,
					in:   toSM,
					ctx:  ctx,
					wg:   &sync.WaitGroup{},
				}
			}
			streamStateMachine(test.stream, createSm, func(iter *RowIterator) {
				got, inlineErrors, err := iterateRowsWithErrors(iter)

				assertValues(t, test.err, err, test.want, got, test.inlineErrors, inlineErrors)
			})

			streamStateMachine(test.stream, createSm, func(iter *RowIterator) {
				got, err := iterateRows(iter)

				testErr := test.err
				if testErr == nil && test.inlineErrors != nil && len(test.inlineErrors) > 0 {
					testErr = test.inlineErrors[0]
				}

				want := test.want
				if test.wantWithoutInlineErrors != nil {
					want = test.wantWithoutInlineErrors
				}

				assertValues(t, testErr, err, want, got, nil, nil)
			})

		})
	}
}
