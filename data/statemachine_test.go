package data

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"azure-kusto-go/data/errors"
	"azure-kusto-go/data/types"

	"github.com/kylelemons/godebug/pretty"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestNonProgressive(t *testing.T) {
	nowish := time.Now().UTC()

	tests := []struct {
		desc       string
		ctx        context.Context
		stream     []frame
		err        bool
		want       Rows
		nonPrimary map[string]frame
	}{
		{
			desc:   "No completion frame error",
			ctx:    context.Background(),
			stream: []frame{},
			err:    true,
		},
		{
			desc: "Cancelled context",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			}(),
			stream: []frame{},
			err:    true,
		},
		{
			desc:   "No DataSetCompletion Frame",
			ctx:    context.Background(),
			stream: []frame{dataTable{TableKind: tkPrimaryResult}},
			err:    true,
		},
		{
			desc: "Frame after DataSetCompletion",
			ctx:  context.Background(),
			stream: []frame{
				dataTable{TableKind: tkPrimaryResult},
				dataSetCompletion{},
				dataTable{TableKind: tkPrimaryResult},
			},
			err: true,
		},
		{
			desc: "The expected frame set",
			ctx:  context.Background(),
			stream: []frame{
				dataTable{
					baseFrame: baseFrame{FrameType: ftDataTable},
					TableKind: tkQueryProperties,
					TableName: tnExtendedProperties,
					Columns: Columns{
						{ColumnName: "TableId", ColumnType: "int"},
						{ColumnName: "Key", ColumnType: "string"},
						{ColumnName: "Value", ColumnType: "dynamic"},
					},
					Rows: []types.KustoValues{
						{
							types.Int{Value: 1, Valid: true},
							types.String{Value: "Visualization", Valid: true},
							types.Dynamic{Value: `{\\"Visualization\\":null,\\"Title\\":null,\\"XColumn\\":null,\\"Series\\":null,\\"YColumns\\":null,\\"XTitle\\":null,\\`, Valid: true},
						},
					},
				},
				dataTable{
					baseFrame: baseFrame{FrameType: ftDataTable},
					TableKind: tkPrimaryResult,
					TableName: tnPrimaryResult,
					Columns: Columns{
						{ColumnName: "Timestamp", ColumnType: "datetime"},
						{ColumnName: "Name", ColumnType: "string"},
						{ColumnName: "ID", ColumnType: "long"},
					},
					Rows: []types.KustoValues{
						{
							types.DateTime{Value: nowish, Valid: true},
							types.String{Value: "Doak", Valid: true},
							types.Long{Value: 10, Valid: true},
						},
						{
							types.DateTime{Value: nowish, Valid: true},
							types.String{Value: "Dubovski", Valid: true},
							types.Long{Value: 0, Valid: false},
						},
					},
				},
				dataSetCompletion{},
			},
			want: Rows{
				&Row{
					columns: Columns{
						{ColumnName: "Timestamp", ColumnType: "datetime"},
						{ColumnName: "Name", ColumnType: "string"},
						{ColumnName: "ID", ColumnType: "long"},
					},
					row: types.KustoValues{
						types.DateTime{Value: nowish, Valid: true},
						types.String{Value: "Doak", Valid: true},
						types.Long{Value: 10, Valid: true},
					},
					op: errors.OpQuery,
				},
				&Row{
					columns: Columns{
						{ColumnName: "Timestamp", ColumnType: "datetime"},
						{ColumnName: "Name", ColumnType: "string"},
						{ColumnName: "ID", ColumnType: "long"},
					},
					row: types.KustoValues{
						types.DateTime{Value: nowish, Valid: true},
						types.String{Value: "Dubovski", Valid: true},
						types.Long{Value: 0, Valid: false},
					},
					op: errors.OpQuery,
				},
			},
			nonPrimary: map[string]frame{
				tkQueryProperties: dataTable{
					baseFrame: baseFrame{FrameType: ftDataTable},
					TableKind: tkQueryProperties,
					TableName: tnExtendedProperties,
					Columns: Columns{
						{ColumnName: "TableId", ColumnType: "int"},
						{ColumnName: "Key", ColumnType: "string"},
						{ColumnName: "Value", ColumnType: "dynamic"},
					},
					Rows: []types.KustoValues{
						{
							types.Int{Value: 1, Valid: true},
							types.String{Value: "Visualization", Valid: true},
							types.Dynamic{Value: `{\\"Visualization\\":null,\\"Title\\":null,\\"XColumn\\":null,\\"Series\\":null,\\"YColumns\\":null,\\"XTitle\\":null,\\`, Valid: true},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		// Sends the frames like the upstream provider.
		toSM := make(chan frame)
		go func() {
			defer close(toSM)
			for _, fr := range test.stream {
				toSM <- fr
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		iter, gotColumns := newRowIterator(ctx, cancel, dataSetHeader{}, errors.OpQuery)

		sm := nonProgressiveSM{
			iter: iter,
			in:   toSM,
			ctx:  test.ctx,
		}

		runSM(&sm)
		<-gotColumns

		// Pulls the frames from the downstream RowIterator.
		got := Rows{}
		var err error
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = sm.iter.Do(func(r *Row) error {
				got = append(got, r)
				return nil
			})
		}()

		wg.Wait()

		switch {
		case err == nil && test.err:
			t.Errorf("TestNonProgressive(%s): got err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestNonProgressive(%s): got err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestNonProgressive(%s): -want/+got(Rows):\n%s", test.desc, diff)
			continue
		}

		if diff := pretty.Compare(test.nonPrimary, iter.nonPrimary); diff != "" {
			t.Errorf("TestNonProgressive(%s) -want/+got(nonPrimary):\n%s", test.desc, diff)
		}
	}
}

func TestProgressive(t *testing.T) {
	//nowish := time.Now().UTC()

	tests := []struct {
		desc       string
		ctx        context.Context
		stream     []frame
		err        bool
		want       Rows
		nonPrimary map[string]frame
	}{
		{
			desc:   "No completion frame error",
			ctx:    context.Background(),
			stream: []frame{},
			err:    true,
		},
		{
			desc: "Cancelled context",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			}(),
			stream: []frame{},
			err:    true,
		},
		{
			desc:   "No dataSetCompletion Frame",
			ctx:    context.Background(),
			stream: []frame{dataTable{TableKind: tkQueryProperties}},
			err:    true,
		},
		{
			desc: "dataTable was PrimaryResult",
			ctx:  context.Background(),
			stream: []frame{
				dataTable{TableKind: tkPrimaryResult},
				dataSetCompletion{},
			},
			err: true,
		},
		{
			desc: "Frame after dataSetCompletion",
			ctx:  context.Background(),
			stream: []frame{
				dataTable{TableKind: tkQueryProperties},
				dataSetCompletion{},
				dataTable{TableKind: tkQueryProperties},
			},
			err: true,
		},
		{
			desc: "TableFragment with no TableHeader",
			ctx:  context.Background(),
			stream: []frame{
				dataTable{TableKind: tkQueryProperties},
				tableFragment{},
			},
			err: true,
		},
		/*
			{
				desc: "The expected frame set",
				ctx:  context.Background(),
				stream: []Frame{
					DataTable{
						baseFrame: baseFrame{FrameType: FTDataTable},
						TableKind: tkQueryProperties,
						TableName: tnExtendedProperties,
						Columns: Columns{
							{ColumnName: "TableId", ColumnType: "int"},
							{ColumnName: "Key", ColumnType: "string"},
							{ColumnName: "Value", ColumnType: "dynamic"},
						},
						Rows: []types.KustoValues{
							{
								types.Int{Value: 1, Valid: true},
								types.String{Value: "Visualization", Valid: true},
								types.Dynamic{Value: `{\\"Visualization\\":null,\\"Title\\":null,\\"XColumn\\":null,\\"Series\\":null,\\"YColumns\\":null,\\"XTitle\\":null,\\`, Valid: true},
							},
						},
					},
					DataTable{
						baseFrame: baseFrame{FrameType: FTDataTable},
						TableKind: tkPrimaryResult,
						TableName: tnPrimaryResult,
						Columns: Columns{
							{ColumnName: "Timestamp", ColumnType: "datetime"},
							{ColumnName: "Name", ColumnType: "string"},
							{ColumnName: "ID", ColumnType: "long"},
						},
						Rows: []types.KustoValues{
							{
								types.DateTime{Value: nowish, Valid: true},
								types.String{Value: "Doak", Valid: true},
								types.Long{Value: 10, Valid: true},
							},
							{
								types.DateTime{Value: nowish, Valid: true},
								types.String{Value: "Dubovski", Valid: true},
								types.Long{Value: 0, Valid: false},
							},
						},
					},
					DataSetCompletion{},
				},
				want: Rows{
					&Row{
						columns: Columns{
							{ColumnName: "Timestamp", ColumnType: "datetime"},
							{ColumnName: "Name", ColumnType: "string"},
							{ColumnName: "ID", ColumnType: "long"},
						},
						row: types.KustoValues{
							types.DateTime{Value: nowish, Valid: true},
							types.String{Value: "Doak", Valid: true},
							types.Long{Value: 10, Valid: true},
						},
						op: errors.OpQuery,
					},
					&Row{
						columns: Columns{
							{ColumnName: "Timestamp", ColumnType: "datetime"},
							{ColumnName: "Name", ColumnType: "string"},
							{ColumnName: "ID", ColumnType: "long"},
						},
						row: types.KustoValues{
							types.DateTime{Value: nowish, Valid: true},
							types.String{Value: "Dubovski", Valid: true},
							types.Long{Value: 0, Valid: false},
						},
						op: errors.OpQuery,
					},
				},
				nonPrimary: map[string]Frame{
					tkQueryProperties: DataTable{
						baseFrame: baseFrame{FrameType: FTDataTable},
						TableKind: tkQueryProperties,
						TableName: tnExtendedProperties,
						Columns: Columns{
							{ColumnName: "TableId", ColumnType: "int"},
							{ColumnName: "Key", ColumnType: "string"},
							{ColumnName: "Value", ColumnType: "dynamic"},
						},
						Rows: []types.KustoValues{
							{
								types.Int{Value: 1, Valid: true},
								types.String{Value: "Visualization", Valid: true},
								types.Dynamic{Value: `{\\"Visualization\\":null,\\"Title\\":null,\\"XColumn\\":null,\\"Series\\":null,\\"YColumns\\":null,\\"XTitle\\":null,\\`, Valid: true},
							},
						},
					},
				},
			},
		*/
	}

	for _, test := range tests {
		// Sends the frames like the upstream provider.
		toSM := make(chan frame)
		go func() {
			defer close(toSM)
			for _, fr := range test.stream {
				toSM <- fr
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		iter, gotColumns := newRowIterator(ctx, cancel, dataSetHeader{}, errors.OpQuery)

		sm := progressiveSM{
			iter: iter,
			in:   toSM,
			ctx:  test.ctx,
		}

		runSM(&sm)
		<-gotColumns

		// Pulls the frames from the downstream RowIterator.
		got := Rows{}
		var err error
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = sm.iter.Do(func(r *Row) error {
				got = append(got, r)
				return nil
			})
		}()

		wg.Wait()

		switch {
		case err == nil && test.err:
			t.Errorf("TestProgressive(%s): got err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestProgressive(%s): got err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestProgressive(%s): -want/+got(Rows):\n%s", test.desc, diff)
			continue
		}

		if diff := pretty.Compare(test.nonPrimary, iter.nonPrimary); diff != "" {
			t.Errorf("TestProgressive(%s) -want/+got(nonPrimary):\n%s", test.desc, diff)
		}
	}
}
