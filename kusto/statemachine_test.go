package kusto

import (
	"context"
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

	"github.com/kylelemons/godebug/pretty"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestNonProgressive(t *testing.T) {
	t.Parallel()

	nowish := time.Now().UTC()

	tests := []struct {
		desc       string
		ctx        context.Context
		stream     []frames.Frame
		err        bool
		want       table.Rows
		nonPrimary map[frames.TableKind]frames.Frame
	}{
		{
			desc:   "No completion frame error",
			ctx:    context.Background(),
			stream: []frames.Frame{},
			err:    true,
		},
		{
			desc: "Cancelled context",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			}(),
			stream: []frames.Frame{},
			err:    true,
		},
		{
			desc:   "No DataSetCompletion Frame",
			ctx:    context.Background(),
			stream: []frames.Frame{v2.DataTable{TableKind: frames.PrimaryResult}},
			err:    true,
		},
		{
			desc: "Frame after DataSetCompletion",
			ctx:  context.Background(),
			stream: []frames.Frame{
				v2.DataTable{TableKind: frames.PrimaryResult},
				v2.DataSetCompletion{},
				v2.DataTable{TableKind: frames.PrimaryResult},
			},
			err: true,
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
			nonPrimary: map[frames.TableKind]frames.Frame{
				frames.QueryProperties: v2.DataTable{
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
	}

	for _, test := range tests {
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

		// Pulls the frames from the downstream RowIterator.
		got := table.Rows{}
		err := sm.iter.Do(func(r *table.Row) error {
			got = append(got, r)
			return nil
		})

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
	t.Parallel()

	// TODO(jdoak/daniel): There are other edge cases worth testing for, like TableHeader/Fragments with nonPrimary tables.

	nowish := time.Now().UTC()

	tests := []struct {
		desc       string
		ctx        context.Context
		stream     []frames.Frame
		err        bool
		want       table.Rows
		nonPrimary map[frames.TableKind]frames.Frame
	}{
		{
			desc:   "No completion frame error",
			ctx:    context.Background(),
			stream: []frames.Frame{},
			err:    true,
		},
		{
			desc: "Cancelled context",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			}(),
			stream: []frames.Frame{},
			err:    true,
		},
		{
			desc:   "No frames.DataSetCompletion Frame",
			ctx:    context.Background(),
			stream: []frames.Frame{v2.DataTable{TableKind: frames.QueryProperties}},
			err:    true,
		},
		{
			desc: "dataTable was PrimaryResult",
			ctx:  context.Background(),
			stream: []frames.Frame{
				v2.DataTable{TableKind: frames.PrimaryResult},
				v2.DataSetCompletion{},
			},
			err: true,
		},
		{
			desc: "Frame after frames.DataSetCompletion",
			ctx:  context.Background(),
			stream: []frames.Frame{
				v2.DataTable{TableKind: frames.QueryProperties},
				v2.DataSetCompletion{},
				v2.DataTable{TableKind: frames.QueryProperties},
			},
			err: true,
		},
		{
			desc: "TableFragment with no TableHeader",
			ctx:  context.Background(),
			stream: []frames.Frame{
				v2.DataTable{TableKind: frames.QueryProperties},
				v2.TableFragment{},
			},
			err: true,
		},
		{
			desc: "Had a Primary DataTable",
			err:  true,
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
			nonPrimary: map[frames.TableKind]frames.Frame{
				frames.QueryProperties: v2.DataTable{
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
					},
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
					Op: errors.OpQuery,
				},
			},
			nonPrimary: map[frames.TableKind]frames.Frame{
				frames.QueryProperties: v2.DataTable{
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
	}

	// TODO(jdoak): This could use some cleanup. Rarely are their reasons to have WaitGroup and channel canceling.
	// That was there to prevent "test" from being used in the goroutine still when an error had occured. But I think
	// we can do better.
	for _, test := range tests {
		// Sends the frames like the upstream provider.
		toSM := make(chan frames.Frame)
		sendCtx, sendCancel := context.WithCancel(context.Background())
		sendDone := make(chan struct{})
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

		// Pulls the frames from the downstream RowIterator.
		got := table.Rows{}
		err := sm.iter.Do(func(r *table.Row) error {
			got = append(got, r)
			return nil
		})

		switch {
		case err == nil && test.err:
			t.Errorf("TestProgressive(%s): got err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			sendCancel()
			<-sendDone
			t.Errorf("TestProgressive(%s): got err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			sendCancel()
			<-sendDone
			continue
		}

		<-sendDone

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestProgressive(%s): -want/+got(Rows):\n%s", test.desc, diff)
			continue
		}

		if diff := pretty.Compare(test.nonPrimary, iter.nonPrimary); diff != "" {
			t.Errorf("TestProgressive(%s) -want/+got(nonPrimary):\n%s", test.desc, diff)
		}
	}
}

func TestV1SM(t *testing.T) {
	t.Parallel()

	nowish := time.Now().UTC()

	tests := []struct {
		desc   string
		ctx    context.Context
		stream []frames.Frame
		err    bool
		want   table.Rows
	}{
		{
			desc:   "No DataTable frame error",
			ctx:    context.Background(),
			stream: []frames.Frame{},
			err:    true,
		},
		{
			desc: "Cancelled context",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			}(),
			stream: []frames.Frame{},
			err:    true,
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
						{
							value.Long{Value: 0, Valid: true},
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
						value.String{Value: "DD", Valid: true},
						value.Long{Value: 101, Valid: true},
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
						value.String{Value: "Doak", Valid: true},
						value.Long{Value: 10, Valid: true},
					},
					Op: errors.OpQuery,
				},
			},
		},
	}

	for _, test := range tests {
		// Sends the frames like the upstream provider.
		toSM := make(chan frames.Frame)

		sendCtx, sendCancel := context.WithCancel(context.Background())
		sendDone := make(chan struct{})
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

		// Pulls the frames from the downstream RowIterator.
		got := table.Rows{}
		err := sm.iter.Do(func(r *table.Row) error {
			got = append(got, r)
			return nil
		})

		switch {
		case err == nil && test.err:
			t.Errorf("TestV1SM(%s): got err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			sendCancel()
			<-sendDone
			t.Errorf("TestV1SM(%s): got err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			sendCancel()
			<-sendDone
			continue
		}

		<-sendDone

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestV1SM(%s): -want/+got(Rows):\n%s", test.desc, diff)
			continue
		}
	}
}
