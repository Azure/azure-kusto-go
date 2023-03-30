package kusto_test

import (
	"context"
	"errors"
	"fmt"
	kustoErrors "github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/kql"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/Azure/azure-kusto-go/kusto/data/value"

	"github.com/kylelemons/godebug/pretty"
)

/*****************************************/
// What would be in our package
/*****************************************/

// querier provides a single method, Query(), which is used to query Kusto for some information.
// This can be substituted for our fake during tests.
type querier interface {
	Query(context.Context, string, kusto.Statement, ...kusto.QueryOption) (*kusto.RowIterator, error)
}

// NodeRec represents our Kusto data that will be returned.
type NodeRec struct {
	// ID is the table's NodeId. We use the field tag here to to instruct our client to convert NodeId to ID.
	ID int64 `kusto:"NodeId"`
	// CollectionTime is Go representation of the Kusto datetime type.
	CollectionTime time.Time
}

// NodeInfo is the type we are going to test.
type NodeInfo struct {
	stmt    *kql.Builder
	querier querier // This can be a fakeQuerier or *kusto.Client
}

// New is the constructor for NodeInfo.
func New(client *kusto.Client) (*NodeInfo, error) {
	return &NodeInfo{
		querier: client,
		stmt:    kql.New("systemNodes | project CollectionTime, NodeId | where NodeId == ParamNodeId"),
	}, nil
}

// Node queries the datastore for the Node with ID "id".
func (n *NodeInfo) Node(ctx context.Context, id int64) (NodeRec, error) {
	iter, err := n.querier.Query(ctx, "db", n.stmt, kusto.QueryParameters(kql.NewParameters().AddLong("ParamNodeId", id)))
	if err != nil {
		return NodeRec{}, err
	}

	rec := NodeRec{}
	err = iter.DoOnRowOrError(
		func(row *table.Row, _ *kustoErrors.Error) error {
			return row.ToStruct(&rec)
		},
	)
	if rec.ID == 0 {
		return rec, fmt.Errorf("could not find Node with ID %d", id)
	}

	return rec, err
}

/*****************************************/
// What would be in our _test.go file
/*****************************************/

// fakeQuerier implements querier.querier so we can do hermetic testing.
type fakeQuerier struct {
	mock        *kusto.MockRows
	expectQuery string
}

// Query implements querier.querier.
func (f *fakeQuerier) Query(_ context.Context, _ string, passedQuery kusto.Statement, _ ...kusto.QueryOption) (*kusto.RowIterator, error) {
	if passedQuery.String() != f.expectQuery {
		panic("we expect the query to be " + f.expectQuery)
	}

	ri := &kusto.RowIterator{}
	_ = ri.Mock(f.mock)
	return ri, nil
}

func ExampleMockRows(t *testing.T) { // nolint:govet // Example code
	now := time.Now()

	tests := []struct {
		desc      string // Description of the what the test is doing
		id        int64
		rows      []value.Values // The rows to return
		kustoErr  bool           // If the mock should return an error to the iterator
		wantQuery string         // The query string that the Stmt should give us

		err  bool    // If we expected an error
		want NodeRec // What we expect to get if there is no error
	}{
		{
			desc:      "Error: kusto returns an error",
			id:        1,
			wantQuery: "systemNodes | project CollectionTime, NodeId | where NodeId == 1",
			kustoErr:  true,
			err:       true,
		},
		{
			desc:      "Error: No records returned",
			id:        1,
			wantQuery: "systemNodes | project CollectionTime, NodeId | where NodeId == 1",
			err:       true,
		},
		{
			desc:      "Success",
			id:        1,
			wantQuery: "systemNodes | project CollectionTime, NodeId | where NodeId == 1",
			rows: []value.Values{
				{value.Long{Valid: true, Value: 1}, value.DateTime{Valid: true, Value: now}},
			},
			want: NodeRec{ID: 1, CollectionTime: now},
		},
	}

	var columns = table.Columns{
		{Name: "NodeId", Type: types.Long},
		{Name: "CollectedOn", Type: types.DateTime},
	}

	for _, test := range tests {
		// Create our mock replay from the expected columns and data we want from this test.
		m, err := kusto.NewMockRows(columns)
		if err != nil {
			panic(err) // This panic and all others are setup errors, not test errors
		}

		for _, row := range test.rows {
			if err := m.Row(row); err != nil {
				panic(err)
			}
		}
		if test.kustoErr {
			_ = m.Error(errors.New("kusto error"))
		}

		// Create our client and add in our fake querier, which pretends to be Kusto.
		info, err := New(nil)
		if err != nil {
			panic(err)
		}
		info.querier = &fakeQuerier{mock: m, expectQuery: test.wantQuery}

		// Run our test.
		got, err := info.Node(context.Background(), test.id)
		switch {
		case err == nil && test.err:
			t.Errorf("TestNodeInfo(%s): got err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestNodeInfo(%s): got err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestNodeInfo(%s): -want/+got:\n%s", test.desc, diff)
		}
	}
}
