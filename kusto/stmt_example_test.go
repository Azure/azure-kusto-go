package kusto_test

import (
	"fmt"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/data/types"
)

const (
	// qRoot is a a root query text that will be used in a Stmt. It contains everything that any
	// other Stmt objects will need.
	qRoot = "set notruncation;dcmInventoryComponentSystem\n| project NodeId, Version"
	// singleNode includes syntax that will be included only when we want to grab a single node.
	// The word "Node" here is substituted for variable.
	singleNode = "\n| where NodeId == Node\n"
)

var (
	// rootStmt represents our root Stmt object in which we can derive other Stmts. This definition
	// will always include NodeId and Version fields.
	rootStmt = kusto.NewStmt(qRoot)
	// singleStmt is derived from the rootStmt but includes a where clause to limit the query to a
	// single node. You will see that we input a Definitions object to define the "Node" word in the
	// query as a string.
	singleStmt = rootStmt.Add(singleNode).MustDefinitions(
		kusto.NewDefinitions().Must(
			kusto.ParamTypes{
				"Node": kusto.ParamType{Type: types.String},
			},
		),
	)
)

func ExampleStmt() {
	var err error

	// This will print the rootStmt that could be used to list all nodes in the table.
	fmt.Println("All Nodes Statement:\n", rootStmt.String())

	// If we wanted to query for a single node, we could build a Stmt fron singleStmt like so:
	params := kusto.NewParameters()
	params, err = params.With(kusto.QueryValues{"Node": "my_id"}) // Substitute "my_id" in every place in the query where "Node" is
	if err != nil {
		panic(err)
	}

	stmt, err := singleStmt.WithParameters(params)
	if err != nil {
		panic(err)
	}

	fmt.Println("Single Statement:\n", stmt)
	j, err := stmt.ValuesJSON()
	if err != nil {
		panic(err)
	}
	fmt.Println("Single Statement Parameters:\n", j)

	// Here is a more condensed version:
	stmt, err = singleStmt.WithParameters(kusto.NewParameters().Must(kusto.QueryValues{"Node": "my_id2"}))
	if err != nil {
		panic(err)
	}

	fmt.Println("Single Statement(Condensed):\n", stmt)

	// For repeated queries off a channel or loop, we can further optimize.
	params = kusto.NewParameters()
	qv := kusto.QueryValues{}

	qv["Node"] = "node id from channel"
	stmt, err = singleStmt.WithParameters(params.Must(qv))
	if err != nil {
		panic(err)
	}

	fmt.Println("Single Statement(Repeat):\n", stmt)

	// Output:
	// All Nodes Statement:
	//  set notruncation;dcmInventoryComponentSystem
	// | project NodeId, Version
	// Single Statement:
	//  declare query_parameters(Node:string);
	// set notruncation;dcmInventoryComponentSystem
	// | project NodeId, Version
	// | where NodeId == Node
	//
	// Single Statement Parameters:
	//  {"Node":"my_id"}
	// Single Statement(Condensed):
	//  declare query_parameters(Node:string);
	// set notruncation;dcmInventoryComponentSystem
	// | project NodeId, Version
	// | where NodeId == Node
	//
	// Single Statement(Repeat):
	//  declare query_parameters(Node:string);
	// set notruncation;dcmInventoryComponentSystem
	// | project NodeId, Version
	// | where NodeId == Node
}
