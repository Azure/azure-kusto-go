package data_test

import (
	"fmt"
	"github.com/Azure/azure-kusto-go/data/kql"
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
	rootStmt = kql.New(qRoot)
	// singleStmt is derived from the rootStmt but includes a where clause to limit the query to a
	// single node. Node is an implicit parameter that will be substituted for a value when we run the query.
	singleStmt = kql.FromBuilder(rootStmt).AddLiteral(singleNode)
)

func ExampleStmt() {
	// This will print the rootStmt that could be used to list all nodes in the table.
	fmt.Println("All Nodes Builder:")
	fmt.Println(rootStmt.String())

	// This will build the parameters for the singleStmt. We can use these parameters to run the query.
	params := kql.NewParameters().AddString("Node", "my_id")

	fmt.Println("Single Builder:")
	fmt.Println(singleStmt.String())
	fmt.Println("Single Builder Parameter declaration:")
	fmt.Println(params.ToDeclarationString())

	// Alternatively, we can build the statement with the value in it.
	stmt := kql.New(qRoot).AddLiteral("\n| where NodeId == ").AddString("my_id")

	fmt.Println("Single Builder(Built):")
	fmt.Println(stmt.String())

	// Output:
	//All Nodes Builder:
	//set notruncation;dcmInventoryComponentSystem
	//| project NodeId, Version
	//Single Builder:
	//set notruncation;dcmInventoryComponentSystem
	//| project NodeId, Version
	//| where NodeId == Node
	//
	//Single Builder Parameter declaration:
	//declare query_parameters(Node:string);
	//Single Builder(Built):
	//set notruncation;dcmInventoryComponentSystem
	//| project NodeId, Version
	//| where NodeId == "my_id"
}
