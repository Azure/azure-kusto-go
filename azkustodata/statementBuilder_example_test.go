package azkustodata_test

import (
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
)

var (
	// rootStatement represents our root statementBuilder object in which we can derive other statementBuilders.
	rootStatement = kql.New("").AddTable("systemNodes")
	// singleBasicStatement is derived from the rootStatement but includes a where clause to limit the query to a wanted result.
	singleBasicStatement = rootStatement.AddLiteral(" | where ").
				AddColumn("NodeId").AddLiteral(" == ").AddInt(1)

	// We will also define a similar Statement, but this time with a Parameters object as well to define the "NodeId" word in the
	// query as an int (aka, using KQL query parameters).
	singleParameterStatement = kql.New("systemNodes").AddLiteral(" | where NodeId == id")
	singleQueryParameter     = kql.NewParameters().AddInt("id", 1)
)

func ExampleStatement() {

	// If we wanted to build a query , we could build it from singleBasicStatement like so :
	fmt.Println("Basic Builder:\n", singleBasicStatement.String())
	// and send it to querying: client.Query(ctx, "database", singleBasicStatement)

	// Or we can use the query parameters option:
	fmt.Println("Basic Builder with parameters:\n", singleParameterStatement)
	for k, v := range singleQueryParameter.ToParameterCollection() {
		fmt.Printf("Query parameters:\n{%s: %s}\n", k, v)
	}

	// and send it to querying: client.Query(ctx, "database", singleParameterStatement,
	//	[]kusto.QueryOption{kusto.QueryParameters(*singleQueryParameter)})
	// Where the query will be:
	fmt.Printf("Actual query:\n%s\n%s\n", singleQueryParameter.ToDeclarationString(), singleParameterStatement)

	// Output:
	// Basic Builder:
	//  systemNodes | where NodeId == int(1)
	// Basic Builder with parameters:
	//  systemNodes | where NodeId == id
	//Query parameters:
	//{id: int(1)}
	//Actual query:
	//declare query_parameters(id:int);
	//systemNodes | where NodeId == id
}
