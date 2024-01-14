package azkustodata

import (
	"context"
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"net/http"
	"net/url"
	"time"
)

func Example_simple() {
	// Query and capture the values and put them in a slice of structs representing the row.

	// NodeRec represents our Kusto data that will be returned.
	type NodeRec struct {
		// ID is the table's NodeId. We use the field tag here to instruct our client to convert NodeId to ID.
		ID int64 `kusto:"NodeId"`
		// CollectionTime is Go representation of the Kusto datetime type.
		CollectionTime time.Time
	}

	kcsb := NewConnectionStringBuilder("endpoint").WithAadAppKey("clientID", "clientSecret", "tenentID")
	client, err := New(kcsb)
	if err != nil {
		panic("add error handling")
	}

	// Be sure to close the client when you're done. (Error handling omitted for brevity.)
	defer client.Close()

	ctx := context.Background()

	// Query our database table "systemNodes" for the CollectionTimes and the NodeIds.
	data, err := client.Query(ctx, "database", kql.New("systemNodes | project CollectionTime, NodeId"))
	if err != nil {
		panic("add error handling")
	}

	primary := data.Results()[0]

	recs, err := query.ToStructs[NodeRec](primary)

	if err != nil {
		panic("add error handling")
	}

	for _, rec := range recs {
		fmt.Println(rec.ID)
	}
}

func Example_complex() {
	// This example sets up a Query where we want to query for nodes that have a NodeId (a Kusto Long type) that has a
	// particular NodeId. The will require inserting a value where ParamNodeId is in the query.
	// We will used a parameterized query to do this.
	q := kql.New("systemNodes | project CollectionTime, NodeId | where NodeId == ParamNodeId")
	params := kql.NewParameters()

	// NodeRec represents our Kusto data that will be returned.
	type NodeRec struct {
		// ID is the table's NodeId. We use the field tag here to instruct our client to convert NodeId in the Kusto
		// table to ID in our struct.
		ID int64 `kusto:"NodeId"`
		// CollectionTime is Go representation of the Kusto datetime type.
		CollectionTime time.Time
	}

	kcsb := NewConnectionStringBuilder("endpoint").WithAadAppKey("clientID", "clientSecret", "tenentID")

	client, err := New(kcsb)
	if err != nil {
		panic("add error handling")
	}
	// Be sure to close the client when you're done. (Error handling omitted for brevity.)
	defer client.Close()

	ctx := context.Background()

	// Query our database table "systemNodes" for our specific node. We are only doing a single query here as an example,
	// normally you would take in requests of some type for different NodeIds.
	data, err := client.Query(ctx, "database", q, QueryParameters(params))
	if err != nil {
		panic("add error handling")
	}

	primary := data.Results()[0]

	recs, err := query.ToStructs[NodeRec](primary)

	if err != nil {
		panic("add error handling")
	}

	for _, rec := range recs {
		fmt.Println(rec.ID)
	}
}

func ExampleAuthorization_config() {
	kcsb := NewConnectionStringBuilder("endpoint").WithAadAppKey("clientID", "clientSecret", "tenentID")

	// Normally here you take a client.
	_, err := New(kcsb)
	if err != nil {
		panic("add error handling")
	}
}

func ExampleAuthorization_msi() {

	kcsb := NewConnectionStringBuilder("endpoint").WithUserManagedIdentity("clientID")

	// Normally here you take a client.
	_, err := New(kcsb)
	if err != nil {
		panic("add error handling")
	}
}

func ExampleClient_Query_rows() {

	kcsb := NewConnectionStringBuilder("endpoint").WithAadAppKey("clientID", "clientSecret", "tenentID")

	client, err := New(kcsb)
	if err != nil {
		panic("add error handling")
	}
	// Be sure to close the client when you're done. (Error handling omitted for brevity.)
	defer client.Close()

	ctx := context.Background()

	// Query our database table "systemNodes" for the CollectionTimes and the NodeIds.
	iter, err := client.IterativeQuery(ctx, "database", kql.New("systemNodes | project CollectionTime, NodeId"))
	if err != nil {
		panic("add error handling")
	}
	defer iter.Close()

	for res := range iter.Results() {
		if res.Err() != nil {
			panic("add error handling")
		}
		var tb = res.Table()
		for rowResult := range tb.Rows() {
			if rowResult.Err() != nil {
				panic("add error handling")
			}
			var row = rowResult.Row()
			for _, v := range row.Values() {
				fmt.Printf("%s,", v)
			}
			fmt.Println("") // Add a carriage return
		}
	}
}

func ExampleCustomHttpClient() { // nolint:govet // Example code
	// Create a connection string builder with your Azure ClientID, Secret and TenantID.
	kcsb := NewConnectionStringBuilder("endpoint").WithAadAppKey("clientID", "clientSecret", "tenentID")
	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			MaxConnsPerHost:     100,
			TLSHandshakeTimeout: 60 * time.Second,
		},
	}
	url, err := url.Parse("squid-proxy.corp.mycompany.com:2323")
	if err != nil {
		panic(err.Error())
	}

	httpClient.Transport = &http.Transport{Proxy: http.ProxyURL(url)}

	// Normally here you take a client.
	_, err = New(kcsb, WithHttpClient(httpClient))
	if err != nil {
		panic(err.Error())
	}
}
