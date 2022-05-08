package kusto

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"github.com/Azure/azure-kusto-go/kusto/data/types"

	"github.com/Azure/go-autorest/autorest/azure/auth"
)

func Example_simple() {
	// Query and capture the values and put them in a slice of structs representing the row.

	// NodeRec represents our Kusto data that will be returned.
	type NodeRec struct {
		// ID is the table's NodeId. We use the field tag here to to instruct our client to convert NodeId to ID.
		ID int64 `kusto:"NodeId"`
		// CollectionTime is Go representation of the Kusto datetime type.
		CollectionTime time.Time
	}

	authorizer := Authorization{
		Config: auth.NewClientCredentialsConfig("clientID", "clientSecret", "tenantID"),
	}

	client, err := New("endpoint", authorizer)
	if err != nil {
		panic("add error handling")
	}

	// Be sure to close the client when you're done. (Error handling omitted for brevity.)
	defer client.Close()

	ctx := context.Background()

	// Query our database table "systemNodes" for the CollectionTimes and the NodeIds.
	iter, err := client.Query(ctx, "database", NewStmt("systemNodes | project CollectionTime, NodeId"))
	if err != nil {
		panic("add error handling")
	}
	defer iter.Stop()

	recs := []NodeRec{}

	err = iter.Do(
		func(row *table.Row) error {
			rec := NodeRec{}
			if err := row.ToStruct(&rec); err != nil {
				return err
			}
			if row.Replace {
				recs = recs[:0]
			}
			recs = append(recs, rec)
			return nil
		},
	)

	if err != nil {
		panic("add error handling")
	}

	for _, rec := range recs {
		fmt.Println(rec.ID)
	}
}

func Example_complex() {
	// This example sets up a Query where we want to query for nodes that have a NodeId (a Kusto Long type) that has a
	// particular NodeId. The will require inserting a value where ParamNodeId is in the query. We create the query
	// and attach a Definition to it that indicates which words we will be substituing for and what the expected type will be.
	// the MustDefinitions() will panic if the Definition is not valid. There is a non-panicing version that returns an
	// error instead.
	rootStmt := NewStmt("systemNodes | project CollectionTime, NodeId | where NodeId == ParamNodeId").MustDefinitions(
		NewDefinitions().Must(
			ParamTypes{
				"ParamNodeId": ParamType{Type: types.Long},
			},
		),
	)

	// This takes our rootStmt and creates a new Stmt that will insert 100 where ParamNodeId is in the rootStmt.
	// rootStmt will remain unchanged. The Must() will panic if the QueryValues{} passed is not valid. This can
	// happen because you use a type that isn't valid, like a string or int32.
	// There is a non-panicing version that returns an error instead.
	stmt, err := rootStmt.WithParameters(NewParameters().Must(QueryValues{"ParamNodeId": int64(100)}))
	if err != nil {
		panic("add error handling")
	}

	// NodeRec represents our Kusto data that will be returned.
	type NodeRec struct {
		// ID is the table's NodeId. We use the field tag here to to instruct our client to convert NodeId in the Kusto
		// table to ID in our struct.
		ID int64 `kusto:"NodeId"`
		// CollectionTime is Go representation of the Kusto datetime type.
		CollectionTime time.Time
	}

	authorizer := Authorization{Config: auth.NewClientCredentialsConfig("clientID", "clientSecret", "tenantID")}

	client, err := New("endpoint", authorizer)
	if err != nil {
		panic("add error handling")
	}
	// Be sure to close the client when you're done. (Error handling omitted for brevity.)
	defer client.Close()

	ctx := context.Background()

	// Query our database table "systemNodes" for our specific node. We are only doing a single query here as an example,
	// normally you would take in requests of some type for different NodeIds.
	iter, err := client.Query(ctx, "database", stmt)
	if err != nil {
		panic("add error handling")
	}
	defer iter.Stop()

	rec := NodeRec{} // We are assuming unique NodeId, so we will only get 1 row.
	err = iter.Do(
		func(row *table.Row) error {
			return row.ToStruct(&rec)
		},
	)

	if err != nil {
		panic("add error handling")
	}

	fmt.Println(rec.ID)
}

func ExampleAuthorization_config() {
	// Create an authorizer with your Azure ClientID, Secret and TenantID.
	authorizer := Authorization{
		Config: auth.NewClientCredentialsConfig("clientID", "clientSecret", "tenantID"),
	}

	// Normally here you take a client.
	_, err := New("endpoint", authorizer)
	if err != nil {
		panic("add error handling")
	}
}

func ExampleAuthorization_msi() {
	// Create an authorizer with an Azure MSI (managed identities).
	msi := auth.NewMSIConfig()

	authorizer := Authorization{
		Config: msi,
	}

	// Normally here you take a client.
	_, err := New("endpoint", authorizer)
	if err != nil {
		panic("add error handling")
	}
}

func ExampleClient_Query_rows() {
	authorizer := Authorization{
		Config: auth.NewClientCredentialsConfig("clientID", "clientSecret", "tenantID"),
	}

	client, err := New("endpoint", authorizer)
	if err != nil {
		panic("add error handling")
	}
	// Be sure to close the client when you're done. (Error handling omitted for brevity.)
	defer client.Close()

	ctx := context.Background()

	// Query our database table "systemNodes" for the CollectionTimes and the NodeIds.
	iter, err := client.Query(ctx, "database", NewStmt("systemNodes | project CollectionTime, NodeId"))
	if err != nil {
		panic("add error handling")
	}
	defer iter.Stop()

	// Iterate through the returned rows until we get an error or receive an io.EOF, indicating the end of
	// the data being returned.
	for {
		row, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			if err != nil {
				panic("add error handling")
			}
		}

		// Print out the row values
		for _, v := range row.Values {
			fmt.Printf("%s,", v)
		}
		fmt.Println("") // Add a carriage return
	}
}

func ExampleClient_Query_do() {
	// This is similar to our (Row) example. In this one though, we use the RowIterator.Do() method instead of
	// manually iterating over the row. This makes for shorter code while maintaining readability.

	authorizer := Authorization{
		Config: auth.NewClientCredentialsConfig("clientID", "clientSecret", "tenantID"),
	}

	client, err := New("endpoint", authorizer)
	if err != nil {
		panic("add error handling")
	}
	// Be sure to close the client when you're done. (Error handling omitted for brevity.)
	defer client.Close()

	ctx := context.Background()

	// Query our database table "systemNodes" for the CollectionTimes and the NodeIds.
	iter, err := client.Query(ctx, "database", NewStmt("systemNodes | project CollectionTime, NodeId"))
	if err != nil {
		panic("add error handling")
	}
	defer iter.Stop()

	// Iterate through the returned rows until we get an error or receive an io.EOF, indicating the end of
	// the data being returned.

	err = iter.Do(
		func(row *table.Row) error {
			if row.Replace {
				fmt.Println("---") // Replace flag indicates that the query result should be cleared and replaced with this row
			}
			for _, v := range row.Values {
				fmt.Printf("%s,", v)
			}
			fmt.Println("") // Add a carriage return
			return nil
		},
	)
	if err != nil {
		panic("add error handling")
	}
}

func ExampleClient_Query_struct() {
	// Capture our values into a struct and sends those values into a channel. Normally this would be done between
	// a couple of functions representing a sender and a receiver.

	// NodeRec represents our Kusto data that will be returned.
	type NodeRec struct {
		// ID is the table's NodeId. We use the field tag here to to instruct our client to convert NodeId to ID.
		ID int64 `kusto:"NodeId"`
		// CollectionTime is Go representation of the Kusto datetime type.
		CollectionTime time.Time

		// err is used internally to signal downstream that we encounter an error.
		err error
	}

	authorizer := Authorization{
		Config: auth.NewClientCredentialsConfig("clientID", "clientSecret", "tenantID"),
	}

	client, err := New("endpoint", authorizer)
	if err != nil {
		panic("add error handling")
	}
	// Be sure to close the client when you're done. (Error handling omitted for brevity.)
	defer client.Close()

	ctx := context.Background()

	// Query our database table "systemNodes" for the CollectionTimes and the NodeIds.
	iter, err := client.Query(ctx, "database", NewStmt("systemNodes | project CollectionTime, NodeId"))
	if err != nil {
		panic("add error handling")
	}
	defer iter.Stop()

	// printCh is used to receive NodeRecs for printing.
	printCh := make(chan NodeRec, 1)

	// Iterate through the returned rows, convert them to NodeRecs and send them on printCh to be printed.
	go func() {
		// Note: we ignore the error here because we send it on a channel and an error will automatically
		// end the iteration.
		iter.Do(
			func(row *table.Row) error {
				rec := NodeRec{}
				rec.err = row.ToStruct(&rec)
				printCh <- rec
				return rec.err
			},
		)
	}()

	// Receive the NodeRecs on printCh and print them to the screen.
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for rec := range printCh {
			if rec.err != nil {
				fmt.Println("Got error: ", err)
				return
			}
			fmt.Printf("NodeID: %d, CollectionTime: %s\n", rec.ID, rec.CollectionTime)
		}
	}()

	wg.Wait()
}

func ExampleCustomHttpClient() {
	// Create an authorizer with your Azure ClientID, Secret and TenantID.
	authorizer := Authorization{
		Config: auth.NewClientCredentialsConfig("clientID", "clientSecret", "tenantID"),
	}
	httpClient := &http.Client{}
	url, err := url.Parse("squid-proxy.corp.mycompany.com:2323")
	if err != nil {
		panic(err.Error())
	}

	httpClient.Transport = &http.Transport{Proxy: http.ProxyURL(url)}

	// Normally here you take a client.
	_, err = New("endpoint", authorizer, WithHttpClient(httpClient))
	if err != nil {
		panic(err.Error())
	}
}
