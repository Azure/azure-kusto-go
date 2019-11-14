package azkustodata

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Azure/go-autorest/autorest/azure/auth"
	"github.com/kylelemons/godebug/pretty"
)

func Example() {
	const (
		// endpoint is the name of our Kusto endpoint.
		endpoint = `https://clustername.kusto.windows.net`
		// db is the name of the Kusto database.
		db = "Reports"
		// query is just an example query to use.
		query = "userIDs | limit 100"
	)

	// Client ids and client secrets can be set in multiple places.
	// For apps, usually these can be found in AD for the user.
	// Tenant ids can be found in the subscription info.
	const (
		clientID     = "clientID"
		clientSecret = "clientSecret"
		tenantID     = "tenantID"
	)

	// UserID represents a user ID record stored in Kusto DB "Reports", table "userIDs".
	type UserID struct {
		// ID is a unique integer identifier for a user.
		ID int64 `kusto:"Id"` // The tag creates a mapping from Kusto column "Id" to our field name "ID".
		// UserName is the user's user name.
		UserName string
		// LastSeen represents the last time the system saw this user.
		LastSeen time.Time
	}

	authConfig := auth.NewClientCredentialsConfig(clientID, clientSecret, tenantID)

	// This creates a Reader, the most common way to interact with Kusto.
	reader, err := New(endpoint, Authorization{Config: authConfig})
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	// Queries the service and asks the service to use progressive mode, which provides a set of frame
	// fragments instead of one large DataTable holding the results.
	iter, err := reader.Query(ctx, db, query)
	if err != nil {
		panic(err)
	}
	defer iter.Stop()

	// Loop through the iterated results, read them into our UserID structs and append them
	// to our list of recs.
	var recs []UserID
	for {
		row, err := iter.Next()
		if err != nil {
			// This indicates we are done.
			if err == io.EOF {
				break
			}
			// We ran into an error during the stream.
			panic(err)
		}
		rec := UserID{}
		if err := row.ToStruct(&rec); err != nil {
			panic(err)
		}
		recs = append(recs, rec)
	}
	fmt.Println("100 User Records:")
	fmt.Println(pretty.Sprint(recs))
}
