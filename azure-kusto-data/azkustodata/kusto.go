/*
Package kusto provides a Kusto client for accessing Kusto storage.
Author: jdoak@microsoft.com
*/
package azkustodata

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"azure-kusto-go/azure-kusto-data/azkustodata/errors"

	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure/auth"
)

// version is the version of this client package that is communicated to the server.
const version = "0.0.1"

// queryer provides for getting a stream of Kusto frames. Exists to allow fake Kusto streams in tests.
type queryer interface {
	// Query queries a Kusto database and returns Frames that the server sends.
	query(ctx context.Context, db, query string, options ...QueryOption) (chan frame, error)
}

// Authorization provides the ADAL authorizer needed to access the resource. You can set Authorizer or
// Config, but not both.
type Authorization struct {
	// Authorizer provides an authorizer to use when talking to Kusto. If this is set, the
	// Authorizer must have its Resource (also called Resource ID) set to the endpoint passed
	// to the New() constructor. This will be something like "https://somename.westus.kusto.windows.net".
	Authorizer autorest.Authorizer
	// Config provides the authorizer's config that can create the authorizer. We recommending setting
	// this instead of Authorizer, as we will automatically set the Resource ID with the endpoint passed.
	Config auth.AuthorizerConfig
}

func (a *Authorization) validate(endpoint string) error {
	const rescField = "Resource"

	if a.Authorizer != nil && a.Config != nil {
		return errors.E(errors.OpServConn, errors.KClientArgs, fmt.Errorf("cannot set Authoriztion.Authorizer and Authorizer.Config"))
	}
	if a.Authorizer == nil && a.Config == nil {
		return errors.E(errors.OpServConn, errors.KClientArgs, fmt.Errorf("cannot leave all Authoriztion fields as zero values"))
	}
	if a.Authorizer != nil {
		return nil
	}

	// This is sort of hacky, in that we are using what we know about the current auth library's internals
	// structure to try and make this fix. But the auth library is confusing and this will stem off a bunch of
	// support calls, so it is worth attempting.
	v := reflect.ValueOf(a.Config)
	switch v.Kind() {
	// This piece of code is what I call hopeful thinking. The New* in auth.go should return pointers, they don't (bad).
	// So this is hoping someone passed a pointer in the Authorizer interface.
	case reflect.Ptr:
		if reflect.PtrTo(v.Type()).Kind() == reflect.Struct {
			v = v.Elem()
			if f := v.FieldByName(rescField); f.IsZero() {
				if f.Kind() == reflect.String {
					f.SetString(endpoint)
				}
			} else {
				return errors.E(errors.OpServConn, errors.KClientArgs, fmt.Errorf("the Authorization.Config passed to the Kusto client did not have an underlying .Resource field"))
			}
		} else {
			return errors.E(errors.OpServConn, errors.KClientArgs, fmt.Errorf("the Authorization.Config passed to the Kusto client was a pointer to a %T, which is not a struct.", a.Config))
		}
		// This is how we are likely to get the Authorizer. So since we can't change the fields, now we have to type assert
		// to the underlying type and put back a new copy. Note: it seems to me that we should be get a copy of a.Config
		// and then set the field (without using unsafe), then do the re-assignment. But I haven't been able to parse this out atm.
	case reflect.Struct:
		switch t := a.Config.(type) {
		case auth.ClientCredentialsConfig:
			t.Resource = endpoint
			a.Config = t
		case auth.DeviceFlowConfig:
			t.Resource = endpoint
			a.Config = t
		case auth.MSIConfig:
			t.Resource = endpoint
			a.Config = t
		default:
			return errors.E(errors.OpServConn, errors.KClientArgs, fmt.Errorf("the Authiorization.Config passed to the Kusto client  "+"is not a type we know how to deal with: %T", t))
		}
	default:
		return errors.E(errors.OpServConn, errors.KClientArgs, fmt.Errorf("the Authorization.Config passed to the Kusto client was not a Pointer to a struct or a struct, is a: %T", a.Config))

	}
	var err error
	a.Authorizer, err = a.Config.Authorizer()
	if err != nil {
		return errors.E(errors.OpServConn, errors.KClientArgs, err)
	}
	return nil
}

// Client is a client to a Kusto instance.
type Client struct {
	conn    queryer
	timeout time.Duration
	mu      sync.Mutex
}

// Option is an optional argument type for New().
type Option func(c *Client)

// Timeout adjusts the maximum time any query can take from the client side. This defaults to 5 minutes.
// Note that the server has a timeout of 4 minutes for a query, 10 minutes for a management action.
// You will need to use the ServerTimeout() QueryOption in order to allow the server to take larger queries.
func Timeout(d time.Duration) Option {
	return func(c *Client) {
		c.timeout = d
	}
}

// New returns a new Client.
func New(endpoint string, auth Authorization, options ...Option) (*Client, error) {
	client := &Client{}
	for _, o := range options {
		o(client)
	}

	if err := auth.validate(endpoint); err != nil {
		return nil, err
	}
	conn, err := newConn(endpoint, auth, client.timeout)
	if err != nil {
		return nil, err
	}
	client.conn = conn

	return client, nil
}

// QueryOption provides options for Query().
type QueryOption func(q *queryOptions)

// Query makes a query for the purpose of extracting data from Kusto. Context can be used to set
// a timeout or cancel the query. Queries cannot take longer than 5 minutes.
func (c *Client) Query(ctx context.Context, db, query string, options ...QueryOption) (*RowIterator, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background()) // Note: cancel is called when *RowIterator has Stop() called.
	frameCh, err := c.conn.query(ctx, db, query, options...)
	if err != nil {
		return nil, fmt.Errorf("error with Query: %s", err)
	}

	var header dataSetHeader

	ff := <-frameCh
	switch v := ff.(type) {
	case dataSetHeader:
		header = v
	case errorFrame:
		return nil, v
	}

	iter, columnsReady := newRowIterator(ctx, cancel, header, errors.OpQuery)

	var sm stateMachine
	if header.IsProgressive {
		log.Println("progressive")
		sm = &progressiveSM{
			op:   errors.OpQuery,
			iter: iter,
			in:   frameCh,
			ctx:  ctx,
		}
	} else {
		sm = &nonProgressiveSM{
			op:   errors.OpQuery,
			iter: iter,
			in:   frameCh,
			ctx:  ctx,
		}
	}
	go runSM(sm)

	<-columnsReady

	return iter, nil
}

// Mgmt is used to do management queries to Kusto.
func (c *Client) Mgmt(ctx context.Context, db, query string, options ...QueryOption) (chan frame, error) {
	panic("not implemented")
}
