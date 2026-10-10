// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package etoe

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/testshared"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/httpclient"
)

// Config represents a config.json file that must be in the directory and hold information to do the integration tests.
type Config struct {
	Endpoint          string `json:"Endpoint"`
	SecondaryEndpoint string `json:"SecondaryEndpoint"`
	Database          string `json:"Database"`
	SecondaryDatabase string `json:"SecondaryDatabase"`
	ClientID          string `json:"ClientID"`
	ClientSecret      string `json:"ClientSecret"`
	TenantID          string `json:"TenantID"`
	Blob              string `json:"Blob"`
	AccessToken       string `json:"AccessToken"`

	// Computed fields (not from JSON)
	engineURL string
	dmURL     string

	// kcsb for azkustodata query client (to verify ingestion results)
	kcsb *azkustodata.ConnectionStringBuilder
}

func (c *Config) validate() error {
	if c.Endpoint == "" || c.Database == "" {
		return fmt.Errorf("Endpoint and Database must be set in config.json or environment variables")
	}
	return nil
}

// newTokenProvider creates an httpclient.TokenProvider for v2 client construction.
// If a static access token is configured, it returns that directly.
// Otherwise it leverages the azkustodata token provider mechanism.
func (c *Config) newTokenProvider() (httpclient.TokenProvider, func(), error) {
	// Static token mode — just return the token as-is
	if c.AccessToken != "" {
		return func(ctx context.Context) (string, error) {
			return c.AccessToken, nil
		}, func() {}, nil
	}

	// Use azkustodata to acquire tokens
	client, err := azkustodata.New(c.kcsb)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create azkustodata client for token: %w", err)
	}
	tkp := client.Auth().TokenProvider
	// SetHttp must be called before AcquireToken — the token provider needs an HTTP client
	// to fetch cloud metadata, and it's normally only set during actual query execution.
	tkp.SetHttp(client.HttpClient())
	cleanup := func() { client.Close() }
	return func(ctx context.Context) (string, error) {
		token, _, err := tkp.AcquireToken(ctx)
		return token, err
	}, cleanup, nil
}

var (
	skipETOE   bool = true
	testConfig Config
)

func init() {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		fmt.Println("Failed calling runtime.Caller()")
		return
	}

	p := filepath.Join(filepath.Dir(filename), "config.json")
	b, err := os.ReadFile(p)

	if err == nil {
		if err := json.Unmarshal(b, &testConfig); err != nil {
			fmt.Printf("Failed reading test settings from '%s'\n", p)
			return
		}
	} else {
		testConfig = Config{
			Endpoint:          os.Getenv("ENGINE_CONNECTION_STRING"),
			SecondaryEndpoint: os.Getenv("SECONDARY_ENGINE_CONNECTION_STRING"),
			Database:          os.Getenv("TEST_DATABASE"),
			SecondaryDatabase: os.Getenv("SECONDARY_DATABASE"),
			ClientID:          os.Getenv("AZURE_CLIENT_ID"),
			ClientSecret:      os.Getenv("AZURE_CLIENT_SECRET"),
			TenantID:          os.Getenv("AZURE_TENANT_ID"),
			Blob:              os.Getenv("BLOB_URI_FOR_TEST"),
			AccessToken:       os.Getenv("KUSTO_ACCESS_TOKEN"),
		}
		if testConfig.Endpoint == "" {
			fmt.Println("Skipping E2E Tests - No json config and no test environment")
			return
		}
	}

	if err := testConfig.validate(); err != nil {
		fmt.Println(err)
		return
	}

	testConfig.engineURL = testConfig.Endpoint
	testConfig.dmURL = strings.Replace(testConfig.Endpoint, "://", "://ingest-", 1)

	// Build kcsb for the azkustodata query client used to verify ingestion
	switch {
	case testConfig.AccessToken != "":
		// Static token — use WithApplicationToken (appId can be empty for bearer tokens)
		testConfig.kcsb = azkustodata.NewConnectionStringBuilder(testConfig.Endpoint).WithApplicationToken("", testConfig.AccessToken)
	case testConfig.ClientID != "" && testConfig.ClientSecret != "" && testConfig.TenantID != "":
		testConfig.kcsb = azkustodata.NewConnectionStringBuilder(testConfig.Endpoint).WithAadAppKey(testConfig.ClientID, testConfig.ClientSecret, testConfig.TenantID)
	default:
		testConfig.kcsb = azkustodata.NewConnectionStringBuilder(testConfig.Endpoint).WithAzCli()
	}

	skipETOE = false
	testshared.SetDefaultDatabase(testConfig.Database)
}
