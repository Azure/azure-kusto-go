package etoe

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/Azure/azure-kusto-go/kusto"
)

// Config represents a config.json file that must be in the directory and hold information to do the integration tests.
type Config struct {
	// Endpoint is the endpoint name to connect with
	Endpoint string
	// SecondaryEndpoint is the endpoint name to connect with for the secondary cluster
	SecondaryEndpoint string
	// Database is the name of an existing database that can be used for tests
	Database string
	// SecondaryDatabase is the name of an existing database in the secondary that can be used for tests
	SecondaryDatabase string
	// ClientID is the object-id of the principal authorized to connect to the database
	ClientID string
	// ClientSecret is the key used to get a token on behalf of the principal
	ClientSecret string
	// TenantID is the tenant on which the principal exists
	TenantID string
	// Connection string builder to get a new kusto client
	kcsb *kusto.ConnectionStringBuilder
}

func (c *Config) validate() error {
	switch "" {
	case c.Endpoint, c.Database:
		return fmt.Errorf("no field in the end to end test config.json file can be empty")
	}

	return nil
}

var (
	// skipETOE will be set if the ./config.json file does not exist to let us suppress these tests.
	skipETOE bool = true
	// testConfig is the configuration file that we read in via init().
	testConfig Config
)

// initEnv will read in our config file and if it can't be read, will set skipETOE so the tests will be suppressed.
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
			fmt.Printf("Failed reading test settings from '%s\n'", p)
			return
		}
	} else {
		// if couldn't find a config file, we try to read them from env
		testConfig = Config{
			Endpoint:          os.Getenv("ENGINE_CONNECTION_STRING"),
			SecondaryEndpoint: os.Getenv("SECONDARY_ENGINE_CONNECTION_STRING"),
			Database:          os.Getenv("TEST_DATABASE"),
			SecondaryDatabase: os.Getenv("SECONDARY_DATABASE"),
			ClientID:          os.Getenv("AZURE_CLIENT_ID"),
			ClientSecret:      os.Getenv("AZURE_CLIENT_SECRET"),
			TenantID:          os.Getenv("AZURE_TENANT_ID"),
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

	if testConfig.ClientID == "" {
		testConfig.kcsb = kusto.NewConnectionStringBuilder(testConfig.Endpoint).WithAzCli()
	} else {
		testConfig.kcsb = kusto.NewConnectionStringBuilder(testConfig.Endpoint).WithAadAppKey(testConfig.ClientID, testConfig.ClientSecret, testConfig.TenantID)
	}
	testConfig.kcsb.UserForTracing = "GoLang_E2ETest_ø"
	skipETOE = false
}
