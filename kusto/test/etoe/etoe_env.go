package etoe

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/go-autorest/autorest/azure/auth"
)

// Config represents a config.json file that must be in the directory and hold information to do the integration tests.
type Config struct {
	// Endpoint is the  endpoint name to connect with
	Endpoint string
	// Database is the name of an exisiting database that can be used for tests
	Database string
	// ClientID is the object-id of the principal authorized to connect to the database
	ClientID string
	// ClientSecret is the key used to get a token on behalf of the principal
	ClientSecret string
	// TenantID is the tenant on which the prinicpal exisets
	TenantID string
	// Authorizer generates bearer tokens on behalf of the principal
	Authorizer kusto.Authorization
}

func (c *Config) validate() error {
	switch "" {
	case c.Endpoint, c.Database:
		return fmt.Errorf("no field in the end to end test config.json file can be empty")
	}

	return nil
}

var (
	// has init occured
	wasInit bool = false
	// skipETOE will be set if the ./config.json file does not exist to let us suppress these tests.
	skipETOE bool = false
	// testConfig is the configuration file that we read in via init().
	testConfig Config
)

// initEnv will read in our config file and if it can't be read, will set skipETOE so the tests will be suppressed.
func initEnv() {
	skipETOE = true

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return
	}

	p := filepath.Join(filepath.Dir(filename), "config.json")

	b, err := ioutil.ReadFile(p)

	if err != nil {
		// if couldn't find a config file, we try to read them from env
		testConfig = Config{
			Endpoint:     os.Getenv("ENGINE_CONNECTION_STRING"),
			Database:     os.Getenv("TEST_DATABASE"),
			ClientID:     os.Getenv("APP_ID"),
			ClientSecret: os.Getenv("APP_KEY"),
			TenantID:     os.Getenv("AUTH_ID"),
		}

		if testConfig.Endpoint == "" {
			return
		}

	} else if err := json.Unmarshal(b, &testConfig); err != nil {
		return
	}

	if err := testConfig.validate(); err != nil {
		return
	}

	if testConfig.ClientID == "" {
		azAuthorizer, err := auth.NewAuthorizerFromCLIWithResource(testConfig.Endpoint)
		if err != nil {
			fmt.Println("failed to acquire auth token from az-cli" + err.Error())
			return
		}

		testConfig.Authorizer = kusto.Authorization{Authorizer: azAuthorizer}
	} else {
		testConfig.Authorizer = kusto.Authorization{Config: auth.NewClientCredentialsConfig(testConfig.ClientID, testConfig.ClientSecret, testConfig.TenantID)}
	}

	skipETOE = false
}

// NewConfig returns e2e environent configuration data
func NewConfig() (*Config, error) {
	if !wasInit {
		initEnv()
		wasInit = true
	}

	if skipETOE {
		return nil, fmt.Errorf("E2E environment is not set")
	}

	return &testConfig, nil
}
