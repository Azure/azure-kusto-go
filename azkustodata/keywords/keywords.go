package keywords

import (
	_ "embed"
	"encoding/json"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"strings"
)

const (
	DataSource                                     = "Data Source"
	InitialCatalog                                 = "Initial Catalog"
	FederatedSecurity                              = "AAD Federated Security"
	ApplicationClientId                            = "Application Client Id"
	ApplicationKey                                 = "Application Key"
	UserId                                         = "User ID"
	AuthorityId                                    = "Authority Id"
	ApplicationToken                               = "Application Token"
	UserToken                                      = "User Token"
	ApplicationCertificateX5C                      = "Application Certificate SendX5c"
	ApplicationNameForTracing                      = "Application Name for Tracing"
	UserNameForTracing                             = "User Name for Tracing"
	Password                                       = "Password"
	ApplicationCertificateBlob                     = "Application Certificate Blob"
	ApplicationCertificateThumbprint               = "Application Certificate Thumbprint"
	DstsFederatedSecurity                          = "dSTS Federated Security"
	Streaming                                      = "Streaming"
	Uncompressed                                   = "Uncompressed"
	EnforceMfa                                     = "EnforceMfa"
	Accept                                         = "Accept"
	QueryConsistency                               = "Query Consistency"
	DataSourceUri                                  = "Data Source Uri"
	AzureRegion                                    = "Azure Region"
	Namespace                                      = "Namespace"
	ApplicationCertificateIssuerDistinguishedName  = "Application Certificate Issuer Distinguished Name"
	ApplicationCertificateSubjectDistinguishedName = "Application Certificate Subject Distinguished Name"
)

var (
	Instance = createInstance()
	//go:embed kcsb.json
	jsonFile []byte
)

var (
	supportedKeywords = map[string]bool{
		DataSource:                true,
		InitialCatalog:            true,
		FederatedSecurity:         true,
		ApplicationClientId:       true,
		ApplicationKey:            true,
		UserId:                    true,
		AuthorityId:               true,
		ApplicationToken:          true,
		UserToken:                 true,
		ApplicationCertificateX5C: true,
		ApplicationNameForTracing: true,
		UserNameForTracing:        true,
		Password:                  true,

		ApplicationCertificateBlob:       false,
		ApplicationCertificateThumbprint: false,
		DstsFederatedSecurity:            false,
		Streaming:                        false,
		Uncompressed:                     false,
		EnforceMfa:                       false,
		Accept:                           false,
		QueryConsistency:                 false,
		DataSourceUri:                    false,
		AzureRegion:                      false,
		Namespace:                        false,
		ApplicationCertificateIssuerDistinguishedName:  false,
		ApplicationCertificateSubjectDistinguishedName: false,
	}
	lookup = map[string]Keyword{}
)

// Keyword represents an individual keyword in the JSON.
type Keyword struct {
	Name        string   `json:"name"`
	Aliases     []string `json:"aliases"`
	Type        string   `json:"type"`
	Secret      bool     `json:"secret"`
	IsSupported bool
}

// Config represents the root structure of the JSON.
type Config struct {
	Version  string    `json:"version"`
	Keywords []Keyword `json:"keywords"`
}

// normalizeKeyword normalizes a keyword by making it lowercase and removing spaces.
func normalizeKeyword(keyword string) string {
	return strings.ReplaceAll(strings.ToLower(keyword), " ", "")
}

func createInstance() *Config {
	config := Config{}
	err := json.Unmarshal(jsonFile, &config)
	if err != nil {
		panic(err.Error())
	}

	for i, word := range config.Keywords {
		supported, ok := supportedKeywords[word.Name]
		if !ok {
			panic("Keyword " + word.Name + " is unexpected. This is a bug - please report it.")
		}

		config.Keywords[i].IsSupported = supported

		lookup[normalizeKeyword(word.Name)] = word
		for _, alias := range word.Aliases {
			lookup[normalizeKeyword(alias)] = word
		}
	}

	return &config
}

func GetKeyword(keyword string) (*Keyword, error) {
	word, ok := lookup[normalizeKeyword(keyword)]
	if !ok {
		return nil, errors.ES(errors.OpUnknown, errors.KFailedToParse, "The Connection String keyword `%s` is unknown.", keyword)
	}

	if !word.IsSupported {
		return nil, errors.ES(errors.OpUnknown, errors.KFailedToParse, "The Connection String keyword `%s` is not supported.", keyword)
	}

	return &word, nil
}
