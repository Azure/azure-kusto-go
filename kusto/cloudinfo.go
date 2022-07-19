package kusto

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
)

// abstraction to query metadata and use this information for providing all
// information needed for connection string builder to provide all the requisite information

const (
	metadataEndpoint              = "v1/rest/auth/metadata"
	defaultAuthEnvVarName         = "AadAuthorityUri"
	defaultKustoClientAppId       = "db662dc1-0cfe-4e1c-a843-19a68e65be58"
	defaultPublicLoginUrl         = "https://login.microsoftonline.com"
	defaultRedirectUri            = "https://microsoft/kustoclient"
	defaultKustoServiceResourceId = "https://kusto.kusto.windows.net"
	defaultFirstPartyAuthorityUrl = "https://login.microsoftonline.com/f8cdef31-a31e-4b4a-93e4-5f571e91255a"
	azureADKey                    = "AzureAD"
)

// retrieved metadata
type cloudInfo struct {
	loginEndpoint          string
	loginMfaRequired       bool
	kustoClientAppId       string
	kustoClientRedirectUri string
	kustoServiceResourceId string
	firstPartyAuthorityUrl string
}

var doOnce sync.Once
var defaultCloudInfo = &cloudInfo{
	loginEndpoint:          getEnvOrDefault(defaultAuthEnvVarName, defaultPublicLoginUrl),
	loginMfaRequired:       false,
	kustoClientAppId:       defaultKustoClientAppId,
	kustoClientRedirectUri: defaultRedirectUri,
	kustoServiceResourceId: defaultKustoServiceResourceId,
	firstPartyAuthorityUrl: defaultFirstPartyAuthorityUrl,
}

// cache to query it once per instance
var cloudInfoCache map[string]*cloudInfo

func RetrieveCloudInfoMetadata(kustoUrl string) (*cloudInfo, error) {
	if cloudInfoCache != nil {
		cachedCloudInfo, isExisting := cloudInfoCache[kustoUrl]
		if isExisting {
			return cachedCloudInfo, nil
		}
	} else {
		//init the map here
		cloudInfoCache = make(map[string]*cloudInfo)
	}
	var errorToReturn error
	doOnce.Do(func() {
		fullMetadataEndpoint := fmt.Sprintf("%s/%s", strings.TrimRight(kustoUrl, "/"), metadataEndpoint)
		metadataResponse, err := http.Get(fullMetadataEndpoint)

		var metadataMap map[string]map[string]interface{}

		if err != nil {
			// TODO how do we log
			errorToReturn = err
		}
		// metadata retrieval was successful
		if metadataResponse.StatusCode == 200 {
			// close once read
			defer metadataResponse.Body.Close()
			jsonBytes, resError := ioutil.ReadAll(metadataResponse.Body)
			if resError != nil {
				// TODO how do we log
				errorToReturn = err
			} else if len(jsonBytes) == 0 {
				// Call succeeded but no body
				cloudInfoCache[kustoUrl] = defaultCloudInfo
			} else {
				// there is a body , then parse it
				json.Unmarshal(jsonBytes, &metadataMap)
				// there is both dSTS key and the AzureAD key information.
				nestedMap := metadataMap[azureADKey]
				if len(nestedMap) == 0 {
					// The call was a success , but no response was returned
					// TODO warn logging here
					cloudInfoCache[kustoUrl] = defaultCloudInfo
				}
				cloudInfoRetrieved := &cloudInfo{
					loginEndpoint:          nestedMap["LoginEndpoint"].(string),
					loginMfaRequired:       nestedMap["LoginMfaRequired"].(bool),
					kustoClientAppId:       nestedMap["KustoClientAppId"].(string),
					kustoClientRedirectUri: nestedMap["KustoClientRedirectUri"].(string),
					kustoServiceResourceId: nestedMap["KustoServiceResourceId"].(string),
					firstPartyAuthorityUrl: nestedMap["FirstPartyAuthorityUrl"].(string),
				}
				// Add this into the cache
				cloudInfoCache[kustoUrl] = cloudInfoRetrieved
			}

		} else if metadataResponse.StatusCode == 404 {
			// the URL is not reachable , fallback to default
			// For now as long not all proxies implement the metadata endpoint, if no endpoint exists return public cloud data
			// TODO warn logging here
			cloudInfoCache[kustoUrl] = defaultCloudInfo
		} else {
			// Some other HTTP error code here
			errorToReturn = fmt.Errorf("retrieved error code %d when querying endpoint %s", metadataResponse.StatusCode, fullMetadataEndpoint)
		}
	})
	if errorToReturn != nil {
		return nil, errorToReturn
	}
	// this should be set in the map by now
	return cloudInfoCache[kustoUrl], nil
}

func getEnvOrDefault(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
