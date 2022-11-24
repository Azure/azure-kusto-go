package kusto

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

// abstraction to query metadata and use this information for providing all
// information needed for connection string builder to provide all the requisite information

const (
	metadataPath                  = "v1/rest/auth/metadata"
	defaultAuthEnvVarName         = "AadAuthorityUri"
	defaultKustoClientAppId       = "db662dc1-0cfe-4e1c-a843-19a68e65be58"
	defaultPublicLoginUrl         = "https://login.microsoftonline.com"
	defaultRedirectUri            = "https://microsoft/kustoclient"
	defaultKustoServiceResourceId = "https://kusto.kusto.windows.net"
	defaultFirstPartyAuthorityUrl = "https://login.microsoftonline.com/f8cdef31-a31e-4b4a-93e4-5f571e91255a"
)

// retrieved metadata
type metaResp struct {
	AzureAD CloudInfo
}

type CloudInfo struct {
	LoginEndpoint          string `json:"LoginEndpoint"`
	LoginMfaRequired       bool   `json:"LoginMfaRequired"`
	KustoClientAppID       string `json:"KustoClientAppId"`
	KustoClientRedirectURI string `json:"KustoClientRedirectUri"`
	KustoServiceResourceID string `json:"KustoServiceResourceId"`
	FirstPartyAuthorityURL string `json:"FirstPartyAuthorityUrl"`
}

var defaultCloudInfo = CloudInfo{
	LoginEndpoint:          getEnvOrDefault(defaultAuthEnvVarName, defaultPublicLoginUrl),
	LoginMfaRequired:       false,
	KustoClientAppID:       defaultKustoClientAppId,
	KustoClientRedirectURI: defaultRedirectUri,
	KustoServiceResourceID: defaultKustoServiceResourceId,
	FirstPartyAuthorityURL: defaultFirstPartyAuthorityUrl,
}

// cache to query it once per instance
var cloudInfoCache sync.Map

func GetMetadata(ctx context.Context, kustoUri string) (CloudInfo, error) {
	// retrieve &return if exists
	clusterCachedCloudInfo, ok := cloudInfoCache.Load(kustoUri)
	if ok {
		return clusterCachedCloudInfo.(CloudInfo), nil
	}
	u, err := url.Parse(kustoUri)
	if err != nil {
		return CloudInfo{}, err
	}

	u.Path = metadataPath
	// TODO should we make this timeout configurable.
	metadataClient := http.Client{Timeout: time.Duration(5) * time.Second}
	req, err := http.NewRequest("GET", u.String(), nil)

	if err != nil {
		return CloudInfo{}, err
	}
	clusterCachedCloudInfo, ok = cloudInfoCache.Load(kustoUri)
	if ok {
		return clusterCachedCloudInfo.(CloudInfo), nil
	}
	resp, err := metadataClient.Do(req)

	if err != nil {
		return CloudInfo{}, err
	}

	// Handle internal server error as a special case and return as an error (to be consistent with other SDK's)
	if resp.StatusCode >= http.StatusInternalServerError {
		return CloudInfo{}, fmt.Errorf("error %s when querying endpoint %s", resp.Status, u.String())
	}

	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return CloudInfo{}, err
	}

	// Covers scenarios of 200/OK with no body or a 404 where there is no body
	if len(b) == 0 {
		cloudInfoCache.Store(kustoUri, defaultCloudInfo)
		return defaultCloudInfo, nil
	}

	md := metaResp{}

	if err := json.Unmarshal(b, &md); err != nil {
		return CloudInfo{}, err
	}
	// this should be set in the map by now
	cloudInfoCache.Store(kustoUri, md.AzureAD)
	return md.AzureAD, nil
}

func getEnvOrDefault(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
