package azkustodata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"io/ioutil"
	"net/http"
	"strings"
)

type KustoClient struct {
	cluster            string
	managementEndpoint string
	queryEndpoint      string
	tokenGetter        TokenGetter
	defaultHeaders     map[string]string
}

type Executor interface {
	Execute(database string, query string) KustoResults
}

func NewKustoClient(authContext KustoAuthContext) *KustoClient {
	return &KustoClient{
		cluster:            authContext.cluster,
		tokenGetter:        authContext,
		managementEndpoint: authContext.cluster + "/v1/rest/mgmt",
		queryEndpoint:      authContext.cluster + "/v2/rest/query",
		defaultHeaders: map[string]string{
			//"Accept":              "application/json",
			// Don't uncomment before reading https://golang.org/src/net/http/transport.go#L181
			//"Accept-Encoding":     "gzip",
			"x-ms-client-version": "Kusto.Python.Client:0.1.0",
		},
	}
}

func (kc *KustoClient) Execute(database string, query string) (KustoResults, error) {
	token, err := kc.tokenGetter.GetToken()

	if err != nil {
		return nil, err
	}

	payload := map[string]string{
		"db":  database,
		"csl": query,
	}

	requestId, err := uuid.NewUUID()

	if err != nil {
		return nil, err
	}

	client := &http.Client{}

	jsonBody, err := json.Marshal(payload)
	q := strings.TrimSpace(query)
	endpoint := kc.queryEndpoint

	if q[:1] == "." {
		endpoint = kc.managementEndpoint
	}

	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(jsonBody))

	for k, v := range kc.defaultHeaders {
		req.Header.Set(k, v)
	}

	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("x-ms-client-request-id", fmt.Sprintf("KGC.execute; %s", requestId))
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token.AccessToken))

	resp, err := client.Do(req)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	result, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}

	print(string(result))

	switch {
	case strings.Contains(endpoint, "v1"):
		v1result := KustoResponseDataSetV1{}
		err = json.Unmarshal(result, &v1result.Tables)

		return v1result, err
	case strings.Contains(endpoint, "v2"):
		v2result := KustoResponseDataSetV2{}
		err = json.Unmarshal(result, &v2result.Tables)

		return v2result, err
	}

	return nil, nil
}
