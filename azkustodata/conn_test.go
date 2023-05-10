package azkustodata

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestHeaders(t *testing.T) {
	tests := []struct {
		name                              string
		kcsbApplication, kcsbUser         string
		propApplication, propUser         string
		expectedApplication, expectedUser string
	}{
		{
			name: "TestDefault",
		},
		{
			name:                "TestKcsb",
			kcsbApplication:     "kcsbApplication",
			kcsbUser:            "kcsbUser",
			expectedApplication: "kcsbApplication",
			expectedUser:        "kcsbUser",
		},
		{
			name:                "TestProp",
			propApplication:     "propApplication",
			propUser:            "propUser",
			expectedApplication: "propApplication",
			expectedUser:        "propUser",
		},
		{
			name:                "TestKcsbProp",
			kcsbApplication:     "kcsbApplication",
			kcsbUser:            "kcsbUser",
			propApplication:     "propApplication",
			propUser:            "propUser",
			expectedApplication: "propApplication",
			expectedUser:        "propUser",
		},
	}
	for _, tt := range tests {
		tt := tt // Capture
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			kcsb := NewConnectionStringBuilder("https://test.kusto.windows.net")

			if tt.kcsbApplication != "" {
				kcsb.ApplicationForTracing = tt.kcsbApplication
			}
			if tt.kcsbUser != "" {
				kcsb.UserForTracing = tt.kcsbUser
			}

			queryOptions := make([]QueryOption, 0)
			queryOptions = append(queryOptions, Application(tt.propApplication))
			queryOptions = append(queryOptions, User(tt.propUser))

			opts, err := setQueryOptions(context.Background(), errors.OpQuery, kql.New("test"), queryCall, queryOptions...)
			require.NoError(t, err)

			client, err := New(kcsb)
			require.NoError(t, err)

			headers := client.conn.(*Conn).getHeaders(*opts.requestProperties)

			if tt.expectedApplication != "" {
				assert.Equal(t, tt.expectedApplication, headers.Get("x-ms-app"))
			} else {
				assert.Greater(t, len(headers.Get("x-ms-app")), 0)
			}
			if tt.expectedUser != "" {
				assert.Equal(t, tt.expectedUser, headers.Get("x-ms-user"))
			} else {
				assert.Greater(t, len(headers.Get("x-ms-user")), 0)
			}
			assert.True(t, strings.HasPrefix(headers.Get("x-ms-client-version"), "Kusto.Go.Client:"))
		})
	}
}

func TestSetConnectorDetails(t *testing.T) {
	tests := []struct {
		testName                          string
		name, version                     string
		sendUser                          bool
		overrideUser, appName, appVersion string
		additionalFields                  []StringPair
		expectedApp, expectedUser         string
		appPrefix                         bool
		expectAnyUser                     bool
	}{
		{
			testName: "TestNameAndVersion",
			name:     "testName", version: "testVersion",
			expectedApp:  "Kusto.testName:{testVersion}|App.",
			appPrefix:    true,
			expectedUser: "[none]",
		},
		{
			testName: "TestNameAndVersionAndUser",
			name:     "testName", version: "testVersion", sendUser: true,
			expectedApp:   "Kusto.testName:{testVersion}|App.",
			appPrefix:     true,
			expectAnyUser: true,
		},
		{
			testName: "TestAll",
			name:     "testName", version: "testVersion", sendUser: true, overrideUser: "testUser", appName: "testApp", appVersion: "testAppVersion", additionalFields: []StringPair{{"testKey", "testValue"}},
			expectedApp:  "Kusto.testName:{testVersion}|App.{testApp}:{testAppVersion}|testKey:{testValue}",
			expectedUser: "testUser",
		},
	}
	for _, tt := range tests {
		tt := tt // Capture
		t.Run(tt.testName, func(t *testing.T) {
			t.Parallel()
			kcsb := NewConnectionStringBuilder("https://test.kusto.windows.net")
			kcsb.SetConnectorDetails(tt.name, tt.version, tt.appName, tt.appVersion, tt.sendUser, tt.overrideUser, tt.additionalFields...)

			if tt.appPrefix {
				assert.True(t, strings.HasPrefix(kcsb.ApplicationForTracing, tt.expectedApp))
			} else {
				assert.Equal(t, tt.expectedApp, kcsb.ApplicationForTracing)
			}

			if tt.expectAnyUser {
				assert.Greater(t, len(kcsb.UserForTracing), 0)
			} else {
				assert.Equal(t, tt.expectedUser, kcsb.UserForTracing)
			}
		})
	}
}
