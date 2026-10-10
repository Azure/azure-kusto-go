// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package ingestoptions

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
)

// ClientDetails holds tracing metadata for the ingestion client.
type ClientDetails struct {
	// ApplicationForTracing is the application name used in tracing headers.
	ApplicationForTracing string
	// UserNameForTracing is the user name used in tracing headers.
	UserNameForTracing string
	// ClientVersionForTracing is the client version used in tracing headers.
	ClientVersionForTracing string
}

// EffectiveApplicationForTracing returns the application name for tracing,
// falling back to the process name if not provided.
func (d *ClientDetails) EffectiveApplicationForTracing() string {
	if d.ApplicationForTracing != "" {
		return d.ApplicationForTracing
	}
	return getProcessName()
}

// EffectiveUserNameForTracing returns the user name for tracing,
// falling back to the system user if not provided.
func (d *ClientDetails) EffectiveUserNameForTracing() string {
	if d.UserNameForTracing != "" {
		return d.UserNameForTracing
	}
	return getUserName()
}

// EffectiveClientVersionForTracing returns the client version for tracing,
// falling back to the SDK version if not provided.
func (d *ClientDetails) EffectiveClientVersionForTracing() string {
	if d.ClientVersionForTracing != "" {
		return d.ClientVersionForTracing
	}
	return sdkVersion()
}

// ClientHeader returns the formatted x-ms-client-version header value.
func (d *ClientDetails) ClientHeader() string {
	return fmt.Sprintf("Kusto.Go.Client:%s", d.EffectiveClientVersionForTracing())
}

func getProcessName() string {
	exe, err := os.Executable()
	if err != nil {
		return "unknown"
	}
	return filepath.Base(exe)
}

func getUserName() string {
	u, err := user.Current()
	if err != nil {
		return "unknown"
	}
	return u.Username
}

func sdkVersion() string {
	return fmt.Sprintf("2.0.0-preview;Go %s;%s/%s",
		strings.TrimPrefix(runtime.Version(), "go"),
		runtime.GOOS, runtime.GOARCH)
}

// NewClientDetails creates a new ClientDetails with the given values.
func NewClientDetails(appName, userName, clientVersion string) *ClientDetails {
	return &ClientDetails{
		ApplicationForTracing:   appName,
		UserNameForTracing:      userName,
		ClientVersionForTracing: clientVersion,
	}
}
