// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package ingestoptions

import "fmt"

// S2SToken represents an S2S (Service-to-Service) authentication token
// used for Fabric Private Link authentication.
type S2SToken struct {
	// Scheme is the authentication scheme (e.g., "Bearer").
	Scheme string
	// Token is the authentication token value.
	Token string
}

// ToHeaderValue formats the token as an HTTP Authorization header value.
func (t S2SToken) ToHeaderValue() string {
	return fmt.Sprintf("%s %s", t.Scheme, t.Token)
}

// NewBearerS2SToken creates an S2SToken with the Bearer scheme.
func NewBearerS2SToken(token string) S2SToken {
	return S2SToken{Scheme: "Bearer", Token: token}
}
