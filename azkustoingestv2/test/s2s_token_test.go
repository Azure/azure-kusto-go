// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package test

import (
	"testing"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

func TestNewBearerS2SToken(t *testing.T) {
	token := ingestoptions.NewBearerS2SToken("my-token-value")
	if token.Scheme != "Bearer" {
		t.Errorf("expected scheme 'Bearer', got '%s'", token.Scheme)
	}
	if token.Token != "my-token-value" {
		t.Errorf("expected token 'my-token-value', got '%s'", token.Token)
	}
}

func TestS2STokenToHeaderValue(t *testing.T) {
	token := ingestoptions.S2SToken{Scheme: "Bearer", Token: "abc123"}
	val := token.ToHeaderValue()
	if val != "Bearer abc123" {
		t.Errorf("expected 'Bearer abc123', got '%s'", val)
	}
}

func TestS2STokenEmptyScheme(t *testing.T) {
	token := ingestoptions.S2SToken{Scheme: "", Token: "abc123"}
	val := token.ToHeaderValue()
	if val != " abc123" {
		t.Errorf("expected ' abc123', got '%s'", val)
	}
}
