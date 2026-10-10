// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package resources

import (
	"testing"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

func TestRoundRobinContainerListNext(t *testing.T) {
	containers := []*ExtendedContainerInfo{
		{ContainerName: "c1", SasURL: "https://storage.blob.core.windows.net/c1?sas"},
		{ContainerName: "c2", SasURL: "https://storage.blob.core.windows.net/c2?sas"},
		{ContainerName: "c3", SasURL: "https://storage.blob.core.windows.net/c3?sas"},
	}

	list := NewRoundRobinContainerList(containers)

	if list.Len() != 3 {
		t.Errorf("expected length 3, got %d", list.Len())
	}

	// First call should return c1
	c := list.Next()
	if c.ContainerName != "c1" {
		t.Errorf("expected c1, got %s", c.ContainerName)
	}

	// Second should return c2
	c = list.Next()
	if c.ContainerName != "c2" {
		t.Errorf("expected c2, got %s", c.ContainerName)
	}

	// Third should return c3
	c = list.Next()
	if c.ContainerName != "c3" {
		t.Errorf("expected c3, got %s", c.ContainerName)
	}

	// Fourth should wrap around to c1
	c = list.Next()
	if c.ContainerName != "c1" {
		t.Errorf("expected c1 (wrap), got %s", c.ContainerName)
	}
}

func TestRoundRobinContainerListEmpty(t *testing.T) {
	list := NewRoundRobinContainerList(nil)
	if list.Len() != 0 {
		t.Errorf("expected length 0, got %d", list.Len())
	}
	c := list.Next()
	if c != nil {
		t.Error("expected nil for empty list")
	}
}

func TestExtendedContainerInfoBuildBlobPath(t *testing.T) {
	c := &ExtendedContainerInfo{
		ContainerName: "mycontainer",
		SasURL:        "https://storage.blob.core.windows.net/mycontainer?sv=2020-08-04&sig=abc",
	}
	path := c.BuildBlobPath("test-blob.csv", ingestoptions.CompressionNone)
	expected := "https://storage.blob.core.windows.net/mycontainer/test-blob.csv?sv=2020-08-04&sig=abc"
	if path != expected {
		t.Errorf("expected %s, got %s", expected, path)
	}
}
