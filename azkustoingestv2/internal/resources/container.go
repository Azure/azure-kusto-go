// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package resources

import (
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

// ExtendedContainerInfo wraps container information with its upload method.
type ExtendedContainerInfo struct {
	ContainerName string
	SasURL        string
	UploadMethod  ingestoptions.UploadMethod
}

// BuildBlobPath constructs the full blob path within this container.
func (c *ExtendedContainerInfo) BuildBlobPath(blobName string, compression ingestoptions.CompressionType) string {
	u, err := url.Parse(c.SasURL)
	if err != nil {
		return c.SasURL + "/" + blobName
	}
	// Append blob name to the path
	u.Path = strings.TrimRight(u.Path, "/") + "/" + blobName
	return u.String()
}

// RoundRobinContainerList is a thread-safe list of containers that provides
// round-robin selection across all clients sharing the same list instance.
//
// The counter is stored within this struct, so all uploaders sharing the same
// ConfigurationCache will distribute their uploads evenly across containers.
type RoundRobinContainerList struct {
	containers   []*ExtendedContainerInfo
	currentIndex atomic.Int64
}

// NewRoundRobinContainerList creates a new RoundRobinContainerList from containers.
func NewRoundRobinContainerList(containers []*ExtendedContainerInfo) *RoundRobinContainerList {
	r := &RoundRobinContainerList{
		containers: containers,
	}
	// Initialize to -1 so first Add(1) returns 0
	r.currentIndex.Store(-1)
	return r
}

// EmptyRoundRobinContainerList creates an empty RoundRobinContainerList.
func EmptyRoundRobinContainerList() *RoundRobinContainerList {
	return NewRoundRobinContainerList(nil)
}

// Next returns the next container in round-robin order.
// Returns nil if the list is empty.
func (r *RoundRobinContainerList) Next() *ExtendedContainerInfo {
	if len(r.containers) == 0 {
		return nil
	}
	next := r.currentIndex.Add(1)
	size := int64(len(r.containers))
	idx := next % size
	if idx < 0 {
		idx += size
	}
	return r.containers[idx]
}

// Len returns the number of containers.
func (r *RoundRobinContainerList) Len() int {
	return len(r.containers)
}

// IsEmpty returns true if the list is empty.
func (r *RoundRobinContainerList) IsEmpty() bool {
	return len(r.containers) == 0
}

// Get returns the container at the given index.
func (r *RoundRobinContainerList) Get(idx int) *ExtendedContainerInfo {
	return r.containers[idx]
}

// All returns a copy of all containers.
func (r *RoundRobinContainerList) All() []*ExtendedContainerInfo {
	result := make([]*ExtendedContainerInfo, len(r.containers))
	copy(result, r.containers)
	return result
}
