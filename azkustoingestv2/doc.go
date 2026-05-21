// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Package azkustoingestv2 provides the next-generation ingestion client for Azure Data Explorer (Kusto).
//
// The v2 package implements a modernized ingestion layer with:
//   - HTTP-based configuration fetching from DM endpoints
//   - Round-robin container selection with shared atomic counters
//   - Lake folder support (Storage vs Lake upload methods)
//   - Policy-driven managed streaming fallback
//   - S2S authentication for Fabric Private Link
//   - Trusted endpoint validation
//   - Enhanced uploader abstraction with retry logic
package azkustoingestv2
