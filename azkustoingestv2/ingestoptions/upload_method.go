// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package ingestoptions

// UploadMethod specifies the upload method to use for blob uploads.
type UploadMethod int

const (
	// UploadMethodDefault uses server preference or Storage as fallback.
	UploadMethodDefault UploadMethod = iota
	// UploadMethodStorage uses Azure Storage blob.
	UploadMethodStorage
	// UploadMethodLake uses OneLake.
	UploadMethodLake
)

// String returns the string representation of the upload method.
func (m UploadMethod) String() string {
	switch m {
	case UploadMethodStorage:
		return "Storage"
	case UploadMethodLake:
		return "Lake"
	default:
		return "Default"
	}
}
