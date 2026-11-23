// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package upload

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/httpclient"
)

func (u *ContainerUploaderBase) uploadUsingBlobAPI(ctx context.Context, blobPath string, stream io.Reader) error {
	client, err := blockblob.NewClientWithNoCredential(blobPath, &blockblob.ClientOptions{
		ClientOptions: u.clientOptions,
	})
	if err != nil {
		return fmt.Errorf("create blob upload client: %w", err)
	}
	_, err = client.UploadStream(ctx, stream, &blockblob.UploadStreamOptions{
		BlockSize:   ingestoptions.UploadBlockSizeBytes,
		Concurrency: u.maxConcurrency,
	})
	if err != nil {
		return fmt.Errorf("upload to blob storage: %w", err)
	}
	return nil
}

func (u *ContainerUploaderBase) uploadUsingDataLakeAPI(ctx context.Context, blobPath string, stream io.Reader) error {
	clientOptions := u.clientOptions
	if u.s2sTokenProvider != nil {
		clientOptions.PerRetryPolicies = append(
			append([]policy.Policy(nil), clientOptions.PerRetryPolicies...),
			&fabricPrivateLinkPolicy{provider: u.s2sTokenProvider, accessContext: u.s2sAccessContext},
		)
	}
	options := &file.ClientOptions{ClientOptions: clientOptions}
	var client *file.Client
	var err error
	if u.tokenCredential != nil {
		target, parseErr := url.Parse(blobPath)
		if parseErr != nil {
			return fmt.Errorf("parse data lake upload URL: %w", parseErr)
		}
		target.RawQuery = ""
		client, err = file.NewClient(target.String(), u.tokenCredential, options)
	} else {
		client, err = file.NewClientWithNoCredential(blobPath, options)
	}
	if err != nil {
		return fmt.Errorf("create data lake upload client: %w", err)
	}
	if _, err = client.Create(ctx, nil); err != nil {
		return fmt.Errorf("create data lake file: %w", err)
	}
	err = client.UploadStream(ctx, stream, &file.UploadStreamOptions{
		ChunkSize:   ingestoptions.UploadBlockSizeBytes,
		Concurrency: uint16(u.maxConcurrency),
	})
	if err != nil {
		return fmt.Errorf("upload to data lake: %w", err)
	}
	return nil
}

type fabricPrivateLinkPolicy struct {
	provider      httpclient.S2STokenProvider
	accessContext string
}

func (p *fabricPrivateLinkPolicy) Do(req *policy.Request) (*http.Response, error) {
	token, err := p.provider(req.Raw().Context())
	if err != nil {
		return nil, fmt.Errorf("get Fabric Private Link S2S token: %w", err)
	}
	if token == nil || strings.TrimSpace(token.Token) == "" || strings.TrimSpace(token.Scheme) == "" {
		return nil, fmt.Errorf("Fabric Private Link S2S provider returned an empty token or scheme")
	}
	req.Raw().Header.Set(ingestoptions.HeaderMSS2SActorAuthorization, token.ToHeaderValue())
	req.Raw().Header.Set(ingestoptions.HeaderMSFabricS2SAccessContext, p.accessContext)
	return req.Next()
}
