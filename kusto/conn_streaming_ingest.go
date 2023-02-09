package kusto

import (
	"context"
	"fmt"
	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"io"
	"net/url"
)

type DataFormatForStreaming interface {
	CamelCase() string
	ForceDefault() DataFormatForStreaming
}

func (c *Conn) StreamIngest(ctx context.Context, db, table string, payload io.Reader, format DataFormatForStreaming, mappingName string, clientRequestId string) error {
	streamUrl, err := url.Parse(c.endStreamIngest.String())
	if err != nil {
		return errors.ES(errors.OpIngestStream, errors.KClientArgs, "could not parse the stream endpoint(%s): %s", c.endStreamIngest.String(), err).SetNoRetry()
	}
	path, err := url.JoinPath(streamUrl.Path, db, table)
	if err != nil {
		return errors.ES(errors.OpIngestStream, errors.KClientArgs, "could not join the stream endpoint(%s) with the db(%s) and table(%s): %s", c.endStreamIngest.String(), db, table, err).SetNoRetry()
	}
	streamUrl.Path = path

	qv := url.Values{}
	if mappingName != "" {
		qv.Add("mappingName", mappingName)
	}
	qv.Add("streamFormat", format.ForceDefault().CamelCase())
	streamUrl.RawQuery = qv.Encode()

	var closeablePayload io.ReadCloser
	var ok bool
	if closeablePayload, ok = payload.(io.ReadCloser); !ok {
		closeablePayload = io.NopCloser(payload)
	}

	headers := c.getHeaders(requestProperties{})

	_, body, err := c.doRequestImpl(ctx, errors.OpIngestStream, streamUrl, closeablePayload, headers, fmt.Sprintf("With db: %s, table: %s, mappingName: %s, clientRequestId: %s", db, table, mappingName, clientRequestId))
	if body != nil {
		body.Close()
	}

	if err != nil {
		return errors.ES(errors.OpIngestStream, errors.KHTTPError, "streaming ingestion failed: endpoint(%s): %s", streamUrl.String(), err)
	}

	return nil
}
