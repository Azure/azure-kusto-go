/*
Package ingest provides data ingestion from various external sources into Kusto.

For more information on Kusto Data Ingestion, please see: https://docs.microsoft.com/en-us/azure/kusto/management/data-ingestion/


Create a client

Creating a client simply requires a *kusto.Client, the name of the database and the name of the table to be ingested into.

	in, err := kusto.New(kustoClient, "database", "table")
	if err != nil {
		panic("add error handling")
	}


Ingestion from a local file

Ingesting a local file requires simply passing the path to the file to be ingested:

	if err := in.FromFile(ctx, "/path/to/a/local/file"); err != nil {
		panic("add error handling")
	}

FromFile() will accept Unix path names on Unix platforms and Windows path names on Windows platforms.
The file will not be deleted after upload (there is an option that will allow that though).


Ingestion from an Azure Blob Storage file

This package will also accept ingestion from an Azure Blob Storage file:

	if err := in.FromFile(ctx, "https://myaccount.blob.core.windows.net/$root/myblob"); err != nil {
		panic("add error handling")
	}

This will ingest a file from Azure Blob Storage. We only support https:// paths and your domain name may differ than what is here.

Ingestion from a Stream

Instestion from a stream commits blocks of fully formed data encodes (JSON, AVRO, ...) into Kusto:

	if err := in.Stream(ctx , jsonEncodedData, ingest.JSON, "mappingName"); err != nil {
		panic("add error handling")
	}
*/
package ingest
