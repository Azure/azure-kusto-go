### Prerequisites

- Go, version 1.19 or higher
- An [Azure subscription](https://azure.microsoft.com/free/)
- An [Azure Data Explorer Cluster](https://learn.microsoft.com/en-us/azure/data-explorer/).
- An Azure Data Explorer Database. You can create a Database in your Azure Data Explorer Cluster using the [Azure Portal](https://learn.microsoft.com/en-us/azure/data-explorer/create-cluster-database-portal).

### Instructions

1. Download the app files from this GitHub repo.
1. Modify the `kusto_sample_config.json` file, changing `KustoUri`, `IngestUri` and `DatabaseName` appropriately for your cluster.
1. Open a command line window and navigate to the folder where you extracted the app.
1. Run `go build` to compile the source code and files into a binary.
1. Run the binary using `.\quickstart.exe` or whichever name you provided.

### Troubleshooting

* If you are having trouble running the app from your IDE, first check if the app runs from the command line, then consult the troubleshooting references of your IDE.