# Quickstart App

The quickstart application is a **self-contained and runnable** example app that demonstrates authenticating, connecting to, administering, ingesting data into and querying Azure Data Explorer using the azure-kusto-java SDK.
You can use it as a baseline to write your own first kusto client application, altering the code as you go, or copy code sections out of it into your app.

**Tip:** The app includes comments with tips on recommendations, coding best practices, links to reference materials and recommended TODO changes when adapting the code to your needs.


## Using the App for the first time

### Prerequisites

1. Set up Java version 8 or higher on your machine. For instructions, consult a Java environment setup tutorial, like [this](https://www.tutorialspoint.com/java/java_environment_setup.htm).
1. Set up [Apache Maven](https://maven.apache.org/install.html), which is the most popular Java dependency management tool.

### Retrieving the app from GitHub

1. Download the app files from this GitHub repo.
1. Modify the `kusto_sample_config.json` file, changing `KustoUri`, `IngestUri` and `DatabaseName` appropriately for your cluster.

### Retrieving the app from OneClick

1. Open a browser and type your cluster's URL (e.g. https://mycluster.westeurope.kusto.windows.net/). You will be redirected to the _Azure Data Explorer_ Web UI.
1. On the left menu, select **Data**.
1. Click **Generate Sample App Code** and then follow the instructions in the wizard.
1. Download the app as a ZIP file.
1. Extract the app source code.
**Note**: The configuration parameters defined in the `kusto_sample_config.json` file are preconfigured with the appropriate values for your cluster. Verify that these are correct.

### Run the app

1. Open a command line window and navigate to the folder where you extracted the app.
1. Run `mvn clean install` to compile the source code into a binary.
1. Run the binary using `java -jar target\kusto-quickstart-[version]-jar-with-dependencies.jar`.

#### Troubleshooting

* If you are having trouble running the app from your IDE, first check if the app runs from the command line, then consult the troubleshooting references of your IDE.