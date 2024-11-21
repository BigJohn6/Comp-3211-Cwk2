# Running the function app

## Run Locally

### Prerequitisites
1. Install Azure Functions Core Tools
2. Install Azure Functions extensions on VSCode
3. Install Azurite extension on VSCode
4. A python interpreter

### Requirements
1. azure-identity
2. azure-storage-blob

### Steps
1. Add your IP address as a valid IP address on azure - There is a firewall preventing unkown IP addresses editing the database
2. Authorize DefaultAzureCredential - You can do this by typing `az login` in the Azure CLI. This is required for editing the blob storage
3. Press F1 to pen the function pallette and select `Azurite Start`
4. Run code with F5
5. The changes will be visible on in the storage account `comp3211blobstorage` then going to the `statistics` container and viewing the `statistics.txt` file. New database additions can also be seen by running the query editor on the database

## Run on Azure

### Steps
1. Select the function app `Comp3211-James-IoT` and press play
2. The changes will be visible on in the storage account `comp3211blobstorage` then going to the `statistics` container and viewing the `statistics.txt` file. New database additions can also be seen by running the query editor on the database