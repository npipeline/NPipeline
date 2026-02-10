# Azure Blob Storage Provider Sample

This sample demonstrates comprehensive usage of the Azure Blob Storage Provider in NPipeline. It showcases all key features of the provider including reading, writing, listing, metadata operations, and error handling.

## What This Sample Demonstrates

This sample application demonstrates the following features of the Azure Blob Storage Provider:

1. **Basic Read/Write Operations** - Writing and reading simple text files
2. **CSV Processing** - Creating, uploading, reading, and transforming CSV data
3. **Large File Handling** - Uploading large files (>64MB) using block blob upload
4. **Listing and Filtering** - Listing blobs recursively and with prefix filters
5. **Metadata Operations** - Uploading with custom metadata and retrieving blob properties
6. **Error Handling** - Proper exception handling for various error scenarios
7. **Authentication Methods** - Information about different authentication options

## Prerequisites

### Required Software

- [.NET 10.0 SDK](https://dotnet.microsoft.com/download) or later
- An Azure Storage account OR Azurite for local development

### Azurite (Local Development)

For local development, you can use Azurite, the Azure Storage emulator:

#### Option 1: Install via npm

```bash
npm install -g azurite
azurite
```

#### Option 2: Using Docker

```bash
docker run -p 10000:10000 -p 10001:10001 -p 10002:10002 \
  mcr.microsoft.com/azure-storage/azurite
```

Azurite provides:

- Blob service: `http://127.0.0.1:10000/devstoreaccount1`
- Queue service: `http://127.0.0.1:10001/devstoreaccount1`
- Table service: `http://127.0.0.1:10002/devstoreaccount1`

### Azure Storage Account (Production)

For production use, you'll need an Azure Storage account:

1. Create an Azure Storage account in the Azure Portal
2. Get your connection string from the Azure Portal
3. Configure the connection string in the sample (see Configuration section)

## Configuration

The sample supports multiple configuration methods in the following priority order:

### 1. appsettings.json (Recommended for Development)

Edit `appsettings.json`:

```json
{
  "AzureStorage": {
    "DefaultConnectionString": "UseDevelopmentStorage=true",
    "ServiceUrl": "http://127.0.0.1:10000/devstoreaccount1"
  }
}
```

For Azure Storage:

```json
{
  "AzureStorage": {
    "DefaultConnectionString": "DefaultEndpointsProtocol=https;AccountName=youraccount;AccountKey=yourkey;EndpointSuffix=core.windows.net"
  }
}
```

### 2. Environment Variables (Recommended for Production)

Set environment variables:

```bash
# For Azurite
export AZURE_STORAGE_CONNECTION_STRING="UseDevelopmentStorage=true"

# For Azure Storage
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=youraccount;AccountKey=yourkey;EndpointSuffix=core.windows.net"
```

### 3. Individual Environment Variables

```bash
export AZURE_STORAGE_ACCOUNT_NAME="youraccount"
export AZURE_STORAGE_ACCOUNT_KEY="yourkey"
```

### 4. Code Configuration (Fallback)

The sample includes a fallback to `UseDevelopmentStorage=true` for Azurite if no configuration is provided.

## Authentication Methods

The Azure Blob Storage Provider supports multiple authentication methods:

### Connection String (Recommended for Development)

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.DefaultConnectionString = "UseDevelopmentStorage=true";
    // or
    options.DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...";
});
```

### Account Key

Set environment variables or configure in appsettings.json:

```bash
export AZURE_STORAGE_ACCOUNT_NAME="youraccount"
export AZURE_STORAGE_ACCOUNT_KEY="yourkey"
```

### SAS Token (Shared Access Signature)

Include the SAS token in your connection string or blob URI:

```
azure://container/blob?sas_token=...
```

### Default Azure Credential Chain (Production)

Uses the `DefaultAzureCredential` from Azure.Identity, which supports:

- Managed Identity (for Azure resources)
- Service Principal
- Visual Studio credentials
- Azure CLI credentials
- Environment variables

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.UseDefaultCredentialChain = true;
});
```

### Custom Token Credential

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.DefaultCredential = new CustomTokenCredential();
});
```

## How to Run

### 1. Restore Dependencies

```bash
dotnet restore
```

### 2. Build the Sample

```bash
cd samples/Sample_AzureStorageProvider
dotnet build
```

### 3. Run the Sample

```bash
dotnet run
```

Or from the solution root:

```bash
dotnet run --project samples/Sample_AzureStorageProvider/Sample_AzureStorageProvider.csproj
```

## Expected Output

When you run the sample, you'll see output similar to:

```
╔════════════════════════════════════════════════════════════════╗
║                                                                ║
║   NPipeline Azure Blob Storage Provider Sample                ║
║                                                                ║
╚════════════════════════════════════════════════════════════════╝

╔════════════════════════════════════════════════════════════════╗
║   Configuration Information                                    ║
╚════════════════════════════════════════════════════════════════╝

  Azure Storage Configuration:
    Connection String: UseDevelopmentStorage=true
    Service URL: http://127.0.0.1:10000/devstoreaccount1

  Upload Configuration:
    Block Blob Threshold: 64 MB
    Max Concurrency: 4
    Max Transfer Size: 4 MB

╔════════════════════════════════════════════════════════════════╗
║   Azure Blob Storage Provider - Comprehensive Demo Suite      ║
╚════════════════════════════════════════════════════════════════╝

┌──────────────────────────────────────────────────────────────────┐
│  Provider Metadata                                               │
└──────────────────────────────────────────────────────────────────┘
  Name: Azure Blob Storage
  Supported Schemes: azure
  Supports Read: True
  Supports Write: True
  Supports Delete: True
  Supports Listing: True
  Supports Metadata: True
  Supports Hierarchy: False
  Capabilities:
    - blockBlobUploadThresholdBytes: 67108864
    - supportsServiceUrl: True
    - supportsConnectionString: True
    - supportsSasToken: True
    - supportsAccountKey: True
    - supportsDefaultCredentialChain: True

┌──────────────────────────────────────────────────────────────────┐
│  Demo 1: Basic Read/Write                                        │
└──────────────────────────────────────────────────────────────────┘
  Writing to: azure://demo-container/demo/hello.txt
  ✓ Write successful
  Reading from: azure://demo-container/demo/hello.txt
  ✓ Read successful
  Content: Hello, Azure Blob Storage! This is a test file created by NPipeline.
  ✓ Blob exists: True
  ✓ Blob deleted

┌──────────────────────────────────────────────────────────────────┐
│  Demo 2: CSV Processing                                           │
└──────────────────────────────────────────────────────────────────┘
  Uploading CSV to: azure://demo-container/demo/sensor-data.csv
  ✓ CSV uploaded successfully
  Reading and processing CSV from: azure://demo-container/demo/sensor-data.csv
    - Sensor S001 at Server Room: 23.5°C, 65.2% humidity at 2024-01-15 10:00:00
    - Sensor S002 at Data Center: 24.1°C, 62.8% humidity at 2024-01-15 11:00:00
    - ...
  ✓ Processed 5 sensor readings
  Filtering data (temperature > 23°C)...
  ✓ Filtered to 4 readings
  Writing processed data to: azure://demo-container/demo/processed-sensor-data.csv
  ✓ Processed data written successfully
  ✓ Cleanup completed

┌──────────────────────────────────────────────────────────────────┐
│  Demo 3: Large File Handling (>64MB)                              │
└──────────────────────────────────────────────────────────────────┘
  Generating large file (10MB)...
  ✓ File generated
  Uploading large file to: azure://demo-container/demo/large-file.bin
  ✓ Upload completed in 234ms
    Upload speed: 42.74 MB/s
  ✓ Blob verified
    Size: 10.00 MB
    Last Modified: 2024-01-15 12:00:00
  ✓ Read 1024 bytes successfully
  ✓ Large file deleted

┌──────────────────────────────────────────────────────────────────┐
│  Demo 4: Listing and Filtering                                    │
└──────────────────────────────────────────────────────────────────┘
  Uploading test files...
  ✓ Uploaded 6 files

  Listing all blobs (recursive):
    - /demo/data/file1.txt (29 bytes)
    - /demo/data/file2.txt (29 bytes)
    - /demo/data/archive/file3.txt (35 bytes)
    - /demo/logs/app.log (20 bytes)
    - /demo/logs/error.log (21 bytes)
    - /demo/config/settings.json (28 bytes)
  ✓ Found 6 blobs

  Listing blobs with prefix 'demo/data/':
    - /demo/data/file1.txt
    - /demo/data/file2.txt
    - /demo/data/archive/file3.txt
  ✓ Found 3 blobs

  Listing blobs non-recursively (demo/):
    - /demo/data/file1.txt
    - /demo/logs/app.log
    - /demo/config/settings.json
  ✓ Found 3 blobs at top level

  Listing blobs with prefix 'demo/logs/':
    - /demo/logs/app.log
    - /demo/logs/error.log
  ✓ Found 2 log files

  Cleaning up test files...
  ✓ All test files deleted

┌──────────────────────────────────────────────────────────────────┐
│  Demo 5: Metadata Operations                                      │
└──────────────────────────────────────────────────────────────────┘
  Uploading blob with content type: azure://demo-container/demo/metadata-test.txt
  ✓ Blob uploaded with content type

  Retrieving metadata:
    Size: 53 bytes
    Content Type: text/plain
    Last Modified: 2024-01-15 12:00:00
    ETag: "0x8D..."
    Is Directory: False
  ✓ Metadata retrieved successfully

  Blob exists: True
  Non-existent blob exists: False
  ✓ Blob deleted

┌──────────────────────────────────────────────────────────────────┐
│  Demo 6: Error Handling                                           │
└──────────────────────────────────────────────────────────────────┘
  Scenario 1: Reading non-existent blob
    ✓ Caught expected FileNotFoundException: Azure container 'demo-container' or blob 'demo/non-existent-file.txt' not found.

  Scenario 2: Deleting non-existent blob (DeleteIfExists)
    ✓ Delete completed without exception (DeleteIfExists behavior)

  Scenario 3: Getting metadata for non-existent blob
    ✓ Returned null as expected for non-existent blob

  Scenario 4: Accessing invalid container name
    ✓ Caught expected ArgumentException: Invalid Azure container 'invalid-container-name-with-invalid-chars!' or blob 'file.txt'.

  Scenario 5: Using null URI
    ✓ Caught expected ArgumentNullException: uri

┌──────────────────────────────────────────────────────────────────┐
│  Demo 7: Authentication Methods                                   │
└──────────────────────────────────────────────────────────────────┘
  The Azure Blob Storage Provider supports multiple authentication methods:

  1. Connection String (Recommended for development):
     - UseDevelopmentStorage=true (for Azurite emulator)
     - DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net

  2. Account Key:
     - Set AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY environment variables
     - Or configure via AzureBlobStorageProviderOptions.DefaultConnectionString

  3. SAS Token (Shared Access Signature):
     - Include SAS token in the connection string or blob URI
     - Example: azure://container/blob?sas_token=...

  4. Default Azure Credential Chain (Production):
     - Uses DefaultAzureCredential from Azure.Identity
     - Supports: Managed Identity, Service Principal, Visual Studio, Azure CLI, etc.
     - Enabled by default via AzureBlobStorageProviderOptions.UseDefaultCredentialChain

  5. Custom Token Credential:
     - Provide a custom TokenCredential via AzureBlobStorageProviderOptions.DefaultCredential

  Configuration Priority:
    1. Connection string in URI parameters
    2. AzureBlobStorageProviderOptions.DefaultConnectionString
    3. AzureBlobStorageProviderOptions.DefaultCredential
    4. Default credential chain (if UseDefaultCredentialChain is true)

  For this demo, we're using the Azurite emulator with:
    ConnectionString: UseDevelopmentStorage=true
    ServiceUrl: http://127.0.0.1:10000/devstoreaccount1

  To configure authentication in your application:

  Option A - Environment Variables:
    export AZURE_STORAGE_CONNECTION_STRING="UseDevelopmentStorage=true"

  Option B - appsettings.json:
    {
      "AzureStorage": {
        "DefaultConnectionString": "UseDevelopmentStorage=true",
        "ServiceUrl": "http://127.0.0.1:10000/devstoreaccount1"
      }
    }

  Option C - Code Configuration:
    services.AddAzureBlobStorageProvider(options =>
    {
        options.DefaultConnectionString = "UseDevelopmentStorage=true";
        options.ServiceUrl = new Uri("http://127.0.0.1:10000/devstoreaccount1");
    });

╔════════════════════════════════════════════════════════════════╗
║   All demos completed successfully!                           ║
╚════════════════════════════════════════════════════════════════╝
```

## Project Structure

```
Sample_AzureStorageProvider/
├── Program.cs                          # Main entry point with DI configuration
├── AzureStorageProviderDemo.cs         # Demo scenarios class
├── Models/
│   └── SensorData.cs                   # Sample data model for CSV processing
├── appsettings.json                    # Configuration file
├── Sample_AzureStorageProvider.csproj  # Project file with dependencies
└── README.md                           # This file
```

## Key Concepts

### StorageUri Format

Azure Blob Storage URIs follow this format:

```
azure://<container-name>/<blob-path>?<parameters>
```

Examples:

- `azure://mycontainer/file.txt`
- `azure://mycontainer/data/file.csv?contentType=text/csv`
- `azure://mycontainer/archive/data.json`

### Dependency Injection

The sample uses Microsoft.Extensions.DependencyInjection for service registration:

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.DefaultConnectionString = "UseDevelopmentStorage=true";
    options.ServiceUrl = new Uri("http://127.0.0.1:10000/devstoreaccount1");
    options.BlockBlobUploadThresholdBytes = 64 * 1024 * 1024;
    options.UploadMaximumConcurrency = 4;
    options.UploadMaximumTransferSizeBytes = 4 * 1024 * 1024;
});
```

### Async/Await Pattern

All operations in the Azure Blob Storage Provider are async-first:

```csharp
// Read a blob
await using var stream = await provider.OpenReadAsync(uri, cancellationToken);

// Write a blob
await using var writeStream = await provider.OpenWriteAsync(uri, cancellationToken);
await writeStream.WriteAsync(data, cancellationToken);

// Check existence
var exists = await provider.ExistsAsync(uri, cancellationToken);

// List blobs
await foreach (var item in provider.ListAsync(prefix, recursive, cancellationToken))
{
    Console.WriteLine(item.Uri.Path);
}
```

### Error Handling

The provider translates Azure SDK exceptions into .NET standard exceptions:

- `FileNotFoundException` - Blob not found
- `UnauthorizedAccessException` - Authentication/authorization failed
- `ArgumentException` - Invalid container or blob name
- `IOException` - General I/O errors

## Troubleshooting

### Azurite Connection Issues

If you encounter connection errors with Azurite:

1. Verify Azurite is running:

   ```bash
   curl http://127.0.0.1:10000/devstoreaccount1
   ```

2. Check the port configuration (default: 10000 for Blob service)

3. Ensure no firewall is blocking the connection

### Azure Storage Connection Issues

If you encounter connection errors with Azure Storage:

1. Verify your connection string is correct
2. Check that your account key is valid
3. Ensure your storage account exists and is accessible
4. Verify network connectivity to Azure

### Build Errors

If you encounter build errors:

1. Restore NuGet packages:

   ```bash
   dotnet restore
   ```

2. Clean and rebuild:

   ```bash
   dotnet clean
   dotnet build
   ```

3. Ensure you have .NET 10.0 SDK installed:

   ```bash
   dotnet --version
   ```

## Additional Resources

- [Azure Blob Storage Documentation](https://docs.microsoft.com/azure/storage/blobs/)
- [Azurite Documentation](https://docs.microsoft.com/azure/storage/common/storage-use-azurite)
- [NPipeline Documentation](../../docs/)
- [Azure Storage Provider Design](../../docs/design/azure-storage-provider.md)

## License

This sample is part of the NPipeline project and follows the same license terms.
