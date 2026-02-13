---
title: Azure Blob Storage Provider
description: Read from and write to Azure Blob Storage using the Azure Blob Storage provider.
sidebar_position: 4
---

## Azure Blob Storage Provider

The Azure Blob Storage provider enables NPipeline applications to read from and write to Azure Blob Storage using a unified storage abstraction. This provider implements the `IStorageProvider` interface and supports the `azure://` URI scheme.

### Overview

The Azure Blob Storage provider provides seamless integration with Azure Blob Storage. It offers:

- **Stream-based I/O** for efficient handling of large files
- **Async-first API** for scalable, non-blocking operations
- **Flexible authentication** via Azure credential chain or explicit credentials
- **Azure Blob Storage support** with full feature parity
- **Comprehensive error handling** with proper exception translation
- **Metadata support** for retrieving blob metadata
- **Listing operations** with recursive and non-recursive modes
- **Block blob upload** for large files with configurable thresholds

### When to Use This Provider

Use the Azure Blob Storage provider when your application needs to:

- Store and retrieve data in Azure Blob Storage
- Integrate cloud storage into NPipeline data pipelines
- Leverage Azure's scalability and durability for data storage
- Handle large files through streaming and block blob uploads
- Work with Azure Storage Emulator (Azurite) for local development

## Dependencies

The Azure Blob Storage provider depends on the following packages:

- `Azure.Storage.Blobs` - Azure SDK for Blob Storage operations
- `Azure.Identity` - Azure SDK for authentication
- `NPipeline.StorageProviders` - Core storage abstractions (IStorageProvider, StorageUri, StorageItem, StorageMetadata, StorageProviderMetadata, StorageResolverOptions, StorageProviderFactory)
- `NPipeline.Connectors` - Core connectors for using storage providers with connectors

### Key Storage Types

- **`IStorageProvider`** - Core storage provider interface
  - Location: `NPipeline.StorageProviders.Abstractions.IStorageProvider`
  - Defines methods for reading, writing, listing, and checking existence of storage objects

- **`StorageUri`** - URI type for storage resources
  - Location: `NPipeline.StorageProviders.StorageUri`
  - Represents a URI for storage resources with scheme, host, path, and parameters

- **`StorageItem`** - Represents a storage item (file or directory)
  - Location: `NPipeline.StorageProviders.StorageItem`
  - Contains URI, size, last modified date, and directory flag

- **`StorageMetadata`** - Metadata for storage objects
  - Location: `NPipeline.StorageProviders.StorageMetadata`
  - Contains size, content type, last modified date, ETag, and custom metadata

- **`StorageProviderMetadata`** - Metadata about a storage provider's capabilities
  - Location: `NPipeline.StorageProviders.StorageProviderMetadata`
  - Contains capability flags and supported schemes

- **`StorageResolverOptions`** - Configuration options for creating storage resolvers
  - Location: `NPipeline.StorageProviders.StorageResolverOptions`
  - Controls which providers are included in the resolver

- **`StorageProviderFactory`** - Factory for creating storage resolvers
  - Location: `NPipeline.StorageProviders.StorageProviderFactory`
  - Provides methods for creating resolvers with custom providers

## Installation

### Prerequisites

- .NET 6.0 or later
- An Azure Storage account (or Azurite for local development)
- Appropriate Azure Storage permissions

### Package Installation

Add the project reference to your solution:

```bash
dotnet add src/NPipeline.StorageProviders.Azure/NPipeline.StorageProviders.Azure.csproj
```

Or add it to your `.csproj` file:

```xml
<ItemGroup>
  <ProjectReference Include="..\NPipeline.StorageProviders.Azure\NPipeline.StorageProviders.Azure.csproj" />
</ItemGroup>
```

## Quick Start

### Basic Usage with Connectors

The Azure Blob Storage provider works seamlessly with all NPipeline connectors. Here's a quick example using the CSV connector:

```csharp
using NPipeline;
using NPipeline.Connectors;
using NPipeline.Connectors.Csv;
using NPipeline.StorageProviders.Azure;

// Create a resolver with Azure support
var resolver = StorageProviderFactory.CreateResolver(
    new StorageResolverOptions
    {
        IncludeFileSystem = true,
        AdditionalProviders = new[] { new AzureBlobStorageProvider() }
    }
);

public sealed record User(int Id, string Name, string Email);

public sealed class AzureCsvPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Read CSV from Azure Blob Storage
        var sourceNode = new CsvSourceNode<User>(
            StorageUri.Parse("azure://my-container/users.csv"),
            row => new User(
                row.Get<int>("Id") ?? 0,
                row.Get<string>("Name") ?? string.Empty,
                row.Get<string>("Email") ?? string.Empty),
            resolver: resolver);
        var source = builder.AddSource(sourceNode, "azure_csv_source");
        
        // ... add transforms ...
        
        // Write CSV to Azure Blob Storage
        var sinkNode = new CsvSinkNode<UserSummary>(
            StorageUri.Parse("azure://my-container/summaries.csv"),
            resolver: resolver);
        var sink = builder.AddSink(sinkNode, "azure_csv_sink");
        
        builder.Connect(source, sink);
    }
}
```

## Configuration

### Using Dependency Injection

The recommended way to configure the Azure Blob Storage provider is through dependency injection:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Azure;

var services = new ServiceCollection();

services.AddAzureBlobStorageProvider(options =>
{
    options.UseDefaultCredentialChain = true;
    options.BlockBlobUploadThresholdBytes = 64 * 1024 * 1024; // 64 MB
});

var serviceProvider = services.BuildServiceProvider();
var provider = serviceProvider.GetRequiredService<AzureBlobStorageProvider>();
```

### AzureBlobStorageProviderOptions

The `AzureBlobStorageProviderOptions` class provides configuration options for the Azure Blob Storage provider:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `DefaultCredential` | `TokenCredential?` | `null` | Default Azure credential for authentication. If not specified, uses `DefaultAzureCredential` chain when `UseDefaultCredentialChain` is true. |
| `DefaultConnectionString` | `string?` | `null` | Default connection string for Azure Storage. Takes precedence over `DefaultCredential` if specified. |
| `UseDefaultCredentialChain` | `bool` | `true` | Whether to use the default Azure credential chain (environment variables, managed identity, Visual Studio, Azure CLI). |
| `ServiceUrl` | `Uri?` | `null` | Optional service URL for Azure Storage-compatible endpoints (e.g., Azurite). If not specified, uses the Azure Blob Storage endpoint. |
| `BlockBlobUploadThresholdBytes` | `long` | `64 * 1024 * 1024` (64 MB) | Threshold in bytes for using block blob upload when writing files. |
| `UploadMaximumConcurrency` | `int?` | `null` | Maximum concurrent upload requests for large blobs. If not specified, uses SDK default. |
| `UploadMaximumTransferSizeBytes` | `int?` | `null` | Maximum transfer size in bytes for each upload chunk. If not specified, uses SDK default. |

### Configuration Examples

#### Basic Configuration with Default Credentials

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.UseDefaultCredentialChain = true;
});
```

#### Configuration for Azurite (Local Development)

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://localhost:10000/devstoreaccount1");
    options.DefaultConnectionString = "UseDevelopmentStorage=true";
});
```

#### Configuration with Connection String

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey=mykey;EndpointSuffix=core.windows.net";
    options.UseDefaultCredentialChain = false;
});
```

#### Configuration with Custom Upload Settings

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.BlockBlobUploadThresholdBytes = 128 * 1024 * 1024; // 128 MB
    options.UploadMaximumConcurrency = 8; // 8 concurrent uploads
    options.UploadMaximumTransferSizeBytes = 8 * 1024 * 1024; // 8 MB chunks
});
```

## URI Format

The Azure Blob Storage provider uses URIs with the `azure://` scheme to identify blobs.

### Basic Format

```
azure://container-name/path/to/blob.csv
```

### With Account Name and Key

```
azure://container-name/path/to/blob.csv?accountName=mystorageaccount&accountKey=mykey
```

### With SAS Token (URL-encoded)

```
azure://container-name/path/to/blob.csv?sasToken=sp%3Dr%26st%3D2023-01-01
```

### With Azurite Endpoint

```
azure://container-name/path/to/blob.csv?serviceUrl=http://localhost:10000/devstoreaccount1
```

### Complete Parameter Table

| Parameter | Description | Example |
|-----------|-------------|---------|
| `accountName` | Azure storage account name | `accountName=mystorageaccount` |
| `accountKey` | Azure storage account key | `accountKey=mykey` |
| `sasToken` | Shared Access Signature token (URL-encoded) | `sasToken=sp%3Dr%26st%3D2023-01-01` |
| `connectionString` | Full connection string | `connectionString=DefaultEndpointsProtocol=https;AccountName=xxx;AccountKey=yyy` |
| `serviceUrl` | Custom service URL (e.g., Azurite) | `serviceUrl=http://localhost:10000/devstoreaccount1` |
| `contentType` | Content type for the object when writing | `contentType=application/json` |

### URI Examples

```csharp
// Basic Azure blob
var uri1 = StorageUri.Parse("azure://my-container/data/input.csv");

// With account name and key
var uri2 = StorageUri.Parse("azure://my-container/data/input.csv?accountName=mystorageaccount&accountKey=mykey");

// With SAS token (URL-encoded)
var uri3 = StorageUri.Parse("azure://my-container/data/output.json?sasToken=sp%3Dr%26st%3D2023-01-01");

// With Azurite endpoint
var uri4 = StorageUri.Parse("azure://my-container/data/file.csv?serviceUrl=http://localhost:10000/devstoreaccount1");

// With content type
var uri5 = StorageUri.Parse("azure://my-container/data/output.json?contentType=application/json");
```

## Authentication

The Azure Blob Storage provider supports multiple authentication methods.

### Default Azure Credential Chain (Recommended)

The default credential chain automatically searches for credentials in the following order:

1. Environment variables (`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`)
2. Workload identity (for AKS and other Kubernetes environments)
3. Managed identity (for Azure App Service, Functions, and VMs)
4. Visual Studio credentials
5. Azure CLI credentials
6. Azure PowerShell credentials

**Configuration:**

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.UseDefaultCredentialChain = true;
});
```

**Environment Variables:**

```bash
export AZURE_TENANT_ID=your_tenant_id
export AZURE_CLIENT_ID=your_client_id
export AZURE_CLIENT_SECRET=your_client_secret
```

### Connection String Authentication

Connection strings provide the simplest authentication method for development and testing.

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey=mykey;EndpointSuffix=core.windows.net";
});
```

**Connection string formats:**

```csharp
// Azure Storage
"DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey=mykey;EndpointSuffix=core.windows.net"

// Azurite (Development)
"UseDevelopmentStorage=true"

// With SAS token
"BlobEndpoint=https://mystorageaccount.blob.core.windows.net/;SharedAccessSignature=sv=2023-01-01&ss=b&srt=sco&sp=rwdlac&se=2024-01-01T00:00:00Z&st=2023-01-01T00:00:00Z&spr=https&sig=mysignature"
```

### Account Key Authentication

Use account key authentication for explicit credential management:

```csharp
// Via URI parameters
var uri = StorageUri.Parse("azure://my-container/blob.csv?accountName=mystorageaccount&accountKey=mykey");

// Via connection string
services.AddAzureBlobStorageProvider(options =>
{
    options.DefaultConnectionString = $"DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey={accountKey}";
});
```

⚠️ **Security Warning:** Avoid passing credentials in URIs in production code. URIs may be logged, displayed in error messages, or stored in configuration files. Use the credential chain instead.

### SAS Token Authentication

Shared Access Signature (SAS) tokens provide time-limited, scoped access:

```csharp
// Via URI parameters (URL-encoded)
var uri = StorageUri.Parse("azure://my-container/blob.csv?sasToken=sp%3Dr%26st%3D2023-01-01");

// Via connection string
services.AddAzureBlobStorageProvider(options =>
{
    options.DefaultConnectionString = "BlobEndpoint=https://mystorageaccount.blob.core.windows.net/;SharedAccessSignature=sv=2023-01-01&ss=b&sp=rwdlac&se=2024-01-01T00:00:00Z&st=2023-01-01T00:00:00Z&spr=https&sig=mysignature";
});
```

> **Note:** SAS tokens must be URL-encoded when included as URI parameters.

### Custom TokenCredential

Provide a custom credential implementation for advanced scenarios:

```csharp
using Azure.Identity;

services.AddAzureBlobStorageProvider(options =>
{
    options.DefaultCredential = new ClientSecretCredential(
        tenantId: "your-tenant-id",
        clientId: "your-client-id",
        clientSecret: "your-client-secret");
});
```

## Azurite (Local Development)

Azurite provides a local Azure Storage emulator for development and testing.

### Configuration

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://localhost:10000/devstoreaccount1");
    options.DefaultConnectionString = "UseDevelopmentStorage=true";
});
```

### URI Example

```csharp
var uri = StorageUri.Parse("azure://my-container/data/file.csv?serviceUrl=http://localhost:10000/devstoreaccount1");
```

**Azurite Configuration:**

- **Account Name:** `devstoreaccount1`
- **Account Key:** `Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==`
- **Service URL:** `http://localhost:10000/devstoreaccount1`

## Examples

### Reading from Azure Blob Storage

```csharp
using NPipeline.Connectors;
using NPipeline.StorageProviders.Azure;

var provider = new AzureBlobStorageProvider(
    new AzureBlobClientFactory(new AzureBlobStorageProviderOptions()),
    new AzureBlobStorageProviderOptions());
var uri = StorageUri.Parse("azure://my-container/data.csv");

using var stream = await provider.OpenReadAsync(uri);
using var reader = new StreamReader(stream);
var content = await reader.ReadToEndAsync();
```

### Writing to Azure Blob Storage

```csharp
var provider = new AzureBlobStorageProvider(
    new AzureBlobClientFactory(new AzureBlobStorageProviderOptions()),
    new AzureBlobStorageProviderOptions());
var uri = StorageUri.Parse("azure://my-container/output.csv");

using var stream = await provider.OpenWriteAsync(uri);
using var writer = new StreamWriter(stream);
await writer.WriteLineAsync("id,name,value");
await writer.WriteLineAsync("1,Item A,100");
```

### Listing Files

```csharp
var provider = new AzureBlobStorageProvider(
    new AzureBlobClientFactory(new AzureBlobStorageProviderOptions()),
    new AzureBlobStorageProviderOptions());
var uri = StorageUri.Parse("azure://my-container/data/");

// List all files recursively
await foreach (var item in provider.ListAsync(uri, recursive: true))
{
    Console.WriteLine($"{item.Uri} - {item.Size} bytes - Modified: {item.LastModified}");
}

// List only immediate children (non-recursive)
await foreach (var item in provider.ListAsync(uri, recursive: false))
{
    var type = item.IsDirectory ? "[DIR]" : "[FILE]";
    Console.WriteLine($"{type} {item.Uri} - {item.Size} bytes");
}
```

### Checking File Existence

```csharp
var provider = new AzureBlobStorageProvider(
    new AzureBlobClientFactory(new AzureBlobStorageProviderOptions()),
    new AzureBlobStorageProviderOptions());
var uri = StorageUri.Parse("azure://my-container/data.csv");

var exists = await provider.ExistsAsync(uri);
if (exists)
{
    Console.WriteLine("Blob exists!");
}
else
{
    Console.WriteLine("Blob not found.");
}
```

### Deleting Files

```csharp
var provider = new AzureBlobStorageProvider(
    new AzureBlobClientFactory(new AzureBlobStorageProviderOptions()),
    new AzureBlobStorageProviderOptions());
var uri = StorageUri.Parse("azure://my-container/data.csv");

await provider.DeleteAsync(uri);
Console.WriteLine("Blob deleted successfully.");
```

> **Note:** `DeleteAsync` uses `DeleteIfExistsAsync` internally, so it does not throw if the blob does not exist.

### Getting Metadata

```csharp
var provider = new AzureBlobStorageProvider(
    new AzureBlobClientFactory(new AzureBlobStorageProviderOptions()),
    new AzureBlobStorageProviderOptions());
var uri = StorageUri.Parse("azure://my-container/data.csv");

var metadata = await provider.GetMetadataAsync(uri);
if (metadata != null)
{
    Console.WriteLine($"Size: {metadata.Size} bytes");
    Console.WriteLine($"Content Type: {metadata.ContentType}");
    Console.WriteLine($"Last Modified: {metadata.LastModified}");
    Console.WriteLine($"ETag: {metadata.ETag}");
    
    foreach (var (key, value) in metadata.CustomMetadata)
    {
        Console.WriteLine($"  {key}: {value}");
    }
}
```

## Error Handling

The Azure Blob Storage provider translates Azure SDK exceptions to standard .NET exceptions for consistent error handling.

### Exception Mapping

| Azure Error Code | HTTP Status | .NET Exception | Description |
|------------------|--------------|------------------|-------------|
| `AuthenticationFailed` | 401 | `UnauthorizedAccessException` | Authentication or authorization failure |
| `AuthorizationFailed` | 403 | `UnauthorizedAccessException` | Authorization failure |
| `ContainerNotFound` | 404 | `FileNotFoundException` | Container not found |
| `BlobNotFound` | 404 | `FileNotFoundException` | Blob not found |
| `InvalidQueryParameterValue` | 400 | `ArgumentException` | Invalid query parameter value |
| `InvalidResourceName` | 400 | `ArgumentException` | Invalid container or blob name |
| Other | Various | `IOException` | General Azure storage access failure |

### Error Handling Example

```csharp
try
{
    using var stream = await provider.OpenReadAsync(uri);
    // Process stream...
}
catch (FileNotFoundException ex)
{
    Console.WriteLine($"Blob not found: {ex.Message}");
}
catch (UnauthorizedAccessException ex)
{
    Console.WriteLine($"Access denied: {ex.Message}");
    Console.WriteLine("Check your credentials and permissions.");
}
catch (ArgumentException ex)
{
    Console.WriteLine($"Invalid URI: {ex.Message}");
}
catch (IOException ex)
{
    Console.WriteLine($"Azure storage access error: {ex.Message}");
    if (ex.InnerException is RequestFailedException azureEx)
    {
        Console.WriteLine($"Azure Error Code: {azureEx.ErrorCode}");
        Console.WriteLine($"Status: {azureEx.Status}");
    }
}
```

## Azure Storage Permissions

To use the Azure Blob Storage provider, your Azure credentials must have appropriate permissions.

### Required Permissions by Operation

| Operation | Required Permission |
|-----------|---------------------|
| Read (OpenReadAsync) | `Microsoft.Storage/storageAccounts/blobServices/containers/read` |
| Write (OpenWriteAsync) | `Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write` |
| List (ListAsync) | `Microsoft.Storage/storageAccounts/blobServices/containers/read` |
| Metadata (GetMetadataAsync) | `Microsoft.Storage/storageAccounts/blobServices/containers/read` |
| Existence (ExistsAsync) | `Microsoft.Storage/storageAccounts/blobServices/containers/read` |
| Delete (DeleteAsync) | `Microsoft.Storage/storageAccounts/blobServices/containers/blobs/delete` |

### Example RBAC Role

Assign the `Storage Blob Data Contributor` role for full read/write/delete access:

```json
{
  "roleName": "Storage Blob Data Contributor",
  "description": "Allows for read, write and delete access to Azure Storage blob containers and data",
  "assignableScopes": [
    "/subscriptions/{subscription-id}/resourceGroups/{resource-group}/providers/Microsoft.Storage/storageAccounts/{storage-account}"
  ],
  "permissions": [
    {
      "actions": [
        "Microsoft.Storage/storageAccounts/blobServices/containers/read",
        "Microsoft.Storage/storageAccounts/blobServices/containers/write",
        "Microsoft.Storage/storageAccounts/blobServices/containers/delete",
        "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/read",
        "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write",
        "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/delete"
      ]
    }
  ]
}
```

## API Reference

### Core Interfaces and Types

- **`IStorageProvider`** - Core storage provider interface
  - Location: [`NPipeline.StorageProviders.Abstractions.IStorageProvider`](../../src/NPipeline.StorageProviders/Abstractions/IStorageProvider.cs)
  - Defines methods for reading, writing, listing, and checking existence of storage objects

- **`StorageUri`** - URI type for storage resources
  - Location: [`NPipeline.StorageProviders.StorageUri`](../../src/NPipeline.StorageProviders/Models/StorageUri.cs)
  - Represents a URI for storage resources with scheme, host, path, and parameters

- **`StorageItem`** - Represents a storage item (file or directory)
  - Location: [`NPipeline.StorageProviders.StorageItem`](../../src/NPipeline.StorageProviders/Models/StorageItem.cs)
  - Contains URI, size, last modified date, and directory flag

- **`StorageMetadata`** - Metadata for storage objects
  - Location: [`NPipeline.StorageProviders.StorageMetadata`](../../src/NPipeline.StorageProviders/Models/StorageMetadata.cs)
  - Contains size, content type, last modified date, ETag, and custom metadata

### Azure-Specific Types

- **`AzureBlobStorageProvider`** - Azure Blob Storage provider implementation
  - Location: [`AzureBlobStorageProvider.cs`](../../src/NPipeline.StorageProviders.Azure/AzureBlobStorageProvider.cs)
  - Implements `IStorageProvider` and `IStorageProviderMetadataProvider`

- **`AzureBlobStorageProviderOptions`** - Configuration options
  - Location: [`AzureBlobStorageProviderOptions.cs`](../../src/NPipeline.StorageProviders.Azure/AzureBlobStorageProviderOptions.cs)
  - Contains credential, service URL, and upload settings

- **`AzureBlobClientFactory`** - Factory for creating Azure Blob clients
  - Location: [`AzureBlobClientFactory.cs`](../../src/NPipeline.StorageProviders.Azure/AzureBlobClientFactory.cs)
  - Creates and caches `BlobServiceClient` instances

- **`AzureStorageException`** - Custom exception for Azure errors
  - Location: [`AzureStorageException.cs`](../../src/NPipeline.StorageProviders.Azure/AzureStorageException.cs)
  - Wraps `RequestFailedException` with container/blob context

### Extension Methods

- **`ServiceCollectionExtensions.AddAzureBlobStorageProvider`**
  - Location: [`ServiceCollectionExtensions.cs`](../../src/NPipeline.StorageProviders.Azure/ServiceCollectionExtensions.cs)
  - Extension method for registering Azure Blob Storage provider in DI container

## Limitations

The Azure Blob Storage provider has the following limitations:

### Delete Operations

- `DeleteAsync` is **supported** and uses `DeleteIfExistsAsync` internally
- It does not throw an exception if the blob does not exist
- Full delete access requires appropriate Azure Storage permissions

### Flat Storage Model

- Azure Blob Storage is a flat object storage system (no true hierarchical directories)
- Directory-like paths are simulated through blob name prefixes
- The provider treats paths with "/" delimiters as virtual directories for listing purposes

### Large File Handling

- Block blob upload is used for files larger than the `BlockBlobUploadThresholdBytes` (default 64 MB)
- The threshold is configurable via `AzureBlobStorageProviderOptions`
- For very large files, ensure sufficient memory and network bandwidth

### Concurrent Operations

- The provider is thread-safe for read operations
- Concurrent writes to the same blob may result in race conditions
- Use appropriate locking or versioning strategies if needed

## Additional Resources

- [Azure Blob Storage Documentation](https://docs.microsoft.com/azure/storage/blobs/)
- [Azure SDK for .NET Documentation](https://docs.microsoft.com/azure/sdk/)
- [Azurite Documentation](https://docs.microsoft.com/azure/storage/common/storage-use-azurite?tabs=visual-studio)
