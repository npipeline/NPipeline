# Azure Blob Storage Provider

The Azure Blob Storage Provider enables NPipeline applications to read from and write to Azure Blob Storage using a unified storage abstraction. This provider
implements the `IStorageProvider` interface and supports the `azure://` URI scheme.

## Overview

The Azure Blob Storage Provider provides seamless integration with Azure Blob Storage. It offers:

- **Stream-based I/O** for efficient handling of large files
- **Async-first API** for scalable, non-blocking operations
- **Flexible authentication** via Azure credential chain or explicit credentials
- **Comprehensive error handling** with proper exception translation
- **Metadata support** for retrieving blob metadata
- **Listing operations** with recursive and non-recursive modes
- **Streaming block blob upload** - blocks upload while you write, with no local temporary file, and a small object uploads in one request

### Why Use This Provider

Use the Azure Blob Storage Provider when your application needs to:

- Store and retrieve data in Azure Blob Storage
- Integrate cloud storage into NPipeline data pipelines
- Leverage Azure's scalability and durability for data storage
- Handle large files through streaming block blob uploads
- Work with Azure Storage Emulator (Azurite) for local development

## Installation

### Prerequisites

- .NET 6.0 or later
- An Azure Storage account (or Azurite for local development)
- Appropriate Azure Storage permissions

### NuGet Package Installation

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

### Required Dependencies

The Azure Blob Storage Provider depends on the following packages:

- `Azure.Storage.Blobs` - Azure SDK for Blob Storage operations
- `Azure.Identity` - Azure SDK for authentication
- `NPipeline.StorageProviders` - Core storage abstractions

These dependencies are automatically resolved when adding the project reference.

### .NET Version Requirements

- **Minimum**: .NET 6.0
- **Recommended**: .NET 8.0 or later for best performance

## Quick Start

### Basic Setup with Dependency Injection

The recommended way to configure the Azure Blob Storage Provider is through dependency injection:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Azure;

var services = new ServiceCollection();

services.AddAzureBlobStorageProvider(options =>
{
    options.AccountName = "mystorageaccount";
    options.UseDefaultCredentialChain = true;
    options.PartSizeBytes = 8 * 1024 * 1024; // 8 MiB blocks (the default)
});

var serviceProvider = services.BuildServiceProvider();
var provider = serviceProvider.GetRequiredService<AzureBlobStorageProvider>();
```

### Simple Read Example

```csharp
using NPipeline.StorageProviders.Azure;
using NPipeline.StorageProviders.Models;

var options = new AzureBlobStorageProviderOptions { AccountName = "mystorageaccount" };
var provider = new AzureBlobStorageProvider(new AzureBlobClientFactory(options), options);

var uri = StorageUri.Parse("azure://my-container/data.csv");

using var stream = await provider.OpenReadAsync(uri);
using var reader = new StreamReader(stream);
var content = await reader.ReadToEndAsync();
Console.WriteLine(content);
```

### Simple Write Example

```csharp
var options = new AzureBlobStorageProviderOptions { AccountName = "mystorageaccount", CreateContainerIfMissing = true };
var provider = new AzureBlobStorageProvider(new AzureBlobClientFactory(options), options);

var uri = StorageUri.Parse("azure://my-container/output.csv");

await using var stream = await provider.OpenWriteAsync(uri);
await using (var writer = new StreamWriter(stream, leaveOpen: true))
{
    await writer.WriteLineAsync("id,name,value");
    await writer.WriteLineAsync("1,Item A,100");
}

// The blob appears only after CommitAsync. Disposing without it leaves any existing blob unchanged.
await stream.CommitAsync();
```

### Minimal Working Code

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Azure;
using NPipeline.StorageProviders.Models;

// Setup
var services = new ServiceCollection();
services.AddAzureBlobStorageProvider(options =>
{
    options.AccountName = "mystorageaccount";
    options.CreateContainerIfMissing = true;
});
var provider = services.BuildServiceProvider().GetRequiredService<AzureBlobStorageProvider>();

// Write
var uri = StorageUri.Parse("azure://demo-container/hello.txt");
await using (var writeStream = await provider.OpenWriteAsync(uri))
{
    await writeStream.WriteAsync(System.Text.Encoding.UTF8.GetBytes("Hello, Azure!"));
    await writeStream.CommitAsync();
}

// Read
await using (var readStream = await provider.OpenReadAsync(uri))
using (var reader = new StreamReader(readStream))
{
    Console.WriteLine(await reader.ReadToEndAsync());
}
```

## Configuration

### Dependency Injection Setup

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.AccountName = "mystorageaccount";
    options.UseDefaultCredentialChain = true;
    options.PartSizeBytes = 8 * 1024 * 1024; // 8 MiB
    options.MaxConcurrency = 4;
});
```

### Configuration Options (AzureBlobStorageProviderOptions)

`AzureBlobStorageProviderOptions` derives from `AzureAccountOptions`, which the ADLS Gen2 provider shares. Credentials belong in these options and never in the URI.

| Property                    | Type                                | Default | Description                                                                                                                                       |
|-----------------------------|-------------------------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `AccountName`               | `string?`                           | `null`  | Default storage account name. An `accountName` URI parameter overrides it. Required with `DefaultAccountKey`.                                     |
| `DefaultConnectionString`   | `string?`                           | `null`  | Connection string. It carries its own endpoint and credentials, so it can't be combined with `ServiceUrl`.                                        |
| `DefaultSasToken`           | `string?`                           | `null`  | Shared access signature token. The provider sends it exactly as you set it.                                                                       |
| `DefaultAccountKey`         | `string?`                           | `null`  | Storage account key. Needs `AccountName` or an `accountName` URI parameter.                                                                       |
| `DefaultCredential`         | `TokenCredential?`                  | `null`  | Default Azure credential. If not specified, uses the `DefaultAzureCredential` chain when `UseDefaultCredentialChain` is true.                     |
| `UseDefaultCredentialChain` | `bool`                              | `true`  | Whether to use the default Azure credential chain (environment variables, managed identity, Visual Studio, Azure CLI).                            |
| `AllowAnonymousAccess`      | `bool`                              | `false` | Connect without credentials when none are configured, for public containers. When `false`, a missing credential throws.                          |
| `ServiceUrl`                | `Uri?`                              | `null`  | Service URL for Azure Storage-compatible endpoints (e.g., Azurite). A `serviceUrl` URI parameter overrides it.                                    |
| `ServiceVersion`            | `BlobClientOptions.ServiceVersion?` | `null`  | Blob service API version                                                                                                                          |
| `Retry`                     | `AzureRetryOptions`                 | see below | Retry settings of the Azure SDK clients the provider creates.                                                                                  |
| `PartSizeBytes`             | `int`                               | 8 MiB   | Size of each upload block. An object that fits in one block uploads in one request. Memory use while writing is about `PartSizeBytes × (MaxConcurrency + 1)`. |
| `MaxConcurrency`            | `int`                               | `4`     | Most blocks of one object that upload at the same time.                                                                                           |
| `CreateContainerIfMissing`  | `bool`                              | `false` | Create a container on the first write to it, once for each endpoint. Off by default, because creating containers needs permissions a least-privilege token usually lacks. |
| `ClientCacheSizeLimit`      | `int`                               | `100`   | Most `BlobServiceClient` instances kept in the cache, one per endpoint (account name and service URL). The cache never uses a credential as a key. |

`AzureRetryOptions` (`Mode`, `MaxRetries`, `Delay`, `MaxDelay`, `NetworkTimeout`) has the defaults of the Azure SDK: exponential retries, 5 retries, an 800 ms base delay, an 8 s longest delay, and a 100 s network timeout. It was `AdlsGen2RetryOptions` in the ADLS package in earlier releases.

### Custom Configuration Examples

#### Configuration for Azurite (Local Development)

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://127.0.0.1:10000/devstoreaccount1");
    options.AccountName = "devstoreaccount1";
    options.DefaultAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    options.CreateContainerIfMissing = true;
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
    options.PartSizeBytes = 16 * 1024 * 1024; // 16 MiB blocks
    options.MaxConcurrency = 8; // 8 blocks upload at once
});
```

#### Configuration with Custom Retry Settings

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.Retry = new AzureRetryOptions
    {
        MaxRetries = 3,
        NetworkTimeout = TimeSpan.FromSeconds(30)
    };
});
```

#### Configuration with Managed Identity (Production)

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.AccountName = "mystorageaccount";
    options.UseDefaultCredentialChain = true; // Uses Managed Identity in Azure
    options.DefaultCredential = new DefaultAzureCredential();
});
```

### Service URL Configuration (Azurite)

For local development with Azurite, configure the service URL, the account name, and the account key:

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://127.0.0.1:10000/devstoreaccount1");
    options.AccountName = "devstoreaccount1";
    options.DefaultAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
});
```

**Important:** Azurite uses the account name `devstoreaccount1` and requires this specific service URL format.

## Authentication Methods

The Azure Blob Storage Provider supports multiple authentication methods. You set credentials in the provider options. A URI never carries a credential, because URIs are logged and compared. When you set more than one, the first of these wins: connection string, SAS token, account key, `DefaultCredential`, the default credential chain.

> **Breaking change:** The `connectionString`, `sasToken`, and `accountKey` URI parameters are no longer supported. A URI that carries one throws `ArgumentException` that names the option to set instead.

### Connection String Authentication

Connection strings provide the simplest authentication method for development and testing. A connection string names its own endpoint, so you can't combine it with `ServiceUrl`.

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
services.AddAzureBlobStorageProvider(options =>
{
    options.AccountName = "mystorageaccount";
    options.DefaultAccountKey = accountKey; // not recommended for production
});
```

### SAS Token Authentication

Shared Access Signature (SAS) tokens provide time-limited, scoped access:

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.AccountName = "mystorageaccount";
    options.DefaultSasToken = "sv=2023-01-01&ss=b&sp=rwdlac&se=2024-01-01T00:00:00Z&st=2023-01-01T00:00:00Z&spr=https&sig=mysignature";
});
```

> **Note:** Set the token as the Azure portal gives it to you, with its signature still percent-encoded. The provider doesn't decode or re-encode it.

### Default Azure Credential Chain

The default credential chain automatically searches for credentials in the following order:

1. **Environment variables** - `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`
2. **Workload identity** - For AKS and other Kubernetes environments
3. **Managed identity** - For Azure App Service, Functions, and VMs
4. **Visual Studio** - Credentials from Visual Studio sign-in
5. **Azure CLI** - Credentials from `az login`
6. **Azure PowerShell** - Credentials from `Connect-AzAccount`

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.UseDefaultCredentialChain = true; // Default is true
});
```

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

### When to Use Each Method

| Method                       | Best For                                            | Security Level |
|------------------------------|-----------------------------------------------------|----------------|
| **Connection String**        | Development, testing, simple scenarios              | Medium         |
| **Account Key**              | Legacy applications, explicit credential management | Low            |
| **SAS Token**                | Time-limited access, sharing resources              | High           |
| **Default Credential Chain** | Production, Azure-hosted applications               | Very High      |
| **Custom TokenCredential**   | Service principals, specific auth scenarios         | High           |

> **Recommendation:** Use the default credential chain with managed identity for production applications running on Azure.

## URI Format

The Azure Blob Storage Provider uses URIs with the `azure://` scheme to identify blobs.

### Azure URI Scheme Format

```
azure://container-name/path/to/blob.csv?parameter1=value1&parameter2=value2
```

### URI Components

| Component      | Description                                                    | Example                          |
|----------------|----------------------------------------------------------------|----------------------------------|
| **Scheme**     | URI scheme, must be `azure`                                    | `azure`                          |
| **Host**       | Container name                                                 | `my-container`                   |
| **Path**       | Blob name (can include "/" for virtual directories)            | `data/input.csv`                 |
| **Parameters** | Optional query parameters that select the endpoint or set the content type | `accountName=xxx&contentType=text/csv` |

### Supported Parameters

| Parameter     | Description                                          | Example                                              |
|---------------|------------------------------------------------------|------------------------------------------------------|
| `accountName` | Azure storage account name. Overrides `AccountName`. | `accountName=mystorageaccount`                       |
| `serviceUrl`  | Custom service URL (e.g., Azurite). Overrides `ServiceUrl`. | `serviceUrl=http://localhost:10000/devstoreaccount1` |
| `contentType` | Content type for uploads                             | `contentType=application/json`                       |

The `connectionString`, `sasToken`, and `accountKey` parameters throw `ArgumentException`. Set `DefaultConnectionString`, `DefaultSasToken`, or `DefaultAccountKey` in the options instead.

### URI Examples

#### Basic Azure Blob

```csharp
var uri = StorageUri.Parse("azure://my-container/data/input.csv");
```

#### With Another Account Name

The credentials come from the options. The URI selects the account.

```csharp
var uri = StorageUri.Parse("azure://my-container/data/input.csv?accountName=mystorageaccount");
```

#### With Azurite Endpoint

```csharp
var uri = StorageUri.Parse("azure://my-container/data/file.csv?serviceUrl=http://localhost:10000/devstoreaccount1");
```

#### With Content Type

```csharp
var uri = StorageUri.Parse("azure://my-container/data/output.json?contentType=application/json");
```

### Azurite URI Format

For local development with Azurite, use the following format:

```csharp
// URI with Azurite service URL
var uri = StorageUri.Parse("azure://my-container/data/file.csv?serviceUrl=http://localhost:10000/devstoreaccount1");

// Or configure service URL in options
services.AddAzureBlobStorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://localhost:10000/devstoreaccount1");
    options.AccountName = "devstoreaccount1";
    options.DefaultAccountKey = "<azurite-key>";
});

// Then use simple URI
var uri = StorageUri.Parse("azure://my-container/data/file.csv");
```

**Azurite Configuration:**

- **Account Name:** `devstoreaccount1`
- **Account Key:** `Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==`
- **Service URL:** `http://localhost:10000/devstoreaccount1`

## Usage Examples

### Reading from Azure Blob Storage

```csharp
using NPipeline.StorageProviders.Azure;
using NPipeline.StorageProviders.Models;

var provider = serviceProvider.GetRequiredService<AzureBlobStorageProvider>();
var uri = StorageUri.Parse("azure://my-container/data.csv");

using var stream = await provider.OpenReadAsync(uri);
using var reader = new StreamReader(stream);
var content = await reader.ReadToEndAsync();
Console.WriteLine(content);
```

### Writing to Azure Blob Storage

```csharp
var provider = serviceProvider.GetRequiredService<AzureBlobStorageProvider>();
var uri = StorageUri.Parse("azure://my-container/output.csv");

await using var stream = await provider.OpenWriteAsync(uri);
await using (var writer = new StreamWriter(stream, leaveOpen: true))
{
    await writer.WriteLineAsync("id,name,value");
    await writer.WriteLineAsync("1,Item A,100");
    await writer.WriteLineAsync("2,Item B,200");
}

await stream.CommitAsync();
```

The provider uploads blocks while you write and doesn't use a local temporary file. A block fills, and its upload starts while you keep writing; when `MaxConcurrency` blocks are in flight, `WriteAsync` waits. `CommitAsync` uploads the last block and commits the block list, and only then does the blob appear. If the whole object fits in one block, `CommitAsync` uploads it in a single request. Disposing the stream without committing leaves an existing blob as it was; the staged blocks stay uncommitted, and Azure discards them after a week. A failed block surfaces from the next `WriteAsync` or from `CommitAsync`.

The provider doesn't create the container unless you set `CreateContainerIfMissing`. A write to a missing container fails with `FileNotFoundException` from `CommitAsync`.

The provider declares `StorageCapabilities.ConditionalWrite`. With `new StorageWriteOptions { Overwrite = false }` the commit fails if the blob exists. With `new StorageWriteOptions { IfMatch = etag }` (an ETag from `GetMetadataAsync`) it commits only if the current ETag matches. A refused condition throws `StoragePreconditionFailedException` from `CommitAsync`. `StorageWriteOptions.ContentType` sets the content type; the `contentType` URI parameter is the fallback.

### Checking Blob Existence

```csharp
var provider = serviceProvider.GetRequiredService<AzureBlobStorageProvider>();
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

### Listing Blobs (Recursive)

```csharp
var provider = serviceProvider.GetRequiredService<AzureBlobStorageProvider>();
var prefix = StorageUri.Parse("azure://my-container/data/");

// List all blobs recursively
await foreach (var item in provider.ListAsync(prefix, recursive: true))
{
    Console.WriteLine($"{item.Uri.Path} - {item.Size} bytes - Modified: {item.LastModified}");
}
```

### Listing Blobs (Non-Recursive)

```csharp
var provider = serviceProvider.GetRequiredService<AzureBlobStorageProvider>();
var prefix = StorageUri.Parse("azure://my-container/");

// List only immediate children (non-recursive)
await foreach (var item in provider.ListAsync(prefix, recursive: false))
{
    var type = item.IsDirectory ? "[DIR]" : "[FILE]";
    Console.WriteLine($"{type} {item.Uri.Path} - {item.Size} bytes");
}
```

### Retrieving Metadata

```csharp
var provider = serviceProvider.GetRequiredService<AzureBlobStorageProvider>();
var uri = StorageUri.Parse("azure://my-container/data.csv");

var metadata = await provider.GetMetadataAsync(uri);
if (metadata != null)
{
    Console.WriteLine($"Size: {metadata.Size} bytes");
    Console.WriteLine($"Content Type: {metadata.ContentType}");
    Console.WriteLine($"Last Modified: {metadata.LastModified}");
    Console.WriteLine($"ETag: {metadata.ETag}");

    if (metadata.CustomMetadata != null && metadata.CustomMetadata.Count > 0)
    {
        Console.WriteLine("Custom Metadata:");
        foreach (var (key, value) in metadata.CustomMetadata)
        {
            Console.WriteLine($"  {key}: {value}");
        }
    }
}
```

### Using with Different Content Types

```csharp
var provider = serviceProvider.GetRequiredService<AzureBlobStorageProvider>();

// JSON with content type
var jsonUri = StorageUri.Parse("azure://my-container/data.json?contentType=application/json");
await using (var stream = await provider.OpenWriteAsync(jsonUri))
{
    await stream.WriteAsync(Encoding.UTF8.GetBytes("{\"name\":\"value\"}"));
    await stream.CommitAsync();
}

// CSV with content type
var csvUri = StorageUri.Parse("azure://my-container/data.csv?contentType=text/csv");
await using (var stream = await provider.OpenWriteAsync(csvUri))
{
    await stream.WriteAsync(Encoding.UTF8.GetBytes("id,name\n1,Item A"));
    await stream.CommitAsync();
}

// Plain text
var txtUri = StorageUri.Parse("azure://my-container/data.txt?contentType=text/plain");
await using (var stream = await provider.OpenWriteAsync(txtUri))
{
    await stream.WriteAsync(Encoding.UTF8.GetBytes("Hello, World!"));
    await stream.CommitAsync();
}
```

### Handling Large Files

The provider streams a large file in blocks of `PartSizeBytes` (8 MiB by default). Each block uploads while you keep writing, so the provider never holds the whole file in memory or on disk:

```csharp
var provider = serviceProvider.GetRequiredService<AzureBlobStorageProvider>();
var uri = StorageUri.Parse("azure://my-container/large-file.bin");

// Pass the expected length so the provider picks a block size that stays within 50,000 blocks.
var options = new StorageWriteOptions { LengthHint = 100L * 1024 * 1024 };

await using (var stream = await provider.OpenWriteAsync(uri, options))
{
    await using var source = File.OpenRead("large-file.bin");
    await source.CopyToAsync(stream); // blocks upload while this runs
    await stream.CommitAsync();       // uploads the last block and commits the block list
}

Console.WriteLine("Large file uploaded successfully!");
```

#### Customizing Large File Upload

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.PartSizeBytes = 16 * 1024 * 1024; // 16 MiB blocks
    options.MaxConcurrency = 8; // 8 blocks upload at once
});
```

## Advanced Features

### Block Blob Upload

The provider writes block blobs. Each full block is staged while you write, and `CommitAsync` commits the block list. This provides:

- **Streaming** - Blocks upload as they fill, with no local temporary file
- **Parallel uploads** - Up to `MaxConcurrency` blocks upload at the same time. When all the slots are busy, `WriteAsync` waits, which limits memory use to about `PartSizeBytes × (MaxConcurrency + 1)`
- **A one-request fast path** - An object that fits in one block uploads with a single `Put Blob` request
- **No partial blobs** - The blob appears only when you commit. A write that you dispose without committing leaves any existing blob as it was, and its staged blocks expire on the service after a week

```csharp
var largeUri = StorageUri.Parse("azure://my-container/large-file.bin");
await using (var stream = await provider.OpenWriteAsync(largeUri))
{
    await stream.WriteAsync(largeData);
    await stream.CommitAsync();
}
```

A block blob holds at most 50,000 blocks. If you know the object's length, pass it in `StorageWriteOptions.LengthHint` and the provider raises the block size to fit. Without a hint, the provider enlarges the blocks after the first 10,000.

### Upload Concurrency

`MaxConcurrency` sets how many blocks of one object upload at the same time:

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.MaxConcurrency = 8; // Upload 8 blocks in parallel
});
```

**Recommended values:**

- **Small files (< 10 MB):** 2-4 concurrent uploads
- **Medium files (10-100 MB):** 4-8 concurrent uploads
- **Large files (> 100 MB):** 8-16 concurrent uploads

### Block Size

`PartSizeBytes` sets the size of each block:

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.PartSizeBytes = 16 * 1024 * 1024; // 16 MiB blocks
});
```

A larger block means fewer requests but more memory for each writer. Memory use while writing is about `PartSizeBytes × (MaxConcurrency + 1)`.

### Client Caching

The `AzureBlobClientFactory` caches one `BlobServiceClient` for each endpoint, which is the pair of:

- Storage account name
- Service endpoint URL

The cache never uses a credential, or a value derived from one, as a key, because every client the provider creates uses the credentials in the options. The cache holds at most `ClientCacheSizeLimit` clients (100 by default) and drops the least recently used one when it is full.

```csharp
// First call creates and caches the client
var client1 = await factory.GetClientAsync(uri1);

// Second call with same configuration returns cached client
var client2 = await factory.GetClientAsync(uri2); // Returns cached client
```

### Streaming I/O

All operations use streaming to avoid loading entire files into memory:

```csharp
// Streaming read - reads data incrementally
await using (var stream = await provider.OpenReadAsync(uri))
{
    var buffer = new byte[8192];
    int bytesRead;
    while ((bytesRead = await stream.ReadAsync(buffer)) > 0)
    {
        // Process chunk
        ProcessChunk(buffer.AsSpan(0, bytesRead));
    }
}

// Streaming write - uploads blocks while you write, then commits the block list
await using (var stream = await provider.OpenWriteAsync(uri))
{
    // Write data in chunks
    foreach (var chunk in dataChunks)
    {
        await stream.WriteAsync(chunk);
    }

    await stream.CommitAsync();
}
```

### Metadata Support

Retrieve blob metadata including custom metadata:

```csharp
var provider = serviceProvider.GetRequiredService<AzureBlobStorageProvider>();
var uri = StorageUri.Parse("azure://my-container/data.csv");

var metadata = await provider.GetMetadataAsync(uri);
if (metadata != null)
{
    // Standard properties
    Console.WriteLine($"Size: {metadata.Size} bytes");
    Console.WriteLine($"Content Type: {metadata.ContentType}");
    Console.WriteLine($"Last Modified: {metadata.LastModified}");
    Console.WriteLine($"ETag: {metadata.ETag}");

    // Custom metadata
    if (metadata.CustomMetadata != null && metadata.CustomMetadata.Count > 0)
    {
        Console.WriteLine("Custom Metadata:");
        foreach (var (key, value) in metadata.CustomMetadata)
        {
            Console.WriteLine($"  {key}: {value}");
        }
    }
}
```

## Error Handling

The Azure Blob Storage Provider translates Azure SDK exceptions to standard .NET exceptions for consistent error handling.
One internal translator decides on the HTTP status first and uses the error code only when the status is missing or generic.
The `RequestFailedException` is always the inner exception.

### Exception Types Thrown

| Exception Type                | When Thrown                                                  |
|-------------------------------|--------------------------------------------------------------|
| `ArgumentNullException`       | When a required argument is null                             |
| `ArgumentException`           | When URI format is invalid or container/blob name is invalid |
| `UnauthorizedAccessException` | When authentication or authorization fails                   |
| `FileNotFoundException`       | When the blob or container is not found                      |
| `IOException`                 | For general Azure storage access failures                    |

### Exception Translation (Azure to .NET Exceptions)

| Azure Error Code             | HTTP Status | .NET Exception                |
|------------------------------|-------------|-------------------------------|
| `AuthenticationFailed`       | 401         | `UnauthorizedAccessException` |
| `AuthorizationFailed`        | 403         | `UnauthorizedAccessException` |
| `ContainerNotFound`          | 404         | `FileNotFoundException`       |
| `BlobNotFound`               | 404         | `FileNotFoundException`       |
| `InvalidQueryParameterValue` | 400         | `ArgumentException`           |
| `InvalidResourceName`        | 400         | `ArgumentException`           |
| Other                        | Various     | `IOException`                 |

### Common Error Scenarios

#### 1. Blob Not Found

```csharp
try
{
    using var stream = await provider.OpenReadAsync(uri);
}
catch (FileNotFoundException ex)
{
    Console.WriteLine($"Blob not found: {ex.Message}");
    // Handle missing blob
}
```

#### 2. Access Denied

```csharp
try
{
    using var stream = await provider.OpenReadAsync(uri);
}
catch (UnauthorizedAccessException ex)
{
    Console.WriteLine($"Access denied: {ex.Message}");
    Console.WriteLine("Check your credentials and permissions.");
    // Handle authentication/authorization error
}
```

#### 3. Invalid Container Name

```csharp
try
{
    var uri = StorageUri.Parse("azure://invalid-container!/file.txt");
    using var stream = await provider.OpenReadAsync(uri);
}
catch (ArgumentException ex)
{
    Console.WriteLine($"Invalid URI: {ex.Message}");
    // Handle invalid URI
}
```

#### 4. Network/Connection Issues

```csharp
try
{
    using var stream = await provider.OpenReadAsync(uri);
}
catch (IOException ex)
{
    Console.WriteLine($"Storage access error: {ex.Message}");
    if (ex.InnerException is RequestFailedException azureEx)
    {
        Console.WriteLine($"Azure Error Code: {azureEx.ErrorCode}");
        Console.WriteLine($"Status: {azureEx.Status}");
    }
    // Handle network/connection error
}
```

### Best Practices for Error Handling

```csharp
try
{
    using var stream = await provider.OpenReadAsync(uri);
    // Process stream...
}
catch (FileNotFoundException ex)
{
    // Blob doesn't exist - create it or notify user
    logger.LogWarning(ex, "Blob not found: {Uri}", uri);
}
catch (UnauthorizedAccessException ex)
{
    // Authentication/authorization failure - check credentials
    logger.LogError(ex, "Access denied to blob: {Uri}", uri);
    throw;
}
catch (ArgumentException ex)
{
    // Invalid URI or parameters
    logger.LogError(ex, "Invalid URI: {Uri}", uri);
    throw;
}
catch (IOException ex)
{
    // General storage error - may be transient
    logger.LogError(ex, "Storage error accessing blob: {Uri}", uri);
    throw;
}
```

## Performance Considerations

### Client Caching

The `AzureBlobClientFactory` caches `BlobServiceClient` instances to reduce overhead:

- **Cache key:** Based on storage account, credentials, and service URL
- **Cache scope:** Application lifetime (singleton)
- **Benefit:** Reuses HTTP connections and authentication tokens

```csharp
// Client is cached and reused
var provider = serviceProvider.GetRequiredService<AzureBlobStorageProvider>();

// Multiple operations reuse the same client
await using (var stream1 = await provider.OpenReadAsync(uri1)) { }
await using (var stream2 = await provider.OpenReadAsync(uri2)) { }
```

### Streaming for Large Files

Always use streaming for large files to avoid memory issues:

```csharp
// Good: Streaming read
await using (var stream = await provider.OpenReadAsync(uri))
{
    var buffer = new byte[8192];
    int bytesRead;
    while ((bytesRead = await stream.ReadAsync(buffer)) > 0)
    {
        ProcessChunk(buffer.AsSpan(0, bytesRead));
    }
}

// Bad: Loading entire file into memory
var content = await File.ReadAllTextAsync(localPath);
await using (var stream = await provider.OpenWriteAsync(uri))
{
    await stream.WriteAsync(Encoding.UTF8.GetBytes(content));
    await stream.CommitAsync();
}
```

### Pagination for Large Containers

`ListAsync` automatically handles pagination for containers with many blobs:

```csharp
// Efficiently lists all blobs, even with thousands of items
await foreach (var item in provider.ListAsync(prefix, recursive: true))
{
    // Process each item as it's received
    ProcessItem(item);
}
```

### Async Operations

All methods are async to avoid blocking threads:

```csharp
// Use async/await throughout
await using (var stream = await provider.OpenReadAsync(uri))
{
    await stream.ReadAsync(buffer);
}

// ConfigureAwait(false) in library code
await blobClient.ExistsAsync(cancellationToken).ConfigureAwait(false);
```

### Performance Tips

1. **Reuse providers** - Register as singleton in DI container
2. **Use appropriate regions** - Choose region closest to your application
3. **Batch operations** - List multiple blobs at once instead of individual existence checks
4. **Tune the block size** - Adjust `PartSizeBytes` based on your typical file sizes
5. **Use compression** - Compress data before uploading for large files
6. **Configure concurrency** - Tune `MaxConcurrency` for your network
7. **Use streaming** - Always stream large files instead of loading into memory

## Testing

### Unit Testing with FakeItEasy

Unit tests use mocking to isolate the provider from Azure services:

```csharp
using FakeItEasy;
using NPipeline.StorageProviders.Azure;

// Create fake options
var options = new AzureBlobStorageProviderOptions();

// Create fake client factory
var fakeFactory = A.Fake<IAzureBlobClientFactory>();

// Create provider with fake factory
var provider = new AzureBlobStorageProvider(fakeFactory, options);

// Test operations
var uri = StorageUri.Parse("azure://test-container/test-blob");
var exists = await provider.ExistsAsync(uri);

// Verify interactions
A.CallTo(() => fakeFactory.GetClientAsync(uri, A<CancellationToken>._))
    .MustHaveHappenedOnceExactly();
```

### Integration Testing with TestContainers/Azurite

Integration tests use Azurite for real Azure Blob Storage testing:

```csharp
using DotNet.Testcontainers.Containers;
using NPipeline.StorageProviders.Azure;

// Start Azurite container
var azuriteContainer = new AzuriteBuilder()
    .WithImage("mcr.microsoft.com/azure-storage/azurite")
    .WithPortBinding(10000, 10000)
    .Build();

await azuriteContainer.StartAsync();

// Configure provider for Azurite
var options = new AzureBlobStorageProviderOptions
{
    ServiceUrl = new Uri("http://localhost:10000/devstoreaccount1"),
    DefaultConnectionString = "UseDevelopmentStorage=true"
};

var provider = new AzureBlobStorageProvider(
    new AzureBlobClientFactory(options),
    options);

// Test real operations
var uri = StorageUri.Parse("azure://test-container/test-blob");
await using (var stream = await provider.OpenWriteAsync(uri))
{
    await stream.WriteAsync(Encoding.UTF8.GetBytes("Test data"));
    await stream.CommitAsync();
}

var exists = await provider.ExistsAsync(uri);
Assert.True(exists);

// Cleanup
await azuriteContainer.StopAsync();
```

### Test Patterns

#### Arrange-Act-Assert Pattern

```csharp
[Fact]
public async Task OpenReadAsync_ReturnsStream_WhenBlobExists()
{
    // Arrange
    var uri = StorageUri.Parse("azure://test-container/test-blob");
    var fakeFactory = CreateFakeFactory();
    var provider = new AzureBlobStorageProvider(fakeFactory, new AzureBlobStorageProviderOptions());

    // Act
    var stream = await provider.OpenReadAsync(uri);

    // Assert
    Assert.NotNull(stream);
}
```

#### Exception Testing

```csharp
[Fact]
public async Task OpenReadAsync_ThrowsFileNotFoundException_WhenBlobNotFound()
{
    // Arrange
    var uri = StorageUri.Parse("azure://test-container/non-existent");
    var fakeFactory = CreateFakeFactoryThatThrowsBlobNotFound();
    var provider = new AzureBlobStorageProvider(fakeFactory, new AzureBlobStorageProviderOptions());

    // Act & Assert
    await Assert.ThrowsAsync<FileNotFoundException>(
        () => provider.OpenReadAsync(uri));
}
```

#### Parameterized Testing

```csharp
[Theory]
[InlineData("azure://valid-container/blob.txt")]
[InlineData("azure://my-container/path/to/file.json")]
public async Task OpenReadAsync_HandlesValidUris(string uriString)
{
    // Arrange
    var uri = StorageUri.Parse(uriString);
    var fakeFactory = CreateFakeFactory();
    var provider = new AzureBlobStorageProvider(fakeFactory, new AzureBlobStorageProviderOptions());

    // Act
    var schemeMatches = provider.Schemes.Contains(uri.Scheme);

    // Assert
    Assert.True(schemeMatches);
}
```

## Security Considerations

### Credential Management

- **Keep credentials out of URIs** - URIs appear in logs, error messages, and debugging output, so the provider rejects the `connectionString`, `sasToken`, and `accountKey` URI parameters. Set credentials in the options
- **Use managed identity** when running on Azure infrastructure (App Service, Functions, AKS)
- **Use credential chain** - Prefer environment variables or managed identity over explicit credentials
- **Rotate credentials regularly** - Use Azure Key Vault for credential management
- **Use temporary credentials** - When possible, use short-lived SAS tokens

### Never Log Credentials

```csharp
// Bad: Logs credentials
Console.WriteLine($"Connecting with: {connectionString}");

// Good: Mask sensitive information
Console.WriteLine($"Connecting to account: {MaskAccountName(connectionString)});

private string MaskAccountName(string connectionString)
{
    // Mask account key in connection string
    var masked = Regex.Replace(connectionString, @"AccountKey=([^;]+)", "AccountKey=*****");
    return masked;
}
```

### Use Managed Identity in Production

```csharp
// Production configuration with managed identity
services.AddAzureBlobStorageProvider(options =>
{
    options.UseDefaultCredentialChain = true;
    // No explicit credentials - uses managed identity automatically
});
```

### Use Credential Chain

The default credential chain provides the most secure authentication:

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.UseDefaultCredentialChain = true;
});
```

**Credential chain priority:**

1. Environment variables
2. Workload identity
3. Managed identity
4. Visual Studio
5. Azure CLI
6. Azure PowerShell

### Rotate Credentials Regularly

Use Azure Key Vault for credential rotation:

```csharp
// Retrieve credentials from Key Vault
var connectionString = await keyVaultClient.GetSecretAsync("storage-connection-string");

services.AddAzureBlobStorageProvider(options =>
{
    options.DefaultConnectionString = connectionString.Value.Value;
});
```

### Use Temporary Credentials

SAS tokens provide time-limited access:

```csharp
// Generate SAS token with limited scope and time
var sasBuilder = new BlobSasBuilder
{
    BlobContainerName = containerName,
    BlobName = blobName,
    Resource = "b",
    StartsOn = DateTimeOffset.UtcNow,
    ExpiresOn = DateTimeOffset.UtcNow.AddHours(1)
};

sasBuilder.SetPermissions(BlobSasPermissions.Read);

var sasToken = sasBuilder.ToSasQueryParameters(
    new StorageSharedKeyCredential(accountName, accountKey))
    .ToString();

// Use the SAS token through the options
services.AddAzureBlobStorageProvider(options =>
{
    options.AccountName = accountName;
    options.DefaultSasToken = sasToken;
});
```

## Limitations

### Flat Storage Model

Azure Blob Storage is a flat object storage system (no true hierarchical directories):

- Directory-like paths are simulated through blob name prefixes
- The provider treats paths with "/" delimiters as virtual directories
- Non-recursive listing skips virtual directory markers

```csharp
// This is a flat storage system
// "data/file1.txt" and "data/subdir/file2.txt" are just blob names
// There is no actual "data" directory
```

### Large File Handling

- Blocks of `PartSizeBytes` (8 MiB by default) upload while you write; an object that fits in one block uploads in one request
- A block blob holds at most 50,000 blocks. Pass `StorageWriteOptions.LengthHint` for objects larger than about 390 GiB at the default block size
- Memory use while writing is about `PartSizeBytes × (MaxConcurrency + 1)` for each open write stream

```csharp
// Configure block size and concurrency based on your needs
services.AddAzureBlobStorageProvider(options =>
{
    options.PartSizeBytes = 16 * 1024 * 1024; // 16 MiB
    options.MaxConcurrency = 8;
});
```

### Move Is Not Atomic

`MoveAsync` copies the blob within the account and then deletes the source, so a failure between the two steps can leave both blobs. Moving between accounts throws `ArgumentException`.

### Concurrent Operations

- The provider is thread-safe for read operations
- Concurrent writes to the same blob may result in race conditions
- Use appropriate locking or versioning strategies if needed

```csharp
// Safe: Concurrent reads from different blobs
var task1 = provider.OpenReadAsync(uri1);
var task2 = provider.OpenReadAsync(uri2);
await Task.WhenAll(task1, task2);

// Unsafe: Concurrent writes to the same blob
var task1 = provider.OpenWriteAsync(uri); // May overwrite task2
var task2 = provider.OpenWriteAsync(uri);
await Task.WhenAll(task1, task2);
```

## Troubleshooting

### Common Issues

#### Issue: Authentication Failed

**Symptoms:** `UnauthorizedAccessException` with status 401 or 403

**Solutions:**

1. Verify credentials are correct
2. Check credential chain configuration
3. Ensure managed identity has proper permissions
4. Verify SAS token is not expired

```csharp
// Check credential configuration
services.AddAzureBlobStorageProvider(options =>
{
    options.UseDefaultCredentialChain = true;
    // Or provide explicit credentials
    options.DefaultConnectionString = "YourConnectionString";
});
```

#### Issue: Blob Not Found

**Symptoms:** `FileNotFoundException` with status 404

**Solutions:**

1. Verify container name in URI host
2. Verify blob name in URI path
3. Check if blob exists using Azure Storage Explorer
4. Ensure case matches (Azure Blob Storage is case-sensitive for blob names)

```csharp
// Verify URI format
var uri = StorageUri.Parse("azure://test-container/test-blob");
//                           ^^^^^^^^^^^^ container
//                                      ^^^^^^^^^^^^^^^ blob path
```

#### Issue: Connection Timeout

**Symptoms:** `IOException` or timeout errors

**Solutions:**

1. Check network connectivity
2. Verify service URL is correct
3. Increase timeout values in Azure SDK
4. Check firewall settings

```csharp
// For Azurite, ensure service URL is correct
services.AddAzureBlobStorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://localhost:10000/devstoreaccount1");
});
```

### Authentication Failures

#### Default Credential Chain Not Working

**Symptoms:** Authentication fails when using `UseDefaultCredentialChain = true`

**Solutions:**

1. Set environment variables for Azure credentials
2. Sign in to Azure CLI: `az login`
3. Sign in to Visual Studio with Azure account
4. For managed identity, ensure identity has proper permissions

```bash
# Set environment variables
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
```

#### SAS Token Expired

**Symptoms:** `UnauthorizedAccessException` with SAS token

**Solutions:**

1. Generate new SAS token with extended expiration
2. Use connection string or managed identity for long-term access
3. Verify SAS token permissions

```csharp
// Generate SAS token with longer expiration
var sasBuilder = new BlobSasBuilder
{
    StartsOn = DateTimeOffset.UtcNow,
    ExpiresOn = DateTimeOffset.UtcNow.AddDays(7) // 7 days
};
```

### Connection Issues

#### Azurite Not Running

**Symptoms:** Connection refused or timeout errors

**Solutions:**

1. Start Azurite: `azurite`
2. Or use Docker: `docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite`
3. Verify Azurite is listening on port 10000
4. Check firewall settings

```bash
# Start Azurite
azurite

# Or use Docker
docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite
```

#### Invalid Service URL

**Symptoms:** `ArgumentException` or connection errors

**Solutions:**

1. Verify service URL format
2. Ensure URL includes `/devstoreaccount1` for Azurite
3. Use HTTPS for production Azure Storage

```csharp
// Correct Azurite URL
options.ServiceUrl = new Uri("http://localhost:10000/devstoreaccount1");

// Correct Azure Storage URL
options.ServiceUrl = new Uri("https://mystorageaccount.blob.core.windows.net");
```

### Performance Issues

#### Slow Uploads

**Symptoms:** Large files take too long to upload

**Solutions:**

1. Increase `MaxConcurrency`
2. Increase `PartSizeBytes`
3. Check network bandwidth
4. Use region closer to your application

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.MaxConcurrency = 16; // More concurrent uploads
    options.PartSizeBytes = 16 * 1024 * 1024; // 16 MiB blocks
});
```

#### Slow Listing

**Symptoms:** `ListAsync` takes too long for large containers

**Solutions:**

1. Use prefix filtering to reduce results
2. Use non-recursive listing when possible
3. Process results incrementally

```csharp
// Use prefix to filter results
var prefix = StorageUri.Parse("azure://my-container/data/2024/");
await foreach (var item in provider.ListAsync(prefix, recursive: true))
{
    // Only process 2024 data
    ProcessItem(item);
}
```

## API Reference

### AzureBlobStorageProvider

Main storage provider implementation for Azure Blob Storage.

**Location:** [`AzureBlobStorageProvider.cs`](AzureBlobStorageProvider.cs)

Derives from `StorageProvider`. Declares `StorageCapabilities.Read | Write | List | Delete | Move | ConditionalWrite` (move is a same-account server-side copy, then delete) (the namespace is flat, so no `Hierarchy`).
`DeleteAsync` is idempotent. `MoveAsync` copies the blob server-side within one storage account (overwriting the destination), then deletes the source; a missing source throws `FileNotFoundException`, and a different account throws `ArgumentException`.

**Properties:**

| Property       | Type                          | Description                                          |
|----------------|-------------------------------|------------------------------------------------------|
| `Name`         | `string`                      | `"Azure Blob Storage"`                               |
| `Schemes`      | `IReadOnlyList<StorageScheme>` | `[StorageScheme.Azure]`                              |
| `Capabilities` | `StorageCapabilities`         | `Read \| Write \| List \| Delete \| Move \| ConditionalWrite`       |

**Methods:**

| Method                                                                         | Return Type                     | Description                                                      |
|--------------------------------------------------------------------------------|---------------------------------|------------------------------------------------------------------|
| `OpenReadAsync(StorageUri uri, CancellationToken ct)`                          | `Task<Stream>`                  | Opens a readable stream for the specified blob                   |
| `OpenWriteAsync(StorageUri uri, StorageWriteOptions? options, CancellationToken ct)` | `Task<StorageWriteStream>` | Opens a writable stream; `options.ContentType` overrides the `contentType` URI parameter |
| `ExistsAsync(StorageUri uri, CancellationToken ct)`                            | `Task<bool>`                    | Checks if the blob exists                                        |
| `ListAsync(StorageUri directory, bool recursive, CancellationToken ct)`        | `IAsyncEnumerable<StorageItem>` | Lists blobs below a directory (the URI is treated as ending in `/`) |
| `GetMetadataAsync(StorageUri uri, CancellationToken ct)`                       | `Task<StorageMetadata?>`        | Retrieves metadata for the blob, or `null` when it is missing    |

Listing: a non-recursive listing yields blobs and prefix entries (`IsDirectory = true`, `Size` and `LastModified` null). A recursive listing yields blobs only. Listed URIs keep the caller's parameters. Listing makes no request to check that the container exists and requests no blob metadata, and listing a container that doesn't exist yields no items.

### AzureBlobStorageProviderOptions

Configuration options for the Azure Blob Storage Provider.

**Location:** [`AzureBlobStorageProviderOptions.cs`](AzureBlobStorageProviderOptions.cs)

**Properties:**

`AzureBlobStorageProviderOptions` derives from `AzureAccountOptions`, which also supplies the properties below. The ADLS Gen2 provider shares them.

| Property                    | Type                                | Default   | Description                                                                 |
|-----------------------------|-------------------------------------|-----------|-----------------------------------------------------------------------------|
| `AccountName`               | `string?`                           | `null`    | Default storage account name                                                |
| `DefaultConnectionString`   | `string?`                           | `null`    | Connection string                                                           |
| `DefaultSasToken`           | `string?`                           | `null`    | Shared access signature token                                               |
| `DefaultAccountKey`         | `string?`                           | `null`    | Storage account key                                                         |
| `DefaultCredential`         | `TokenCredential?`                  | `null`    | Default Azure credential for authentication                                 |
| `UseDefaultCredentialChain` | `bool`                              | `true`    | Whether to use the default Azure credential chain                           |
| `AllowAnonymousAccess`      | `bool`                              | `false`   | Connect without credentials when none are configured                        |
| `ServiceUrl`                | `Uri?`                              | `null`    | Optional service URL for Azure Storage-compatible endpoints                 |
| `ServiceVersion`            | `BlobClientOptions.ServiceVersion?` | `null`    | Blob service API version                                                    |
| `Retry`                     | `AzureRetryOptions`                 | see above | Azure SDK retry settings                                                    |
| `PartSizeBytes`             | `int`                               | 8 MiB     | Size of each upload block                                                   |
| `MaxConcurrency`            | `int`                               | `4`       | Most blocks of one object that upload at the same time                      |
| `CreateContainerIfMissing`  | `bool`                              | `false`   | Create a container on the first write to it                                 |
| `ClientCacheSizeLimit`      | `int`                               | `100`     | Most clients kept in the cache                                              |

### ServiceCollectionExtensions

Extension methods for configuring the Azure Blob Storage Provider in dependency injection.

**Location:** [`ServiceCollectionExtensions.cs`](ServiceCollectionExtensions.cs)

**Methods:**

#### AddAzureBlobStorageProvider(IServiceCollection, Action<AzureBlobStorageProviderOptions>?)

Registers the provider with optional configuration. The provider is added to the `IStorageProvider` collection that `AddStorageResolver()` reads, and calling the method twice registers one provider.

```csharp
public static IServiceCollection AddAzureBlobStorageProvider(
    this IServiceCollection services,
    Action<AzureBlobStorageProviderOptions>? configure = null)
```

**Example:**

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.AccountName = "mystorageaccount";
    options.UseDefaultCredentialChain = true;
    options.PartSizeBytes = 8 * 1024 * 1024;
});
```

#### AddAzureBlobStorageProvider(IServiceCollection, AzureBlobStorageProviderOptions)

Registers the provider with pre-configured options.

```csharp
public static IServiceCollection AddAzureBlobStorageProvider(
    this IServiceCollection services,
    AzureBlobStorageProviderOptions options)
```

**Example:**

```csharp
var options = new AzureBlobStorageProviderOptions
{
    ServiceUrl = new Uri("http://localhost:10000/devstoreaccount1"),
    AccountName = "devstoreaccount1",
    DefaultAccountKey = "<azurite-key>",
    CreateContainerIfMissing = true
};

services.AddAzureBlobStorageProvider(options);
```

## See Also

- [Azure Blob Storage Documentation](https://docs.microsoft.com/azure/storage/blobs/)
- [Azure SDK for .NET Documentation](https://docs.microsoft.com/azure/sdk/)
- [Azurite Documentation](https://docs.microsoft.com/azure/storage/common/storage-use-azurite?tabs=visual-studio)
- [NPipeline Storage Providers Documentation](../../docs/storage-providers/index.md)
- [AWS S3 Storage Provider](../../docs/storage-providers/aws-s3.md)
- [Design Document](../../docs/design/azure-storage-provider.md)
- [Sample Application](../../samples/Sample_AzureStorageProvider/)

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.
