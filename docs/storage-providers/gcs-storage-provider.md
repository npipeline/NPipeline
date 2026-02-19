---
title: Google Cloud Storage Provider
description: Read from and write to Google Cloud Storage using the GCS storage provider.
sidebar_position: 5
---

## Google Cloud Storage Provider

The Google Cloud Storage (GCS) provider enables NPipeline applications to read from and write to Google Cloud Storage using a unified storage abstraction. This provider implements the `IStorageProvider` interface and supports the `gs://` URI scheme.

### Overview

The GCS provider provides seamless integration with Google Cloud Storage. It offers:

- **Stream-based I/O** for efficient handling of large files
- **Async-first API** for scalable, non-blocking operations
- **Flexible authentication** via Application Default Credentials (ADC), service account JSON, or explicit credentials
- **Comprehensive error handling** with proper exception translation
- **Metadata support** for retrieving object metadata
- **Listing operations** with recursive and non-recursive modes
- **Resumable uploads** for large files with configurable chunk sizes

### When to Use This Provider

Use the GCS provider when your application needs to:

- Store and retrieve data in Google Cloud Storage
- Integrate cloud storage into NPipeline data pipelines
- Leverage GCS's scalability and durability for data storage
- Handle large files through streaming and resumable uploads
- Work with GCS emulators (fake-gcs-server) for local development

## Dependencies

The GCS provider depends on the following packages:

- `Google.Cloud.Storage.V1` - Google Cloud SDK for Storage operations
- `Google.Apis.Auth` - Google SDK for authentication
- `NPipeline.StorageProviders` - Core storage abstractions (IStorageProvider, StorageUri, StorageItem, StorageMetadata, StorageProviderMetadata, StorageResolverOptions, StorageProviderFactory)
- `NPipeline.Connectors` - Core connectors for using storage providers with connectors

### Key Storage Types

> **Note:** The shared storage types (IStorageProvider, StorageUri, StorageItem, StorageMetadata, StorageProviderMetadata, StorageResolverOptions, StorageProviderFactory) are common across all NPipeline storage providers. Refer to the [Storage Provider Interface](./storage-provider.md) documentation for details on these types.

GCS-specific configuration types:

- **`GcsStorageProviderOptions`** - Configuration options for the GCS provider
  - Location: `NPipeline.StorageProviders.Gcp.GcsStorageProviderOptions`
  - Essential settings: project ID, credentials, service URL, upload chunk size

## Installation

### Prerequisites

- .NET 6.0 or later
- A Google Cloud project with Cloud Storage API enabled
- Appropriate GCS permissions (IAM roles)

### Package Installation

Add the project reference to your solution:

```bash
dotnet add src/NPipeline.StorageProviders.Gcp/NPipeline.StorageProviders.Gcp.csproj
```

Or add it to your `.csproj` file:

```xml
<ItemGroup>
  <ProjectReference Include="..\NPipeline.StorageProviders.Gcp\NPipeline.StorageProviders.Gcp.csproj" />
</ItemGroup>
```

## Quick Start

### Basic Usage with Connectors

The GCS provider works seamlessly with all NPipeline connectors. Here's a quick example using the CSV connector:

```csharp
using NPipeline;
using NPipeline.Connectors;
using NPipeline.Connectors.Csv;
using NPipeline.StorageProviders.Gcp;

// Create a resolver with GCS support
var resolver = StorageProviderFactory.CreateResolver(
    new StorageResolverOptions
    {
        IncludeFileSystem = true,
        AdditionalProviders = new[] { new GcsStorageProvider(
            new GcsClientFactory(new GcsStorageProviderOptions()),
            new GcsStorageProviderOptions()) }
    }
);

public sealed record User(int Id, string Name, string Email);

public sealed class GcsCsvPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Read CSV from GCS
        var sourceNode = new CsvSourceNode<User>(
            StorageUri.Parse("gs://my-bucket/users.csv"),
            row => new User(
                row.Get<int>("Id") ?? 0,
                row.Get<string>("Name") ?? string.Empty,
                row.Get<string>("Email") ?? string.Empty),
            resolver: resolver);
        var source = builder.AddSource(sourceNode, "gcs_csv_source");
        
        // ... add transforms ...
        
        // Write CSV to GCS
        var sinkNode = new CsvSinkNode<UserSummary>(
            StorageUri.Parse("gs://my-bucket/summaries.csv"),
            resolver: resolver);
        var sink = builder.AddSink(sinkNode, "gcs_csv_sink");
        
        builder.Connect(source, sink);
    }
}
```

## Configuration

### Using Dependency Injection

The recommended way to configure the GCS provider is through dependency injection:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Gcp;

var services = new ServiceCollection();

services.AddGcsStorageProvider(options =>
{
    options.DefaultProjectId = "my-project-id";
    options.UseDefaultCredentials = true;
    options.UploadChunkSizeBytes = 16 * 1024 * 1024; // 16 MB
});

var serviceProvider = services.BuildServiceProvider();
var provider = serviceProvider.GetRequiredService<GcsStorageProvider>();
```

### GcsStorageProviderOptions

The `GcsStorageProviderOptions` class provides configuration options for the GCS provider:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `DefaultProjectId` | `string?` | `null` | Default Google Cloud project ID. Used when bucket operations require project context. |
| `DefaultCredentials` | `GoogleCredential?` | `null` | Default Google credentials for authentication. If not specified and `UseDefaultCredentials` is true, ADC will be used. |
| `UseDefaultCredentials` | `bool` | `true` | Whether to use Application Default Credentials (ADC) when `DefaultCredentials` is not provided. |
| `ServiceUrl` | `Uri?` | `null` | Optional service URL for GCS emulator or custom endpoints. If not specified, uses the default GCS endpoint. |
| `UploadChunkSizeBytes` | `int` | `16777216` (16 MB) | Chunk size in bytes for resumable uploads. Must be a multiple of 256 KiB. |
| `RetrySettings` | `GcsRetrySettings?` | `null` | Optional retry policy for transient errors. |

### Configuration Examples

#### Basic Configuration with ADC

```csharp
services.AddGcsStorageProvider(options =>
{
    options.DefaultProjectId = "my-project-id";
    options.UseDefaultCredentials = true;
});
```

#### Configuration for GCS Emulator (Local Development)

```csharp
services.AddGcsStorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://localhost:4443");
    options.DefaultProjectId = "test-project";
    options.UseDefaultCredentials = true;
});
```

#### Configuration with Service Account JSON

```csharp
using Google.Apis.Auth.OAuth2;

services.AddGcsStorageProvider(options =>
{
    options.DefaultProjectId = "my-project-id";
    options.DefaultCredentials = GoogleCredential.FromFile("/path/to/service-account.json");
});
```

#### Configuration with Custom Upload Settings

```csharp
services.AddGcsStorageProvider(options =>
{
    options.UploadChunkSizeBytes = 32 * 1024 * 1024; // 32 MB chunks
});
```

## URI Format

The GCS provider uses URIs with the `gs://` scheme to identify objects.

### Basic Format

```
gs://bucket-name/path/to/object.csv
```

### Parameter Table

| Parameter | Description | Example |
|-----------|-------------|---------|
| `projectId` | Google Cloud project ID | `projectId=my-project-id` |
| `contentType` | Content type for the object when writing | `contentType=application/json` |
| `serviceUrl` | Custom service URL (e.g., emulator) | `serviceUrl=http://localhost:4443` |
| `accessToken` | OAuth2 access token | `accessToken=ya29.a0...` |
| `credentialsPath` | Path to service account JSON file | `credentialsPath=%2Fpath%2Fto%2Fkey.json` |

### URI Examples

```csharp
// Basic GCS object
var uri1 = StorageUri.Parse("gs://my-bucket/data/input.csv");

// With project ID
var uri2 = StorageUri.Parse("gs://my-bucket/data/input.csv?projectId=my-project-id");

// With content type
var uri3 = StorageUri.Parse("gs://my-bucket/data/output.json?contentType=application/json");

// With GCS emulator
var uri4 = StorageUri.Parse("gs://my-bucket/data/file.csv?serviceUrl=http://localhost:4443");
```

## Authentication

The GCS provider supports multiple authentication methods.

### Application Default Credentials (ADC) - Recommended

The default credential chain automatically searches for credentials in the following order:

1. Environment variable (`GOOGLE_APPLICATION_CREDENTIALS` pointing to a service account JSON file)
2. gcloud CLI credentials (`gcloud auth application-default login`)
3. Compute Engine metadata service (when running on GCE)
4. Workload Identity (for GKE and Cloud Run)

**Configuration:**

```csharp
services.AddGcsStorageProvider(options =>
{
    options.DefaultProjectId = "my-project-id";
    options.UseDefaultCredentials = true;
});
```

**Environment Variables:**

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
gcloud auth application-default login
```

### Service Account JSON Authentication

Load credentials from a service account JSON file:

```csharp
using Google.Apis.Auth.OAuth2;

services.AddGcsStorageProvider(options =>
{
    options.DefaultProjectId = "my-project-id";
    options.DefaultCredentials = GoogleCredential.FromFile("/path/to/service-account.json");
});
```

### Access Token Authentication

For short-lived access tokens:

```csharp
// Via URI parameters
var uri = StorageUri.Parse("gs://my-bucket/data.csv?accessToken=ya29.a0...");

// Via options
services.AddGcsStorageProvider(options =>
{
    options.DefaultCredentials = GoogleCredential.FromAccessToken("ya29.a0...");
});
```

⚠️ **Security Warning:** Avoid passing credentials in URIs in production code. Use the credential chain instead.

## GCS Emulator

The GCS provider supports local development using [fake-gcs-server](https://github.com/fsouza/fake-gcs-server).

**Configuration:**

```csharp
services.AddGcsStorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://localhost:4443");
    options.DefaultProjectId = "test-project";
    options.UseDefaultCredentials = true;
});
```

**Starting fake-gcs-server with Docker:**

```bash
docker run -d --name fake-gcs-server \
  -p 4443:4443 \
  fsouza/fake-gcs-server \
  -scheme http -public-host localhost:4443
```

**URI Example:**

```csharp
var uri = StorageUri.Parse("gs://test-bucket/data/file.csv?serviceUrl=http://localhost:4443");
```

## Examples

### Reading from GCS

```csharp
using NPipeline.StorageProviders.Gcp;
using NPipeline.StorageProviders.Models;

var provider = new GcsStorageProvider(
    new GcsClientFactory(new GcsStorageProviderOptions()),
    new GcsStorageProviderOptions());
var uri = StorageUri.Parse("gs://my-bucket/data.csv");

using var stream = await provider.OpenReadAsync(uri);
using var reader = new StreamReader(stream);
var content = await reader.ReadToEndAsync();
```

### Writing to GCS

```csharp
var provider = new GcsStorageProvider(
    new GcsClientFactory(new GcsStorageProviderOptions()),
    new GcsStorageProviderOptions());
var uri = StorageUri.Parse("gs://my-bucket/output.csv");

using var stream = await provider.OpenWriteAsync(uri);
using var writer = new StreamWriter(stream);
await writer.WriteLineAsync("id,name,value");
await writer.WriteLineAsync("1,Item A,100");
```

### Listing Objects

```csharp
var provider = new GcsStorageProvider(
    new GcsClientFactory(new GcsStorageProviderOptions()),
    new GcsStorageProviderOptions());
var uri = StorageUri.Parse("gs://my-bucket/data/");

// List all objects recursively
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

### Checking Object Existence

```csharp
var provider = new GcsStorageProvider(
    new GcsClientFactory(new GcsStorageProviderOptions()),
    new GcsStorageProviderOptions());
var uri = StorageUri.Parse("gs://my-bucket/data.csv");

var exists = await provider.ExistsAsync(uri);
if (exists)
{
    Console.WriteLine("Object exists!");
}
else
{
    Console.WriteLine("Object not found.");
}
```

### Getting Metadata

```csharp
var provider = new GcsStorageProvider(
    new GcsClientFactory(new GcsStorageProviderOptions()),
    new GcsStorageProviderOptions());
var uri = StorageUri.Parse("gs://my-bucket/data.csv");

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

The GCS provider translates Google API exceptions to standard .NET exceptions for consistent error handling.

### Exception Mapping

| HTTP Status | .NET Exception | Description |
|-------------|----------------|-------------|
| 401 (Unauthorized) | `UnauthorizedAccessException` | Authentication failure |
| 403 (Forbidden) | `UnauthorizedAccessException` | Authorization failure |
| 404 (Not Found) | `FileNotFoundException` | Bucket or object not found |
| 400 (Bad Request) | `ArgumentException` | Invalid request parameters |
| Other | `IOException` | General GCS access failure |

## IAM Permissions

To use the GCS provider, your Google Cloud credentials must have appropriate IAM permissions.

### Required Permissions by Operation

| Operation | Required Role |
|-----------|---------------|
| Read (OpenReadAsync) | `roles/storage.objectViewer` |
| Write (OpenWriteAsync) | `roles/storage.objectCreator` |
| List (ListAsync) | `roles/storage.objectViewer` |
| Metadata (GetMetadataAsync) | `roles/storage.objectViewer` |
| Existence (ExistsAsync) | `roles/storage.objectViewer` |

### Example IAM Configuration

Assign the `Storage Object Admin` role for full read/write access:

```bash
gcloud projects add-iam-policy-binding my-project-id \
  --member="serviceAccount:my-service-account@my-project-id.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

### Minimal Policies

**Read-only access:**

```bash
gcloud projects add-iam-policy-binding my-project-id \
  --member="serviceAccount:my-service-account@my-project-id.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"
```

**Write-only access:**

```bash
gcloud projects add-iam-policy-binding my-project-id \
  --member="serviceAccount:my-service-account@my-project-id.iam.gserviceaccount.com" \
  --role="roles/storage.objectCreator"
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

### GCS-Specific Types

- **`GcsStorageProvider`** - GCS provider implementation
  - Location: [`GcsStorageProvider.cs`](../../src/NPipeline.StorageProviders.Gcp/GcsStorageProvider.cs)
  - Implements `IStorageProvider` and `IStorageProviderMetadataProvider`

- **`GcsStorageProviderOptions`** - Configuration options
  - Location: [`GcsStorageProviderOptions.cs`](../../src/NPipeline.StorageProviders.Gcp/GcsStorageProviderOptions.cs)
  - Contains project ID, credentials, service URL, and upload settings

- **`GcsClientFactory`** - Factory for creating GCS clients
  - Location: [`GcsClientFactory.cs`](../../src/NPipeline.StorageProviders.Gcp/GcsClientFactory.cs)
  - Creates and caches `StorageClient` instances

- **`GcsStorageException`** - Custom exception for GCS errors
  - Location: [`GcsStorageException.cs`](../../src/NPipeline.StorageProviders.Gcp/GcsStorageException.cs)
  - Wraps Google API exceptions with bucket/object context

### Extension Methods

- **`ServiceCollectionExtensions.AddGcsStorageProvider`**
  - Location: [`ServiceCollectionExtensions.cs`](../../src/NPipeline.StorageProviders.Gcp/ServiceCollectionExtensions.cs)
  - Extension method for registering GCS provider in DI container

## Limitations

The GCS provider has the following limitations:

### Delete Operations

- `DeleteAsync` is **not supported** and throws `NotSupportedException`
- This is by design to prevent accidental data loss
- Use Google Cloud Console or `gcloud` CLI for delete operations

### Flat Storage Model

- GCS is a flat object storage system (no true hierarchical directories)
- Directory-like paths are simulated through object name prefixes
- The provider treats paths with "/" delimiters as virtual directories for listing purposes

### Large File Handling

- Resumable uploads are used for all writes with configurable chunk size
- Chunk size must be a multiple of 256 KiB
- For very large files, ensure sufficient memory and network bandwidth

## Additional Resources

- [Google Cloud Storage Documentation](https://cloud.google.com/storage/docs)
- [Google Cloud SDK for .NET](https://cloud.google.com/dotnet/docs/reference)
- [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
- [fake-gcs-server (GCS Emulator)](https://github.com/fsouza/fake-gcs-server)
