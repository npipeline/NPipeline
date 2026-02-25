---
title: Google Cloud Storage Provider
description: Read from and write to Google Cloud Storage using the GCS storage provider.
sidebar_position: 5
---

## Google Cloud Storage Provider

The Google Cloud Storage (GCS) provider enables NPipeline applications to read from and write to Google Cloud Storage using a unified storage abstraction. This provider implements the `IStorageProvider` interface and supports the `gs://` URI scheme.

### Overview

The GCS provider offers:

- **Stream-based I/O** for efficient large file handling
- **Async-first API** for scalable, non-blocking operations
- **Flexible authentication** via Application Default Credentials, service account JSON, or explicit credentials
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
- `NPipeline.StorageProviders` - Core storage abstractions
- `NPipeline.Connectors` - Core connectors for using storage providers

### Key Storage Types

> **Note:** Shared storage types (IStorageProvider, StorageUri, StorageItem, StorageMetadata, etc.) are common across all NPipeline storage providers. Refer to the [Storage Provider Interface](./storage-provider.md) documentation for details.

GCS-specific configuration type:

- **`GcsStorageProviderOptions`** - Configuration options for the GCS provider (project ID, credentials, service URL, upload chunk size)

## Installation

### Prerequisites

- .NET 8.0 or later
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
| `DefaultProjectId` | `string?` | `null` | Default Google Cloud project ID. |
| `DefaultCredentials` | `GoogleCredential?` | `null` | Default Google credentials for authentication. If not specified, ADC is used when `UseDefaultCredentials` is true. |
| `UseDefaultCredentials` | `bool` | `true` | Whether to use Application Default Credentials (ADC). |
| `ServiceUrl` | `Uri?` | `null` | Optional service URL for GCS emulator or custom endpoints. |
| `UploadChunkSizeBytes` | `int` | `16777216` (16 MB) | Chunk size in bytes for resumable uploads (must be a multiple of 256 KiB). |

### Configuration Examples

#### Basic Configuration with Default Credentials

```csharp
services.AddGcsStorageProvider(options =>
{
    options.DefaultProjectId = "my-project-id";
    options.UseDefaultCredentials = true;
});
```

#### Configuration for fake-gcs-server (Local Development)

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

The GCS provider uses the `gs://` URI scheme.

```csharp
// Basic format
var uri = StorageUri.Parse("gs://bucket-name/path/to/object.csv");

// With project ID
var uri = StorageUri.Parse("gs://bucket-name/object.csv?projectId=my-project-id");

// With custom endpoint (emulator)
var uri = StorageUri.Parse("gs://bucket-name/object.csv?serviceUrl=http://localhost:4443");

// With content type
var uri = StorageUri.Parse("gs://bucket-name/output.json?contentType=application/json");
```

## Authentication

The GCS provider supports multiple authentication methods through the credential hierarchy:

1. **Explicit credentials** - `DefaultCredentials` option set to a `GoogleCredential`
2. **Application Default Credentials (ADC)** - Environment variable, gcloud CLI, Compute Engine metadata
3. **Access tokens** - For short-lived credentials

When `UseDefaultCredentials` is true (default), the provider searches for credentials in this order:

- `GOOGLE_APPLICATION_CREDENTIALS` environment variable (path to service account JSON)
- `gcloud auth application-default login` credentials
- Compute Engine metadata service (on GCE)
- Workload Identity (on GKE/Cloud Run)

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

Your credentials must have appropriate IAM permissions:

| Operation | Required Role |
|-----------|---------------|
| Read | `roles/storage.objectViewer` |
| Write | `roles/storage.objectCreator` |
| Full Access | `roles/storage.objectAdmin` |

Use the `Storage Object Admin` role for testing, or assign minimal permissions based on your use case.

## Limitations

- GCS is a flat object storage system; directory-like paths are simulated through object name prefixes
- Chunk size for uploads must be a multiple of 256 KiB

## Resources

- [Google Cloud Storage Documentation](https://cloud.google.com/storage/docs)
- [Google Cloud SDK for .NET](https://cloud.google.com/dotnet/docs/reference)
- [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
- [fake-gcs-server](https://github.com/fsouza/fake-gcs-server)
