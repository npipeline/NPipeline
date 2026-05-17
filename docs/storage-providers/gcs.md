---
title: "Google Cloud Storage Provider"
description: "Read and write files in Google Cloud Storage with Application Default Credentials, resumable uploads, and retry."
order: 5
---

# Google Cloud Storage Provider

> **Prerequisites:** [Storage Providers Overview](index.md)

The `NPipeline.StorageProviders.Gcp` package implements `IStorageProvider` for [Google Cloud Storage](https://cloud.google.com/storage). Supports Application Default Credentials (ADC), explicit service account credentials, resumable uploads with configurable chunk sizes, and exponential backoff retry.

## Installation

```bash
dotnet add package NPipeline.StorageProviders.Gcp
```

**Dependencies:** [Google.Cloud.Storage.V1](https://www.nuget.org/packages/Google.Cloud.Storage.V1) 4.x, [Google.Apis.Auth](https://www.nuget.org/packages/Google.Apis.Auth) 1.x

## Quick Start

```csharp
var options = new GcsStorageProviderOptions
{
    DefaultProjectId = "my-project",
    UseDefaultCredentials = true
};
var factory = new GcsClientFactory(options);
var provider = new GcsStorageProvider(factory, options);

var stream = await provider.OpenReadAsync(
    StorageUri.Parse("gs://my-bucket/data/orders.csv"));
```

## URI Format

```
gs://bucket-name/object/path?projectId=my-project&contentType=text/csv
```

| Component | Description |
|-----------|-------------|
| `bucket-name` | GCS bucket (URI host) |
| `object/path` | Object key (URI path) |
| `projectId` | Optional - override `DefaultProjectId` |
| `contentType` | Optional - set content type on write |
| `serviceUrl` | Optional - custom GCS endpoint |
| `accessToken` | Optional - per-request access token |
| `credentialsPath` | Optional - path to service account JSON |

## Authentication

1. **Explicit credentials** - `DefaultCredentials` (`GoogleCredential` instance)
2. **Application Default Credentials** (default) - `GOOGLE_APPLICATION_CREDENTIALS` env var → GCE metadata → gcloud CLI

```csharp
// Application Default Credentials (recommended)
var options = new GcsStorageProviderOptions
{
    DefaultProjectId = "my-project",
    UseDefaultCredentials = true    // default
};

// Explicit service account
var options = new GcsStorageProviderOptions
{
    DefaultProjectId = "my-project",
    DefaultCredentials = GoogleCredential.FromFile("/path/to/sa.json")
};
```

## Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `DefaultProjectId` | `string?` | `null` | GCP project ID |
| `DefaultCredentials` | `GoogleCredential?` | `null` | Explicit credentials |
| `UseDefaultCredentials` | `bool` | `true` | Use Application Default Credentials |
| `ServiceUrl` | `Uri?` | `null` | Custom GCS endpoint (for emulator) |
| `UploadChunkSizeBytes` | `int` | `16 MB` | Resumable upload chunk size (must be multiple of 256 KiB) |
| `UploadBufferThresholdBytes` | `long` | `64 MB` | Reserved for future use |
| `ClientCacheSizeLimit` | `int` | `100` | Max cached `StorageClient` instances |
| `RetrySettings` | `GcsRetrySettings?` | `null` | Retry configuration |

### Retry Settings

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `InitialDelay` | `TimeSpan` | `1s` | First retry delay |
| `MaxDelay` | `TimeSpan` | `32s` | Maximum retry delay |
| `DelayMultiplier` | `double` | `2.0` | Exponential backoff multiplier |
| `MaxAttempts` | `int` | `3` | Total attempts (0 = disable) |
| `RetryOnRateLimit` | `bool` | `true` | Retry on HTTP 429 |
| `RetryOnServerErrors` | `bool` | `true` | Retry on HTTP 5xx |

## Dependency Injection

```csharp
services.AddGcsStorageProvider();

services.AddGcsStorageProvider(options =>
{
    options.DefaultProjectId = "my-project";
    options.RetrySettings = new GcsRetrySettings
    {
        MaxAttempts = 5,
        RetryOnRateLimit = true
    };
});
```

Registers: `IStorageProvider`, `IStorageProviderMetadataProvider`

## Features

- **Resumable uploads** - large files upload in chunks (256 KiB aligned)
- **Exponential backoff** - configurable retry with rate-limit and server error handling
- **Client caching** - `StorageClient` instances cached per project; LRU eviction
- **Metadata** - `Size`, `LastModified`, `ContentType`, `ETag`

## Configuration Examples

### fake-gcs-server (Local Development)

```csharp
services.AddGcsStorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://localhost:4443");
    options.DefaultProjectId = "test-project";
    options.UseDefaultCredentials = true;
});
```

### Service Account JSON

```csharp
services.AddGcsStorageProvider(options =>
{
    options.DefaultProjectId = "my-project-id";
    options.DefaultCredentials = GoogleCredential.FromFile("/path/to/service-account.json");
});
```

### Custom Upload Chunk Size

```csharp
services.AddGcsStorageProvider(options =>
{
    options.UploadChunkSizeBytes = 32 * 1024 * 1024; // 32 MB (must be multiple of 256 KiB)
});
```

## Examples

### Reading

```csharp
var uri = StorageUri.Parse("gs://my-bucket/data.csv");

using var stream = await provider.OpenReadAsync(uri);
using var reader = new StreamReader(stream);
var content = await reader.ReadToEndAsync();
```

### Writing

```csharp
var uri = StorageUri.Parse("gs://my-bucket/output.csv?contentType=text/csv");

using var stream = await provider.OpenWriteAsync(uri);
using var writer = new StreamWriter(stream);
await writer.WriteLineAsync("id,name,value");
```

### Listing

```csharp
var prefix = StorageUri.Parse("gs://my-bucket/data/");

await foreach (var item in provider.ListAsync(prefix, recursive: true))
{
    Console.WriteLine($"{item.Uri} - {item.Size} bytes");
}
```

### Metadata

```csharp
var metadata = await provider.GetMetadataAsync(uri);
if (metadata is not null)
    Console.WriteLine($"Size: {metadata.Size}, ContentType: {metadata.ContentType}");
```

## Error Handling

| HTTP Status | .NET Exception | Description |
|-------------|----------------|-------------|
| 401 (Unauthorized) | `UnauthorizedAccessException` | Authentication failure |
| 403 (Forbidden) | `UnauthorizedAccessException` | Authorization failure |
| 404 (Not Found) | `FileNotFoundException` | Bucket or object missing |
| 400 (Bad Request) | `ArgumentException` | Invalid request parameters |
| Other | `IOException` | General GCS failure |

## IAM Permissions

| Operation | Required IAM Role |
|-----------|-------------------|
| Read | `roles/storage.objectViewer` |
| Write | `roles/storage.objectCreator` |
| Full Access | `roles/storage.objectAdmin` |
| List Buckets | `roles/storage.admin` |

## Limitations

- **Flat storage** - GCS uses prefix-based hierarchy (no real directories)
- **Chunk size** - upload chunk size must be a multiple of 256 KiB
- **Authentication** - ADC requires a GCP environment or service account JSON file

## Next Steps

- [AWS S3 Provider](aws-s3.md) - Amazon alternative
- [Azure Blob Provider](azure-blob.md) - Azure alternative
- [Storage Providers Overview](index.md) - choosing between providers
