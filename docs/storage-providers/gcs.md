---
title: "Google Cloud Storage Provider"
description: "Read and write files in Google Cloud Storage with Application Default Credentials, resumable uploads, and resilient retries."
order: 5
---

# Google Cloud Storage Provider

> **Prerequisites:** [Storage Providers Overview](index.md)

The `NPipeline.StorageProviders.Gcp` package implements `IStorageProvider` for [Google Cloud Storage](https://cloud.google.com/storage). Supports Application Default Credentials (ADC), explicit service account credentials, resumable uploads with configurable chunk sizes, and retries with jittered exponential backoff.

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
| `Resilience` | `Resilience` | `GcsStorageResilience.Default` | Retries and timeouts for each request. See [Resilience](#resilience) |

## Resilience

The provider sends every GCS request (object metadata, each page of a listing, downloads, and uploads) through
[NResilience](https://github.com/nresilience/NResilience). The `Resilience` option configures it. The default,
`GcsStorageResilience.Default`, does the following:

- Makes up to three attempts (two retries), the same count as the Google SDK's own default retry.
- Retries network failures, HTTP client timeouts, 408, and 5xx responses. A 429 response is treated as throttling:
  it takes the throttled backoff curve and honors `Retry-After` when the server sends one. Other 4xx responses, such
  as 403 and 404, are not retried.
- Waits with exponential backoff and full jitter, from 1 second up to 32 seconds, so parallel writers don't retry
  in lockstep.
- Has no attempt timeout and no overall deadline, because a large download or upload can run for a long time. The
  SDK's HTTP client timeout (100 seconds by default) still bounds each HTTP request.

To change a setting, derive a policy with a `with` expression. To turn retries off, use `Resilience.None`:

```csharp
services.AddGcsStorageProvider(options =>
{
    options.Resilience = GcsStorageResilience.Default with { Attempts = 5 };
    // or: options.Resilience = Resilience.None;
});
```

A retried download starts again with an empty buffer, and a retried upload re-sends the whole object from its
first byte in a new upload session. Uploading an object replaces it, so a retry can't leave a partial or duplicated
object.

The provider is the only layer that retries. Clients built by `GcsClientFactory` send each HTTP request once
(`ConfigurableMessageHandler.NumTries = 1`), which turns off the Google SDK's retry of metadata calls and its
in-session resume of resumable uploads, and metadata requests also pass `RetryOptions.Never`. Two consequences:

- A transient failure part-way through a large upload restarts the upload after a backoff instead of resuming the
  session immediately.
- If you subclass `GcsClientFactory` and build your own `StorageClient`, set
  `client.Service.HttpClient.MessageHandler.NumTries = 1` on it. Otherwise the SDK's upload resume runs inside each
  provider attempt and the attempts multiply.

A `GcsWriteStream` that you construct directly, rather than through `OpenWriteAsync`, uploads once without retrying.

## Dependency Injection

```csharp
services.AddGcsStorageProvider();

services.AddGcsStorageProvider(options =>
{
    options.DefaultProjectId = "my-project";
    options.Resilience = GcsStorageResilience.Default with { Attempts = 5 };
});
```

Registers: `IStorageProvider`, `IStorageProviderMetadataProvider`

## Features

- **Resumable uploads** - large files upload in chunks (256 KiB aligned)
- **Retries** - jittered exponential backoff for network failures, 408, 429 (honoring `Retry-After`), and 5xx
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
