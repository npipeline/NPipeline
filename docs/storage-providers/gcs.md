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

**Dependencies:** [Google.Cloud.Storage.V1](https://www.nuget.org/packages/Google.Cloud.Storage.V1) 5.x, [Google.Apis.Auth](https://www.nuget.org/packages/Google.Apis.Auth) 1.x

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

## Authentication

Credentials come from `GcsStorageProviderOptions` only. A URI never carries a secret: the `accessToken` and `credentialsPath` URI parameters are gone, and a URI that still has one throws an `ArgumentException` that points you to `DefaultCredentials`.

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
| `UploadChunkSizeBytes` | `int` | `16 MB` | Resumable upload chunk size (must be a positive multiple of 256 KiB). A write holds about two chunks in memory |
| `ClientCacheSizeLimit` | `int` | `100` | Max cached `StorageClient` instances |
| `Resilience` | `Resilience` | `GcsStorageResilience.Default` | Retries and timeouts for each request. See [Resilience](#resilience) |

## Resilience

The provider sends GCS requests (object metadata, each page of a listing, delete, copy, and reopening a failed download) through
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

Reads and writes use different retry layers, because a stream cannot be replayed:

- **Metadata, list, delete and copy calls** run under `Resilience`. The Google SDK's own retry is off for them
  (`RetryOptions.Never`), so only one layer retries each request.
- **Reads** stream the object (see [Streaming reads](#streaming-reads)). If the connection fails part-way, the
  stream reopens the object at the current offset under `Resilience`. `Resilience.None` turns the resume off.
- **Writes** stream to GCS while you write (see [Streaming writes](#streaming-writes)), so the provider can't re-send
  the data from the start. Transient chunk failures are retried inside the Google SDK's resumable-upload session:
  the SDK asks the server how many bytes it holds and re-sends from that offset. `Resilience` does not apply to
  writes. If the SDK gives up, the next `WriteAsync` or `CommitAsync` throws, and you write the object again from
  the start.

If you subclass `GcsClientFactory` and build your own `StorageClient`, keep the SDK's default retry on it so
resumable uploads can retry. Pass `RetryOptions.Never` on any metadata calls you add.

## Streaming reads

`OpenReadAsync` returns as soon as GCS answers with the response headers. The object is not downloaded to disk first, so time to first byte doesn't depend on the object's size, and no local disk is used. The stream is forward-only (`CanSeek` is `false`).

If a transient failure interrupts the body, the stream reopens the object with `Range: bytes={offset}-` and `ifGenerationMatch` set to the generation of the first response, under `Resilience`. If someone overwrites the object in between, the precondition fails and the read throws an `IOException` instead of splicing two versions together. A missing object throws `FileNotFoundException`.

## Streaming writes

`OpenWriteAsync` starts a resumable upload when you first write, and the upload runs while you keep writing. Your writes go into a bounded pipe whose reader feeds the SDK. When the pipe holds one chunk (`UploadChunkSizeBytes`) the writer waits, so memory use is about two chunks regardless of the object's size. Nothing is written to local disk.

- `CommitAsync` ends the stream, which lets the SDK send the final chunk, and waits for the upload. Cancelling the token you pass aborts the upload. The object appears only when GCS receives the final chunk. `stream.ETag` is set from the result.
- Disposing without committing cancels the upload and never sends the final chunk, so no object appears at the target and an existing object stays as it was. The abandoned upload session expires on the server.
- Retries happen inside the SDK's resumable session (see [Resilience](#resilience)), not by re-sending the whole object.

## Dependency Injection

```csharp
services.AddGcsStorageProvider();

services.AddGcsStorageProvider(options =>
{
    options.DefaultProjectId = "my-project";
    options.Resilience = GcsStorageResilience.Default with { Attempts = 5 };
});
```

Registers: `IStorageProvider` (capabilities: Read, Write, List, Delete, Move)

## Features

- **Streaming reads** - the object streams from the HTTP response; no temporary file, and a failure mid-read resumes from the offset
- **Streaming writes** - the upload runs while you write, as a resumable session fed from a bounded pipe; no temporary file
- **Retries** - jittered exponential backoff for network failures, 408, 429 (honoring `Retry-After`), and 5xx
- **Client caching** - one `StorageClient` per distinct `serviceUrl` and `projectId`, bounded by `ClientCacheSizeLimit` with LRU eviction; the factory disposes them when it is disposed
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

await using var stream = await provider.OpenWriteAsync(uri, cancellationToken: ct);
await using (var writer = new StreamWriter(stream, leaveOpen: true))
{
    await writer.WriteLineAsync("id,name,value");
}

// The object appears only after CommitAsync. Disposing the stream without it discards the data.
await stream.CommitAsync(ct);
```

Call `CommitAsync` once, after the last write. This provider doesn't declare `ConditionalWrite`. See [Streaming writes](#streaming-writes) for what commit and dispose do.

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
- **Forward-only streams** - reads and writes can't seek, and a failed write can't be resumed from your side: write the object again
- **Authentication** - ADC requires a GCP environment or service account JSON file

## Next Steps

- [AWS S3 Provider](aws-s3.md) - Amazon alternative
- [Azure Blob Provider](azure-blob.md) - Azure alternative
- [Storage Providers Overview](index.md) - choosing between providers
