# Google Cloud Storage Provider

`NPipeline.StorageProviders.Gcs` provides a fully-featured `IStorageProvider` implementation for Google Cloud Storage, enabling seamless integration with `gs://` URIs. Use this provider to read, write, and manage objects in Google Cloud Storage buckets within your NPipeline workflows.

## Features

- **Stream-based I/O** — Efficient read/write operations for large objects with streaming support
- **Metadata & Existence** — Check if objects exist and retrieve detailed metadata
- **Flexible Listing** — List objects by prefix with recursive and non-recursive options
- **Built-in Retries** — Automatic retry handling for transient failures (HTTP 429/5xx errors)
- **Multiple Auth Methods** — Support for default credentials, service account keys, access tokens, or emulator endpoints
- **URI-based Configuration** — Override settings per-object through query parameters
- **Emulator Support** — Full compatibility with Google Cloud Storage emulator for local development

## Prerequisites

- .NET 6.0 or later
- Google Cloud project with Storage API enabled (or local emulator)
- Proper authentication configured (see [Authentication](#authentication) section below)

## Install

Add a project reference to your application:

```bash
dotnet add package NPipeline.StorageProviders.Gcs
```

Or add directly to your project file:

```xml
<ItemGroup>
  <ProjectReference Include="path/to/NPipeline.StorageProviders.Gcs/NPipeline.StorageProviders.Gcs.csproj" />
</ItemGroup>
```

## Configure with DI

Register the GCS storage provider in your dependency injection container:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Gcs;

var services = new ServiceCollection();

services.AddGcsStorageProvider(options =>
{
    // Use default credentials (Application Default Credentials)
    options.UseDefaultCredentials = true;
    
    // Set default project ID for all operations
    options.DefaultProjectId = "my-project-id";
    
    // Optional: Configure retries for transient failures
    options.MaxRetries = 3;
    options.RetryDelayMs = 1000;
});

var provider = services
    .BuildServiceProvider()
    .GetRequiredService<GcsStorageProvider>();
```

### Configuration Options

| Option | Purpose | Default |
| --- | --- | --- |
| `DefaultProjectId` | Project ID used for all operations | (required) |
| `UseDefaultCredentials` | Use Application Default Credentials | `false` |
| `ServiceUrl` | Custom GCS endpoint (for emulator) | GCS production |
| `MaxRetries` | Max retries for transient errors | `3` |
| `RetryDelayMs` | Initial delay between retries (ms) | `1000` |

## Usage Examples

### Writing Objects

```csharp
using NPipeline.StorageProviders.Models;

var uri = StorageUri.Parse("gs://my-bucket/sample/hello.txt?contentType=text/plain");

// Write text content
await using (var write = await provider.OpenWriteAsync(uri))
{
    var bytes = System.Text.Encoding.UTF8.GetBytes("hello gcs");
    await write.WriteAsync(bytes, CancellationToken.None);
}
```

### Reading Objects

```csharp
await using (var read = await provider.OpenReadAsync(uri))
using var reader = new StreamReader(read);
var content = await reader.ReadToEndAsync();
Console.WriteLine(content);
```

### Checking Object Existence

```csharp
var exists = await provider.ExistsAsync(uri);
if (exists)
{
    var metadata = await provider.GetMetadataAsync(uri);
    Console.WriteLine($"Size: {metadata.Size} bytes");
    Console.WriteLine($"Updated: {metadata.Updated}");
}
```

### Listing Objects

```csharp
// List all objects with a given prefix
var objects = await provider.ListAsync("gs://my-bucket/logs/");

foreach (var obj in objects)
{
    Console.WriteLine($"{obj.Path} ({obj.Size} bytes)");
}

// Recursively list nested objects
var recursive = await provider.ListAsync("gs://my-bucket/data/", recursive: true);
```

## URI Parameters

You can override configuration settings per-object using query parameters in the `gs://` URI:

| Parameter | Purpose | Example |
| --- | --- | --- |
| `projectId` | Override default project ID | `gs://bucket/file?projectId=alt-project` |
| `contentType` | MIME type for the object | `gs://bucket/file?contentType=application/json` |
| `serviceUrl` | Custom endpoint (emulator) | `gs://bucket/file?serviceUrl=http://localhost:4443` |
| `accessToken` | OAuth 2.0 access token | `gs://bucket/file?accessToken=ya29.xxx` |
| `credentialsPath` | Path to service account JSON | `gs://bucket/file?credentialsPath=/path/to/key.json` |

### Examples

```csharp
// Override project ID
var uri1 = StorageUri.Parse("gs://bucket/file.txt?projectId=other-project");

// Set content type on upload
var uri2 = StorageUri.Parse("gs://bucket/data.json?contentType=application/json");

// Access emulator
var uri3 = StorageUri.Parse("gs://bucket/test.txt?serviceUrl=http://localhost:4443");
```

## Authentication

The provider supports multiple authentication methods:

### Application Default Credentials (Recommended)

The simplest approach for most use cases. GCS automatically searches for credentials in this order:

1. Credentials file specified by `GOOGLE_APPLICATION_CREDENTIALS` environment variable
2. Credentials in the gcloud CLI default location
3. Credentials from Google Cloud Compute metadata service (for Cloud Run, GCE, etc.)

```csharp
services.AddGcsStorageProvider(options =>
{
    options.DefaultProjectId = "my-project";
    options.UseDefaultCredentials = true;
});
```

### Service Account Key

Explicitly provide a service account JSON file:

```csharp
services.AddGcsStorageProvider(options =>
{
    options.DefaultProjectId = "my-project";
    // Path can be relative or absolute
    options.CredentialsPath = "/secure/service-account-key.json";
});
```

### Access Token

Use a manually-provided OAuth 2.0 access token. Tokens are short-lived and must be refreshed periodically:

```csharp
var uri = StorageUri.Parse("gs://bucket/file?accessToken=ya29.xxx");
```

### Local Emulator

For development without credentials, use the Google Cloud Storage emulator:

```csharp
# Start the emulator (requires gcloud)
gcloud beta emulators firestore start --host-port=localhost:4443

services.AddGcsStorageProvider(options =>
{
    options.DefaultProjectId = "test-project";
    options.ServiceUrl = "http://localhost:4443";
});
```

Or set the environment variable:

```bash
export STORAGE_EMULATOR_HOST="http://localhost:4443"
```

## Important Notes

- **No Delete Support** — `DeleteAsync` is intentionally not supported. Use the GCS Console or `gsutil rm` for deletion.
- **Upload Chunking** — For large objects, uploads are split into 256 KiB chunks. The chunk size parameter must be a positive multiple of 256 KiB.
- **Transient Errors** — The provider automatically retries HTTP 429 (rate limit) and 5xx errors with exponential backoff. Configure retry behavior via `MaxRetries` and `RetryDelayMs`.
- **Streaming** — Use `OpenReadAsync` and `OpenWriteAsync` for efficient handling of large objects without loading them entirely into memory.
- **Metadata Freshness** — Object metadata may be cached briefly. For critical operations requiring current state, consider adding a small delay between checks.
- **Special Characters** — Object names with special characters must be URL-encoded in URIs.

## Troubleshooting

### "Application Default Credentials not found"

Ensure credentials are available:

```bash
# Option 1: Set explicit credentials file
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json

# Option 2: Use gcloud to authenticate
gcloud auth application-default login

# Option 3: Use individual methods (service account, access token, emulator)
```

### "Project ID not set"

Ensure `DefaultProjectId` is configured:

```csharp
services.AddGcsStorageProvider(options =>
{
    options.DefaultProjectId = "your-project-id";
});
```

Or pass it per-URI:

```csharp
var uri = StorageUri.Parse("gs://bucket/file?projectId=your-project-id");
```

## More

- **Full Documentation** — [GCS Storage Provider Guide](../../../docs/storage-providers/gcs-storage-provider.md)
- **Working Example** — [Sample_GcsStorageProvider](../../../samples/Sample_GcsStorageProvider)
- **NPipeline Architecture** — [Documentation](../../../docs/architecture/)
- **Google Cloud Storage Docs** — <https://cloud.google.com/storage/docs>
