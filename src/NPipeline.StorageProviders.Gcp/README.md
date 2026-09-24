# Google Cloud Storage Provider

`NPipeline.StorageProviders.Gcp` provides a fully-featured `IStorageProvider` implementation for Google Cloud Storage, enabling seamless integration with
`gs://` URIs. Use this provider to read, write, and manage objects in Google Cloud Storage buckets within your NPipeline workflows.

## Features

- **Stream-based I/O** - Efficient read/write operations for large objects with streaming support
- **Metadata & Existence** - Check if objects exist and retrieve detailed metadata
- **Flexible Listing** - List objects by prefix with recursive and non-recursive options
- **Built-in Retries** - Retries network failures and HTTP 408, 429, and 5xx errors with jittered exponential backoff
- **Multiple Auth Methods** - Support for default credentials, service account keys, access tokens, or emulator endpoints
- **URI-based Configuration** - Override settings per-object through query parameters
- **Emulator Support** - Full compatibility with Google Cloud Storage emulator for local development

## Prerequisites

- .NET 6.0 or later
- Google Cloud project with Storage API enabled (or local emulator)
- Proper authentication configured (see [Authentication](#authentication) section below)

## Install

Add a project reference to your application:

```bash
dotnet add package NPipeline.StorageProviders.Gcp
```

Or add directly to your project file:

```xml
<ItemGroup>
  <ProjectReference Include="path/to/NPipeline.StorageProviders.Gcp/NPipeline.StorageProviders.Gcp.csproj" />
</ItemGroup>
```

## Configure with DI

Register the GCS storage provider in your dependency injection container:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Gcp;
using NPipeline.StorageProviders.Gcp.Reliability;

var services = new ServiceCollection();

services.AddGcsStorageProvider(options =>
{
    // Use default credentials (Application Default Credentials)
    options.UseDefaultCredentials = true;

    // Set default project ID for all operations
    options.DefaultProjectId = "my-project-id";

    // Optional: change how transient failures are retried
    options.Resilience = GcsStorageResilience.Default with { Attempts = 5 };
});

var provider = services
    .BuildServiceProvider()
    .GetRequiredService<GcsStorageProvider>();
```

### Configuration Options

| Option                  | Purpose                                  | Default           |
|-------------------------|------------------------------------------|-------------------|
| `DefaultProjectId`      | Project ID used for all operations       | (not required)    |
| `UseDefaultCredentials` | Use Application Default Credentials      | `true`            |
| `ServiceUrl`            | Custom GCS endpoint (for emulator)       | GCS production    |
| `Resilience`            | Retries and timeouts for each request    | `GcsStorageResilience.Default` |

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
    Console.WriteLine($"Last Modified: {metadata.LastModified}");
}
```

### Listing Objects

```csharp
// List all objects with a given prefix
var objects = provider.ListAsync(StorageUri.Parse("gs://my-bucket/logs/"));

await foreach (var obj in objects)
{
    Console.WriteLine($"{obj.Uri} ({obj.Size} bytes)");
}

// Recursively list nested objects
var recursive = provider.ListAsync(StorageUri.Parse("gs://my-bucket/data/"), recursive: true);
```

## URI Parameters

You can override configuration settings per-object using query parameters in the `gs://` URI:

| Parameter         | Purpose                      | Example                                              |
|-------------------|------------------------------|------------------------------------------------------|
| `projectId`       | Override default project ID  | `gs://bucket/file?projectId=alt-project`             |
| `contentType`     | MIME type for the object     | `gs://bucket/file?contentType=application/json`      |
| `serviceUrl`      | Custom endpoint (emulator)   | `gs://bucket/file?serviceUrl=http://localhost:4443`  |
| `accessToken`     | OAuth 2.0 access token       | `gs://bucket/file?accessToken=ya29.xxx`              |
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

Explicitly provide a service account JSON file via the `credentialsPath` URI parameter:

```csharp
var uri = StorageUri.Parse("gs://my-bucket/data.csv?credentialsPath=/secure/service-account-key.json");
await using var stream = await provider.OpenReadAsync(uri);
```

Alternatively, load credentials in code and set `DefaultCredentials`:

```csharp
using Google.Apis.Auth.OAuth2;

services.AddGcsStorageProvider(options =>
{
    options.DefaultProjectId = "my-project";
    options.DefaultCredentials = GoogleCredential.FromFile("/secure/service-account-key.json");
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

## Important Notes

- **Upload Chunking** - For large objects, uploads are split into 256 KiB chunks. The chunk size parameter must be a positive multiple of 256 KiB.
- **Transient Errors** - The provider retries transient failures according to `Resilience`. See [Resilience](#resilience).
- **Streaming** - Use `OpenReadAsync` and `OpenWriteAsync` for efficient handling of large objects without loading them entirely into memory.
- **Metadata Freshness** - Object metadata may be cached briefly. For critical operations requiring current state, consider adding a small delay between checks.
- **Special Characters** - Object names with special characters must be URL-encoded in URIs.

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

- **Full Documentation** - [GCS Storage Provider Guide](../../../docs/storage-providers/gcs-storage-provider.md)
- **Working Example** - [Sample_GcsStorageProvider](../../../samples/Sample_GcsStorageProvider)
- **NPipeline Architecture** - [Documentation](../../../docs/architecture/)
- **Google Cloud Storage Docs** - <https://cloud.google.com/storage/docs>

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.
