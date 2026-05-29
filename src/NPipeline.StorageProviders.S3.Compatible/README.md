# NPipeline.StorageProviders.S3.Compatible

S3-compatible storage provider for NPipeline. Implements `IStorageProvider` for non-AWS S3-compatible object stores - MinIO, Cloudflare R2, DigitalOcean Spaces, Backblaze B2, Wasabi, LocalStack, and any other service that speaks the S3 protocol.

## Features

- **Static credentials** - access key and secret key configured once in options
- **Custom endpoint** - point to any S3-compatible service URL
- **Path-style addressing** - enabled by default (`ForcePathStyle = true`), required by most S3-compatible services
- **Configurable signing region** - override the AWS signing region (use `"auto"` for Cloudflare R2)
- **Multipart uploads** - automatic multipart upload for objects above the configurable threshold
- **Client caching** - a single `IAmazonS3` client is created and reused across all requests
- **Async streaming** - all reads, writes, and listings stream data without materialising full objects in memory

## Installation

```bash
dotnet add package NPipeline.StorageProviders.S3.Compatible
```

**Dependencies:** [AWSSDK.S3](https://www.nuget.org/packages/AWSSDK.S3) 4.x, [AWSSDK.Core](https://www.nuget.org/packages/AWSSDK.Core) 4.x

## Quick Start

```csharp
using NPipeline.StorageProviders.S3.Compatible;

var options = new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("https://minio.example.com:9000"),
    AccessKey  = "minioadmin",
    SecretKey  = "minioadmin"
};
var factory  = new S3CompatibleClientFactory(options);
var provider = new S3CompatibleStorageProvider(factory, options);

// Read
using var stream = await provider.OpenReadAsync(
    StorageUri.Parse("s3://my-bucket/data/orders.csv"));
```

## URI Format

Uses the same `s3://` scheme as the AWS S3 provider:

```
s3://bucket-name/key/path
```

| Component | Description |
|-----------|-------------|
| `bucket-name` | Bucket name (URI host) |
| `key/path` | Object key (URI path) |

> **Note:** The S3-Compatible provider does not support per-URI credential or endpoint overrides - all requests use the credentials and `ServiceUrl` from options.

## Configuration

`ServiceUrl`, `AccessKey`, and `SecretKey` are `required init` properties and must be supplied at construction time.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ServiceUrl` | `Uri` | **(required)** | Base URL of the S3-compatible endpoint |
| `AccessKey` | `string` | **(required)** | Access key (equivalent to AWS access key ID) |
| `SecretKey` | `string` | **(required)** | Secret key (equivalent to AWS secret access key) |
| `SigningRegion` | `string` | `"us-east-1"` | Region string used only for request signing |
| `ForcePathStyle` | `bool` | `true` | Use path-style URLs - required by most S3-compatible services |
| `MultipartUploadThresholdBytes` | `long` | `67108864` (64 MB) | Objects above this size use S3 multipart upload |

## Service-Specific Configuration

### MinIO

```csharp
new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("https://minio.example.com:9000"),
    AccessKey  = "minioadmin",
    SecretKey  = "minioadmin",
    ForcePathStyle = true   // required for MinIO
}
```

### Cloudflare R2

```csharp
new S3CompatibleStorageProviderOptions
{
    ServiceUrl    = new Uri("https://<account-id>.r2.cloudflarestorage.com"),
    AccessKey     = "your-r2-access-key",
    SecretKey     = "your-r2-secret-key",
    SigningRegion = "auto"   // required for R2
}
```

### DigitalOcean Spaces

```csharp
new S3CompatibleStorageProviderOptions
{
    ServiceUrl    = new Uri("https://nyc3.digitaloceanspaces.com"),
    AccessKey     = "DO00...",
    SecretKey     = "...",
    SigningRegion = "nyc3"
}
```

### Backblaze B2

```csharp
new S3CompatibleStorageProviderOptions
{
    ServiceUrl    = new Uri("https://s3.us-west-002.backblazeb2.com"),
    AccessKey     = "keyID",
    SecretKey     = "applicationKey",
    SigningRegion = "us-west-002",
    ForcePathStyle = true
}
```

### LocalStack (Testing)

```csharp
new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("http://localhost:4566"),
    AccessKey  = "test",
    SecretKey  = "test",
    ForcePathStyle = true
}
```

## Supported Services

| Service | Path Style | Signing Region |
|---------|-----------|----------------|
| MinIO | `true` | `us-east-1` |
| Cloudflare R2 | `true` | `auto` |
| DigitalOcean Spaces | `false` | Region name (e.g., `nyc3`) |
| Backblaze B2 | `true` | Region name (e.g., `us-west-002`) |
| Wasabi | `true` | Region name (e.g., `us-central-1`) |
| LocalStack | `true` | `us-east-1` |

## Dependency Injection

```csharp
using NPipeline.StorageProviders.S3.Compatible;

services.AddS3CompatibleStorageProvider(new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("https://minio.example.com:9000"),
    AccessKey  = "minioadmin",
    SecretKey  = "minioadmin"
});
```

> The `required` init properties mean there is no parameterless overload - a pre-built options instance is always required.

Registers `S3CompatibleStorageProvider` as a singleton, along with `S3CompatibleClientFactory` and `S3CompatibleStorageProviderOptions`.

## Examples

### Reading

```csharp
var uri = StorageUri.Parse("s3://my-bucket/data/orders.csv");
using var stream = await provider.OpenReadAsync(uri);
using var reader = new StreamReader(stream);
var content = await reader.ReadToEndAsync();
```

### Writing

```csharp
var uri = StorageUri.Parse("s3://my-bucket/output/results.csv");
using var stream = await provider.OpenWriteAsync(uri);
using var writer = new StreamWriter(stream);
await writer.WriteLineAsync("id,name,value");
await writer.WriteLineAsync("1,Widget,42.00");
```

### Listing

```csharp
var prefix = StorageUri.Parse("s3://my-bucket/data/");

await foreach (var item in provider.ListAsync(prefix, recursive: true))
    Console.WriteLine($"{item.Uri}  {item.Size} bytes");
```

### Metadata

```csharp
var metadata = await provider.GetMetadataAsync(uri);
if (metadata is not null)
    Console.WriteLine($"Size: {metadata.Size}, ContentType: {metadata.ContentType}");
```

## Error Handling

| S3 Error Code | .NET Exception | Cause |
|---------------|----------------|-------|
| `AccessDenied`, `InvalidAccessKeyId`, `SignatureDoesNotMatch` | `UnauthorizedAccessException` | Auth or permission failure |
| `InvalidBucketName`, `InvalidKey` | `ArgumentException` | Malformed bucket or key |
| `NoSuchBucket`, `NotFound` | `FileNotFoundException` | Bucket or object does not exist |
| Other S3 API errors | `IOException` | General failure |

## Limitations

- **No per-URI credential overrides** - credentials and `ServiceUrl` are fixed at configuration time. To use multiple endpoints, register separate provider instances.
- **Flat storage** - S3-compatible services use prefix-based hierarchy; `SupportsHierarchy = false`.
- **Provider-specific differences** - multipart upload support, metadata handling, and custom headers vary by service.

## Requirements

- .NET 8.0, 9.0, or 10.0
- `AWSSDK.S3` 4.x (automatically included)
- `AWSSDK.Core` 4.x (automatically included)

## Related Packages

- **[NPipeline.StorageProviders.S3.Aws](https://www.nuget.org/packages/NPipeline.StorageProviders.S3.Aws)** - AWS S3 with IAM credential chain and per-URI region overrides
- **[NPipeline.StorageProviders.Azure](https://www.nuget.org/packages/NPipeline.StorageProviders.Azure)** - Azure Blob Storage provider
- **[NPipeline.StorageProviders](https://www.nuget.org/packages/NPipeline.StorageProviders)** - Base abstractions (`IStorageProvider`, `StorageUri`)

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.
