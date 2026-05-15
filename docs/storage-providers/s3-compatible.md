---
title: "S3-Compatible Storage Provider"
description: "Read and write files on S3-compatible services like MinIO, DigitalOcean Spaces, and Cloudflare R2."
order: 7
---

# S3-Compatible Storage Provider

> **Prerequisites:** [Storage Providers Overview](index.md)

The `NPipeline.StorageProviders.S3.Compatible` package implements `IStorageProvider` for any S3-compatible object storage service. It extends the same `S3CoreStorageProvider` base as the [AWS S3 provider](aws-s3.md) but targets non-AWS endpoints with static credentials and path-style addressing.

## Installation

```bash
dotnet add package NPipeline.StorageProviders.S3.Compatible
```

**Dependencies:** [AWSSDK.S3](https://www.nuget.org/packages/AWSSDK.S3) 4.x, [AWSSDK.Core](https://www.nuget.org/packages/AWSSDK.Core) 4.x

## Quick Start

```csharp
var options = new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("https://minio.example.com:9000"),
    AccessKey = "minioadmin",
    SecretKey = "minioadmin"
};
var factory = new S3CompatibleClientFactory(options);
var provider = new S3CompatibleStorageProvider(factory, options);

var stream = await provider.OpenReadAsync(
    StorageUri.Parse("s3://my-bucket/data/orders.csv"));
```

## URI Format

Uses the same `s3://` scheme as the AWS S3 provider:

```
s3://bucket-name/key/path
```

> **Note:** When both the AWS S3 and S3-Compatible providers are registered, the `StorageResolver` routes based on the `CanHandle()` check. Register only the provider you need, or use separate resolvers.

## Configuration

All three required properties use `required init` — they must be set at construction time.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ServiceUrl` | `Uri` | **(required)** | S3-compatible endpoint URL |
| `AccessKey` | `string` | **(required)** | Access key |
| `SecretKey` | `string` | **(required)** | Secret key |
| `SigningRegion` | `string` | `"us-east-1"` | AWS signing region (use `"auto"` for Cloudflare R2) |
| `ForcePathStyle` | `bool` | `true` | Path-style URLs (required by most S3-compatible services) |
| `MultipartUploadThresholdBytes` | `long` | `64 MB` | Switch to multipart upload above this size |

## Service-Specific Configuration

### MinIO

```csharp
new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("https://minio.example.com:9000"),
    AccessKey = "minioadmin",
    SecretKey = "minioadmin",
    ForcePathStyle = true    // required for MinIO
}
```

### DigitalOcean Spaces

```csharp
new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("https://nyc3.digitaloceanspaces.com"),
    AccessKey = "DO00...",
    SecretKey = "...",
    SigningRegion = "nyc3"
}
```

### Cloudflare R2

```csharp
new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("https://<account-id>.r2.cloudflarestorage.com"),
    AccessKey = "...",
    SecretKey = "...",
    SigningRegion = "auto"    // required for R2
}
```

## Dependency Injection

```csharp
services.AddS3CompatibleStorageProvider(new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("https://minio.example.com:9000"),
    AccessKey = "minioadmin",
    SecretKey = "minioadmin"
});
```

> The `required` properties mean there is no parameterless overload — you must pass a pre-built options instance.

Registers: `IStorageProvider`, `IStorageProviderMetadataProvider`

## Supported Services

| Service | Path Style | Signing Region | Notes |
|---------|-----------|----------------|-------|
| MinIO | `true` (required) | `us-east-1` | Self-hosted; Docker available |
| DigitalOcean Spaces | `false` | Region name (e.g., `nyc3`) | CDN included |
| Cloudflare R2 | `true` | `auto` | No egress fees |
| Backblaze B2 | `true` | `us-west-002` etc. | Low-cost archival |
| Wasabi | `true` | Region name | No API request charges |
| LocalStack | `true` | `us-east-1` | Accepts any credentials |

## Additional Service Examples

### LocalStack (Testing)

```csharp
new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("http://localhost:4566"),
    AccessKey = "test",
    SecretKey = "test",
    ForcePathStyle = true
}
```

### Wasabi

```csharp
new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("https://s3.us-central-1.wasabisys.com"),
    AccessKey = "your-access-key",
    SecretKey = "your-secret-key",
    SigningRegion = "us-central-1",
    ForcePathStyle = true
}
```

## Examples

### Reading

```csharp
var uri = StorageUri.Parse("s3://my-bucket/data.csv");

using var stream = await provider.OpenReadAsync(uri);
using var reader = new StreamReader(stream);
var content = await reader.ReadToEndAsync();
```

### Writing

```csharp
var uri = StorageUri.Parse("s3://my-bucket/output.csv");

using var stream = await provider.OpenWriteAsync(uri);
using var writer = new StreamWriter(stream);
await writer.WriteLineAsync("id,name,value");
```

### Listing

```csharp
var prefix = StorageUri.Parse("s3://my-bucket/data/");

await foreach (var item in provider.ListAsync(prefix, recursive: true))
{
    Console.WriteLine($"{item.Uri} — {item.Size} bytes");
}
```

### Metadata

```csharp
var metadata = await provider.GetMetadataAsync(uri);
if (metadata is not null)
    Console.WriteLine($"Size: {metadata.Size}, ContentType: {metadata.ContentType}");
```

## Error Handling

| S3 Error Code | .NET Exception | Description |
|---------------|----------------|-------------|
| `AccessDenied`, `InvalidAccessKeyId`, `SignatureDoesNotMatch` | `UnauthorizedAccessException` | Auth or permission failure |
| `InvalidKey`, `InvalidBucketName` | `ArgumentException` | Invalid bucket/key |
| `NoSuchBucket`, `NoSuchKey`, `NotFound` | `FileNotFoundException` | Bucket or object missing |
| Other S3 API errors | `IOException` | General failure |

## Limitations

- **No per-URI overrides** — credentials and endpoint are set globally via options (unlike the AWS S3 provider)
- **Flat storage** — S3-compatible services use prefix-based hierarchy
- **Provider-specific differences** — multipart upload, metadata, and custom header support varies by service

If you need multiple endpoints, register separate provider instances.

## Next Steps

- [AWS S3 Provider](aws-s3.md) — native AWS S3 with IAM credential chain
- [Storage Providers Overview](index.md) — choosing between providers
