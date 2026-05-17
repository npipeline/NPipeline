---
title: "AWS S3 Storage Provider"
description: "Read and write files in Amazon S3 with default credential chain, multipart uploads, and region-based endpoints."
order: 2
---

# AWS S3 Storage Provider

> **Prerequisites:** [Storage Providers Overview](index.md)

The `NPipeline.StorageProviders.S3.Aws` package implements `IStorageProvider` for [Amazon S3](https://aws.amazon.com/s3/). Supports the default AWS credential chain, explicit credentials, multipart uploads, virtual-hosted and path-style addressing, and per-URI region overrides.

## Installation

```bash
dotnet add package NPipeline.StorageProviders.S3.Aws
```

**Dependencies:** [AWSSDK.S3](https://www.nuget.org/packages/AWSSDK.S3) 4.x, [AWSSDK.Core](https://www.nuget.org/packages/AWSSDK.Core) 4.x

## Quick Start

```csharp
var options = new AwsS3StorageProviderOptions
{
    DefaultRegion = RegionEndpoint.USEast1
};
var factory = new AwsS3ClientFactory(options);
var provider = new AwsS3StorageProvider(factory, options);

var stream = await provider.OpenReadAsync(
    StorageUri.Parse("s3://my-bucket/data/orders.csv"));
```

## URI Format

```
s3://bucket-name/key/path?region=ap-southeast-2
```

| Component | Description |
|-----------|-------------|
| `bucket-name` | S3 bucket (URI host) |
| `key/path` | Object key (URI path) |
| `region` | Optional - override `DefaultRegion` for this request |

## Authentication

Credentials are resolved in this order:

1. **Explicit credentials** - set `DefaultCredentials` to any `AWSCredentials` instance
2. **Default credential chain** (default) - environment variables → shared credentials file → EC2 instance profile → ECS task role

```csharp
// Explicit credentials (development only)
var options = new AwsS3StorageProviderOptions
{
    DefaultCredentials = new BasicAWSCredentials("AKIA...", "secret"),
    DefaultRegion = RegionEndpoint.USEast1
};

// Default chain (recommended for production)
var options = new AwsS3StorageProviderOptions
{
    UseDefaultCredentialChain = true,    // default
    DefaultRegion = RegionEndpoint.USEast1
};
```

## Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `DefaultRegion` | `RegionEndpoint?` | `null` | AWS region for S3 API calls |
| `DefaultCredentials` | `AWSCredentials?` | `null` | Explicit AWS credentials |
| `UseDefaultCredentialChain` | `bool` | `true` | Fall back to the default credential chain |
| `ServiceUrl` | `Uri?` | `null` | Custom S3 endpoint (LocalStack, MinIO) |
| `ForcePathStyle` | `bool` | `false` | Use path-style URLs instead of virtual-hosted |
| `MultipartUploadThresholdBytes` | `long` | `64 MB` | Switch to multipart upload above this size |

## Dependency Injection

```csharp
// Default options
services.AddAwsS3StorageProvider();

// Configure inline
services.AddAwsS3StorageProvider(options =>
{
    options.DefaultRegion = RegionEndpoint.EUWest1;
});

// Pre-built options
services.AddAwsS3StorageProvider(new AwsS3StorageProviderOptions
{
    DefaultRegion = RegionEndpoint.USEast1
});
```

Registers: `IStorageProvider`, `IStorageProviderMetadataProvider`

## Features

- **Multipart uploads** - files above `MultipartUploadThresholdBytes` are uploaded using the S3 multipart API
- **Client caching** - S3 clients are cached and reused per region/endpoint
- **Virtual-hosted addressing** - default; set `ForcePathStyle = true` for LocalStack or older S3-compatible services
- **Metadata** - implements `IStorageProviderMetadataProvider` for `Size`, `LastModified`, `ContentType`, `ETag`

## URI Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `region` | AWS region name | `region=ap-southeast-2` |
| `accessKey` | AWS access key (dev only) | `accessKey=AKIAIOSFODNN7EXAMPLE` |
| `secretKey` | AWS secret key (dev only) | `secretKey=wJalrXUtnFEMI/...` |
| `serviceUrl` | Custom S3 endpoint | `serviceUrl=http://localhost:9000` |
| `pathStyle` | Force path-style (`true`/`false`) | `pathStyle=true` |
| `contentType` | Content type on write | `contentType=application/json` |

```csharp
// With region override
var uri = StorageUri.Parse("s3://my-bucket/data/input.csv?region=us-west-2");

// With custom endpoint (LocalStack)
var uri = StorageUri.Parse("s3://local-bucket/data/file.csv?serviceUrl=http://localhost:4566&pathStyle=true");
```

> **Security:** Avoid credentials in URIs for production - URIs may be logged. Use the credential chain or DI.

## Configuration Examples

### LocalStack (Testing)

```csharp
services.AddAwsS3StorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://localhost:4566");
    options.ForcePathStyle = true;
    options.DefaultRegion = RegionEndpoint.USEast1;
});
```

### MinIO (via AWS Provider)

> **Recommended:** Use the [S3-Compatible Provider](s3-compatible.md) instead for non-AWS endpoints.

```csharp
services.AddAwsS3StorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://localhost:9000");
    options.ForcePathStyle = true;
    options.DefaultRegion = RegionEndpoint.USEast1;
    options.DefaultCredentials = new BasicAWSCredentials("minioadmin", "minioadmin");
    options.UseDefaultCredentialChain = false;
});
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
await writer.WriteLineAsync("1,Item A,100");
```

### Listing

```csharp
var prefix = StorageUri.Parse("s3://my-bucket/data/");

await foreach (var item in provider.ListAsync(prefix, recursive: true))
{
    Console.WriteLine($"{item.Uri} - {item.Size} bytes");
}
```

### Metadata

```csharp
var metadata = await provider.GetMetadataAsync(uri);
if (metadata is not null)
{
    Console.WriteLine($"Size: {metadata.Size}, ContentType: {metadata.ContentType}");
    Console.WriteLine($"Last Modified: {metadata.LastModified}, ETag: {metadata.ETag}");
}
```

## Error Handling

| S3 Error Code | .NET Exception | Description |
|---------------|----------------|-------------|
| `AccessDenied`, `InvalidAccessKeyId`, `SignatureDoesNotMatch` | `UnauthorizedAccessException` | Auth or permission failure |
| `InvalidBucketName`, `InvalidKey` | `ArgumentException` | Invalid bucket/key |
| `NoSuchBucket`, `NotFound` | `FileNotFoundException` | Bucket or object missing |
| Other `AmazonS3Exception` | `IOException` | General S3 failure |

## IAM Permissions

| Operation | Required Permission |
|-----------|---------------------|
| `OpenReadAsync` | `s3:GetObject` |
| `OpenWriteAsync` | `s3:PutObject` |
| `ListAsync` | `s3:ListBucket` |
| `ExistsAsync` | `s3:GetObject` |
| `GetMetadataAsync` | `s3:GetObject` |

### Minimal Read/Write Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::my-bucket", "arn:aws:s3:::my-bucket/*"]
    }
  ]
}
```

## Limitations

- **Flat storage** - S3 has no directories; prefixes simulate hierarchy
- **Multipart uploads** - files above threshold use multipart API; ensure sufficient memory and bandwidth
- **Eventually consistent** - S3 standard now provides strong read-after-write consistency for PUT, but LIST operations may lag

## Next Steps

- [S3-Compatible Provider](s3-compatible.md) - for MinIO, DigitalOcean Spaces, Cloudflare R2
- [Azure Blob Provider](azure-blob.md) - Azure alternative
- [Storage Providers Overview](index.md) - choosing between providers
