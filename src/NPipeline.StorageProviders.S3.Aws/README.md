# NPipeline.StorageProviders.S3.Aws

AWS S3 storage provider for NPipeline. Implements `IStorageProvider` using the AWS SDK with the default credential chain, multipart uploads, per-URI region overrides, and client caching.

## Features

- **Default credential chain** - environment variables → shared credentials file → EC2 instance profile → ECS task role
- **Explicit credentials** - `BasicAWSCredentials` or `SessionAWSCredentials` via options or URI parameters
- **Per-URI region overrides** - set region per-request using the `?region=` URI parameter
- **Multipart uploads** - files above the configurable threshold use the S3 multipart API automatically
- **Client caching** - `IAmazonS3` clients are cached by region/endpoint/credentials to minimise overhead
- **Path-style addressing** - opt in with `ForcePathStyle = true` for Floci or older S3-compatible endpoints
- **Async streaming** - all reads, writes, and listings stream data without materialising the full object in memory

## Installation

```bash
dotnet add package NPipeline.StorageProviders.S3.Aws
```

**Dependencies:** [AWSSDK.S3](https://www.nuget.org/packages/AWSSDK.S3) 4.x, [AWSSDK.Core](https://www.nuget.org/packages/AWSSDK.Core) 4.x

## Quick Start

```csharp
using Amazon;
using NPipeline.StorageProviders.S3.Aws;

var options = new AwsS3StorageProviderOptions
{
    DefaultRegion = RegionEndpoint.APSoutheast2
};
var factory = new AwsS3ClientFactory(options);
var provider = new AwsS3StorageProvider(factory, options);

// Read
using var readStream = await provider.OpenReadAsync(
    StorageUri.Parse("s3://my-bucket/data/orders.csv"));

// Write
await using var writeStream = await provider.OpenWriteAsync(
    StorageUri.Parse("s3://my-bucket/output/results.csv"));
await writeStream.WriteAsync(bytes);
await writeStream.CommitAsync(); // uploads the object; disposing without a commit discards the data
```

## URI Format

```
s3://bucket-name/key/path?region=ap-southeast-2
```

| Component | Description |
|-----------|-------------|
| `bucket-name` | S3 bucket (URI host) |
| `key/path` | Object key (URI path) |
| `region` | Optional - overrides `DefaultRegion` for this request |

### URI Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `region` | AWS region name | `region=ap-southeast-2` |
| `accessKey` | AWS access key ID | `accessKey=AKIAIOSFODNN7EXAMPLE` |
| `secretKey` | AWS secret access key | `secretKey=wJalrXUtnFEMI/...` |
| `sessionToken` | STS session token (with `accessKey`+`secretKey`) | `sessionToken=AQoDY...` |
| `serviceUrl` | Custom S3 endpoint URL | `serviceUrl=http%3A%2F%2Flocalhost%3A4566` |
| `pathStyle` | Force path-style addressing | `pathStyle=true` |
| `contentType` | MIME type applied on write | `contentType=application/json` |

> **Security:** Avoid embedding credentials in URIs in production - URIs may appear in logs. Use the credential chain or `DefaultCredentials` in options instead.

## Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `DefaultRegion` | `RegionEndpoint?` | `null` (→ `USEast1`) | AWS region for S3 API calls |
| `DefaultCredentials` | `AWSCredentials?` | `null` | Explicit AWS credentials |
| `UseDefaultCredentialChain` | `bool` | `true` | Fall back to the standard AWS credential chain |
| `ServiceUrl` | `Uri?` | `null` | Custom S3 endpoint (Floci, MinIO via AWS provider) |
| `ForcePathStyle` | `bool` | `false` | Use path-style URLs instead of virtual-hosted-style |
| `MultipartUploadThresholdBytes` | `long` | `67108864` (64 MB) | Objects above this size use the S3 multipart upload API |

## Authentication

Credentials are resolved in priority order:

1. **Per-URI** - `accessKey` + `secretKey` (+ optional `sessionToken`) in the URI query string
2. **Options** - `DefaultCredentials` set on `AwsS3StorageProviderOptions`
3. **Default credential chain** - when `UseDefaultCredentialChain = true` (default)

```csharp
// Production - use the default credential chain (IAM role, instance profile, etc.)
var options = new AwsS3StorageProviderOptions
{
    DefaultRegion = RegionEndpoint.APSoutheast2
};

// Development - explicit credentials
var options = new AwsS3StorageProviderOptions
{
    DefaultRegion = RegionEndpoint.APSoutheast2,
    DefaultCredentials = new BasicAWSCredentials("AKIA...", "secret"),
    UseDefaultCredentialChain = false
};
```

## Dependency Injection

```csharp
using NPipeline.StorageProviders.S3.Aws;

// Default credential chain
services.AddAwsS3StorageProvider();

// Inline configuration
services.AddAwsS3StorageProvider(options =>
{
    options.DefaultRegion = RegionEndpoint.EUWest1;
});

// Pre-built options
services.AddAwsS3StorageProvider(new AwsS3StorageProviderOptions
{
    DefaultRegion = RegionEndpoint.USEast1,
    MultipartUploadThresholdBytes = 128 * 1024 * 1024 // 128 MB
});
```

Registers `AwsS3StorageProvider` as a singleton, along with `AwsS3ClientFactory` and `AwsS3StorageProviderOptions`, and adds it to the `IStorageProvider` collection that `AddStorageResolver()` reads. Calling the method twice registers one provider.

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
await using var stream = await provider.OpenWriteAsync(uri);
await using (var writer = new StreamWriter(stream, leaveOpen: true))
{
    await writer.WriteLineAsync("id,name,value");
    await writer.WriteLineAsync("1,Widget,42.00");
}

// The object appears only after CommitAsync. Disposing the stream without it uploads nothing.
await stream.CommitAsync();
```

The provider buffers the data to a local temporary file and uploads it in `CommitAsync` (as a multipart upload above the threshold). Upload errors surface from `CommitAsync`.

`AwsS3StorageProvider` declares `StorageCapabilities.ConditionalWrite`. With `new StorageWriteOptions { Overwrite = false }` the commit fails if the object exists. With `new StorageWriteOptions { IfMatch = etag }` (an ETag from `GetMetadataAsync`) it commits only if the current ETag matches. A refused condition throws `StoragePreconditionFailedException` from `CommitAsync`.

### Listing

```csharp
var prefix = StorageUri.Parse("s3://my-bucket/data/");

// Non-recursive (top-level objects and prefixes only)
await foreach (var item in provider.ListAsync(prefix))
    Console.WriteLine($"{item.Uri}  dir={item.IsDirectory}");

// Recursive (all objects under prefix)
await foreach (var item in provider.ListAsync(prefix, recursive: true))
    Console.WriteLine($"{item.Uri}  {item.Size} bytes");
```

### Existence Check & Metadata

```csharp
bool exists = await provider.ExistsAsync(uri);

var metadata = await provider.GetMetadataAsync(uri);
if (metadata is not null)
    Console.WriteLine($"Size: {metadata.Size}, ETag: {metadata.ETag}, Modified: {metadata.LastModified}");
```

### Deleting and moving

```csharp
// Idempotent: succeeds when the key is already gone
await provider.DeleteAsync(StorageUri.Parse("s3://my-bucket/data/old.csv"));

// Copy then delete (multipart copy above 5 GiB). Overwrites the destination; not atomic.
await provider.MoveAsync(
    StorageUri.Parse("s3://my-bucket/staging/orders.csv"),
    StorageUri.Parse("s3://my-bucket/data/orders.csv"));
```

The provider declares `Read | Write | List | Delete | Move`.

### Floci (Testing)

```csharp
services.AddAwsS3StorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://localhost:4566");
    options.ForcePathStyle = true;
    options.DefaultRegion = RegionEndpoint.USEast1;
});
```

## Error Handling

The HTTP status decides first; the error code only refines the result when the status is missing or generic. The `AmazonS3Exception` is always the inner exception.

| HTTP status / error code | .NET Exception | Cause |
|---------------|----------------|-------|
| 401, 403, `AccessDenied`, `InvalidAccessKeyId`, `SignatureDoesNotMatch` | `UnauthorizedAccessException` | Auth or permission failure |
| 400 with `InvalidBucketName` or `InvalidKey` | `ArgumentException` | Malformed bucket or key |
| 404, `NoSuchKey`, `NoSuchBucket`, `NotFound` | `FileNotFoundException` | Bucket or object does not exist |
| Other `AmazonS3Exception` | `IOException` | General S3 failure |

## IAM Permissions

| Operation | Required Permission |
|-----------|---------------------|
| `OpenReadAsync` | `s3:GetObject` |
| `OpenWriteAsync` | `s3:PutObject` |
| `ListAsync` | `s3:ListBucket` |
| `DeleteAsync` | `s3:DeleteObject` |
| `MoveAsync` | `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` |
| `ExistsAsync` | `s3:GetObject` |
| `GetMetadataAsync` | `s3:GetObject` |

## Requirements

- .NET 8.0, 9.0, or 10.0
- `AWSSDK.S3` 4.x (automatically included)
- `AWSSDK.Core` 4.x (automatically included)

## Related Packages

- **[NPipeline.StorageProviders.S3.Compatible](https://www.nuget.org/packages/NPipeline.StorageProviders.S3.Compatible)** - MinIO, Cloudflare R2, DigitalOcean Spaces, and other S3-compatible services
- **[NPipeline.StorageProviders.Azure](https://www.nuget.org/packages/NPipeline.StorageProviders.Azure)** - Azure Blob Storage provider
- **[NPipeline.StorageProviders](https://www.nuget.org/packages/NPipeline.StorageProviders)** - Base abstractions (`IStorageProvider`, `StorageUri`)

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.
