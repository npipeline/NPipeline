# S3-Compatible Storage Provider Sample

This sample demonstrates how to use the S3-compatible storage provider in NPipeline to read, write, list,
and manage files on non-AWS S3-compatible services such as MinIO, DigitalOcean Spaces, Cloudflare R2, and
LocalStack.

## Overview

The `Sample_S3CompatibleStorageProvider` application showcases the following features:

- **Reading files** using `OpenReadAsync`
- **Writing files** using `OpenWriteAsync`
- **Listing objects** with recursive and non-recursive options
- **Checking file existence** with `ExistsAsync`
- **Retrieving metadata** with `GetMetadataAsync`
- **Dependency injection** configuration
- **Provider-specific configurations** for MinIO, LocalStack, DigitalOcean Spaces, and Cloudflare R2

## Supported Services

The S3-compatible provider works with any storage service that implements the S3 API:

| Service             | Service URL pattern                             | Notes                               |
|---------------------|-------------------------------------------------|-------------------------------------|
| MinIO               | `http://localhost:9000`                         | `ForcePathStyle = true`             |
| LocalStack          | `http://localhost:4566`                         | Any credentials accepted            |
| DigitalOcean Spaces | `https://<region>.digitaloceanspaces.com`       | Virtual-hosted-style addressing     |
| Cloudflare R2       | `https://<account-id>.r2.cloudflarestorage.com` | `SigningRegion = "auto"` required   |
| Backblaze B2        | `https://s3.<region>.backblazeb2.com`           | Uses B2 application key credentials |
| Wasabi              | `https://s3.<region>.wasabisys.com`             | Standard S3 credentials             |

## Prerequisites

### S3-Compatible Service

You need a running S3-compatible service. The easiest option for local development is **MinIO**:

#### Start MinIO with Docker

```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

Then open the MinIO console at `http://localhost:9001` and create a bucket.

#### Start LocalStack with Docker

```bash
docker run -p 4566:4566 localstack/localstack
```

Create a bucket using the AWS CLI pointed at LocalStack:

```bash
aws --endpoint-url=http://localhost:4566 s3 mb s3://my-bucket \
  --region us-east-1
```

## Setup

### 1. Configure the Sample

Edit [`Program.cs`](Program.cs) and update the configuration at the top of the file:

```csharp
private static readonly Uri ServiceUrl = new("http://localhost:9000");
private const string AccessKey     = "minioadmin";
private const string SecretKey     = "minioadmin";
private const string BucketName    = "my-bucket";
private const string SigningRegion = "us-east-1";
```

### 2. Create Test Data (Optional)

For the read examples to work, upload a sample CSV file to your bucket.

With MinIO CLI (`mc`):

```bash
mc alias set local http://localhost:9000 minioadmin minioadmin
mc cp sample.csv local/my-bucket/data/sample.csv
```

With the AWS CLI pointed at LocalStack:

```bash
aws --endpoint-url=http://localhost:4566 s3 cp sample.csv s3://my-bucket/data/sample.csv
```

## Running the Sample

```bash
cd samples/Sample_S3CompatibleStorageProvider
dotnet run
```

## Examples

### Example 1: Basic Read

Reads a file from the S3-compatible service:

```csharp
var options = new S3CompatibleStorageProviderOptions
{
    ServiceUrl    = new Uri("http://localhost:9000"),
    AccessKey     = "minioadmin",
    SecretKey     = "minioadmin",
    SigningRegion  = "us-east-1",
};

var factory  = new S3CompatibleClientFactory(options);
var provider = new S3CompatibleStorageProvider(factory, options);

var fileUri = StorageUri.Parse($"s3://{BucketName}/data/sample.csv");
await using var stream = await provider.OpenReadAsync(fileUri);
using var reader = new StreamReader(stream);
var content = await reader.ReadToEndAsync();
```

### Example 2: Basic Write

Writes data to the S3-compatible service:

```csharp
var fileUri = StorageUri.Parse($"s3://{BucketName}/data/output/result.txt");
await using var stream = await provider.OpenWriteAsync(fileUri);
await using var writer = new StreamWriter(stream);
await writer.WriteAsync("Hello from NPipeline!");
```

### Example 3: List Objects

Lists objects at a given prefix:

```csharp
var prefix = StorageUri.Parse($"s3://{BucketName}/data/");
await foreach (var item in provider.ListAsync(prefix, recursive: true))
{
    Console.WriteLine($"{item.Uri.Path} | {item.Size} bytes");
}
```

### Example 4: Check File Existence

```csharp
var fileUri = StorageUri.Parse($"s3://{BucketName}/data/sample.csv");
var exists = await provider.ExistsAsync(fileUri);
```

### Example 5: Get File Metadata

```csharp
var metadata = await provider.GetMetadataAsync(fileUri);
Console.WriteLine($"Size: {metadata?.Size}");
Console.WriteLine($"ETag: {metadata?.ETag}");
Console.WriteLine($"ContentType: {metadata?.ContentType}");
```

### Example 6: Using Dependency Injection

```csharp
var services = new ServiceCollection();

var options = new S3CompatibleStorageProviderOptions
{
    ServiceUrl    = new Uri("http://localhost:9000"),
    AccessKey     = "minioadmin",
    SecretKey     = "minioadmin",
    SigningRegion  = "us-east-1",
};

services.AddS3CompatibleStorageProvider(options);

var serviceProvider = services.BuildServiceProvider();
var provider = serviceProvider.GetRequiredService<S3CompatibleStorageProvider>();
```

### Example 7: Provider-Specific Configurations

Shows the `S3CompatibleStorageProviderOptions` for each supported service:

```csharp
// MinIO
new S3CompatibleStorageProviderOptions
{
    ServiceUrl    = new Uri("http://localhost:9000"),
    AccessKey     = "minioadmin",
    SecretKey     = "minioadmin",
    SigningRegion  = "us-east-1",
    ForcePathStyle = true,
};

// LocalStack
new S3CompatibleStorageProviderOptions
{
    ServiceUrl    = new Uri("http://localhost:4566"),
    AccessKey     = "test",
    SecretKey     = "test",
    SigningRegion  = "us-east-1",
    ForcePathStyle = true,
};

// DigitalOcean Spaces
new S3CompatibleStorageProviderOptions
{
    ServiceUrl    = new Uri("https://nyc3.digitaloceanspaces.com"),
    AccessKey     = "<spaces-access-key>",
    SecretKey     = "<spaces-secret-key>",
    SigningRegion  = "us-east-1",
    ForcePathStyle = false,
};

// Cloudflare R2
new S3CompatibleStorageProviderOptions
{
    ServiceUrl    = new Uri("https://<account-id>.r2.cloudflarestorage.com"),
    AccessKey     = "<r2-access-key-id>",
    SecretKey     = "<r2-secret-access-key>",
    SigningRegion  = "auto",    // ← required for Cloudflare R2
    ForcePathStyle = false,
};
```

## Configuration Reference

### S3CompatibleStorageProviderOptions

| Property                        | Type     | Required | Default     | Description                                                        |
|---------------------------------|----------|----------|-------------|--------------------------------------------------------------------|
| `ServiceUrl`                    | `Uri`    | ✓        | —           | Base URL of the S3-compatible endpoint                             |
| `AccessKey`                     | `string` | ✓        | —           | Static access key (equivalent to AWS access key ID)                |
| `SecretKey`                     | `string` | ✓        | —           | Static secret key (equivalent to AWS secret access key)            |
| `SigningRegion`                 | `string` | ✗        | `us-east-1` | Region used for request signing. Use `"auto"` for Cloudflare R2    |
| `ForcePathStyle`                | `bool`   | ✗        | `true`      | Force path-style addressing (required by most compatible services) |
| `MultipartUploadThresholdBytes` | `long`   | ✗        | `67108864`  | File size threshold above which multipart upload is used (64 MB)   |

### URI Format

```
s3://bucket-name/path/to/object
```

You can also pass `contentType` as a query parameter when writing:

```
s3://my-bucket/data/file.json?contentType=application/json
```

## Error Handling

Exceptions from the S3 API are translated into standard .NET types:

| S3 Error Code                                                 | .NET Exception                |
|---------------------------------------------------------------|-------------------------------|
| `AccessDenied`, `InvalidAccessKeyId`, `SignatureDoesNotMatch` | `UnauthorizedAccessException` |
| `InvalidBucketName`, `InvalidKey`                             | `ArgumentException`           |
| `NoSuchBucket`, `NotFound`                                    | `FileNotFoundException`       |
| Other errors                                                  | `IOException`                 |

```csharp
try
{
    await using var stream = await provider.OpenReadAsync(fileUri);
}
catch (UnauthorizedAccessException ex)
{
    Console.WriteLine($"Access denied: {ex.Message}");
}
catch (FileNotFoundException ex)
{
    Console.WriteLine($"Not found: {ex.Message}");
}
catch (IOException ex)
{
    Console.WriteLine($"IO error: {ex.Message}");
}
```

## Relationship to AWS S3 Sample

| Feature              | `Sample_S3StorageProvider`        | `Sample_S3CompatibleStorageProvider`      |
|----------------------|-----------------------------------|-------------------------------------------|
| Credential model     | AWS IAM / credential chain        | Static access key + secret key            |
| Endpoint             | AWS regional endpoints            | Any custom `ServiceUrl`                   |
| Region configuration | `RegionEndpoint` (AWS SDK type)   | `SigningRegion` string                    |
| DI method            | `AddAwsS3StorageProvider(action)` | `AddS3CompatibleStorageProvider(options)` |
| Typical targets      | AWS S3                            | MinIO, DO Spaces, R2, LocalStack          |

Both providers share the same core (`S3CoreStorageProvider`) and therefore have identical read/write/list/metadata semantics.

## Additional Resources

- [MinIO Documentation](https://min.io/docs/)
- [LocalStack Documentation](https://docs.localstack.cloud/)
- [DigitalOcean Spaces Documentation](https://docs.digitalocean.com/products/spaces/)
- [Cloudflare R2 Documentation](https://developers.cloudflare.com/r2/)
- [NPipeline AWS S3 Sample](../Sample_S3StorageProvider/README.md)
