---
title: S3-Compatible Storage Provider
description: Read from and write to S3-compatible storage services (MinIO, DigitalOcean Spaces, Cloudflare R2, etc.) using the S3-compatible storage provider.
sidebar_position: 2
---

## S3-Compatible Storage Provider

The S3-compatible storage provider enables NPipeline applications to read from and write to S3-compatible storage services using a unified storage abstraction. This provider implements the `IStorageProvider` interface and supports the `s3://` URI scheme.

### Overview

The S3-compatible storage provider provides seamless integration with any object storage service that implements the S3 API, without requiring AWS credentials or IAM configuration. It offers:

- **Stream-based I/O** for efficient handling of large files
- **Async-first API** for scalable, non-blocking operations
- **Static credential authentication** (access key + secret key)
- **Customizable endpoints** for any S3-compatible service
- **Multipart upload** for large files (configurable threshold, default 64 MB)
- **Comprehensive error handling** with proper exception translation
- **Metadata support** for retrieving object metadata
- **Listing operations** with recursive and non-recursive modes

### When to Use This Provider

Use the S3-compatible storage provider when your application needs to:

- Store and retrieve data from MinIO, DigitalOcean Spaces, or other S3-compatible services
- Avoid AWS credentials and IAM complexity for self-hosted or alternative cloud storage
- Use a single provider across multiple S3-compatible endpoints
- Integrate object storage into NPipeline data streamlines without vendor lock-in
- Handle large files through streaming and multipart uploads

### Supported Platforms

The S3-compatible provider works with any service implementing the S3 API, including:

- **MinIO** - Self-hosted, high-performance object storage
- **DigitalOcean Spaces** - Simple object storage with CDN integration
- **Cloudflare R2** - S3-compatible object storage without egress fees
- **Wasabi** - Hot cloud storage with S3 compatibility
- **IBM Cloud Object Storage** - Enterprise-grade S3-compatible storage
- **Oracle Cloud Object Storage** - S3-compatible API for OCI
- **LocalStack** - Local AWS cloud stack for testing
- Any other S3-compatible service

## Dependencies

The S3-compatible storage provider depends on the following packages:

- `AWSSDK.S3` - AWS SDK for S3 operations (provides S3-compatible API abstraction)
- `AWSSDK.Core` - Core AWS SDK functionality
- `NPipeline.StorageProviders.S3` - Shared S3 core functionality (IClientFactory, base classes)
- `NPipeline.StorageProviders` - Core storage abstractions (IStorageProvider, StorageUri, StorageItem, StorageMetadata, StorageProviderMetadata, StorageResolverOptions)

### Key Storage Types

> **Note:** The shared storage types (IStorageProvider, StorageUri, StorageItem, StorageMetadata, StorageProviderMetadata, StorageResolverOptions) are common across all NPipeline storage providers. Refer to the [Storage Provider Interface](./storage-provider.md) documentation for details on these types.

S3-compatible-specific configuration types:

- **`S3CompatibleStorageProviderOptions`** - Configuration options for the S3-compatible storage provider
  - Location: `NPipeline.StorageProviders.S3.Compatible.S3CompatibleStorageProviderOptions`
  - Essential settings: service URL, access key, secret key, signing region, path style

## Installation

### Prerequisites

- .NET 6.0 or later
- Access to an S3-compatible storage service with credentials
- Appropriate permissions for your intended operations (read/write/list)

### Package Installation

Add the project reference to your solution:

```bash
dotnet add src/NPipeline.StorageProviders.S3.Compatible/NPipeline.StorageProviders.S3.Compatible.csproj
```

Or add it to your `.csproj` file:

```xml
<ItemGroup>
  <ProjectReference Include="..\NPipeline.StorageProviders.S3.Compatible\NPipeline.StorageProviders.S3.Compatible.csproj" />
</ItemGroup>
```

### Required Dependencies

The S3-compatible storage provider depends on:

- `AWSSDK.S3` and `AWSSDK.Core` - AWS SDK for S3 operations
- `NPipeline.StorageProviders.S3` - Shared S3 core
- `NPipeline.StorageProviders` - Core storage abstractions

These dependencies are automatically resolved when adding the project reference.

## Quick Start

### Basic Usage with Connectors

The S3-compatible storage provider works seamlessly with all NPipeline connectors. Here's a quick example using the CSV connector with MinIO:

```csharp
using NPipeline;
using NPipeline.Connectors;
using NPipeline.Connectors.Csv;
using NPipeline.StorageProviders.S3.Compatible;
using Microsoft.Extensions.DependencyInjection;

// Create a resolver with S3-compatible support
var services = new ServiceCollection();

services.AddS3CompatibleStorageProvider(new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("http://localhost:9000"),  // MinIO endpoint
    AccessKey = "minioadmin",
    SecretKey = "minioadmin",
    ForcePathStyle = true,  // Required for MinIO
});

var resolver = StorageProviderFactory.CreateResolver(
    new StorageResolverOptions
    {
        IncludeFileSystem = true,
        AdditionalProviders = new[] { services.BuildServiceProvider().GetRequiredService<S3CompatibleStorageProvider>() }
    }
);

public sealed record User(int Id, string Name, string Email);

public sealed class S3CompatibleCsvPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Read CSV from MinIO
        var sourceNode = new CsvSourceNode<User>(
            StorageUri.Parse("s3://my-bucket/users.csv"),
            row => new User(
                row.Get<int>("Id") ?? 0,
                row.Get<string>("Name") ?? string.Empty,
                row.Get<string>("Email") ?? string.Empty),
            resolver: resolver);
        var source = builder.AddSource(sourceNode, "minio_csv_source");
        
        // ... add transforms ...
        
        // Write CSV to MinIO
        var sinkNode = new CsvSinkNode<UserSummary>(
            StorageUri.Parse("s3://my-bucket/summaries.csv"),
            resolver: resolver);
        var sink = builder.AddSink(sinkNode, "minio_csv_sink");
        
        builder.Connect(source, sink);
    }
}
```

## Configuration

### Using Dependency Injection (Recommended)

The recommended way to configure the S3-compatible storage provider is through dependency injection:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.S3.Compatible;

var services = new ServiceCollection();

services.AddS3CompatibleStorageProvider(new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("https://nyc3.digitaloceanspaces.com"),
    AccessKey = "your-access-key",
    SecretKey = "your-secret-key",
    SigningRegion = "us-east-1",  // or your preferred region
    ForcePathStyle = false,  // DigitalOcean Spaces supports virtual-hosted style
    MultipartUploadThresholdBytes = 64 * 1024 * 1024,  // 64 MB
});

var serviceProvider = services.BuildServiceProvider();
var provider = serviceProvider.GetRequiredService<S3CompatibleStorageProvider>();
```

### S3CompatibleStorageProviderOptions

The `S3CompatibleStorageProviderOptions` class provides configuration options for the S3-compatible storage provider:

| Property | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `ServiceUrl` | `Uri` | — | ✅ | Base URL of the S3-compatible endpoint (e.g., `http://localhost:9000`) |
| `AccessKey` | `string` | — | ✅ | Static access key ID for authentication |
| `SecretKey` | `string` | — | ✅ | Static secret key for authentication |
| `SigningRegion` | `string` | `"us-east-1"` | | AWS region used for request signing (most providers accept default) |
| `ForcePathStyle` | `bool` | `true` | | Whether to use path-style addressing (e.g., `endpoint/bucket/key` vs `bucket.endpoint/key`). Default `true` is correct for most S3-compatible services. |
| `MultipartUploadThresholdBytes` | `long` | `64 * 1024 * 1024` (64 MB) | | Threshold in bytes for using multipart upload when writing files |

### Configuration Examples

#### MinIO (Self-Hosted)

```csharp
services.AddS3CompatibleStorageProvider(new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("http://localhost:9000"),
    AccessKey = "minioadmin",
    SecretKey = "minioadmin",
    ForcePathStyle = true,  // MinIO requires path-style addressing
});
```

#### DigitalOcean Spaces

```csharp
services.AddS3CompatibleStorageProvider(new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("https://nyc3.digitaloceanspaces.com"),  // Choose your region
    AccessKey = "your-access-key",
    SecretKey = "your-secret-key",
    SigningRegion = "nyc3",
    ForcePathStyle = false,  // Spaces supports virtual-hosted style
});
```

#### Cloudflare R2

```csharp
services.AddS3CompatibleStorageProvider(new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("https://your-account-id.r2.cloudflarestorage.com"),
    AccessKey = "your-access-key-id",
    SecretKey = "your-secret-access-key",
    SigningRegion = "auto",  // R2 uses "auto" as the region
    ForcePathStyle = true,
});
```

#### LocalStack (Testing)

```csharp
services.AddS3CompatibleStorageProvider(new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("http://localhost:4566"),
    AccessKey = "test",  // LocalStack accepts any credentials
    SecretKey = "test",
    ForcePathStyle = true,
});
```

#### Wasabi

```csharp
services.AddS3CompatibleStorageProvider(new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("https://s3.us-central-1.wasabisys.com"),  // Choose your region
    AccessKey = "your-access-key",
    SecretKey = "your-secret-key",
    SigningRegion = "us-central-1",
    ForcePathStyle = true,
});
```

## URI Format

The S3-compatible storage provider uses URIs with the `s3://` scheme to identify objects.

### Basic Format

```
s3://bucket-name/path/to/file.csv
```

### Complete Parameter Table

The S3-compatible provider uses a fixed endpoint and supports:

| Parameter | Description | Notes |
|-----------|-------------|-------|
| (bucket/path) | Bucket name and object key | Derived from URI authority and path |

The service URL, credentials, and signing region are configured via `S3CompatibleStorageProviderOptions` and are **not** overridable per-URI.

### URI Examples

```csharp
// Basic object
var uri1 = StorageUri.Parse("s3://my-bucket/data/input.csv");

// Nested paths
var uri2 = StorageUri.Parse("s3://my-bucket/raw/2024/data.json");

// Directory-like listing
var uri3 = StorageUri.Parse("s3://my-bucket/exports/");
```

## Authentication

The S3-compatible storage provider uses **static credentials** (access key + secret key) for authentication. This is the most common authentication method for S3-compatible services that don't support IAM or credential chains.

### Credential Requirements

You must provide:

1. **Access Key** - A unique identifier for your credentials (equivalent to AWS Access Key ID)
2. **Secret Key** - A secret used to sign requests (equivalent to AWS Secret Access Key)

These credentials are typically generated in your storage provider's console or management interface.

### Obtaining Credentials

#### MinIO

```bash
# MinIO default credentials (for development only)
Access Key: minioadmin
Secret Key: minioadmin

# Or create new credentials in the MinIO console
```

#### DigitalOcean Spaces

1. Navigate to **API** > **Spaces Keys** in DigitalOcean Control Panel
2. Click **Generate New Key** and save your Access Key and Secret Key
3. Associate the key with your Spaces bucket

#### Cloudflare R2

1. Navigate to **Settings** > **API Tokens** in Cloudflare Dashboard
2. Create an **API token** or use **R2 API Tokens**
3. Retrieve your Access Key ID and Secret Access Key

#### Wasabi

1. Navigate to **Account** > **API Credentials**
2. Copy your Access Key and Secret Key
3. Note your region-specific endpoint URL

#### LocalStack

LocalStack accepts any credentials for development/testing:

```csharp
options.AccessKey = "test";
options.SecretKey = "test";
```

⚠️ **Security Warning:** Never hardcode credentials in source code. Use environment variables or secure configuration:

```csharp
services.AddS3CompatibleStorageProvider(new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri(Environment.GetEnvironmentVariable("S3_SERVICE_URL")!),
    AccessKey = Environment.GetEnvironmentVariable("S3_ACCESS_KEY")!,
    SecretKey = Environment.GetEnvironmentVariable("S3_SECRET_KEY")!,
});
```

## S3-Compatible Endpoints by Provider

### MinIO

MinIO is an open-source, self-hosted S3-compatible object storage system.

**Key characteristics:**

- Path-style addressing required
- Supports multipart upload
- Supports metadata and custom headers
- Full S3 compatibility for core operations

**Quick start with Docker:**

```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

**Configuration:**

```csharp
services.AddS3CompatibleStorageProvider(new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("http://localhost:9000"),
    AccessKey = "minioadmin",
    SecretKey = "minioadmin",
    ForcePathStyle = true,
});
```

### DigitalOcean Spaces

DigitalOcean Spaces provides simple, scalable object storage with CDN integration.

**Key characteristics:**

- Virtual-hosted style addressing supported
- Regional endpoints (NYC3, SFO3, SGP1, etc.)
- Includes CDN edge caching
- Separate access keys per Spaces bucket

**Configuration:**

```csharp
services.AddS3CompatibleStorageProvider(new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("https://nyc3.digitaloceanspaces.com"),
    AccessKey = "your-access-key",
    SecretKey = "your-secret-key",
    SigningRegion = "nyc3",
    ForcePathStyle = false,
});
```

### Cloudflare R2

Cloudflare R2 offers S3-compatible storage without the egress fees of AWS S3.

**Key characteristics:**

- Path-style addressing required
- Uses "auto" as signing region (multi-region by default)
- Requires account ID in endpoint URL
- No egress charges for worldwide access

**Configuration:**

```csharp
services.AddS3CompatibleStorageProvider(new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("https://YOUR-ACCOUNT-ID.r2.cloudflarestorage.com"),
    AccessKey = "your-access-key-id",
    SecretKey = "your-secret-access-key",
    SigningRegion = "auto",
    ForcePathStyle = true,
});
```

### Wasabi

Wasabi provides hot cloud storage with S3 compatibility and no API request charges.

**Key characteristics:**

- Regional endpoints (us-central-1, us-east-1, us-west-1, etc.)
- Path-style addressing required
- No API request charges (flat-rate pricing)
- Full multipart upload support

**Configuration:**

```csharp
services.AddS3CompatibleStorageProvider(new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("https://s3.us-central-1.wasabisys.com"),  // Or other regions
    AccessKey = "your-access-key",
    SecretKey = "your-secret-key",
    SigningRegion = "us-central-1",
    ForcePathStyle = true,
});
```

## Examples

### Reading from S3-Compatible Storage

```csharp
using NPipeline.StorageProviders.S3.Compatible;

var provider = serviceProvider.GetRequiredService<S3CompatibleStorageProvider>();
var uri = StorageUri.Parse("s3://my-bucket/data.csv");

using var stream = await provider.OpenReadAsync(uri);
using var reader = new StreamReader(stream);
var content = await reader.ReadToEndAsync();
Console.WriteLine(content);
```

### Writing to S3-Compatible Storage

```csharp
var uri = StorageUri.Parse("s3://my-bucket/output.csv");

using var stream = await provider.OpenWriteAsync(uri);
using var writer = new StreamWriter(stream);
await writer.WriteLineAsync("id,name,value");
await writer.WriteLineAsync("1,Item A,100");
await writer.WriteLineAsync("2,Item B,200");
// Stream is automatically flushed on dispose
```

### Listing Files

```csharp
var uri = StorageUri.Parse("s3://my-bucket/data/");

// List all files recursively
Console.WriteLine("All files:");
await foreach (var item in provider.ListAsync(uri, recursive: true))
{
    Console.WriteLine($"  {item.Uri} - {item.Size} bytes - Modified: {item.LastModified}");
}

// List only immediate children (non-recursive)
Console.WriteLine("\nImmediate items:");
await foreach (var item in provider.ListAsync(uri, recursive: false))
{
    var type = item.IsDirectory ? "[DIR]" : "[FILE]";
    Console.WriteLine($"  {type} {item.Uri} - {item.Size} bytes");
}
```

### Checking File Existence

```csharp
var uri = StorageUri.Parse("s3://my-bucket/data.csv");

var exists = await provider.ExistsAsync(uri);
if (exists)
{
    Console.WriteLine("File exists!");
}
else
{
    Console.WriteLine("File not found.");
}
```

### Getting Metadata

```csharp
var uri = StorageUri.Parse("s3://my-bucket/data.csv");

var metadata = await provider.GetMetadataAsync(uri);
if (metadata != null)
{
    Console.WriteLine($"Size: {metadata.Size} bytes");
    Console.WriteLine($"Content Type: {metadata.ContentType}");
    Console.WriteLine($"Last Modified: {metadata.LastModified}");
    Console.WriteLine($"ETag: {metadata.ETag}");
    
    foreach (var (key, value) in metadata.CustomMetadata)
    {
        Console.WriteLine($"  Custom: {key} = {value}");
    }
}
```

## Error Handling

The S3-compatible storage provider translates S3 API exceptions into standard .NET exceptions for consistent error handling.

### Exception Mapping

| S3 Error Code | .NET Exception | Description |
|---------------|----------------|-------------|
| `AccessDenied`, `InvalidAccessKeyId`, `SignatureDoesNotMatch` | `UnauthorizedAccessException` | Authentication or permission failure |
| `InvalidKey`, `InvalidBucketName` | `ArgumentException` | Invalid bucket name or object key |
| `NoSuchBucket`, `NoSuchKey`, `NotFound` | `FileNotFoundException` | Bucket or object not found |
| Other S3 API errors | `IOException` | General S3-compatible service access failure |

### Error Handling Example

```csharp
try
{
    using var stream = await provider.OpenReadAsync(uri);
    using var reader = new StreamReader(stream);
    var content = await reader.ReadToEndAsync();
}
catch (FileNotFoundException ex)
{
    Console.WriteLine($"Object not found: {ex.Message}");
}
catch (UnauthorizedAccessException ex)
{
    Console.WriteLine($"Access denied: {ex.Message}");
    Console.WriteLine("Check your Access Key, Secret Key, and bucket permissions.");
}
catch (ArgumentException ex)
{
    Console.WriteLine($"Invalid URI: {ex.Message}");
}
catch (IOException ex)
{
    Console.WriteLine($"Storage access error: {ex.Message}");
}
```

## Permissions and Access Control

Permissions for S3-compatible storage vary by provider. Most support bucket policies or access control lists (ACLs).

### Common Permission Requirements

| Operation | Required Permission |
|-----------|---------------------|
| Read (OpenReadAsync) | Read/GetObject on bucket and key |
| Write (OpenWriteAsync) | Write/PutObject on bucket and key |
| List (ListAsync) | List/ListBucket on bucket |
| Metadata (GetMetadataAsync) | Read/GetObject on bucket and key |
| Existence (ExistsAsync) | Read/GetObject on bucket and key |

### MinIO Access Control

MinIO supports IAM-style policies. Example policy for full bucket access:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:*"
      ],
      "Resource": [
        "arn:minio:s3:::my-bucket",
        "arn:minio:s3:::my-bucket/*"
      ]
    }
  ]
}
```

### DigitalOcean Spaces Access Control

DigitalOcean Spaces uses bucket-level access control. Credentials are per-space and carry the permissions assigned to that space.

### Cloudflare R2 Access Control

R2 API tokens can be restricted to specific buckets and operations:

1. Create an API token with scoped permissions
2. **Object R2 Storage**: Select scope as `All buckets` or specific bucket
3. **Permission levels**: `Read`, `Write`, `Admin` (Read + Write)

### Wasabi Access Control

Wasabi uses IAM-style bucket policies and user permissions similar to AWS.

## Limitations

The S3-compatible storage provider has the following limitations:

### Flat Storage Model

- Most S3-compatible services (like AWS S3) use a flat object storage model
- Directory-like paths are simulated through key prefixes
- The provider treats keys ending with `/` as directories for listing purposes

### Provider-Specific Differences

Different S3-compatible services may have:

- Varying support for multipart upload configurations
- Different metadata field limitations
- Custom headers that are not universally supported
- Endpoint variations and regional restrictions

### No Per-URI Overrides

Unlike the AWS S3 provider, the S3-compatible provider:

- Does **not** support per-URI credential overrides (use `AddS3CompatibleStorageProvider` for each distinct endpoint)
- Does **not** support per-URI service URL overrides (URL is configured globally)
- Uses the same credentials and endpoint for all operations in a given DI registration

If you need multiple endpoints, register separate instances:

```csharp
services.AddS3CompatibleStorageProvider(new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("https://minio.example.com"),
    AccessKey = "key1",
    SecretKey = "secret1",
});

// For a second endpoint, you may need to use the factory directly or create named registrations
```

## API Reference

### Core Interfaces and Types

- **`IStorageProvider`** - Core storage provider interface
  - Location: [`NPipeline.StorageProviders.Abstractions.IStorageProvider`](../../src/NPipeline.StorageProviders/Abstractions/IStorageProvider.cs)
  - Defines methods for reading, writing, listing, and checking existence

- **`StorageUri`** - URI type for storage resources
  - Location: [`NPipeline.StorageProviders.StorageUri`](../../src/NPipeline.StorageProviders/Models/StorageUri.cs)
  - Represents S3 resource URIs (scheme, bucket, path)

- **`StorageItem`** - Represents a storage item (file or directory)
  - Location: [`NPipeline.StorageProviders.StorageItem`](../../src/NPipeline.StorageProviders/Models/StorageItem.cs)
  - Contains URI, size, last modified date, and directory flag

- **`StorageMetadata`** - Metadata for storage objects
  - Location: [`NPipeline.StorageProviders.StorageMetadata`](../../src/NPipeline.StorageProviders/Models/StorageMetadata.cs)
  - Contains size, content type, last modified date, ETag, and custom metadata

### S3-Compatible Specific Types

- **`S3CompatibleStorageProvider`** - S3-compatible storage provider implementation
  - Location: [`S3CompatibleStorageProvider.cs`](../../src/NPipeline.StorageProviders.S3.Compatible/S3CompatibleStorageProvider.cs)
  - Implements `IStorageProvider` and `IStorageProviderMetadataProvider`

- **`S3CompatibleStorageProviderOptions`** - Configuration options
  - Location: [`S3CompatibleStorageProviderOptions.cs`](../../src/NPipeline.StorageProviders.S3.Compatible/S3CompatibleStorageProviderOptions.cs)
  - Contains endpoint, access key, secret key, and other settings

- **`S3CompatibleClientFactory`** - Factory for creating S3 clients
  - Location: [`S3CompatibleClientFactory.cs`](../../src/NPipeline.StorageProviders.S3.Compatible/S3CompatibleClientFactory.cs)
  - Creates and caches `AmazonS3Client` instances with provider configuration

### Extension Methods

- **`ServiceCollectionExtensions.AddS3CompatibleStorageProvider`**
  - Location: [`ServiceCollectionExtensions.cs`](../../src/NPipeline.StorageProviders.S3.Compatible/ServiceCollectionExtensions.cs)
  - Extension method for registering S3-compatible storage provider in DI container

## Additional Resources

- [MinIO Documentation](https://docs.min.io/)
- [DigitalOcean Spaces Documentation](https://docs.digitalocean.com/products/spaces/)
- [Cloudflare R2 Documentation](https://developers.cloudflare.com/r2/)
- [Wasabi Documentation](https://wasabi.com/doc/)
- [AWS S3 API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/) (S3 API standard)
- [LocalStack Documentation](https://docs.localstack.cloud/)
