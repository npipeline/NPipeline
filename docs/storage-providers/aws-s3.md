---
title: AWS S3 Storage Provider
description: Read from and write to Amazon S3 using the AWS S3 storage provider with full IAM and credential chain support.
sidebar_position: 1
---

## AWS S3 Storage Provider

The AWS S3 storage provider enables NPipeline applications to read from and write to Amazon S3 buckets using a unified storage abstraction. This provider implements the `IStorageProvider` interface and supports the `s3://` URI scheme.

### Overview

The AWS S3 storage provider provides seamless integration with Amazon S3. It offers:

- **Stream-based I/O** for efficient handling of large files
- **Async-first API** for scalable, non-blocking operations
- **Flexible authentication** via AWS credential chain, explicit credentials, or IAM roles
- **Region-aware endpoint selection** with support for all AWS regions
- **Multipart upload** for large files (configurable threshold, default 64 MB)
- **Comprehensive error handling** with proper exception translation
- **Metadata support** for retrieving object metadata
- **Listing operations** with recursive and non-recursive modes

### When to Use This Provider

Use the AWS S3 storage provider when your application needs to:

- Store and retrieve data in Amazon S3 buckets
- Integrate cloud storage into NPipeline data streamlines
- Leverage the AWS credential chain (environment variables, credential files, IAM roles)
- Use different regions and AWS features
- Handle large files through streaming and multipart uploads

> **For S3-Compatible Endpoints:** If you need to use MinIO, DigitalOcean Spaces, Cloudflare R2, or other S3-compatible services, use the [S3-Compatible Storage Provider](./s3-compatible.md) instead. It's optimized for static credentials and custom endpoints without AWS IAM complexity.

## Dependencies

The AWS S3 storage provider depends on the following packages:

- `AWSSDK.S3` - AWS SDK for S3 operations
- `AWSSDK.Core` - Core AWS SDK functionality
- `NPipeline.StorageProviders.S3` - Shared S3 core functionality (base classes, shared types)
- `NPipeline.StorageProviders` - Core storage abstractions (IStorageProvider, StorageUri, StorageItem, StorageMetadata, StorageProviderMetadata, StorageResolverOptions)

### Key Storage Types

> **Note:** The shared storage types (IStorageProvider, StorageUri, StorageItem, StorageMetadata, StorageProviderMetadata, StorageResolverOptions) are common across all NPipeline storage providers. Refer to the [Storage Provider Interface](./storage-provider.md) documentation for details on these types.

AWS S3-specific configuration types:

- **`AwsS3StorageProviderOptions`** - Configuration options for the AWS S3 storage provider
  - Location: `NPipeline.StorageProviders.S3.Aws.AwsS3StorageProviderOptions`
  - Essential settings: region, credentials, multipart upload thresholds

## Installation

### Prerequisites

- .NET 6.0 or later
- An AWS account with S3 access
- Appropriate IAM permissions for S3 operations

### Package Installation

Add the project reference to your solution:

```bash
dotnet add src/NPipeline.StorageProviders.S3.Aws/NPipeline.StorageProviders.S3.Aws.csproj
```

Or add it to your `.csproj` file:

```xml
<ItemGroup>
  <ProjectReference Include="..\NPipeline.StorageProviders.S3.Aws\NPipeline.StorageProviders.S3.Aws.csproj" />
</ItemGroup>
```

### Required Dependencies

The AWS S3 storage provider depends on:

- `AWSSDK.S3` and `AWSSDK.Core` - AWS SDK for S3 operations
- `NPipeline.StorageProviders.S3` - Shared S3 core functionality
- `NPipeline.StorageProviders` - Core storage abstractions

These dependencies are automatically resolved when adding the project reference.

## Quick Start

### Basic Usage with Connectors

The AWS S3 storage provider works seamlessly with all NPipeline connectors. Here's a quick example using the CSV connector:

```csharp
using NPipeline;
using NPipeline.Connectors;
using NPipeline.Connectors.Csv;
using NPipeline.StorageProviders.S3.Aws;
using Amazon;
using Microsoft.Extensions.DependencyInjection;

// Register and configure AWS S3 provider
var services = new ServiceCollection();

services.AddAwsS3StorageProvider(options =>
{
    options.DefaultRegion = RegionEndpoint.USEast1;
    options.UseDefaultCredentialChain = true;  // Uses AWS credential chain
});

var resolver = StorageProviderFactory.CreateResolver(
    new StorageResolverOptions
    {
        IncludeFileSystem = true,
        AdditionalProviders = new[] { services.BuildServiceProvider().GetRequiredService<AwsS3StorageProvider>() }
    }
);

public sealed record User(int Id, string Name, string Email);

public sealed class S3CsvPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Read CSV from S3
        var sourceNode = new CsvSourceNode<User>(
            StorageUri.Parse("s3://my-bucket/users.csv"),
            row => new User(
                row.Get<int>("Id") ?? 0,
                row.Get<string>("Name") ?? string.Empty,
                row.Get<string>("Email") ?? string.Empty),
            resolver: resolver);
        var source = builder.AddSource(sourceNode, "s3_csv_source");
        
        // ... add transforms ...
        
        // Write CSV to S3
        var sinkNode = new CsvSinkNode<UserSummary>(
            StorageUri.Parse("s3://my-bucket/summaries.csv"),
            resolver: resolver);
        var sink = builder.AddSink(sinkNode, "s3_csv_sink");
        
        builder.Connect(source, sink);
    }
}
```

## Configuration

### Using Dependency Injection

The recommended way to configure the AWS S3 storage provider is through dependency injection:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.S3.Aws;
using Amazon;

var services = new ServiceCollection();

services.AddAwsS3StorageProvider(options =>
{
    options.DefaultRegion = RegionEndpoint.USEast1;
    options.UseDefaultCredentialChain = true;
    options.MultipartUploadThresholdBytes = 64 * 1024 * 1024; // 64 MB
});

var serviceProvider = services.BuildServiceProvider();
var provider = serviceProvider.GetRequiredService<AwsS3StorageProvider>();
```

### AwsS3StorageProviderOptions

The `AwsS3StorageProviderOptions` class provides configuration options for the AWS S3 storage provider:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `DefaultRegion` | `RegionEndpoint?` | `null` | Default AWS region endpoint. If not specified, defaults to US East 1. |
| `DefaultCredentials` | `AWSCredentials?` | `null` | Default AWS credentials. If not specified, the default AWS credential chain is used. |
| `UseDefaultCredentialChain` | `bool` | `true` | Whether to use the default AWS credential chain (environment variables, ~/.aws/credentials, IAM roles). |
| `ServiceUrl` | `Uri?` | `null` | Optional service URL for S3-compatible endpoints (e.g., MinIO, LocalStack). If not specified, uses the AWS S3 endpoint. |
| `ForcePathStyle` | `bool` | `false` | Whether to force path-style addressing. Path-style addressing is required for some S3-compatible services. Default is virtual-hosted-style addressing. |
| `MultipartUploadThresholdBytes` | `long` | `64 * 1024 * 1024` (64 MB) | Threshold in bytes for using multipart upload when writing files. |

### Configuration Examples

#### Basic Configuration with Default Credentials

```csharp
services.AddAwsS3StorageProvider(options =>
{
    options.DefaultRegion = RegionEndpoint.APSoutheast2; // Sydney
    options.UseDefaultCredentialChain = true;
});
```

#### Configuration with Explicit Credentials

```csharp
services.AddAwsS3StorageProvider(options =>
{
    options.DefaultRegion = RegionEndpoint.USEast1;
    options.DefaultCredentials = new BasicAWSCredentials("accessKey", "secretKey");
    options.UseDefaultCredentialChain = false;
});
```

#### Configuration for MinIO (S3-Compatible via AWS Provider)

> **Recommended:** Use the [S3-Compatible Storage Provider](./s3-compatible.md) instead. It's optimized for MinIO and other S3-compatible services without AWS complexity.

If you need to use MinIO with the AWS provider:

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

#### Configuration for LocalStack

```csharp
services.AddAwsS3StorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://localhost:4566");
    options.ForcePathStyle = true;
    options.DefaultRegion = RegionEndpoint.USEast1;
});
```

## URI Format

The AWS S3 storage provider uses URIs with the `s3://` scheme to identify S3 objects.

### Basic Format

```
s3://bucket-name/path/to/file.csv
```

### With Region

```
s3://bucket-name/path/to/file.csv?region=us-east-1
```

### With Service URL (for Custom Endpoints)

```
s3://bucket-name/path/to/file.csv?serviceUrl=http://localhost:9000&pathStyle=true
```

### With Content Type

```
s3://bucket-name/path/to/file.csv?contentType=text/csv
```

### Complete Parameter Table

| Parameter | Description | Example |
|-----------|-------------|---------|
| `region` | AWS region name (e.g., us-east-1, ap-southeast-2) | `region=ap-southeast-2` |
| `accessKey` | AWS access key ID (for explicit credentials) | `accessKey=AKIAIOSFODNN7EXAMPLE` |
| `secretKey` | AWS secret access key (for explicit credentials) | `secretKey=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` |
| `serviceUrl` | Custom service URL for S3-compatible endpoints | `serviceUrl=http://localhost:9000` |
| `pathStyle` | Force path-style addressing (true/false) | `pathStyle=true` |
| `contentType` | Content type for the object when writing | `contentType=application/json` |

### URI Examples

```csharp
// Basic S3 object
var uri1 = StorageUri.Parse("s3://my-bucket/data/input.csv");

// With region
var uri2 = StorageUri.Parse("s3://my-bucket/data/input.csv?region=us-west-2");

// With custom content type
var uri3 = StorageUri.Parse("s3://my-bucket/data/output.json?contentType=application/json");

// With explicit credentials (development only)
var uri4 = StorageUri.Parse("s3://my-bucket/data/file.csv?accessKey=AKIA...&secretKey=...");

// Custom endpoint (for testing or S3-compatible services)
var uri5 = StorageUri.Parse("s3://local-bucket/data/file.csv?serviceUrl=http://localhost:4566&pathStyle=true");
```

## Authentication

The AWS S3 storage provider supports multiple authentication methods, with the AWS credential chain being the recommended approach.

### Default AWS Credential Chain (Recommended)

The default credential chain automatically searches for credentials in the following order:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`)
2. The shared credentials file (`~/.aws/credentials` on Unix, `%USERPROFILE%\.aws\credentials` on Windows)
3. The shared configuration file (`~/.aws/config`)
4. IAM role credentials (when running on EC2, ECS, Lambda, or other AWS services)

**Configuration:**

```csharp
services.AddAwsS3StorageProvider(options =>
{
    options.UseDefaultCredentialChain = true;
    options.DefaultRegion = RegionEndpoint.USEast1;
});
```

**Environment Variables:**

```bash
export AWS_ACCESS_KEY_ID=your_access_key_id
export AWS_SECRET_ACCESS_KEY=your_secret_access_key
export AWS_DEFAULT_REGION=us-east-1
```

### Explicit Credentials

You can provide explicit credentials via `AwsS3StorageProviderOptions`:

```csharp
services.AddAwsS3StorageProvider(options =>
{
    options.DefaultCredentials = new BasicAWSCredentials("accessKey", "secretKey");
    options.DefaultRegion = RegionEndpoint.USEast1;
    options.UseDefaultCredentialChain = false;
});
```

### Per-URI Credentials

For development and testing, credentials can be passed via URI parameters:

```csharp
var uri = StorageUri.Parse("s3://my-bucket/data.csv?accessKey=AKIA...&secretKey=...");
```

⚠️ **Security Warning:** Avoid passing credentials in URIs in production code. URIs may be logged, displayed in error messages, or stored in configuration files. Use the credential chain or explicit configuration instead.

## Working with S3-Compatible Endpoints

The AWS S3 provider can work with S3-compatible services (MinIO, LocalStack) via the `ServiceUrl` option, but this functionality is **not recommended for production use**.

**Instead:** Use the dedicated [S3-Compatible Storage Provider](./s3-compatible.md), which is purpose-built for:

- MinIO
- DigitalOcean Spaces
- Cloudflare R2
- Wasabi
- Other S3-compatible services

The S3-compatible provider:

- Uses static credentials without AWS IAM complexity
- Provides better configuration for non-AWS endpoints
- Avoids the overhead of AWS credential chain lookups
- Offers dedicated documentation and examples

## Examples

### Reading from S3

```csharp
using NPipeline.StorageProviders.S3.Aws;

var provider = serviceProvider.GetRequiredService<AwsS3StorageProvider>();
var uri = StorageUri.Parse("s3://my-bucket/data.csv");

using var stream = await provider.OpenReadAsync(uri);
using var reader = new StreamReader(stream);
var content = await reader.ReadToEndAsync();
```

### Writing to S3

```csharp
var uri = StorageUri.Parse("s3://my-bucket/output.csv");

using var stream = await provider.OpenWriteAsync(uri);
using var writer = new StreamWriter(stream);
await writer.WriteLineAsync("id,name,value");
await writer.WriteLineAsync("1,Item A,100");
```

### Listing Files

```csharp
var uri = StorageUri.Parse("s3://my-bucket/data/");

// List all files recursively
await foreach (var item in provider.ListAsync(uri, recursive: true))
{
    Console.WriteLine($"{item.Uri} - {item.Size} bytes - Modified: {item.LastModified}");
}

// List only immediate children (non-recursive)
await foreach (var item in provider.ListAsync(uri, recursive: false))
{
    var type = item.IsDirectory ? "[DIR]" : "[FILE]";
    Console.WriteLine($"{type} {item.Uri} - {item.Size} bytes");
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
        Console.WriteLine($"  {key}: {value}");
    }
}
```

## Error Handling

The S3 storage provider translates AWS S3 exceptions into standard .NET exceptions for consistent error handling.

### Exception Mapping

| S3 Error Code | .NET Exception | Description |
|---------------|----------------|-------------|
| `AccessDenied`, `InvalidAccessKeyId`, `SignatureDoesNotMatch` | `UnauthorizedAccessException` | Authentication or authorization failure |
| `InvalidBucketName`, `InvalidKey` | `ArgumentException` | Invalid bucket name or object key |
| `NoSuchBucket`, `NotFound` | `FileNotFoundException` | Bucket or object not found |
| Other `AmazonS3Exception` | `IOException` | General S3 access failure |

### Error Handling Example

```csharp
try
{
    using var stream = await provider.OpenReadAsync(uri);
    // Process stream...
}
catch (FileNotFoundException ex)
{
    Console.WriteLine($"File not found: {ex.Message}");
}
catch (UnauthorizedAccessException ex)
{
    Console.WriteLine($"Access denied: {ex.Message}");
    Console.WriteLine("Check your credentials and IAM permissions.");
}
catch (ArgumentException ex)
{
    Console.WriteLine($"Invalid URI: {ex.Message}");
}
catch (IOException ex)
{
    Console.WriteLine($"S3 access error: {ex.Message}");
    if (ex.InnerException is AmazonS3Exception s3Ex)
    {
        Console.WriteLine($"S3 Error Code: {s3Ex.ErrorCode}");
        Console.WriteLine($"S3 Request ID: {s3Ex.RequestId}");
    }
}
```

## IAM Permissions

To use the S3 storage provider, your AWS credentials must have appropriate IAM permissions.

### Required Permissions by Operation

| Operation | Required Permission |
|-----------|---------------------|
| Read (OpenReadAsync) | `s3:GetObject` |
| Write (OpenWriteAsync) | `s3:PutObject` |
| List (ListAsync) | `s3:ListBucket` |
| Metadata (GetMetadataAsync) | `s3:GetObject` |
| Existence (ExistsAsync) | `s3:GetObject` |

### Example IAM Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3ReadAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-bucket",
        "arn:aws:s3:::my-bucket/*"
      ]
    },
    {
      "Sid": "S3WriteAccess",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::my-bucket/*"
    }
  ]
}
```

### Minimal Policy for Read-Only Access

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-bucket",
        "arn:aws:s3:::my-bucket/*"
      ]
    }
  ]
}
```

### Minimal Policy for Write-Only Access

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::my-bucket/*"
    }
  ]
}
```

## API Reference

### Core Interfaces and Types

- **`IStorageProvider`** - Core storage provider interface
  - Location: [`NPipeline.StorageProviders.Abstractions.IStorageProvider`](../../src/NPipeline.StorageProviders/Abstractions/IStorageProvider.cs)
  - Defines methods for reading, writing, listing, and checking existence of storage objects

- **`StorageUri`** - URI type for storage resources
  - Location: [`NPipeline.StorageProviders.StorageUri`](../../src/NPipeline.StorageProviders/Models/StorageUri.cs)
  - Represents a URI for storage resources with scheme, host, path, and parameters

- **`StorageItem`** - Represents a storage item (file or directory)
  - Location: [`NPipeline.StorageProviders.StorageItem`](../../src/NPipeline.StorageProviders/Models/StorageItem.cs)
  - Contains URI, size, last modified date, and directory flag

- **`StorageMetadata`** - Metadata for storage objects
  - Location: [`NPipeline.StorageProviders.StorageMetadata`](../../src/NPipeline.StorageProviders/Models/StorageMetadata.cs)
  - Contains size, content type, last modified date, ETag, and custom metadata

### AWS S3-Specific Types

- **`AwsS3StorageProvider`** - AWS S3 storage provider implementation
  - Location: [`AwsS3StorageProvider.cs`](../../src/NPipeline.StorageProviders.S3.Aws/AwsS3StorageProvider.cs)
  - Implements `IStorageProvider` and `IStorageProviderMetadataProvider`

- **`AwsS3StorageProviderOptions`** - Configuration options
  - Location: [`AwsS3StorageProviderOptions.cs`](../../src/NPipeline.StorageProviders.S3.Aws/AwsS3StorageProviderOptions.cs)
  - Contains region, credentials, service URL, and other settings

- **`AwsS3ClientFactory`** - Factory for creating S3 clients
  - Location: [`AwsS3ClientFactory.cs`](../../src/NPipeline.StorageProviders.S3.Aws/AwsS3ClientFactory.cs)
  - Creates and caches `AmazonS3Client` instances

### Shared S3 Core Types

- **`S3ClientFactoryBase`** - Abstract base class for S3 client factories
  - Location: [`S3ClientFactoryBase.cs`](../../src/NPipeline.StorageProviders.S3/S3ClientFactoryBase.cs)
  - Shared client caching logic used by AWS and compatible providers

- **`S3CoreStorageProvider`** - Base class for S3 provider implementations
  - Location: [`S3CoreStorageProvider.cs`](../../src/NPipeline.StorageProviders.S3/S3CoreStorageProvider.cs)
  - Shared core logic for all S3 providers

### Extension Methods

- **`ServiceCollectionExtensions.AddAwsS3StorageProvider`**
  - Location: [`ServiceCollectionExtensions.cs`](../../src/NPipeline.StorageProviders.S3.Aws/ServiceCollectionExtensions.cs)
  - Extension method for registering AWS S3 storage provider in DI container

## Limitations

The S3 storage provider has the following limitations:

### Flat Storage Model

- S3 is a flat object storage system (no true hierarchical directories)
- Directory-like paths are simulated through key prefixes
- The provider treats keys ending with `/` as directories for listing purposes

### Large File Handling

- Multipart upload is used for files larger than the `MultipartUploadThresholdBytes` (default 64 MB)
- The threshold is configurable via `S3StorageProviderOptions`
- For very large files, ensure sufficient memory and network bandwidth

## Additional Resources

- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [AWS SDK for .NET Documentation](https://docs.aws.amazon.com/sdk-for-net/)
- [AWS IAM Documentation](https://docs.aws.amazon.com/iam/)
- [AWS Credential Configuration](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html)
- [S3-Compatible Storage Provider](./s3-compatible.md) - For MinIO, DigitalOcean Spaces, Cloudflare R2, and other S3-compatible services
