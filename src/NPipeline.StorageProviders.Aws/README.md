# AWS S3 Storage Provider

The AWS S3 storage provider enables NPipeline applications to read from and write to Amazon S3 buckets using a unified storage abstraction. This provider
implements the `IStorageProvider` interface and supports the `s3://` URI scheme.

## Overview

The S3 storage provider provides seamless integration with Amazon S3 and S3-compatible storage services. It offers:

- **Stream-based I/O** for efficient handling of large files
- **Async-first API** for scalable, non-blocking operations
- **Flexible authentication** via AWS credential chain or explicit credentials
- **S3-compatible endpoint support** for MinIO, LocalStack, and other compatible services
- **Multipart upload** for large files (configurable threshold, default 64 MB)
- **Comprehensive error handling** with proper exception translation
- **Metadata support** for retrieving object metadata
- **Listing operations** with recursive and non-recursive modes

### When to Use This Provider

Use the S3 storage provider when your application needs to:

- Store and retrieve data in Amazon S3
- Work with S3-compatible storage services (MinIO, LocalStack, etc.)
- Integrate cloud storage into NPipeline data pipelines
- Leverage S3's scalability and durability for data storage
- Handle large files through streaming and multipart uploads

## Installation

### Prerequisites

- .NET 6.0 or later
- An AWS account with S3 access (or S3-compatible service)
- Appropriate IAM permissions for S3 operations

### Package Installation

Add the project reference to your solution:

```bash
dotnet add src/NPipeline.StorageProviders.Aws/NPipeline.StorageProviders.Aws.csproj
```

Or add it to your `.csproj` file:

```xml
<ItemGroup>
  <ProjectReference Include="..\NPipeline.StorageProviders.Aws\NPipeline.StorageProviders.Aws.csproj" />
</ItemGroup>
```

### Required Dependencies

The S3 storage provider depends on:

- `AWSSDK.S3` - AWS SDK for S3 operations
- `NPipeline.Connectors` - Core storage abstractions

These dependencies are automatically resolved when adding the project reference.

## Configuration

### Using Dependency Injection

The recommended way to configure the S3 storage provider is through dependency injection:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Aws;
using Amazon;

var services = new ServiceCollection();

services.AddS3StorageProvider(options =>
{
    options.DefaultRegion = RegionEndpoint.USEast1;
    options.UseDefaultCredentialChain = true;
    options.MultipartUploadThresholdBytes = 64 * 1024 * 1024; // 64 MB
});

var serviceProvider = services.BuildServiceProvider();
var provider = serviceProvider.GetRequiredService<S3StorageProvider>();
```

### S3StorageProviderOptions

The `S3StorageProviderOptions` class provides configuration options for the S3 storage provider:

| Property                        | Type              | Default                    | Description                                                                                                                                            |
|---------------------------------|-------------------|----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| `DefaultRegion`                 | `RegionEndpoint?` | `null`                     | Default AWS region endpoint. If not specified, defaults to US East 1.                                                                                  |
| `DefaultCredentials`            | `AWSCredentials?` | `null`                     | Default AWS credentials. If not specified, the default AWS credential chain is used.                                                                   |
| `UseDefaultCredentialChain`     | `bool`            | `true`                     | Whether to use the default AWS credential chain (environment variables, ~/.aws/credentials, IAM roles).                                                |
| `ServiceUrl`                    | `Uri?`            | `null`                     | Optional service URL for S3-compatible endpoints (e.g., MinIO, LocalStack). If not specified, uses the AWS S3 endpoint.                                |
| `ForcePathStyle`                | `bool`            | `false`                    | Whether to force path-style addressing. Path-style addressing is required for some S3-compatible services. Default is virtual-hosted-style addressing. |
| `MultipartUploadThresholdBytes` | `long`            | `64 * 1024 * 1024` (64 MB) | Threshold in bytes for using multipart upload when writing files.                                                                                      |

### Configuration Examples

#### Basic Configuration with Default Credentials

```csharp
services.AddS3StorageProvider(options =>
{
    options.DefaultRegion = RegionEndpoint.APSoutheast2; // Sydney
    options.UseDefaultCredentialChain = true;
});
```

#### Configuration with Explicit Credentials

```csharp
services.AddS3StorageProvider(options =>
{
    options.DefaultRegion = RegionEndpoint.USEast1;
    options.DefaultCredentials = new BasicAWSCredentials("accessKey", "secretKey");
    options.UseDefaultCredentialChain = false;
});
```

#### Configuration for MinIO

```csharp
services.AddS3StorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://localhost:9000");
    options.ForcePathStyle = true;
    options.DefaultRegion = RegionEndpoint.USEast1;
});
```

#### Configuration for LocalStack

```csharp
services.AddS3StorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://localhost:4566");
    options.ForcePathStyle = true;
    options.DefaultRegion = RegionEndpoint.USEast1;
});
```

#### Configuration with Pre-configured Options

```csharp
var options = new S3StorageProviderOptions
{
    DefaultRegion = RegionEndpoint.EUWest1,
    MultipartUploadThresholdBytes = 128 * 1024 * 1024 // 128 MB
};

services.AddS3StorageProvider(options);
```

## URI Format

The S3 storage provider uses URIs with the `s3://` scheme to identify S3 objects.

### Basic Format

```
s3://bucket-name/path/to/file.csv
```

### With Region

```
s3://bucket-name/path/to/file.csv?region=us-east-1
```

### With Explicit Credentials

```
s3://bucket-name/path/to/file.csv?region=us-east-1&accessKey=YOUR_ACCESS_KEY&secretKey=YOUR_SECRET_KEY
```

### With Service URL (S3-Compatible Endpoints)

```
s3://bucket-name/path/to/file.csv?serviceUrl=http://localhost:9000&pathStyle=true
```

### With Content Type

```
s3://bucket-name/path/to/file.csv?contentType=text/csv
```

### Complete Parameter Table

| Parameter     | Description                                       | Example                                              |
|---------------|---------------------------------------------------|------------------------------------------------------|
| `region`      | AWS region name (e.g., us-east-1, ap-southeast-2) | `region=ap-southeast-2`                              |
| `accessKey`   | AWS access key ID (for explicit credentials)      | `accessKey=AKIAIOSFODNN7EXAMPLE`                     |
| `secretKey`   | AWS secret access key (for explicit credentials)  | `secretKey=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` |
| `serviceUrl`  | Custom service URL for S3-compatible endpoints    | `serviceUrl=http://localhost:9000`                   |
| `pathStyle`   | Force path-style addressing (true/false)          | `pathStyle=true`                                     |
| `contentType` | Content type for the object when writing          | `contentType=application/json`                       |

### URI Examples

```csharp
// Basic S3 object
var uri1 = StorageUri.Parse("s3://my-bucket/data/input.csv");

// With region
var uri2 = StorageUri.Parse("s3://my-bucket/data/input.csv?region=us-west-2");

// With custom content type
var uri3 = StorageUri.Parse("s3://my-bucket/data/output.json?contentType=application/json");

// MinIO endpoint
var uri4 = StorageUri.Parse("s3://my-bucket/data/file.csv?serviceUrl=http://localhost:9000&pathStyle=true");

// LocalStack endpoint
var uri5 = StorageUri.Parse("s3://local-bucket/data/file.csv?serviceUrl=http://localhost:4566&pathStyle=true");
```

## Authentication Options

The S3 storage provider supports multiple authentication methods:

### 1. Default AWS Credential Chain (Recommended)

The default credential chain automatically searches for credentials in the following order:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`)
2. The shared credentials file (`~/.aws/credentials` on Unix, `%USERPROFILE%\.aws\credentials` on Windows)
3. The shared configuration file (`~/.aws/config`)
4. IAM role credentials (when running on EC2, ECS, Lambda, or other AWS services)

**Configuration:**

```csharp
services.AddS3StorageProvider(options =>
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

### 2. Explicit Credentials via URI Parameters

You can pass credentials directly in the URI:

```csharp
var uri = StorageUri.Parse(
    "s3://my-bucket/data.csv?region=us-east-1&accessKey=AKIAIOSFODNN7EXAMPLE&secretKey=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
);
```

⚠️ **Security Warning:** Avoid passing credentials in URIs in production code. URIs may be logged, displayed in error messages, or stored in configuration
files. Use the credential chain instead.

### 3. Default Credentials via S3StorageProviderOptions

```csharp
services.AddS3StorageProvider(options =>
{
    options.DefaultCredentials = new BasicAWSCredentials("accessKey", "secretKey");
    options.DefaultRegion = RegionEndpoint.USEast1;
    options.UseDefaultCredentialChain = false;
});
```

### Security Considerations

- **Never log credentials** - Credentials in URIs can appear in logs, error messages, and debugging output
- **Use IAM roles** when running on AWS infrastructure (EC2, ECS, Lambda)
- **Rotate credentials regularly** - Use AWS Secrets Manager or similar services
- **Follow least privilege principle** - Grant only the permissions needed
- **Use credential profiles** for development and testing environments
- **Avoid hardcoding credentials** in source code

## S3-Compatible Endpoints

The S3 storage provider supports S3-compatible storage services such as MinIO and LocalStack.

### MinIO

MinIO is a high-performance, S3-compatible object storage system.

**Configuration:**

```csharp
services.AddS3StorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://localhost:9000");
    options.ForcePathStyle = true;
    options.DefaultRegion = RegionEndpoint.USEast1;
});
```

**URI Example:**

```csharp
var uri = StorageUri.Parse("s3://my-bucket/data/file.csv?serviceUrl=http://localhost:9000&pathStyle=true");
```

### LocalStack

LocalStack provides a fully functional local AWS cloud stack for testing.

**Configuration:**

```csharp
services.AddS3StorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://localhost:4566");
    options.ForcePathStyle = true;
    options.DefaultRegion = RegionEndpoint.USEast1;
});
```

**URI Example:**

```csharp
var uri = StorageUri.Parse("s3://local-bucket/data/file.csv?serviceUrl=http://localhost:4566&pathStyle=true");
```

### Other S3-Compatible Services

The provider works with any S3-compatible service. Configure using:

1. Set the `ServiceUrl` to the endpoint URL
2. Set `ForcePathStyle` to `true` (required for most S3-compatible services)
3. Configure credentials as needed

## Usage Examples

### Basic Read Example

```csharp
using NPipeline.Connectors;
using NPipeline.StorageProviders.Aws;

var provider = new S3StorageProvider(new S3ClientFactory(new S3StorageProviderOptions()), new S3StorageProviderOptions());
var uri = new StorageUri("s3://my-bucket/data.csv");

using var stream = await provider.OpenReadAsync(uri);
using var reader = new StreamReader(stream);
var content = await reader.ReadToEndAsync();
```

### Basic Write Example

```csharp
var provider = new S3StorageProvider(new S3ClientFactory(new S3StorageProviderOptions()), new S3StorageProviderOptions());
var uri = new StorageUri("s3://my-bucket/output.csv");

using var stream = await provider.OpenWriteAsync(uri);
using var writer = new StreamWriter(stream);
await writer.WriteLineAsync("id,name,value");
await writer.WriteLineAsync("1,Item A,100");
```

### With Dependency Injection

```csharp
// Configuration
services.AddS3StorageProvider(options =>
{
    options.DefaultRegion = RegionEndpoint.USEast1;
    options.UseDefaultCredentialChain = true;
});

// Usage
public class MyService
{
    private readonly S3StorageProvider _storageProvider;

    public MyService(S3StorageProvider storageProvider)
    {
        _storageProvider = storageProvider;
    }

    public async Task ProcessDataAsync()
    {
        var uri = new StorageUri("s3://my-bucket/data.csv");
        using var stream = await _storageProvider.OpenReadAsync(uri);
        // Process data...
    }
}
```

### List Files Example

```csharp
var provider = new S3StorageProvider(new S3ClientFactory(new S3StorageProviderOptions()), new S3StorageProviderOptions());
var uri = new StorageUri("s3://my-bucket/data/");

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

### Check File Existence Example

```csharp
var provider = new S3StorageProvider(new S3ClientFactory(new S3StorageProviderOptions()), new S3StorageProviderOptions());
var uri = new StorageUri("s3://my-bucket/data.csv");

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

### Get Metadata Example

```csharp
var provider = new S3StorageProvider(new S3ClientFactory(new S3StorageProviderOptions()), new S3StorageProviderOptions());
var uri = new StorageUri("s3://my-bucket/data.csv");

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

### Writing with Content Type

```csharp
var provider = new S3StorageProvider(new S3ClientFactory(new S3StorageProviderOptions()), new S3StorageProviderOptions());
var uri = new StorageUri("s3://my-bucket/data/output.json?contentType=application/json");

using var stream = await provider.OpenWriteAsync(uri);
using var writer = new StreamWriter(stream);
await writer.WriteAsync("{\"message\": \"Hello, S3!\"}");
```

### Reading with Cancellation

```csharp
var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
var uri = new StorageUri("s3://my-bucket/large-file.csv");

try
{
    using var stream = await provider.OpenReadAsync(uri, cts.Token);
    using var reader = new StreamReader(stream);

    string? line;
    while ((line = await reader.ReadLineAsync(cts.Token)) != null)
    {
        // Process line
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("Operation was cancelled.");
}
```

## Supported Operations

The S3 storage provider supports the following operations:

### Read Operations

| Operation       | Method             | Description                                       |
|-----------------|--------------------|---------------------------------------------------|
| Open Read       | `OpenReadAsync`    | Opens a readable stream for an S3 object          |
| Check Existence | `ExistsAsync`      | Checks if an S3 object exists                     |
| List Objects    | `ListAsync`        | Lists objects in a bucket with optional recursion |
| Get Metadata    | `GetMetadataAsync` | Retrieves metadata for an S3 object               |

### Write Operations

| Operation  | Method           | Description                              |
|------------|------------------|------------------------------------------|
| Open Write | `OpenWriteAsync` | Opens a writable stream for an S3 object |

### Unsupported Operations

| Operation | Method        | Reason                                                   |
|-----------|---------------|----------------------------------------------------------|
| Delete    | `DeleteAsync` | Not supported by design - throws `NotSupportedException` |

## Error Handling

The S3 storage provider translates AWS S3 exceptions into standard .NET exceptions for consistent error handling.

### Exception Mapping

| S3 Error Code                                                 | .NET Exception                | Description                             |
|---------------------------------------------------------------|-------------------------------|-----------------------------------------|
| `AccessDenied`, `InvalidAccessKeyId`, `SignatureDoesNotMatch` | `UnauthorizedAccessException` | Authentication or authorization failure |
| `InvalidBucketName`, `InvalidKey`                             | `ArgumentException`           | Invalid bucket name or object key       |
| `NoSuchBucket`, `NotFound`                                    | `FileNotFoundException`       | Bucket or object not found              |
| Other `AmazonS3Exception`                                     | `IOException`                 | General S3 access failure               |

### Custom S3StorageException

The provider uses a custom `S3StorageException` that includes context about the bucket and key being accessed. This exception wraps the original
`AmazonS3Exception` for detailed debugging.

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

| Operation                   | Required Permission |
|-----------------------------|---------------------|
| Read (OpenReadAsync)        | `s3:GetObject`      |
| Write (OpenWriteAsync)      | `s3:PutObject`      |
| List (ListAsync)            | `s3:ListBucket`     |
| Metadata (GetMetadataAsync) | `s3:GetObject`      |
| Existence (ExistsAsync)     | `s3:GetObject`      |

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

## Limitations

The S3 storage provider has the following limitations:

### Delete Operations

- `DeleteAsync` is **not supported** and throws `NotSupportedException`
- This is by design to prevent accidental data loss
- Use the AWS SDK directly if you need delete functionality

### Flat Storage Model

- S3 is a flat object storage system (no true hierarchical directories)
- Directory-like paths are simulated through key prefixes
- The provider treats keys ending with `/` as directories for listing purposes

### Large File Handling

- Multipart upload is used for files larger than the `MultipartUploadThresholdBytes` (default 64 MB)
- The threshold is configurable via `S3StorageProviderOptions`
- For very large files, ensure sufficient memory and network bandwidth

### Concurrent Operations

- While the provider is thread-safe, concurrent writes to the same object may result in race conditions
- Use appropriate locking or versioning strategies if needed

## Performance Considerations

### Client Caching

- The `S3ClientFactory` caches S3 clients based on configuration (region, credentials, service URL)
- This reduces overhead for repeated operations with the same configuration
- Clients are cached as singletons within the application

### Streaming for Large Files

- All operations use streaming to avoid loading entire files into memory
- `OpenReadAsync` returns a stream that can be read incrementally
- `OpenWriteAsync` uses multipart upload for large files

### Pagination for Large Buckets

- `ListAsync` automatically handles pagination for buckets with many objects
- Uses S3's continuation token mechanism to fetch all objects
- Async enumerable pattern enables efficient iteration

### Async Operations

- All methods are async to avoid blocking threads
- Use `ConfigureAwait(false)` in library code to avoid deadlocks
- Cancellation tokens are supported for long-running operations

### Performance Tips

1. **Reuse providers** - Register as singleton in DI container
2. **Use appropriate regions** - Choose the region closest to your application
3. **Batch operations** - List multiple objects at once instead of individual existence checks
4. **Optimize multipart threshold** - Adjust based on your typical file sizes
5. **Use compression** - Compress data before uploading for large files

## Security Considerations

### Credential Management

- **Never log credentials** - Credentials in URIs can appear in logs
- **Use credential chain** - Prefer environment variables or IAM roles over explicit credentials
- **Rotate credentials regularly** - Use AWS Secrets Manager or similar services
- **Use temporary credentials** - When possible, use short-lived credentials via STS

### IAM Best Practices

- **Follow least privilege principle** - Grant only necessary permissions
- **Use resource-based policies** - For bucket-level access control
- **Enable MFA** - For IAM users with console access
- **Use roles for applications** - Instead of long-lived access keys

### Endpoint Override Risks

- **Validate service URLs** - Ensure custom endpoints are trusted
- **Use HTTPS** - For production endpoints to encrypt data in transit
- **Verify certificates** - When using custom endpoints

### Data Security

- **Enable S3 encryption** - Use server-side encryption (SSE-S3, SSE-KMS, SSE-C)
- **Use bucket policies** - To restrict access at the bucket level
- **Enable versioning** - To protect against accidental deletions or overwrites
- **Consider object locking** - For critical data that must not be modified

## Troubleshooting

### Common Issues and Solutions

#### Access Denied Error

**Symptom:** `UnauthorizedAccessException` when accessing S3 objects

**Solutions:**

1. Verify credentials are correct and have not expired
2. Check IAM permissions include required actions
3. Ensure bucket policy allows access from your account/role
4. Verify region is correct

```csharp
// Debug: Check provider metadata
var metadata = provider.GetMetadata();
Console.WriteLine($"Supported Schemes: {string.Join(", ", metadata.SupportedSchemes)}");
```

#### File Not Found Error

**Symptom:** `FileNotFoundException` when accessing an object

**Solutions:**

1. Verify bucket name is correct
2. Check object key/path is correct (case-sensitive)
3. Ensure object exists in the specified bucket
4. Verify region is correct for the bucket

#### Invalid Bucket Name Error

**Symptom:** `ArgumentException` with "Invalid S3 bucket" message

**Solutions:**

1. Bucket names must be 3-63 characters long
2. Bucket names can only contain lowercase letters, numbers, and hyphens
3. Bucket names must start and end with a letter or number
4. Bucket names must not contain consecutive hyphens

#### Connection Timeout

**Symptom:** `IOException` or timeout when accessing S3

**Solutions:**

1. Check network connectivity
2. Verify firewall allows outbound HTTPS (port 443)
3. Check VPC endpoint configuration if using VPC
4. Increase timeout settings in AWS SDK configuration

#### MinIO/LocalStack Connection Issues

**Symptom:** Cannot connect to S3-compatible endpoint

**Solutions:**

1. Ensure `ForcePathStyle` is set to `true`
2. Verify `ServiceUrl` is correct and accessible
3. Check credentials for the S3-compatible service
4. Ensure bucket exists in the S3-compatible service

```csharp
// Debug: Check client configuration
services.AddS3StorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://localhost:9000");
    options.ForcePathStyle = true;
    // Enable AWS SDK logging for debugging
    AWSConfigs.LoggingConfig.LogTo = LoggingOptions.SystemDiagnostics;
});
```

### Debugging Tips

1. **Enable AWS SDK Logging**

```csharp
AWSConfigs.LoggingConfig.LogTo = LoggingOptions.SystemDiagnostics;
```

1. **Check Provider Capabilities**

```csharp
var metadata = provider.GetMetadata();
Console.WriteLine($"Supports Read: {metadata.SupportsRead}");
Console.WriteLine($"Supports Write: {metadata.SupportsWrite}");
Console.WriteLine($"Supports Delete: {metadata.SupportsDelete}");
Console.WriteLine($"Supports Listing: {metadata.SupportsListing}");
```

1. **Validate URI Format**

```csharp
var uri = StorageUri.Parse("s3://bucket/path");
Console.WriteLine($"Scheme: {uri.Scheme}");
Console.WriteLine($"Host (Bucket): {uri.Host}");
Console.WriteLine($"Path: {uri.Path}");
Console.WriteLine($"Parameters: {string.Join(", ", uri.Parameters)}");
```

1. **Test Connection**

```csharp
try
{
    var exists = await provider.ExistsAsync(new StorageUri("s3://bucket/"));
    Console.WriteLine($"Connection successful. Bucket exists: {exists}");
}
catch (Exception ex)
{
    Console.WriteLine($"Connection failed: {ex.Message}");
}
```

### Error Message Interpretation

| Error Message                | Likely Cause                      | Action                                        |
|------------------------------|-----------------------------------|-----------------------------------------------|
| "Access denied to S3 bucket" | Insufficient permissions          | Check IAM permissions and bucket policies     |
| "Invalid S3 bucket or key"   | Invalid bucket name or key format | Validate bucket name and key format           |
| "S3 bucket or key not found" | Bucket or object doesn't exist    | Verify bucket and object existence            |
| "Failed to access S3 bucket" | Network or service issue          | Check network connectivity and service status |

## API Reference

### Core Interfaces and Types

- **`IStorageProvider`** - Core storage provider interface
  - Location: [`NPipeline.Connectors.Abstractions.IStorageProvider`](../NPipeline.Connectors/Abstractions/IStorageProvider.cs)
  - Defines methods for reading, writing, listing, and checking existence of storage objects

- **`StorageUri`** - URI type for storage resources
  - Location: [`NPipeline.Connectors.StorageUri`](../NPipeline.Connectors/StorageUri.cs)
  - Represents a URI for storage resources with scheme, host, path, and parameters

- **`StorageScheme`** - Type alias for storage scheme
  - Location: [`NPipeline.Connectors.StorageScheme`](../NPipeline.Connectors/StorageScheme.cs)
  - Represents the scheme component of a storage URI (e.g., "s3", "file")

- **`StorageItem`** - Represents a storage item (file or directory)
  - Location: [`NPipeline.Connectors.StorageItem`](../NPipeline.Connectors/StorageItem.cs)
  - Contains URI, size, last modified date, and directory flag

- **`StorageMetadata`** - Metadata for storage objects
  - Location: [`NPipeline.Connectors.StorageMetadata`](../NPipeline.Connectors/StorageMetadata.cs)
  - Contains size, content type, last modified date, ETag, and custom metadata

- **`StorageProviderMetadata`** - Metadata describing a storage provider
  - Location: [`NPipeline.Connectors.StorageProviderMetadata`](../NPipeline.Connectors/StorageProviderMetadata.cs)
  - Contains provider name, supported schemes, and capabilities

### S3-Specific Types

- **`S3StorageProvider`** - S3 storage provider implementation
  - Location: [`S3StorageProvider.cs`](S3StorageProvider.cs)
  - Implements `IStorageProvider` and `IStorageProviderMetadataProvider`

- **`S3StorageProviderOptions`** - Configuration options
  - Location: [`S3StorageProviderOptions.cs`](S3StorageProviderOptions.cs)
  - Contains region, credentials, service URL, and other settings

- **`S3ClientFactory`** - Factory for creating S3 clients
  - Location: [`S3ClientFactory.cs`](S3ClientFactory.cs)
  - Creates and caches `AmazonS3Client` instances

- **`S3WriteStream`** - Stream for writing to S3
  - Location: [`S3WriteStream.cs`](S3WriteStream.cs)
  - Implements multipart upload for large files

- **`S3StorageException`** - Custom exception for S3 errors
  - Location: [`S3StorageException.cs`](S3StorageException.cs)
  - Wraps `AmazonS3Exception` with bucket/key context

### Extension Methods

- **`ServiceCollectionExtensions.AddS3StorageProvider`**
  - Location: [`ServiceCollectionExtensions.cs`](ServiceCollectionExtensions.cs)
  - Extension method for registering S3 storage provider in DI container

## Additional Resources

- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [AWS SDK for .NET Documentation](https://docs.aws.amazon.com/sdk-for-net/)
- [MinIO Documentation](https://docs.min.io/)
- [LocalStack Documentation](https://docs.localstack.cloud/)
- [NPipeline Documentation](../../docs/)

## License

This storage provider is part of the NPipeline project. See the main project LICENSE file for details.
