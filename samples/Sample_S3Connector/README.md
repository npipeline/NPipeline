# AWS S3 Storage Provider Sample

This sample demonstrates how to use the AWS S3 storage provider in NPipeline to read, write, list, and manage files in AWS S3 storage.

## Overview

The `Sample_S3Connector` application showcases the following S3 storage provider features:

- **Reading files** from S3 using `OpenReadAsync`
- **Writing files** to S3 using `OpenWriteAsync`
- **Listing objects** in a bucket with recursive and non-recursive options
- **Checking file existence** with `ExistsAsync`
- **Retrieving metadata** for S3 objects
- **Dependency injection** configuration for S3 storage provider
- **S3-compatible endpoints** configuration (MinIO, LocalStack)

## Prerequisites

### AWS Account and S3 Bucket

1. **AWS Account**: You need an active AWS account with S3 access
2. **S3 Bucket**: Create an S3 bucket to use for testing
3. **IAM Permissions**: Ensure your AWS credentials have the following permissions:
   - `s3:GetObject` - Read objects
   - `s3:PutObject` - Write objects
   - `s3:ListBucket` - List bucket contents
   - `s3:GetObjectMetadata` - Get object metadata

### AWS Credentials

You can configure AWS credentials in several ways:

#### Option 1: Environment Variables (Recommended)

```bash
export AWS_ACCESS_KEY_ID=your-access-key-id
export AWS_SECRET_ACCESS_KEY=your-secret-access-key
export AWS_REGION=us-east-1
```

#### Option 2: AWS Credentials File

Create or edit `~/.aws/credentials`:

```ini
[default]
aws_access_key_id = your-access-key-id
aws_secret_access_key = your-secret-access-key
```

Create or edit `~/.aws/config`:

```ini
[default]
region = us-east-1
```

#### Option 3: IAM Role (EC2/ECS)

If running on EC2 or ECS, you can use an IAM role with appropriate S3 permissions.

### Required NuGet Packages

The sample references the following projects:

- `NPipeline.Connectors` - Core storage abstractions
- `NPipeline.StorageProviders.Aws.S3` - AWS S3 storage provider implementation

## Setup

### 1. Configure the Sample

Edit [`Program.cs`](Program.cs:1) and update the configuration constants at the top of the file:

```csharp
private const string BucketName = "your-bucket-name-here";
private const string AccessKeyId = "your-access-key-id";
private const string SecretAccessKey = "your-secret-access-key";
private const string Region = "us-east-1";
```

**Important**: If you're using the default AWS credential chain (environment variables or AWS credentials file), you can leave `AccessKeyId` and `SecretAccessKey` as placeholder values and set `UseDefaultCredentialChain = true` in the options.

### 2. Create Test Data (Optional)

For the read examples to work, you may want to upload a sample CSV file to your S3 bucket:

```bash
aws s3 cp sample.csv s3://your-bucket-name/data/sample.csv
```

Create a simple `sample.csv` file:

```csv
id,name,value
1,Item A,100
2,Item B,200
3,Item C,300
```

## Running the Sample

### Build the Sample

```bash
cd samples/Sample_S3Connector
dotnet build
```

### Run the Sample

```bash
dotnet run
```

The sample will execute all examples sequentially and display output to the console.

## Examples

### Example 1: Basic Read from S3

Demonstrates reading a CSV file from S3:

```csharp
var fileUri = StorageUri.Parse($"s3://{BucketName}/data/sample.csv");
await using var stream = await provider.OpenReadAsync(fileUri);
using var reader = new StreamReader(stream);
var content = await reader.ReadToEndAsync();
```

**What it demonstrates:**

- Creating an S3 storage provider instance
- Parsing S3 URIs
- Opening a readable stream from S3
- Reading file contents
- Error handling for common S3 exceptions

### Example 2: Basic Write to S3

Demonstrates writing data to S3:

```csharp
var fileUri = StorageUri.Parse($"s3://{BucketName}/data/output/sample.txt");
await using var stream = await provider.OpenWriteAsync(fileUri);
await using var writer = new StreamWriter(stream);
await writer.WriteAsync(sampleData);
```

**What it demonstrates:**

- Creating sample data
- Opening a writable stream to S3
- Writing data to S3
- Verifying the write operation succeeded

### Example 3: List S3 Objects

Demonstrates listing objects in an S3 bucket:

```csharp
var prefixUri = StorageUri.Parse($"s3://{BucketName}/data/");
await foreach (var item in provider.ListAsync(prefixUri, recursive: false))
{
    Console.WriteLine($"{item.Uri.Path} | Size: {item.Size}");
}
```

**What it demonstrates:**

- Non-recursive listing (direct children only)
- Recursive listing (all descendants)
- Displaying object metadata (name, size, last modified)
- Handling directories vs files

### Example 4: Check File Existence

Demonstrates checking if a file exists in S3:

```csharp
var fileUri = StorageUri.Parse($"s3://{BucketName}/data/sample.csv");
var exists = await provider.ExistsAsync(fileUri);
```

**What it demonstrates:**

- Checking if an object exists
- Handling non-existent files gracefully

### Example 5: Get File Metadata

Demonstrates retrieving metadata for a file:

```csharp
var metadata = await provider.GetMetadataAsync(fileUri);
Console.WriteLine($"Size: {metadata.Size}");
Console.WriteLine($"ETag: {metadata.ETag}");
Console.WriteLine($"ContentType: {metadata.ContentType}");
```

**What it demonstrates:**

- Retrieving object metadata
- Displaying ETag, ContentType, Size, LastModified
- Accessing custom metadata

### Example 6: Using Dependency Injection

Demonstrates configuring S3 storage provider with DI:

```csharp
var services = new ServiceCollection();
services.AddS3StorageProvider(options =>
{
    options.DefaultRegion = RegionEndpoint.GetBySystemName(Region);
    options.DefaultCredentials = new BasicAWSCredentials(AccessKeyId, SecretAccessKey);
    options.UseDefaultCredentialChain = false;
});
var serviceProvider = services.BuildServiceProvider();
var provider = serviceProvider.GetRequiredService<S3StorageProvider>();
```

**What it demonstrates:**

- Configuring S3 storage provider with DI
- Resolving the provider from the service container
- Accessing provider metadata and capabilities

### Example 7: S3-Compatible Endpoints

Demonstrates configuration for S3-compatible services:

```csharp
// MinIO configuration
var options = new S3StorageProviderOptions
{
    ServiceUrl = new Uri("http://localhost:9000"),
    ForcePathStyle = true,
    DefaultRegion = RegionEndpoint.USEast1,
    DefaultCredentials = new BasicAWSCredentials("minioadmin", "minioadmin")
};
```

**What it demonstrates:**

- Configuring MinIO endpoint
- Configuring LocalStack endpoint
- Using `ServiceUrl` and `ForcePathStyle` options
- DI configuration for S3-compatible services

## Configuration Options

### S3StorageProviderOptions

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `DefaultRegion` | `RegionEndpoint?` | `null` | Default AWS region endpoint |
| `DefaultCredentials` | `AWSCredentials?` | `null` | Default AWS credentials |
| `UseDefaultCredentialChain` | `bool` | `true` | Use the default AWS credential chain |
| `ServiceUrl` | `Uri?` | `null` | Service URL for S3-compatible endpoints |
| `ForcePathStyle` | `bool` | `false` | Force path-style addressing |
| `MultipartUploadThresholdBytes` | `long` | `64 MB` | Threshold for multipart upload |

### URI Format

S3 URIs follow this format:

```
s3://bucket-name/path/to/object
```

**Examples:**

- `s3://my-bucket/data/file.csv`
- `s3://my-bucket/reports/2024/annual.pdf`
- `s3://my-bucket/images/photo.jpg?contentType=image/jpeg`

### URI Parameters

You can pass additional parameters via the query string:

- `contentType` - Content type for the object (used when writing)

**Example:**

```
s3://my-bucket/data/file.json?contentType=application/json
```

## S3-Compatible Services

### MinIO

MinIO is a high-performance, S3-compatible object storage system.

**Configuration:**

```csharp
var options = new S3StorageProviderOptions
{
    ServiceUrl = new Uri("http://localhost:9000"),
    ForcePathStyle = true,
    DefaultRegion = RegionEndpoint.USEast1,
    DefaultCredentials = new BasicAWSCredentials("minioadmin", "minioadmin"),
    UseDefaultCredentialChain = false
};
```

**Default credentials:**

- Access Key: `minioadmin`
- Secret Key: `minioadmin`

### LocalStack

LocalStack provides a fully functional local AWS cloud stack.

**Configuration:**

```csharp
var options = new S3StorageProviderOptions
{
    ServiceUrl = new Uri("http://localhost:4566"),
    ForcePathStyle = true,
    DefaultRegion = RegionEndpoint.USEast1,
    UseDefaultCredentialChain = true // LocalStack accepts any credentials
};
```

**Note:** LocalStack accepts any credentials, so you can use the default credential chain.

## Error Handling

The S3 storage provider translates AWS S3 exceptions into .NET exceptions:

| AWS Error Code | .NET Exception Type |
|----------------|-------------------|
| `AccessDenied`, `InvalidAccessKeyId`, `SignatureDoesNotMatch` | `UnauthorizedAccessException` |
| `InvalidBucketName`, `InvalidKey` | `ArgumentException` |
| `NoSuchBucket`, `NotFound` | `FileNotFoundException` |
| Other errors | `IOException` |

**Example:**

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
    Console.WriteLine($"File not found: {ex.Message}");
}
catch (IOException ex)
{
    Console.WriteLine($"IO error: {ex.Message}");
}
```

## Best Practices

1. **Use the default credential chain** when possible - it's more secure and flexible
2. **Set appropriate IAM permissions** - follow the principle of least privilege
3. **Use cancellation tokens** for long-running operations
4. **Handle exceptions gracefully** - network issues and permission errors are common
5. **Use multipart uploads** for large files (configured via `MultipartUploadThresholdBytes`)
6. **Consider using S3-compatible services** for local development and testing

## Troubleshooting

### "Access denied" errors

- Verify your AWS credentials are correct
- Check IAM permissions for your bucket
- Ensure the bucket exists in the specified region

### "File not found" errors

- Verify the bucket name and key are correct
- Check that the file exists in the bucket
- Ensure you're using the correct path format

### "Invalid bucket name" errors

- Bucket names must be globally unique across all AWS accounts
- Bucket names must follow S3 naming rules (lowercase, no spaces, etc.)

### Connection timeouts

- Check your network connectivity
- Verify the region is correct
- Consider using a closer AWS region for better performance

## Additional Resources

- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [NPipeline Storage Provider Documentation](../../../docs/connectors/storage-provider.md)
- [AWS SDK for .NET Documentation](https://docs.aws.amazon.com/sdk-for-net/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
- [LocalStack Documentation](https://docs.localstack.cloud/)

## License

This sample is part of the NPipeline project and follows the same license terms.
