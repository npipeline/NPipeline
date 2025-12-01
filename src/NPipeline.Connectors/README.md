# NPipeline.Connectors

NPipeline.Connectors is a comprehensive storage abstraction layer for the NPipeline framework that provides a unified interface for accessing different storage
systems. It enables pipeline components to work with various storage backends (local file system, cloud storage, databases) through a consistent API, supporting
pluggable storage providers with scheme-based URI resolution.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based
applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for
resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Connectors
```

## Key Features

- **Storage Abstraction**: Unified interface for accessing different storage systems through a common API
- **Scheme-based URI Resolution**: Support for standard URI schemes (file, s3, azure, etc.) with automatic provider selection
- **Pluggable Provider Architecture**: Easy extensibility with custom storage providers
- **Stream-based I/O**: Efficient async-first operations with minimal memory footprint
- **Built-in File System Provider**: Out-of-the-box support for local file system operations
- **Dependency Injection Support**: Seamless integration with Microsoft.Extensions.DependencyInjection
- **Configuration-driven Setup**: Flexible provider configuration through code or configuration files
- **Cross-platform Compatibility**: Works on Windows, Linux, and macOS

## Key Components

### StorageProviderFactory

The `StorageProviderFactory` provides factory methods to create and configure storage provider resolvers without requiring dependency injection:

```csharp
// Create a resolver with built-in file system provider
var resolver = StorageProviderFactory.CreateResolver().Resolver;

// Create a resolver with additional custom providers
var customProviders = new[] { new S3StorageProvider(), new AzureBlobStorageProvider() };
var resolverResult = StorageProviderFactory.CreateResolver(new StorageResolverOptions
{
    IncludeFileSystem = true,
    AdditionalProviders = customProviders,
});
var resolver = resolverResult.Resolver;

// Create from configuration and capture errors
var config = new ConnectorConfiguration
{
    Providers = new Dictionary<string, StorageProviderConfig>
    {
        ["S3"] = new StorageProviderConfig
        {
            ProviderType = "MyApp.S3StorageProvider",
            Enabled = true,
            Settings = new Dictionary<string, string>
            {
                ["Region"] = "us-west-2",
                ["AccessKey"] = "your-access-key"
            }
        }
    }
};
var (configuredResolver, errors) = StorageProviderFactory.CreateResolver(new StorageResolverOptions
{
    Configuration = config,
    CollectErrors = true,
});

if (errors.Count > 0)
{
    // log or surface configuration issues here
}

// Register a friendly alias for custom providers
StorageProviderFactory.RegisterProviderAlias("s3", typeof(S3StorageProvider));
```

### StorageResolver

The `StorageResolver` maintains a thread-safe list of explicitly registered providers and resolves them based on URI schemes:

```csharp
var resolver = new StorageResolver();

// Register providers manually (factory helpers call this for you)
resolver.RegisterProvider(new FileSystemStorageProvider());
resolver.RegisterProvider(new S3StorageProvider());

// Resolve a provider for a specific URI
var fileUri = StorageUri.FromFilePath("./data/input.csv");
var provider = resolver.ResolveProvider(fileUri);

// List all available providers
var providers = resolver.GetAvailableProviders();
```

### FileSystemStorageProvider

The built-in `FileSystemStorageProvider` handles local file system operations with the "file" scheme:

```csharp
var provider = new FileSystemStorageProvider();
var fileUri = StorageUri.FromFilePath("./data/output.csv");

// Check if file exists
bool exists = await provider.ExistsAsync(fileUri);

// Open file for reading
using var readStream = await provider.OpenReadAsync(fileUri);

// Open file for writing (creates directories as needed)
using var writeStream = await provider.OpenWriteAsync(fileUri);

// List files in directory
var directoryUri = StorageUri.FromFilePath("./data/");
await foreach (var item in provider.ListAsync(directoryUri, recursive: true))
{
    Console.WriteLine($"{item.Uri} - {item.Size} bytes");
}

// Get file metadata
var metadata = await provider.GetMetadataAsync(fileUri);
if (metadata != null)
{
    Console.WriteLine($"Size: {metadata.Size}, Modified: {metadata.LastModified}");
}
```

## Supported Storage Schemes

NPipeline.Connectors supports an extensible set of storage schemes through its provider architecture:

### Built-in Schemes

- **file** - Local file system access (Windows, Linux, macOS)
    - Supports absolute paths: `file:///C:/data/input.csv`
    - Supports relative paths: `file://./data/input.csv`
    - Supports UNC paths: `file://server/share/data/input.csv`

### Extensible Scheme Support

Additional schemes can be supported by implementing custom storage providers:

- **s3** - Amazon S3 and S3-compatible storage
- **azure** - Microsoft Azure Blob Storage
- **gcs** - Google Cloud Storage
- **ftp** - FTP/FTPS servers
- **sftp** - SFTP servers
- **http/https** - HTTP/HTTPS endpoints
- **database** - Database storage (custom implementations)

## Usage Examples

### Basic File System Access

```csharp
using NPipeline.Connectors;

// Create a file URI from a path
var inputUri = StorageUri.FromFilePath("./data/input.csv");
var outputUri = StorageUri.FromFilePath("./data/output.csv");

// Create resolver with file system provider
var resolver = StorageProviderFactory.CreateResolver().Resolver;
var provider = StorageProviderFactory.GetProviderOrThrow(resolver, inputUri);

// Read from file
using var inputStream = await provider.OpenReadAsync(inputUri);
using var reader = new StreamReader(inputStream);
var content = await reader.ReadToEndAsync();

// Write to file
using var outputStream = await provider.OpenWriteAsync(outputUri);
using var writer = new StreamWriter(outputStream);
await writer.WriteAsync(content.ToUpperInvariant());
```

### Provider Registration with Dependency Injection

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors;
using NPipeline.Connectors.DependencyInjection;

// Configure services
var services = new ServiceCollection();

// Add the storage resolver with file system provider
services.AddStorageResolver(includeFileSystem: true);

// Add custom storage providers
services.AddStorageProvider<S3StorageProvider>();
services.AddStorageProvider<AzureBlobStorageProvider>();

// Add provider instance
services.AddStorageProvider(new CustomDatabaseStorageProvider(connectionString));

// Build service provider
var serviceProvider = services.BuildServiceProvider();

// Resolve and use the storage resolver
var resolver = serviceProvider.GetRequiredService<IStorageResolver>();
var s3Uri = StorageUri.Parse("s3://my-bucket/data/input.csv");
var provider = resolver.ResolveProvider(s3Uri);
```

### Custom Provider Example (S3)

```csharp
using NPipeline.Connectors.Abstractions;

public class S3StorageProvider : IStorageProvider
{
    public StorageScheme Scheme => StorageScheme.S3;

    public bool CanHandle(StorageUri uri)
    {
        return Scheme.Equals(uri.Scheme) && !string.IsNullOrEmpty(uri.Host);
    }

    public async Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        // Implementation for reading from S3
        var client = GetS3Client();
        var request = new GetObjectRequest
        {
            BucketName = uri.Host,
            Key = uri.Path.TrimStart('/')
        };

        var response = await client.GetObjectAsync(request, cancellationToken);
        return response.ResponseStream;
    }

    public async Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        // Implementation for writing to S3
        var client = GetS3Client();
        var request = new PutObjectRequest
        {
            BucketName = uri.Host,
            Key = uri.Path.TrimStart('/'),
            InputStream = new MemoryStream() // Will be replaced with actual stream
        };

        // Return a stream that uploads to S3 when disposed
        return new S3UploadStream(client, request, cancellationToken);
    }

    // Implement other required methods...
}
```

## Configuration

### Provider Registration

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.DependencyInjection;

var services = new ServiceCollection();

// Method 1: Register individual providers
services.AddStorageProvider<FileSystemStorageProvider>();
services.AddStorageProvider<S3StorageProvider>();
services.AddStorageResolver(includeFileSystem: false); // Skip auto-registration

// Method 2: Register from configuration
services.AddStorageProvidersFromConfiguration(config =>
{
    config.Providers["S3"] = new StorageProviderConfig
    {
        ProviderType = "MyApp.Providers.S3StorageProvider",
        Enabled = true,
        Settings = new Dictionary<string, string>
        {
            ["Region"] = "us-west-2",
            ["AccessKey"] = "${S3_ACCESS_KEY}",
            ["SecretKey"] = "${S3_SECRET_KEY}"
        }
    };
});

// Method 3: Register all discovered providers
services.AddConnectorsFromConfiguration(config =>
{
    config.DefaultScheme = "file";
    // Configure providers as needed
});
```

### Configurable Provider Implementation

```csharp
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Abstractions;

public class ConfigurableStorageProvider : IStorageProvider, IConfigurableStorageProvider
{
    public StorageScheme Scheme { get; private set; } = StorageScheme.Custom;

    public void Configure(IReadOnlyDictionary<string, string> settings)
    {
        // Apply configuration settings
        if (settings.TryGetValue("Scheme", out var scheme))
            Scheme = new StorageScheme(scheme);

        // Configure other properties...
    }

    // Implement IStorageProvider methods...
}

// Register with configuration
services.AddStorageProvidersFromConfiguration(config =>
{
    config.Providers["Custom"] = new StorageProviderConfig
    {
        ProviderType = "MyApp.Providers.ConfigurableStorageProvider",
        Enabled = true,
        Settings = new Dictionary<string, string>
        {
            ["Scheme"] = "custom",
            ["ConnectionString"] = "Server=myserver;Database=mydb;"
        }
    };
});
```

## Performance Considerations

### Stream Usage

- Always dispose streams properly to release resources
- Use appropriate buffer sizes for large file operations
- Consider using `FileStream` with `FileOptions.SequentialScan` for sequential reads

### Provider Resolution

- Provider resolution is cached after first use for performance
- Register providers explicitly to avoid reflection overhead
- Use scheme-specific providers when possible for better performance

### Async Operations

- All I/O operations are async-first to prevent thread pool starvation
- Use `ConfigureAwait(false)` in library code to avoid deadlocks
- Consider cancellation tokens for long-running operations

### Memory Management

- Stream-based operations minimize memory usage for large files
- Avoid loading entire files into memory when possible
- Use appropriate buffer sizes based on typical file sizes

## Requirements

- **.NET 8.0** or later
- **Microsoft.Extensions.DependencyInjection.Abstractions** 10.0.0 or later (for DI support)

## License

MIT License - see LICENSE file for details.

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Analyzers](https://www.nuget.org/packages/NPipeline.Analyzers)** - Roslyn analyzers for pipeline development
- **[NPipeline.Extensions](https://www.nuget.org/packages/NPipeline.Extensions)** - Additional pipeline components and utilities

## Support

- **Documentation**: [https://npipeline.readthedocs.io](https://npipeline.readthedocs.io)
- **Issues**: [GitHub Issues](https://github.com/npipeline/NPipeline/issues)
- **Discussions**: [GitHub Discussions](https://github.com/npipeline/NPipeline/discussions)
- **Discord**: [NPipeline Community](https://discord.gg/npipeline)
