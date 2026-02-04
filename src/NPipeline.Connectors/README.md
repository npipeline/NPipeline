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

## Requirements

- **.NET 8.0**, **9.0**, or **10.0**
- **Microsoft.Extensions.DependencyInjection.Abstractions** 10.0.0 or later (for DI support)

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
var resolver = StorageProviderFactory.CreateResolver();

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

### Database Connector Abstractions

NPipeline.Connectors also provides database-agnostic abstractions for implementing database connectors (PostgreSQL, SQL Server, MySQL, Oracle, etc.). These
abstractions enable:

- **Unified Database API**: Common interfaces for database operations across different database systems
- **Extensible Base Classes**: Ready-to-use base classes for source and sink nodes
- **Configuration Management**: Standardized configuration with validation
- **Error Handling**: Comprehensive exception hierarchy for database errors
- **Security**: Built-in SQL injection prevention through identifier validation
- **Retry Logic**: Configurable retry policies for transient errors

#### Core Interfaces

**IDatabaseConnection** - Database connection abstraction:

```csharp
public interface IDatabaseConnection : IAsyncDisposable
{
    bool IsOpen { get; }
    Task OpenAsync(CancellationToken cancellationToken = default);
    Task CloseAsync(CancellationToken cancellationToken = default);
    Task<IDatabaseCommand> CreateCommandAsync(CancellationToken cancellationToken = default);
}
```

**IDatabaseCommand** - Database command abstraction:

```csharp
public interface IDatabaseCommand : IAsyncDisposable
{
    string CommandText { get; set; }
    int CommandTimeout { get; set; }
    System.Data.CommandType CommandType { get; set; }
    void AddParameter(string name, object? value);
    Task<IDatabaseReader> ExecuteReaderAsync(CancellationToken cancellationToken = default);
    Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default);
}
```

**IDatabaseReader** - Database reader abstraction:

```csharp
public interface IDatabaseReader : IAsyncDisposable
{
    bool HasRows { get; }
    int FieldCount { get; }
    string GetName(int ordinal);
    Type GetFieldType(int ordinal);
    Task<bool> ReadAsync(CancellationToken cancellationToken = default);
    Task<bool> NextResultAsync(CancellationToken cancellationToken = default);
    T? GetFieldValue<T>(int ordinal);
    bool IsDBNull(int ordinal);
}
```

**IDatabaseWriter<T>** - Database writer abstraction:

```csharp
public interface IDatabaseWriter<T>
{
    Task WriteAsync(T item, CancellationToken cancellationToken = default);
    Task WriteBatchAsync(IEnumerable<T> items, CancellationToken cancellationToken = default);
    Task FlushAsync(CancellationToken cancellationToken = default);
}
```

**IDatabaseMapper<T>** - Database mapper abstraction:

```csharp
public interface IDatabaseMapper<T>
{
    T MapFromReader(IDatabaseReader reader);
    IEnumerable<DatabaseParameter> MapToParameters(T item);
}
```

#### Base Classes

**DatabaseSourceNode<TReader, T>** - Base class for database source nodes:

```csharp
public abstract class DatabaseSourceNode<TReader, T> : SourceNode<T>
    where TReader : IDatabaseReader
{
    protected abstract Task<IDatabaseConnection> GetConnectionAsync(CancellationToken cancellationToken);
    protected abstract Task<TReader> ExecuteQueryAsync(IDatabaseConnection connection, CancellationToken cancellationToken);
    protected abstract T MapRow(TReader reader);

    protected virtual bool StreamResults => false;
    protected virtual int FetchSize => 100;
    protected virtual DeliverySemantic DeliverySemantic => DeliverySemantic.AtLeastOnce;
    protected virtual CheckpointStrategy CheckpointStrategy => CheckpointStrategy.None;
}
```

**DatabaseSinkNode<T>** - Base class for database sink nodes:

```csharp
public abstract class DatabaseSinkNode<T> : SinkNode<T>
{
    protected abstract Task<IDatabaseConnection> GetConnectionAsync(CancellationToken cancellationToken);
    protected abstract Task<IDatabaseWriter<T>> CreateWriterAsync(IDatabaseConnection connection, CancellationToken cancellationToken);

    protected virtual bool UseTransaction => false;
    protected virtual int BatchSize => 100;
    protected virtual DeliverySemantic DeliverySemantic => DeliverySemantic.AtLeastOnce;
    protected virtual CheckpointStrategy CheckpointStrategy => CheckpointStrategy.None;
    protected virtual bool ContinueOnError => false;
}
```

**DatabaseConfigurationBase** - Base configuration class:

```csharp
public abstract class DatabaseConfigurationBase
{
    public string ConnectionString { get; set; } = string.Empty;
    public int CommandTimeout { get; set; } = 30;
    public int ConnectionTimeout { get; set; } = 15;
    public int MinPoolSize { get; set; } = 1;
    public int MaxPoolSize { get; set; } = 100;
    public bool ValidateIdentifiers { get; set; } = true;
    public DeliverySemantic DeliverySemantic { get; set; } = DeliverySemantic.AtLeastOnce;
    public CheckpointStrategy CheckpointStrategy { get; set; } = CheckpointStrategy.None;

    public virtual void Validate();
}
```

#### Configuration Enums

**DeliverySemantic** - Delivery semantics for database operations:

```csharp
public enum DeliverySemantic
{
    AtLeastOnce,  // Items may be delivered multiple times but never lost
    AtMostOnce,   // Items may be lost but never delivered multiple times
    ExactlyOnce     // Items are delivered exactly once (commercial feature)
}
```

**CheckpointStrategy** - Checkpoint strategies for recovery:

```csharp
public enum CheckpointStrategy
{
    None,      // No checkpointing (free version)
    Offset,     // Offset-based checkpointing (commercial feature)
    KeyBased,   // Key-based checkpointing (commercial feature)
    Cursor,     // Cursor-based checkpointing (commercial feature)
    CDC          // Change Data Capture checkpointing (commercial feature)
}
```

#### Utilities

**DatabaseRetryPolicy** - Retry policy for transient errors:

```csharp
var retryPolicy = new DatabaseRetryPolicy
{
    MaxRetryAttempts = 3,
    InitialDelay = TimeSpan.FromSeconds(1),
    MaxDelay = TimeSpan.FromSeconds(30),
    ShouldRetry = ex => DatabaseErrorClassifier.IsTransientError(ex)
};

var result = await retryPolicy.ExecuteAsync(async ct =>
    await ExecuteDatabaseOperation(ct));
```

**DatabaseErrorClassifier** - Error classification:

```csharp
bool isTransient = DatabaseErrorClassifier.IsTransientError(exception);
bool isConnectionError = DatabaseErrorClassifier.IsConnectionError(exception);
bool isMappingError = DatabaseErrorClassifier.IsMappingError(exception);
bool isConstraintViolation = DatabaseErrorClassifier.IsConstraintViolation(exception);
bool isSyntaxError = DatabaseErrorClassifier.IsSyntaxError(exception);
```

**DatabaseConnectionStringBuilder** - Connection string utilities:

```csharp
// Build connection string from parameters
var parameters = new Dictionary<string, string>
{
    ["Server"] = "localhost",
    ["Database"] = "mydb",
    ["Port"] = "5432"
};
var connectionString = DatabaseConnectionStringBuilder.BuildConnectionString(parameters);

// Parse connection string into parameters
var parsed = DatabaseConnectionStringBuilder.ParseConnectionString(connectionString);
```

**DatabaseIdentifierValidator** - SQL injection prevention:

```csharp
// Validate identifier
if (DatabaseIdentifierValidator.IsValidIdentifier(tableName))
{
    // Safe to use in SQL
}

// Quote identifier for safe SQL usage
var quoted = DatabaseIdentifierValidator.QuoteIdentifier(tableName, "\"");

// Validate and throw if invalid
DatabaseIdentifierValidator.ValidateIdentifier(tableName, nameof(tableName));
```

#### Exceptions

**DatabaseExceptionBase** - Base exception class:

```csharp
public abstract class DatabaseExceptionBase : Exception
{
    public string? ErrorCode { get; }
    public int? SqlState { get; }
}
```

**Specific Exception Types**:

- `DatabaseException` - Generic database errors
- `DatabaseConnectionException` - Connection-related errors
- `DatabaseMappingException` - Mapping errors with property name
- `DatabaseOperationException` - Operation errors with error code and SQL state
- `DatabaseParameter` - Record for database parameters

#### Dependency Injection

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.DependencyInjection;

// Add database options
var services = new ServiceCollection();
services.AddDatabaseOptions(options =>
{
    options.DefaultConnectionString = "Server=localhost;Database=mydb;";
    options.NamedConnections["ReadOnly"] = "Server=localhost;Database=mydb;ReadOnly=true;";
});

// Add database options from configuration
services.AddDatabaseOptions<MyDatabaseOptions>("Database");
```

## Database Storage Providers

NPipeline.Connectors ecosystem includes database storage providers that enable environment-aware configuration through URI-based connections. This approach
allows seamless switching between local development databases and cloud-hosted databases (e.g., AWS RDS, Azure SQL) by simply changing a URI.

### PostgreSQL URI Format

```
postgres://user:pass@host:port/database?sslmode=require
```

### SQL Server URI Format

```
mssql://user:pass@host:port/database?encrypt=true
```

### Environment Switching Example

```csharp
// Development environment
var devUri = StorageUri.Parse("postgres://localhost:5432/mydb?username=postgres&password=devpass");

// Production environment (AWS RDS)
var prodUri = StorageUri.Parse("postgres://mydb.prod.ap-southeast-2.rds.amazonaws.com:5432/mydb?username=produser&password=${DB_PASSWORD}");

// Same pipeline code works in both environments
var source = new PostgresSourceNode<Customer>(uri: devUri, query: "SELECT * FROM customers");

// Switch to production by changing the URI
var prodSource = new PostgresSourceNode<Customer>(uri: prodUri, query: "SELECT * FROM customers");
```

### Benefits

- **Environment-Aware Configuration**: Store database URIs in configuration files (appsettings.json, environment variables)
- **Easy Switching**: Change environments without code modifications
- **Unified API**: Consistent interface across different database systems
- **Secure Credential Management**: Use environment variable expansion for passwords

### Connector-Specific Documentation

For detailed URI parameters and usage examples specific to each database connector, see:

- [PostgreSQL Connector README](../NPipeline.Connectors.PostgreSQL/README.md)
- [SQL Server Connector README](../NPipeline.Connectors.SqlServer/README.md)

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
var resolver = StorageProviderFactory.CreateResolver();
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

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Analyzers](https://www.nuget.org/packages/NPipeline.Analyzers)** - Roslyn analyzers for pipeline development
- **[NPipeline.Extensions](https://www.nuget.org/packages/NPipeline.Extensions)** - Additional pipeline components and utilities

## License

MIT License - see LICENSE file for details.
