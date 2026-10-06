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
- **Configuration-driven Setup**: Provider options bind from `IConfiguration` like any other options object; providers themselves are registered in code
- **Cross-platform Compatibility**: Works on Windows, Linux, and macOS

## Key Components

### StorageResolver

`StorageResolver` (in `NPipeline.StorageProviders`) is immutable: build it once from the providers you want. Two
providers that serve the same scheme make the constructor throw `ArgumentException`, so a misconfiguration fails at
startup instead of silently picking one provider:

```csharp
var resolver = new StorageResolver(new IStorageProvider[]
{
    new FileSystemStorageProvider(),
    new AwsS3StorageProvider(s3ClientFactory, s3Options),
});

// Resolve a provider for a specific URI
var fileUri = StorageUri.FromFilePath("./data/input.csv");
var provider = resolver.Resolve(fileUri); // throws StorageProviderNotFoundException if none serves the scheme

// Or, without throwing:
if (resolver.TryResolve(fileUri, out var maybeProvider)) { /* ... */ }

// All registered providers
var providers = resolver.Providers;
```

`StorageResolver.Default` is a shared resolver that serves the file system only. See
[Storage Providers Overview](../../docs/storage-providers/index.md) for the full contract, capabilities and DI story.

### FileSystemStorageProvider

The built-in `FileSystemStorageProvider` handles local file system operations with the "file" scheme:

```csharp
using NPipeline.StorageProviders;

var provider = new FileSystemStorageProvider();
var fileUri = StorageUri.FromFilePath("./data/output.csv");

// Check if file exists
bool exists = await provider.ExistsAsync(fileUri);

// Open file for reading
using var readStream = await provider.OpenReadAsync(fileUri);

// Open file for writing (creates directories as needed).
// The file appears only after CommitAsync; disposing without it discards the data.
await using var writeStream = await provider.OpenWriteAsync(fileUri);
await writeStream.WriteAsync(bytes);
await writeStream.CommitAsync();

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

NPipeline.Connectors also provides database-agnostic abstractions for implementing database connectors (PostgreSQL, SQL Server, MySQL, etc.). These
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

**IDatabaseConnectionProvider** - Opens a connection, or builds a connection string, from a `StorageUri`:

```csharp
public interface IDatabaseConnectionProvider
{
    string GetConnectionString(StorageUri uri);
    Task<IDatabaseConnection> OpenConnectionAsync(StorageUri uri, CancellationToken cancellationToken = default);
}
```

Each database connector (Postgres, MySQL, SQL Server, Snowflake, Cosmos, Mongo, DuckDB) has its own
`IDatabaseConnectionProvider` implementation. It replaces the storage-provider-based resolution these connectors used
previously: a database connection is never looked up through `IStorageResolver`, because a database isn't a storage
provider. A SQL node that isn't given a provider explicitly falls back to its connector's default one.

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
    ExactlyOnce   // Items are delivered exactly once using transactional semantics
}
```

**CheckpointStrategy** - Checkpoint strategies for recovery:

```csharp
public enum CheckpointStrategy
{
    None,      // No checkpointing
    InMemory,  // In-process checkpoint state (lost on restart)
    Offset,    // Offset-based checkpointing using a monotonic column
    KeyBased,  // Key-based checkpointing using composite keys
    Cursor,    // Cursor-based checkpointing
    CDC        // Change Data Capture checkpointing (WAL/LSN position)
}
```

#### Utilities

**DatabaseUriParser** - Turns a `StorageUri` (`postgres://user:pass@host:port/database?sslmode=require`, and similarly
for the other schemes) into the pieces a connector's connection-string builder needs: host, port, database, user name,
password and any extra parameters. Each connector classifies its own driver's errors as transient, instead of a shared
classifier: see the connector's resilience section (for example `SqlServerConnectorResilience`,
`MongoConnectorResilience`).

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
    public string? SqlState { get; }   // a five-character SQLSTATE code, such as "23505" or "HY000"
}
```

**Specific Exception Types**:

- `DatabaseException` - Generic database errors
- `DatabaseConnectionException` - Connection-related errors
- `DatabaseMappingException` - Mapping errors with property name
- `DatabaseOperationException` - Operation errors with error code and SQL state

`DatabaseParameter` is a separate record for database parameters, not an exception type.

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

## Database Connection Providers

Each database connector has an `IDatabaseConnectionProvider` that turns a `StorageUri` into a connection, enabling
environment-aware configuration through URI-based connections. This approach allows seamless switching between local
development databases and cloud-hosted databases (e.g., AWS RDS, Azure SQL) by simply changing a URI.

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
var source = PostgresConnector.Source<Customer>(devUri, "SELECT * FROM customers");

// Switch to production by changing the URI
var prodSource = PostgresConnector.Source<Customer>(prodUri, "SELECT * FROM customers");
```

See [SQL Connectors: Shared Behaviour](../../docs/connectors/sql-connectors.md) for the full URI format, credential
handling and the node options every SQL source and sink shares.

### Benefits

- **Environment-Aware Configuration**: Store database URIs in configuration files (appsettings.json, environment variables)
- **Easy Switching**: Change environments without code modifications
- **Unified API**: Consistent interface across different database systems
- **Secure Credential Management**: Use environment variable expansion for passwords

### Connector-Specific Documentation

For detailed URI parameters and usage examples specific to each database connector, see:

- [PostgreSQL Connector README](../NPipeline.Connectors.Postgres/README.md)
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

Database schemes (`postgres`, `mssql`, `mysql`, `snowflake`, `cosmos`, `mongodb`, ...) are not storage schemes: a
database connection is opened through the connector's own `IDatabaseConnectionProvider`, not through `IStorageResolver`.

## Usage Examples

### Basic File System Access

```csharp
using NPipeline.Connectors;
using NPipeline.StorageProviders;

// Create a file URI from a path
var inputUri = StorageUri.FromFilePath("./data/input.csv");
var outputUri = StorageUri.FromFilePath("./data/output.csv");

// A resolver that serves the file system only
var resolver = StorageResolver.Default;
var provider = resolver.Resolve(inputUri);

// Read from file
using var inputStream = await provider.OpenReadAsync(inputUri);
using var reader = new StreamReader(inputStream);
var content = await reader.ReadToEndAsync();

// Write to file
await using var outputStream = await provider.OpenWriteAsync(outputUri);
await using (var writer = new StreamWriter(outputStream, leaveOpen: true)) // leaveOpen keeps the stream open for the commit
{
    await writer.WriteAsync(content.ToUpperInvariant());
}

await outputStream.CommitAsync(); // the file appears now; disposing without a commit discards the data
```

### Provider Registration with Dependency Injection

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors;
using NPipeline.Connectors.DependencyInjection;

// Configure services
var services = new ServiceCollection();

// Add cloud providers; each package's AddXxxStorageProvider also registers IStorageProvider
services.AddAwsS3StorageProvider(options => options.DefaultRegion = RegionEndpoint.USWest2);
services.AddAzureBlobStorageProvider(options => options.DefaultConnectionString = "...");

// Add a custom provider instance
services.AddStorageProvider(new CustomStorageProvider());

// Build the resolver from every IStorageProvider above, plus the file system provider
services.AddStorageResolver(includeFileSystem: true);

// Build service provider
var serviceProvider = services.BuildServiceProvider();

// Resolve and use the storage resolver
var resolver = serviceProvider.GetRequiredService<IStorageResolver>();
var s3Uri = StorageUri.Parse("s3://my-bucket/data/input.csv");
var provider = resolver.Resolve(s3Uri);
```

### Custom Provider Example

Derive from the `StorageProvider` base class (in `NPipeline.StorageProviders.Abstractions`) rather than implementing
`IStorageProvider` directly: it validates arguments, observes cancellation and rejects operations you don't declare in
`Capabilities`, so your code only implements the `...CoreAsync` methods for the capabilities you support.

```csharp
public sealed class FtpStorageProvider : StorageProvider
{
    private static readonly IReadOnlyList<StorageScheme> Supported = [new StorageScheme("ftp")];

    public override string Name => "FTP";
    public override IReadOnlyList<StorageScheme> Schemes => Supported;

    public override StorageCapabilities Capabilities =>
        StorageCapabilities.Read | StorageCapabilities.Write | StorageCapabilities.List | StorageCapabilities.Hierarchy;

    protected override async Task<Stream> OpenReadCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var client = new FtpClient(uri.Host);
        await client.ConnectAsync(cancellationToken);
        return await client.OpenReadAsync(uri.Path, cancellationToken);
    }

    protected override async Task<StorageWriteStream> OpenWriteCoreAsync(
        StorageUri uri, StorageWriteOptions? options, CancellationToken cancellationToken)
    {
        // FtpWriteStream derives from StorageWriteStream (or SpooledWriteStream / ChunkedUploadStream). Nothing
        // appears at the target until CommitAsync; disposing without a commit discards what was written.
        var client = new FtpClient(uri.Host);
        await client.ConnectAsync(cancellationToken);
        return new FtpWriteStream(client, uri.Path);
    }

    // Implement the other ...CoreAsync methods for the capabilities you declare.
}
```

See [Custom Storage Provider](../../docs/storage-providers/custom-provider.md) for the full contract (error
translation, the conformance test suite, client caching) and the chunked-upload and spooled-write base classes.

## Configuration

### Provider Registration

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.DependencyInjection;

var services = new ServiceCollection();

// Register individual providers, each as a singleton
services.AddStorageProvider<FileSystemStorageProvider>();
services.AddAwsS3StorageProvider(options => options.DefaultRegion = RegionEndpoint.USWest2);

// Or register a pre-built instance, for example when more than one of the same type must coexist
// (two S3-compatible endpoints, each given its own scheme)
services.AddStorageProvider(new S3CompatibleStorageProvider(minioClientFactory, minioOptions));

// Build the resolver from every IStorageProvider registered above (including those registered after this call)
services.AddStorageResolver(includeFileSystem: false); // false: this example added the file system provider explicitly
```

Provider options are plain objects with no configuration-section binding of their own: bind them from
`IConfiguration` the normal ASP.NET Core way (`services.Configure<AwsS3StorageProviderOptions>(configuration.GetSection("S3"))`,
or read values into the options object yourself) and keep secrets out of `appsettings.json` in source control.

## Performance Considerations

### Stream Usage

- Always dispose streams properly to release resources. For a write stream, call `CommitAsync` first: disposing without a commit discards the data
- Use appropriate buffer sizes for large file operations
- Consider using `FileStream` with `FileOptions.SequentialScan` for sequential reads

### Provider Resolution

- `StorageResolver` looks a provider up by scheme in a `FrozenDictionary` built once at construction, so resolving a
  URI is one dictionary lookup; there's no reflection or discovery at resolve time
- Two providers that serve the same scheme fail at construction (`ArgumentException`), not on first resolve
- Give a provider its own scheme (or pass it to a node explicitly with `Provider = ...`) when you need two instances
  of the same provider type behind one resolver, for example two S3 accounts

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
- **[NPipeline.Extensions](https://www.nuget.org/packages/NPipeline.Extensions)** - Additional pipeline components and utilities

## License

This package is licensed under the [MIT License](LICENSE.txt). You are free to use, modify, and distribute it in personal, open-source, and commercial projects without restriction.
