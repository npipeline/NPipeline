# NPipeline.StorageProviders.Sftp

SFTP storage provider for NPipeline that enables reading and writing files via SFTP (SSH File Transfer Protocol).

## Features

- **High Performance**: Connection pooling and keep-alive for optimal throughput
- **Great Developer Experience**: Simple configuration, intuitive URI scheme, clear error messages
- **Consistency**: Follows established patterns from existing storage providers
- **Security**: Support for both password and key-based authentication

## Installation

```bash
dotnet add package NPipeline.StorageProviders.Sftp
```

## Quick Start

### Basic Usage with Password Authentication

```csharp
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.DependencyInjection;
using NPipeline.StorageProviders.Sftp;
using NPipeline.StorageProviders.Models;

// Register the provider
services.AddSftpStorageProvider(options =>
{
    options.DefaultHost = "sftp.example.com";
    options.HostKeyFingerprints = ["SHA256:ohD8VZEXGWo6Ez8GSEJQ9WpafgLFsOfLOtGGQCQo6Og"]; // ssh-keyscan host | ssh-keygen -lf -
    options.DefaultUsername = "user";
    options.DefaultPassword = "password";
});
services.AddStorageResolver(); // routes sftp:// URIs to the provider

// Use the provider
var uri = StorageUri.Parse("sftp://sftp.example.com/data/file.csv");
var provider = serviceProvider.GetRequiredService<IStorageResolver>().Resolve(uri);

// Read a file
using var stream = await provider.OpenReadAsync(uri);
using var reader = new StreamReader(stream);
var content = await reader.ReadToEndAsync();

// Write a file
using var writeStream = await provider.OpenWriteAsync(uri);
using var writer = new StreamWriter(writeStream);
await writer.WriteAsync("Hello, SFTP!");
```

### Key-Based Authentication

```csharp
services.AddSftpStorageProvider(options =>
{
    options.DefaultHost = "sftp.example.com";
    options.HostKeyFingerprints = ["SHA256:ohD8VZEXGWo6Ez8GSEJQ9WpafgLFsOfLOtGGQCQo6Og"]; // ssh-keyscan host | ssh-keygen -lf -
    options.DefaultUsername = "user";
    options.DefaultKeyPath = "/home/user/.ssh/id_rsa";
    options.DefaultKeyPassphrase = "passphrase"; // Optional
});
```

### URI-Based Configuration

Credentials can be specified in the URI for per-operation configuration:

```csharp
// Password via URI
var uri = StorageUri.Parse("sftp://sftp.example.com/data/file.csv?username=user&password=secret");

// Key via URI
var uri = StorageUri.Parse("sftp://sftp.example.com/data/file.csv?username=user&keyPath=/home/user/.ssh/id_rsa");

// Custom port
var uri = StorageUri.Parse("sftp://sftp.example.com:2222/data/file.csv");
```

### High-Performance Configuration

For high-throughput scenarios, tune the connection pool settings:

```csharp
services.AddSftpStorageProvider(options =>
{
    options.DefaultHost = "sftp.example.com";
    options.HostKeyFingerprints = ["SHA256:ohD8VZEXGWo6Ez8GSEJQ9WpafgLFsOfLOtGGQCQo6Og"]; // ssh-keyscan host | ssh-keygen -lf -
    options.DefaultUsername = "user";
    options.DefaultKeyPath = "/home/user/.ssh/id_rsa";
    options.MaxPoolSize = 20;
    options.ConnectionIdleTimeout = TimeSpan.FromMinutes(10);
    options.KeepAliveInterval = TimeSpan.FromSeconds(15);
    options.ConnectionTimeout = TimeSpan.FromSeconds(15);
    options.ValidateOnAcquire = true;
});
```

## URI Scheme

The provider uses the `sftp://` scheme:

```
sftp://hostname/path/to/file.csv
sftp://hostname:2222/path/to/file.csv?username=user&password=secret
```

### URI Components

| Component      | Source                      | Example                           |
|----------------|-----------------------------|-----------------------------------|
| Scheme         | Fixed                       | `sftp`                            |
| Host           | URI host                    | `sftp.example.com`                |
| Port           | URI port or default         | `22` (default) or `2222`          |
| Path           | URI path                    | `/data/imports/file.csv`          |
| Username       | URI userinfo or query param | `user`                            |
| Password       | Query param                 | `?password=secret`                |
| Key Path       | Query param                 | `?keyPath=/home/user/.ssh/id_rsa` |
| Key Passphrase | Query param                 | `?keyPassphrase=secret`           |

## Configuration Options

| Option                      | Default      | Description                                  |
|-----------------------------|--------------|----------------------------------------------|
| `DefaultHost`               | `null`       | Default host for SFTP connections            |
| `DefaultPort`               | `22`         | Default port for SFTP connections            |
| `DefaultUsername`           | `null`       | Default username for authentication          |
| `DefaultPassword`           | `null`       | Default password for password authentication |
| `DefaultKeyPath`            | `null`       | Path to the private key file                 |
| `DefaultKeyPassphrase`      | `null`       | Passphrase for the private key               |
| `MaxPoolSize`               | `10`         | Maximum connections in the pool              |
| `ConnectionIdleTimeout`     | `5 minutes`  | Time before idle connections are cleaned up  |
| `KeepAliveInterval`         | `30 seconds` | Interval for keep-alive packets              |
| `ConnectionTimeout`         | `30 seconds` | Timeout for establishing connections (cancellable) |
| `HostKeyFingerprints`       | empty        | SHA-256 host key fingerprints to trust (`SHA256:...`); required unless `AcceptAnyHostKey` |
| `AcceptAnyHostKey`          | `false`      | Trust any host key (local development and tests only) |
| `ValidateOnAcquire`         | `true`       | Validate connection health before use        |

## API Reference

### SftpStorageProvider

Derives from `StorageProvider` and implements `IStorageProvider`. `AddSftpStorageProvider` registers it as an `IStorageProvider`, so resolve it through `IStorageResolver` (add `AddStorageResolver()`), not as the concrete type.

#### Capabilities

`Read | Write | List | Delete | Move | AtomicMove | Hierarchy`. A move is an SFTP rename, which is atomic on POSIX servers. When the destination exists, the provider removes it first because a plain rename does not overwrite, so only a move onto a new path is a single atomic step.

#### Methods

| Method                                                   | Description                                                                               |
|----------------------------------------------------------|-------------------------------------------------------------------------------------------|
| `OpenReadAsync(uri, cancellationToken)`                  | Opens a readable stream. Throws `FileNotFoundException` when the file is missing          |
| `OpenWriteAsync(uri, options, cancellationToken)`        | Opens a writable stream, creating missing parent directories and truncating an old file   |
| `ExistsAsync(uri, cancellationToken)`                    | Returns whether a file or directory exists                                                |
| `ListAsync(directory, recursive, cancellationToken)`     | Lists a directory. Non-recursive listings include directory entries; recursive ones list files only |
| `GetMetadataAsync(uri, cancellationToken)`               | Gets metadata, or `null` when the path is missing                                         |
| `DeleteAsync(uri, cancellationToken)`                    | Deletes a file. Deleting a missing file succeeds                                          |
| `MoveAsync(source, destination, cancellationToken)`      | Renames a file on the same server, overwriting the destination                            |

Directory entries from `ListAsync` have a URI that ends with `/`, and a `null` `Size` and `LastModified`. A `LastModified` the server does not report is `null`.

## Connection Pooling

The provider uses a connection pool for high-performance scenarios:

- **Connection Reuse**: Avoids the overhead of establishing new SSH connections
- **Concurrency Control**: Limits total connections to prevent server overload
- **Health Management**: Ensures connections remain valid before use
- **Automatic Cleanup**: Removes stale or failed connections

### Performance Benefits

| Scenario           | Without Pool | With Pool |
|--------------------|--------------|-----------|
| Single operation   | ~500ms       | ~500ms    |
| 10 sequential ops  | ~5000ms      | ~600ms    |
| 10 concurrent ops  | ~5000ms      | ~1000ms   |
| 100 sequential ops | ~50s         | ~3s       |

## Error Handling

The provider translates SSH.NET and socket failures to standard .NET exceptions, with the original as the inner exception:

| SSH.NET Exception                               | .NET Exception                |
|-------------------------------------------------|-------------------------------|
| `SftpPathNotFoundException`                     | `FileNotFoundException`       |
| `SftpPermissionDeniedException`                 | `UnauthorizedAccessException` |
| `SshAuthenticationException`                    | `UnauthorizedAccessException` |
| `SshConnectionException`, other `SshException`, `SocketException` | `IOException`  |
| `OperationCanceledException`                    | `OperationCanceledException`  |

`GetMetadataAsync` returns `null`, `ExistsAsync` returns `false`, and `DeleteAsync` succeeds when the path is missing.

## Examples

### List Files Recursively

```csharp
var uri = StorageUri.Parse("sftp://sftp.example.com/data/");
var provider = serviceProvider.GetRequiredService<IStorageResolver>().Resolve(uri);

await foreach (var item in provider.ListAsync(uri, recursive: true))
{
    Console.WriteLine($"{item.Uri} - {item.Size} bytes - {(item.IsDirectory ? "Directory" : "File")}");
}
```

### Check File Exists

```csharp
var uri = StorageUri.Parse("sftp://sftp.example.com/data/file.csv");
var provider = serviceProvider.GetRequiredService<IStorageResolver>().Resolve(uri);

var exists = await provider.ExistsAsync(uri);
Console.WriteLine($"File exists: {exists}");
```

### Get File Metadata

```csharp
var uri = StorageUri.Parse("sftp://sftp.example.com/data/file.csv");
var provider = serviceProvider.GetRequiredService<IStorageResolver>().Resolve(uri);

var metadata = await provider.GetMetadataAsync(uri);

if (metadata != null)
{
    Console.WriteLine($"Size: {metadata.Size} bytes");
    Console.WriteLine($"Last Modified: {metadata.LastModified}");
    Console.WriteLine($"Is Directory: {metadata.IsDirectory}");
}
```

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.
