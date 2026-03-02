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
using NPipeline.StorageProviders.Sftp;
using NPipeline.StorageProviders.Models;

// Register the provider
services.AddSftpStorageProvider(options =>
{
    options.DefaultHost = "sftp.example.com";
    options.DefaultUsername = "user";
    options.DefaultPassword = "password";
});

// Use the provider
var provider = serviceProvider.GetRequiredService<SftpStorageProvider>();
var uri = StorageUri.Parse("sftp://sftp.example.com/data/file.csv");

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
| `ConnectionTimeout`         | `30 seconds` | Timeout for establishing connections         |
| `ValidateServerFingerprint` | `true`       | Whether to validate server fingerprint       |
| `ExpectedFingerprint`       | `null`       | Expected server fingerprint                  |
| `ValidateOnAcquire`         | `true`       | Validate connection health before use        |

## API Reference

### SftpStorageProvider

The main implementation of `IStorageProvider` for SFTP operations.

#### Methods

| Method                                            | Description                                   |
|---------------------------------------------------|-----------------------------------------------|
| `OpenReadAsync(uri, cancellationToken)`           | Opens a readable stream for the specified URI |
| `OpenWriteAsync(uri, cancellationToken)`          | Opens a writable stream for the specified URI |
| `ExistsAsync(uri, cancellationToken)`             | Checks if a file exists at the specified URI  |
| `ListAsync(prefix, recursive, cancellationToken)` | Lists files at the specified prefix           |
| `GetMetadataAsync(uri, cancellationToken)`        | Gets metadata for the specified file          |

### SftpStorageException

Exception thrown when SFTP operations fail.

#### Properties

| Property    | Type            | Description          |
|-------------|-----------------|----------------------|
| `Host`      | `string?`       | The SFTP server host |
| `Path`      | `string?`       | The remote path      |
| `ErrorCode` | `SftpErrorCode` | The SFTP error code  |

#### Error Codes

| Code                   | Description                                  |
|------------------------|----------------------------------------------|
| `Unknown`              | An unknown error occurred                    |
| `ConnectionFailed`     | Connection to the SFTP server failed         |
| `AuthenticationFailed` | Authentication failed                        |
| `FileNotFound`         | The specified file was not found             |
| `PermissionDenied`     | Permission denied for the operation          |
| `PathNotFound`         | The specified path was not found             |
| `OperationTimeout`     | The operation timed out                      |
| `ConnectionLost`       | The connection was lost during the operation |

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

The provider translates SSH.NET exceptions to standard .NET exceptions:

| SSH.NET Exception               | .NET Exception                |
|---------------------------------|-------------------------------|
| `SshAuthenticationException`    | `UnauthorizedAccessException` |
| `SshConnectionException`        | `IOException`                 |
| `SftpPathNotFoundException`     | `FileNotFoundException`       |
| `SftpPermissionDeniedException` | `UnauthorizedAccessException` |
| `OperationCanceledException`    | `OperationCanceledException`  |

## Examples

### List Files Recursively

```csharp
var provider = serviceProvider.GetRequiredService<SftpStorageProvider>();
var uri = StorageUri.Parse("sftp://sftp.example.com/data/");

await foreach (var item in provider.ListAsync(uri, recursive: true))
{
    Console.WriteLine($"{item.Uri} - {item.Size} bytes - {(item.IsDirectory ? "Directory" : "File")}");
}
```

### Check File Exists

```csharp
var provider = serviceProvider.GetRequiredService<SftpStorageProvider>();
var uri = StorageUri.Parse("sftp://sftp.example.com/data/file.csv");

var exists = await provider.ExistsAsync(uri);
Console.WriteLine($"File exists: {exists}");
```

### Get File Metadata

```csharp
var provider = serviceProvider.GetRequiredService<SftpStorageProvider>();
var uri = StorageUri.Parse("sftp://sftp.example.com/data/file.csv");

var metadata = await provider.GetMetadataAsync(uri);

if (metadata != null)
{
    Console.WriteLine($"Size: {metadata.Size} bytes");
    Console.WriteLine($"Last Modified: {metadata.LastModified}");
    Console.WriteLine($"Is Directory: {metadata.IsDirectory}");
}
```

## License

This project is licensed under the MIT License.
