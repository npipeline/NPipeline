---
title: SFTP Storage Provider
description: Read from and write to SFTP servers using the SFTP storage provider.
sidebar_position: 6
---

## SFTP Storage Provider

The SFTP storage provider enables NPipeline applications to read from and write to SFTP servers using a unified storage abstraction. This provider implements the `IStorageProvider` interface and supports the `sftp://` URI scheme.

### Overview

The SFTP provider offers:

- **Stream-based I/O** for efficient handling of large files
- **Async-first API** for scalable, non-blocking operations
- **Flexible authentication** via password or private key (with optional passphrase)
- **Connection pooling** with configurable pool size for high performance
- **Keep-alive support** to reduce latency and maintain connection health
- **Comprehensive error handling** with proper exception translation
- **Metadata support** for retrieving file metadata
- **Listing operations** with recursive and non-recursive modes
- **Server fingerprint validation** for enhanced security

### When to Use This Provider

Use the SFTP provider when your application needs to:

- Store and retrieve data from SFTP servers
- Integrate SFTP storage into NPipeline data streamlines
- Work with on-premises or managed SFTP storage systems
- Handle large files through streaming and connection pooling
- Support both password and key-based SSH authentication

## Dependencies

The SFTP provider depends on the following packages:

- `SSH.NET` - SSH and SFTP client library
- `NPipeline.StorageProviders` - Core storage abstractions (IStorageProvider, StorageUri, StorageItem, StorageMetadata, StorageProviderMetadata, StorageResolverOptions, StorageProviderFactory)
- `NPipeline.Connectors` - Core connectors for using storage providers with connectors

### Key Storage Types

> **Note:** Shared storage types (IStorageProvider, StorageUri, StorageItem, StorageMetadata, etc.) are common across all NPipeline storage providers. Refer to the [Storage Provider Interface](./storage-provider.md) documentation for details.

SFTP-specific configuration type:

- **`SftpStorageProviderOptions`** - Configuration options for the SFTP provider (host, port, username, password, key path, connection pooling, keep-alive settings)

## Installation

### Prerequisites

- .NET 8.0 or later
- Access to an SFTP server
- Appropriate permissions for read/write operations

### Package Installation

Add the project reference to your solution:

```bash
dotnet add src/NPipeline.StorageProviders.Sftp/NPipeline.StorageProviders.Sftp.csproj
```

## Quick Start

### Basic Usage with Connectors

The SFTP provider works seamlessly with all NPipeline connectors. Here's a quick example using the CSV connector:

```csharp
using NPipeline;
using NPipeline.Connectors;
using NPipeline.Connectors.Csv;
using NPipeline.StorageProviders.Sftp;

// Create a resolver with SFTP support
var sftpOptions = new SftpStorageProviderOptions
{
    DefaultHost = "sftp.example.com",
    DefaultUsername = "user",
    DefaultPassword = "password",
};

var resolver = StorageProviderFactory.CreateResolver(
    new StorageResolverOptions
    {
        IncludeFileSystem = true,
        AdditionalProviders = new[] { new SftpStorageProvider(
            new SftpClientFactory(sftpOptions),
            sftpOptions) }
    }
);

public sealed record User(int Id, string Name, string Email);

public sealed class SftpCsvPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Read CSV from SFTP
        var sourceNode = new CsvSourceNode<User>(
            StorageUri.Parse("sftp://server.example.com:22/data/users.csv"),
            row => new User(
                row.Get<int>("Id") ?? 0,
                row.Get<string>("Name") ?? string.Empty,
                row.Get<string>("Email") ?? string.Empty),
            resolver: resolver);
        var source = builder.AddSource(sourceNode, "sftp_csv_source");
        
        // ... add transforms ...
        
        // Write CSV to SFTP
        var sinkNode = new CsvSinkNode<UserSummary>(
            StorageUri.Parse("sftp://server.example.com:22/output/summaries.csv"),
            resolver: resolver);
        var sink = builder.AddSink(sinkNode, "sftp_csv_sink");
        
        builder.Connect(source, sink);
    }
}
```

## Configuration

### Using Dependency Injection

The recommended way to configure the SFTP provider is through dependency injection:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Sftp;

var services = new ServiceCollection();

services.AddSftpStorageProvider(options =>
{
    options.DefaultHost = "sftp.example.com";
    options.DefaultPort = 22;
    options.DefaultUsername = "username";
    options.DefaultPassword = "password"; // or use DefaultKeyPath
    options.MaxPoolSize = 10;
    options.KeepAliveInterval = TimeSpan.FromSeconds(30);
    options.ConnectionTimeout = TimeSpan.FromSeconds(30);
});
```

### URI Format

SFTP URIs follow the standard format:

```
sftp://[host]:[port]/path/to/file
```

Examples:

```
sftp://server.example.com:22/data/users.csv
sftp://192.168.1.100/home/user/documents/report.csv
```

If port is omitted, the default port 22 is used.

### Authentication

The SFTP provider supports two authentication methods:

#### Password Authentication

```csharp
services.AddSftpStorageProvider(options =>
{
    options.DefaultHost = "sftp.example.com";
    options.DefaultUsername = "username";
    options.DefaultPassword = "password";
});
```

#### Private Key Authentication

```csharp
services.AddSftpStorageProvider(options =>
{
    options.DefaultHost = "sftp.example.com";
    options.DefaultUsername = "username";
    options.DefaultKeyPath = "/path/to/private/key"; // e.g., ~/.ssh/id_rsa
    options.DefaultKeyPassphrase = "passphrase"; // if key is encrypted
});
```

### Connection Pooling

The SFTP provider maintains a connection pool to improve performance. Configure pool settings:

```csharp
services.AddSftpStorageProvider(options =>
{
    options.MaxPoolSize = 10;                                // Maximum connections in pool
    options.ConnectionIdleTimeout = TimeSpan.FromMinutes(5); // Timeout before returning to pool
    options.KeepAliveInterval = TimeSpan.FromSeconds(30);    // Keep-alive ping interval
    options.ConnectionTimeout = TimeSpan.FromSeconds(30);    // Connection timeout
});
```

### Security

#### Server Fingerprint Validation

By default, the SFTP provider validates the server's SSH fingerprint:

```csharp
services.AddSftpStorageProvider(options =>
{
    options.ValidateServerFingerprint = true;
    // options.ExpectedFingerprint = "expected-fingerprint-here"; // Optional: set known fingerprint
});
```

On first connection without a known fingerprint, the server's fingerprint is accepted. For production, specify the expected fingerprint to prevent man-in-the-middle attacks.

#### Connection Health Validation

The provider validates connection health before returning connections from the pool:

```csharp
services.AddSftpStorageProvider(options =>
{
    options.ValidateOnAcquire = true; // Validate connection is still alive when acquiring from pool
});
```

## Advanced Topics

### Pre-configured Options

You can create an options object and pass it directly:

```csharp
var options = new SftpStorageProviderOptions
{
    DefaultHost = "sftp.example.com",
    DefaultUsername = "user",
    DefaultPassword = "pass",
    MaxPoolSize = 20
};

services.AddSftpStorageProvider(options);
```

### Using Different SFTP Servers

If your pipeline needs to access multiple SFTP servers, you can create multiple resolver instances or handle different hosts through connection pooling based on the URI:

```csharp
// The URI determines which host is used
var uri1 = StorageUri.Parse("sftp://server1.example.com/path/file1.csv");
var uri2 = StorageUri.Parse("sftp://server2.example.com/path/file2.csv");

// Both use the same authentication configured in options
var stream1 = await provider.OpenReadAsync(uri1);
var stream2 = await provider.OpenReadAsync(uri2);
```

## Error Handling

The SFTP provider translates common SFTP exceptions into `SftpStorageException`:

```csharp
try
{
    var stream = await provider.OpenReadAsync(uri);
}
catch (SftpStorageException ex)
{
    // Handle SFTP-specific errors
    Console.WriteLine($"SFTP error: {ex.Message}");
}
catch (IOException ex)
{
    // Handle general I/O errors
    Console.WriteLine($"I/O error: {ex.Message}");
}
```

Common error scenarios and their causes:

- **File not found** - The specified file does not exist on the server
- **Permission denied** - Insufficient permissions for the requested operation
- **Connection refused** - Unable to connect to the SFTP server
- **Authentication failed** - Invalid credentials or authentication method
- **Timeout** - Connection or transfer timed out
