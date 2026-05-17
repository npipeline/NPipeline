---
title: "SFTP Storage Provider"
description: "Read and write files over SFTP with connection pooling, key-based authentication, and server fingerprint validation."
order: 6
---

# SFTP Storage Provider

> **Prerequisites:** [Storage Providers Overview](index.md)

The `NPipeline.StorageProviders.Sftp` package implements `IStorageProvider` for SFTP servers. Supports connection pooling, password and private key authentication (including encrypted keys), keep-alive, server fingerprint validation, and health checks on pool acquire.

## Installation

```bash
dotnet add package NPipeline.StorageProviders.Sftp
```

**Dependencies:** [SSH.NET](https://www.nuget.org/packages/SSH.NET) 2025.x

## Quick Start

```csharp
var options = new SftpStorageProviderOptions
{
    DefaultHost = "sftp.example.com",
    DefaultUsername = "etl-user",
    DefaultKeyPath = "/path/to/private_key"
};
var factory = new SftpClientFactory(options);
var provider = new SftpStorageProvider(factory, options);

var stream = await provider.OpenReadAsync(
    StorageUri.Parse("sftp://sftp.example.com/data/orders.csv"));
```

## URI Format

Credentials in the URI override defaults from configuration:

```
sftp://[user[:password]@]host[:port]/path/to/file
```

| Component | Description |
|-----------|-------------|
| `user` | Optional - override `DefaultUsername` |
| `password` | Optional - override `DefaultPassword` |
| `host` | SFTP hostname |
| `port` | Optional - override `DefaultPort` (default: 22) |
| `path/to/file` | File path on the server |

## Authentication

### Password

```csharp
var options = new SftpStorageProviderOptions
{
    DefaultHost = "sftp.example.com",
    DefaultUsername = "etl-user",
    DefaultPassword = "secret"
};
```

### Private Key

```csharp
var options = new SftpStorageProviderOptions
{
    DefaultHost = "sftp.example.com",
    DefaultUsername = "etl-user",
    DefaultKeyPath = "/path/to/id_rsa"
};
```

### Encrypted Private Key

```csharp
var options = new SftpStorageProviderOptions
{
    DefaultHost = "sftp.example.com",
    DefaultUsername = "etl-user",
    DefaultKeyPath = "/path/to/id_rsa",
    DefaultKeyPassphrase = "key-passphrase"
};
```

## Configuration

### Connection

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `DefaultHost` | `string?` | `null` | SFTP hostname |
| `DefaultPort` | `int` | `22` | SSH port |
| `DefaultUsername` | `string?` | `null` | SSH username |
| `DefaultPassword` | `string?` | `null` | Password auth |
| `DefaultKeyPath` | `string?` | `null` | Path to SSH private key |
| `DefaultKeyPassphrase` | `string?` | `null` | Encrypted key passphrase |

### Connection Pool

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `MaxPoolSize` | `int` | `10` | Maximum pooled connections |
| `ConnectionIdleTimeout` | `TimeSpan` | `5 min` | Evict idle connections after this duration |
| `KeepAliveInterval` | `TimeSpan` | `30s` | SSH keepalive interval |
| `ConnectionTimeout` | `TimeSpan` | `30s` | Connection timeout |
| `ValidateOnAcquire` | `bool` | `true` | Health-check connections when borrowed from pool |

### Security

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ValidateServerFingerprint` | `bool` | `true` | Verify the server host key |
| `ExpectedFingerprint` | `string?` | `null` | Expected fingerprint (auto-accepts on first connect if `null`) |

## Dependency Injection

```csharp
services.AddSftpStorageProvider();

services.AddSftpStorageProvider(options =>
{
    options.DefaultHost = "sftp.example.com";
    options.DefaultUsername = "etl-user";
    options.DefaultKeyPath = "/path/to/id_rsa";
    options.MaxPoolSize = 20;
});
```

Registers: `IStorageProvider`, `IStorageProviderMetadataProvider`

## Features

- **Connection pooling** - reuse SSH connections across operations (default pool size: 10)
- **Keep-alive** - prevents server-side idle timeouts (30s default)
- **Health checks** - validates connections on acquire; dead connections are replaced automatically
- **Fingerprint validation** - prevents MITM attacks; auto-accepts on first connect when `ExpectedFingerprint` is `null`
- **Idempotent delete** - treats 404 as success

## Examples

### Reading

```csharp
var uri = StorageUri.Parse("sftp://sftp.example.com/data/orders.csv");

using var stream = await provider.OpenReadAsync(uri);
using var reader = new StreamReader(stream);
var content = await reader.ReadToEndAsync();
```

### Writing

```csharp
var uri = StorageUri.Parse("sftp://sftp.example.com/output/results.csv");

using var stream = await provider.OpenWriteAsync(uri);
using var writer = new StreamWriter(stream);
await writer.WriteLineAsync("id,name,value");
```

### Listing

```csharp
var prefix = StorageUri.Parse("sftp://sftp.example.com/data/");

await foreach (var item in provider.ListAsync(prefix, recursive: true))
{
    var type = item.IsDirectory ? "[DIR]" : "[FILE]";
    Console.WriteLine($"{type} {item.Uri} - {item.Size} bytes");
}
```

### Multiple SFTP Servers

The URI determines which host is used; authentication from configuration applies to all:

```csharp
var uri1 = StorageUri.Parse("sftp://server1.example.com/path/file1.csv");
var uri2 = StorageUri.Parse("sftp://server2.example.com/path/file2.csv");
```

## Error Handling

The provider translates common SFTP exceptions into `SftpStorageException`:

| Scenario | Exception |
|----------|-----------|
| File not found | `FileNotFoundException` |
| Permission denied | `UnauthorizedAccessException` |
| Connection refused | `IOException` |
| Authentication failed | `UnauthorizedAccessException` |
| Timeout | `IOException` (timeout context preserved) |

## Connection Pool Tuning

| Scenario | Recommended Settings |
|----------|---------------------|
| Low-volume, single server | `MaxPoolSize = 3`, `KeepAliveInterval = 60s` |
| High-volume, single server | `MaxPoolSize = 20`, `KeepAliveInterval = 15s` |
| Flaky network | `ValidateOnAcquire = true`, `ConnectionTimeout = 60s` |

## Best Practices

1. **Use key-based auth** in production - avoid passwords
2. **Set `ExpectedFingerprint`** in production to prevent MITM
3. **Enable `ValidateOnAcquire`** (default) - catches dead connections before use
4. **Tune `MaxPoolSize`** to match concurrency needs - too many connections may overwhelm the SFTP server
5. **Use `KeepAliveInterval`** to prevent server idle disconnects

## Next Steps

- [Custom Provider](custom-provider.md) - implement your own storage provider
- [Storage Providers Overview](index.md) - choosing between providers
