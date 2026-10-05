---
title: "SFTP Storage Provider"
description: "Read and write files over SFTP with connection pooling, key-based authentication, and server fingerprint validation."
order: 6
---

# SFTP Storage Provider

> **Prerequisites:** [Storage Providers Overview](index.md)

The `NPipeline.StorageProviders.Sftp` package implements `IStorageProvider` for SFTP servers. Supports connection pooling, password and private key authentication (including encrypted keys), keep-alive, host key verification, and health checks on pool acquire.

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
    DefaultKeyPath = "/path/to/private_key",
    HostKeyFingerprints = ["SHA256:ohD8VZEXGWo6Ez8GSEJQ9WpafgLFsOfLOtGGQCQo6Og"]
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
| `ConnectionTimeout` | `TimeSpan` | `30s` | Bound on establishing a connection (TCP connect and SSH handshake); failures surface as `SshOperationTimeoutException`. The connect also stops when the pipeline is cancelled |
| `ValidateOnAcquire` | `bool` | `true` | Health-check connections when borrowed from pool |

### Security

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `HostKeyFingerprints` | `IReadOnlyCollection<string>` | empty | SHA-256 fingerprints of the host keys the server may present, such as `SHA256:ohD8VZEXGWo6Ez8GSEJQ9WpafgLFsOfLOtGGQCQo6Og` (the `SHA256:` prefix is optional). A server presenting any other key is rejected. |
| `AcceptAnyHostKey` | `bool` | `false` | Trust any host key. Disables protection against man-in-the-middle attacks; use only for local development and tests. |

Connecting fails with `InvalidOperationException` unless you set `HostKeyFingerprints` or `AcceptAnyHostKey`. To get a server's fingerprints, run:

```bash
ssh-keyscan sftp.example.com | ssh-keygen -lf -
```

List every fingerprint the command prints: the server may present any of its key types.

## Dependency Injection

```csharp
services.AddSftpStorageProvider();

services.AddSftpStorageProvider(options =>
{
    options.DefaultHost = "sftp.example.com";
    options.DefaultUsername = "etl-user";
    options.DefaultKeyPath = "/path/to/id_rsa";
    options.HostKeyFingerprints = ["SHA256:ohD8VZEXGWo6Ez8GSEJQ9WpafgLFsOfLOtGGQCQo6Og"];
    options.MaxPoolSize = 20;
});
```

Registers: `IStorageProvider`, `IStorageProviderMetadataProvider`

## Features

- **Connection pooling** - reuse SSH connections across operations (default pool size: 10)
- **Keep-alive** - prevents server-side idle timeouts (30s default)
- **Health checks** - validates connections on acquire; dead connections are replaced automatically
- **Host key verification** - rejects servers whose key is not in `HostKeyFingerprints`, which prevents man-in-the-middle attacks
- **Overwrite truncates** - writing to an existing file replaces its contents

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
2. **Set `HostKeyFingerprints`** for every server; never use `AcceptAnyHostKey` in production
3. **Enable `ValidateOnAcquire`** (default) - catches dead connections before use
4. **Tune `MaxPoolSize`** to match concurrency needs - too many connections may overwhelm the SFTP server
5. **Use `KeepAliveInterval`** to prevent server idle disconnects

## Next Steps

- [Custom Provider](custom-provider.md) - implement your own storage provider
- [Storage Providers Overview](index.md) - choosing between providers
