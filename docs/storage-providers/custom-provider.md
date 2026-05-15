---
title: "Custom Storage Provider"
description: "Implement IStorageProvider to integrate with unsupported storage systems."
order: 8
---

# Custom Storage Provider

> **Prerequisites:** [Storage Providers Overview](index.md)

If NPipeline doesn't have a built-in provider for your storage system, you can implement `IStorageProvider`.

## Implementing IStorageProvider

```csharp
public class FtpStorageProvider : IStorageProvider
{
    public StorageScheme Scheme => new("ftp");

    public bool CanHandle(StorageUri uri) => uri.Scheme == "ftp";

    public async Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken ct)
    {
        var client = new FtpClient(uri.Host);
        await client.ConnectAsync(ct);
        return await client.OpenReadAsync(uri.Path, ct);
    }

    public async Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken ct)
    {
        var client = new FtpClient(uri.Host);
        await client.ConnectAsync(ct);
        return await client.OpenWriteAsync(uri.Path, ct);
    }

    public async Task<bool> ExistsAsync(StorageUri uri, CancellationToken ct)
    {
        var client = new FtpClient(uri.Host);
        await client.ConnectAsync(ct);
        return await client.FileExistsAsync(uri.Path, ct);
    }

    public async IAsyncEnumerable<StorageItem> ListAsync(
        StorageUri prefix, bool recursive,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var client = new FtpClient(prefix.Host);
        await client.ConnectAsync(ct);
        foreach (var item in await client.ListDirectoryAsync(prefix.Path, ct))
        {
            yield return new StorageItem(
                new StorageUri($"ftp://{prefix.Host}{item.Path}"),
                item.Name, item.Size, item.Modified);
        }
    }
}
```

## Optional Interfaces

Implement additional interfaces for extended capabilities:

| Interface | Methods | Description |
|-----------|---------|-------------|
| `IDeletableStorageProvider` | `DeleteAsync(uri, ct)` | Delete files |
| `IMoveableStorageProvider` | `MoveAsync(source, destination, ct)` | Move or rename files |
| `IStorageProviderMetadataProvider` | `GetMetadataAsync(uri, ct)` | Return `Size`, `LastModified`, `ContentType`, `ETag`, `CustomMetadata` |

## Registration

### Manual Registration

```csharp
var resolver = new StorageResolver(new IStorageProvider[]
{
    new FileSystemStorageProvider(),
    new FtpStorageProvider()
});
```

### With StorageProviderFactory

```csharp
var resolver = StorageProviderFactory.CreateResolver(new StorageResolverOptions
{
    IncludeFileSystem = true,
    AdditionalProviders = new[] { new FtpStorageProvider() }
});
```

## Cross-Provider Patterns

All providers (built-in and custom) share these conventions:

| Pattern | Description |
|---------|-------------|
| **Async API** | `OpenReadAsync()`, `OpenWriteAsync()`, `ExistsAsync()`, `ListAsync()` |
| **Cancellation** | All methods accept `CancellationToken` |
| **Exception translation** | 404 → `FileNotFoundException`, Access denied → `UnauthorizedAccessException` |
| **Stream ownership** | Callers are responsible for disposing returned streams |

## Implementation Best Practices

1. **Normalize exceptions** — translate provider-specific errors to standard .NET exceptions (`FileNotFoundException`, `UnauthorizedAccessException`, `IOException`)
2. **Support cancellation** — pass `CancellationToken` to all async operations
3. **Report capabilities accurately** — implement `IStorageProviderMetadataProvider` so the resolver and connectors know what your provider supports
4. **Handle recursion consistently** — when `recursive: false`, list only immediate children; when `true`, traverse all descendants
5. **Stream responsibly** — return streams that can be disposed safely; avoid loading entire files into memory

## Capability Discovery

Implement `IStorageProviderMetadataProvider` to advertise capabilities:

```csharp
public class FtpStorageProvider : IStorageProvider, IStorageProviderMetadataProvider
{
    public StorageProviderMetadata GetMetadata() => new()
    {
        Name = "FTP Storage Provider",
        SupportedSchemes = ["ftp"],
        SupportsRead = true,
        SupportsWrite = true,
        SupportsListing = true,
        SupportsMetadata = false,
        SupportsHierarchy = true
    };
}
```

## Registration

### Manual Registration

```csharp
var resolver = new StorageResolver(new IStorageProvider[]
{
    new FileSystemStorageProvider(),
    new FtpStorageProvider()
});
```

### With StorageProviderFactory

```csharp
var resolver = StorageProviderFactory.CreateResolver(new StorageResolverOptions
{
    IncludeFileSystem = true,
    AdditionalProviders = new[] { new FtpStorageProvider() }
});
```

### Via DI

```csharp
services.AddSingleton<IStorageProvider, FtpStorageProvider>();
```

## Next Steps

- [Storage Providers Overview](index.md) — built-in providers
- [Connectors](../connectors/index.md) — use your provider with file-based connectors
