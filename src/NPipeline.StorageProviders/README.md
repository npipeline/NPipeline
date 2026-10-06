# NPipeline.StorageProviders

Core storage provider abstractions for NPipeline connectors, enabling unified access to filesystems, cloud storage, and custom backends through a common
interface.

## Overview

`NPipeline.StorageProviders` provides the foundational abstractions that all NPipeline connectors depend on for storage operations. This separation allows
connectors to work with any storage backend without code changes.

### Key Features

- **Unified Storage Interface**: Single [`IStorageProvider`](./Abstractions/IStorageProvider.cs) interface for read, write, list, delete, move and metadata operations, plus a `StorageCapabilities` flags property that says what a provider supports
- **URI-Based Resolution**: [`StorageUri`](./Models/StorageUri.cs) is an immutable, value-equal address type that normalizes storage locations across different backends and never rewrites or double-decodes a path
- **Provider Routing**: [`StorageResolver`](./StorageResolver.cs) is an immutable `IStorageResolver` built from a fixed set of providers, one per scheme
- **Explicit commit writes**: `OpenWriteAsync` returns a `StorageWriteStream`; nothing appears at the target until you call `CommitAsync`
- **Extensible Design**: Derive from the `StorageProvider` base class to implement custom providers for specialized storage systems

## Installation

```xml
<PackageReference Include="NPipeline.StorageProviders" Version="*" />
```

Or via .NET CLI:

```bash
dotnet add package NPipeline.StorageProviders
```

## Core Components

### IStorageProvider

Primary interface defining storage operations:

- `OpenReadAsync`: Open a stream for reading
- `OpenWriteAsync`: Open a `StorageWriteStream` for writing. The data appears at the target only after `CommitAsync`; disposing without a commit discards it
- `ListAsync`: Enumerate items under a directory, recursively or not
- `GetMetadataAsync`: Retrieve an object's metadata, or `null` when it doesn't exist
- `ExistsAsync`: Check if an item exists
- `DeleteAsync`: Delete an object (idempotent: deleting a missing object succeeds)
- `MoveAsync`: Move an object, overwriting the destination
- `Capabilities`: Which of the operations above the provider actually supports; an unsupported one throws `UnsupportedStorageCapabilityException`

### StorageUri

Represents storage locations with scheme-based routing. The path is decoded exactly once, at parse time, and is never
rewritten, so it round-trips exactly:

```csharp
// Local file
var fileUri = StorageUri.Parse("file:///path/to/data.csv");

// Cloud storage (requires a provider implementation for the scheme)
var s3Uri = StorageUri.Parse("s3://bucket/key.csv");

// Custom scheme
var customUri = StorageUri.Parse("custom://location/data.csv");
```

`StorageUri` never carries secrets: routing data (region, service URL, account name) stays in the URI, but credentials
belong in a provider's options. `ToString()` redacts the password and any parameter in `StorageUri.SecretParameterNames`.

### StorageResolver

Resolves providers based on URI scheme. It's immutable: build it once from the providers you want. Two providers that
serve the same scheme make the constructor throw `ArgumentException`, so a misconfiguration fails at startup instead of
silently picking one provider:

```csharp
var resolver = new StorageResolver(new IStorageProvider[]
{
    new FileSystemStorageProvider(),
    new AwsS3StorageProvider(s3ClientFactory, s3Options),
});

var provider = resolver.Resolve(uri); // throws StorageProviderNotFoundException if none serves the scheme
```

`StorageResolver.Default` is a shared resolver that serves the file system only.

## Usage Patterns

### Basic File Operations

```csharp
using NPipeline.StorageProviders;

var resolver = StorageResolver.Default;
var uri = StorageUri.FromFilePath("./data.csv");
var provider = resolver.Resolve(uri);

// Check existence
bool exists = await provider.ExistsAsync(uri);

// Read stream
await using var stream = await provider.OpenReadAsync(uri);

// Write stream
await using var writeStream = await provider.OpenWriteAsync(uri);
await writeStream.WriteAsync(bytes);
await writeStream.CommitAsync(); // without this, disposing discards the data
```

### Custom Provider Registration

```csharp
using NPipeline.StorageProviders;

var resolver = new StorageResolver(new IStorageProvider[]
{
    new FileSystemStorageProvider(),
    new CustomStorageProvider(),
});

var provider = resolver.Resolve(customUri);
```

### Dependency injection

```csharp
services.AddFileSystemStorageProvider();
services.AddStorageProvider<CustomStorageProvider>();
services.AddStorageResolver(); // builds IStorageResolver from every IStorageProvider in the container
```

## Architecture

The storage provider system follows dependency inversion principles:

1. **Abstractions Layer**: Interfaces and models in `NPipeline.StorageProviders`
2. **Implementation Layer**: Concrete providers (FileSystem, S3, Azure Blob, ADLS Gen2, GCS, SFTP, ...)
3. **Connector Layer**: Connectors depend only on abstractions

This design enables:

- Swapping storage backends without connector changes
- Testing connectors with fake or in-memory providers
- Adding new storage systems without modifying existing code

## Thread Safety

- `StorageResolver`: Immutable after construction; safe to resolve concurrently
- `StorageUri`: Immutable value type

## Supported Providers

- **FileSystem**: Built-in support for local and network file systems
- **AWS S3**: [`NPipeline.StorageProviders.S3.Aws`](../NPipeline.StorageProviders.S3.Aws/)
- **S3-compatible services** (MinIO, DigitalOcean Spaces, Cloudflare R2, ...): [`NPipeline.StorageProviders.S3.Compatible`](../NPipeline.StorageProviders.S3.Compatible/)
- **Azure Blob Storage**: [`NPipeline.StorageProviders.Azure`](../NPipeline.StorageProviders.Azure/)
- **Azure Data Lake Storage Gen2**: [`NPipeline.StorageProviders.Adls`](../NPipeline.StorageProviders.Adls/)
- **Google Cloud Storage**: [`NPipeline.StorageProviders.Gcp`](../NPipeline.StorageProviders.Gcp/)
- **SFTP**: [`NPipeline.StorageProviders.Sftp`](../NPipeline.StorageProviders.Sftp/)
- **Custom**: Derive from the `StorageProvider` base class in [`Abstractions/StorageProvider.cs`](./Abstractions/StorageProvider.cs) for any other backend

## Documentation

- [Storage Providers Overview](../../docs/storage-providers/index.md)
- [Custom Storage Provider](../../docs/storage-providers/custom-provider.md)
- [AWS S3 Provider](../../docs/storage-providers/aws-s3.md)

## License

This package is licensed under the [MIT License](LICENSE.txt). You are free to use, modify, and distribute it in personal, open-source, and commercial projects without restriction.
