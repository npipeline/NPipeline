# NPipeline.StorageProviders

Core storage provider abstractions for NPipeline connectors, enabling unified access to filesystems, cloud storage, and custom backends through a common
interface.

## Overview

`NPipeline.StorageProviders` provides the foundational abstractions that all NPipeline connectors depend on for storage operations. This separation allows
connectors to work with any storage backend without code changes.

### Key Features

- **Unified Storage Interface**: Single [`IStorageProvider`](./Abstractions/IStorageProvider.cs) interface for read, write, list, and metadata operations
- **URI-Based Resolution**: [`StorageUri`](./Models/StorageUri.cs) class normalizes storage locations across different backends
- **Provider Discovery**: [`IStorageResolver`](./Abstractions/IStorageResolver.cs) discovers appropriate providers for given URIs
- **Thread-Safe Factory**: [`StorageProviderFactory`](./StorageProviderFactory.cs) creates resolvers with error collection
- **Extensible Design**: Implement custom providers for specialized storage systems

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

- `OpenReadAsync`: Open stream for reading
- `OpenWriteAsync`: Open stream for writing
- `ListAsync`: Enumerate items in a location
- `GetMetadataAsync`: Retrieve file metadata
- `ExistsAsync`: Check if an item exists
- `DeleteAsync`: Remove an item (optional)

### StorageUri

Represents storage locations with scheme-based routing:

```csharp
// Local file
var fileUri = StorageUri.Parse("file:///path/to/data.csv");

// Cloud storage (requires provider implementation)
var s3Uri = StorageUri.Parse("s3://bucket/key.csv");

// Custom scheme
var customUri = StorageUri.Parse("custom://location/data.csv");
```

### StorageResolver

Resolves providers based on URI scheme:

```csharp
var resolver = StorageProviderFactory.CreateResolver(
    new StorageResolverOptions
    {
        IncludeFileSystem = true
    }
);

var provider = resolver.ResolveProvider(uri);
```

## Usage Patterns

### Basic File Operations

```csharp
using NPipeline.StorageProviders;

var resolver = StorageProviderFactory.CreateResolver();
var uri = StorageUri.FromFilePath("./data.csv");
var provider = resolver.ResolveProvider(uri)
    ?? throw new InvalidOperationException("No provider registered for the URI scheme.");

// Check existence
bool exists = await provider.ExistsAsync(uri);

// Read stream
await using var stream = await provider.OpenReadAsync(uri);

// Write stream
await using var writeStream = await provider.OpenWriteAsync(uri);
```

### Custom Provider Registration

```csharp
using NPipeline.StorageProviders;

var resolver = new StorageResolver();
resolver.RegisterProvider(new CustomStorageProvider());

var provider = resolver.ResolveProvider(customUri);
```

### Error Collection

```csharp
var (resolver, errors) = StorageProviderFactory.CreateResolverWithErrors(
    new StorageResolverOptions { CollectErrors = true }
);

if (errors.Count > 0)
{
    foreach (var (name, details) in errors)
    {
        Console.WriteLine($"{name}: {string.Join(", ", details)}");
    }
}
```

## Configuration

### StorageResolverOptions

Controls resolver behavior:

- `IncludeFileSystem`: Include built-in filesystem provider
- `Configuration`: Provider configuration from app settings
- `AdditionalProviders`: Custom provider instances
- `CollectErrors`: Capture provider creation errors

### ConnectorConfiguration

Defines provider settings for application configuration:

```csharp
var config = new ConnectorConfiguration
{
    Providers = new Dictionary<string, StorageProviderConfig>
    {
        ["S3"] = new StorageProviderConfig
        {
            ProviderType = "S3StorageProvider",
            Enabled = true,
            Settings = new Dictionary<string, string>
            {
                ["Region"] = "us-east-1"
            }
        }
    }
};
```

## Architecture

The storage provider system follows dependency inversion principles:

1. **Abstractions Layer**: Interfaces and models in `NPipeline.StorageProviders`
2. **Implementation Layer**: Concrete providers (FileSystem, S3, etc.)
3. **Connector Layer**: Connectors depend only on abstractions

This design enables:

- Swapping storage backends without connector changes
- Testing connectors with mock providers
- Adding new storage systems without modifying existing code

## Thread Safety

- `StorageResolver`: Thread-safe registration and resolution
- `StorageProviderRegistry`: Thread-safe alias registration
- `StorageProviderFactory`: Static methods, stateless

## Supported Providers

- **FileSystem**: Built-in support for local and network file systems
- **AWS S3**: Available via [`NPipeline.StorageProviders.Aws`](../NPipeline.StorageProviders.Aws/)
- **Custom**: Implement [`IStorageProvider`](./Abstractions/IStorageProvider.cs) for any backend

## Documentation

- [Storage Providers Overview](../../docs/storage-providers/index.md)
- [Storage Provider Interface](../../docs/storage-providers/storage-provider.md)
- [AWS S3 Provider](../../docs/storage-providers/aws-s3.md)

## License

Part of the NPipeline project. See main project LICENSE file for details.
