---
title: Storage Provider Interface
description: Understanding the IStorageProvider abstraction and implementing custom storage backends.
sidebar_position: 2
---

## Storage Provider Interface

## Overview

The `IStorageProvider` interface is a foundational abstraction that enables NPipeline connectors to work with multiple storage backends—from local filesystems to cloud services and databases. This document explains the core concepts, capabilities, and patterns for working with storage providers.

## Core Interface

`IStorageProvider` defines the following operations:

### Essential Operations

- **`OpenReadAsync(uri, cancellationToken)`** - Opens a readable stream for a resource
- **`OpenWriteAsync(uri, cancellationToken)`** - Opens a writable stream for a resource
- **`ExistsAsync(uri, cancellationToken)`** - Checks whether a resource exists

### Extended Operations (Optional)

- **`DeleteAsync(uri, cancellationToken)`** - Deletes a resource (default throws `NotSupportedException`)
- **`ListAsync(prefix, recursive, cancellationToken)`** - Lists resources at a location (default throws `NotSupportedException`)
- **`GetMetadataAsync(uri, cancellationToken)`** - Retrieves detailed metadata (default returns null)

## Capability Discovery

Use `IStorageProviderMetadataProvider` to discover what operations a provider supports:

```csharp
if (provider is IStorageProviderMetadataProvider metadataProvider)
{
    var metadata = metadataProvider.GetMetadata();
    
    if (metadata.SupportsDelete)
    {
        await provider.DeleteAsync(uri);
    }
    
    if (metadata.SupportsListing)
    {
        await foreach (var item in provider.ListAsync(prefix))
        {
            Console.WriteLine(item.Uri);
        }
    }
}
```

### Capability Flags

| **Flag** | **Description** |
| --- | --- |
| `SupportsRead` | Provider supports `OpenReadAsync()` |
| `SupportsWrite` | Provider supports `OpenWriteAsync()` |
| `SupportsDelete` | Provider supports `DeleteAsync()` |
| `SupportsListing` | Provider supports `ListAsync()` |
| `SupportsMetadata` | Provider supports `GetMetadataAsync()` |
| `SupportsHierarchy` | Provider has meaningful directory/prefix structure |

## Storage Types

### StorageUri

Represents a location in the storage system with structure: `scheme://host/path?param=value`

**Examples:**

```text
file:///C:/data/file.csv                           // Local filesystem
file://\\server\share\data.csv                     // UNC path
s3://my-bucket/data/2024/file.parquet              // AWS S3
azure://mycontainer/logs/app.log                   // Azure Blob Storage
db://server/database/table                         // Database table
```

### StorageItem

Lightweight representation of a resource returned by `ListAsync()`:

```csharp
public sealed record StorageItem
{
    public required StorageUri Uri { get; init; }
    public required long Size { get; init; }
    public required DateTimeOffset LastModified { get; init; }
    public bool IsDirectory { get; init; }  // Logical grouping (S3 prefixes, directories)
}
```

### StorageMetadata

Detailed metadata for a specific resource:

```csharp
public sealed record StorageMetadata
{
    public required long Size { get; init; }
    public required DateTimeOffset LastModified { get; init; }
    public string? ContentType { get; init; }           // MIME type (if available)
    public IReadOnlyDictionary<string, string> CustomMetadata { get; init; }
    public bool IsDirectory { get; init; }
    public string? ETag { get; init; }                  // For optimistic concurrency
}
```

## Built-in Providers

### FileSystem Provider

The default provider for local file access with full feature support.

**Characteristics:**

- True directory hierarchy
- Full read/write/delete/list support
- MIME type detection
- File metadata with timestamps

**Example:**

```csharp
var provider = new FileSystemStorageProvider();
var uri = StorageUri.FromFilePath("C:\\data\\input.csv");

// Read file
using var stream = await provider.OpenReadAsync(uri);

// List directory recursively
await foreach (var item in provider.ListAsync(uri, recursive: true))
{
    Console.WriteLine($"{item.Uri}: {item.Size} bytes");
}

// Get metadata
var metadata = await provider.GetMetadataAsync(uri);
if (metadata != null)
{
    Console.WriteLine($"Content-Type: {metadata.ContentType}");
    Console.WriteLine($"ETag: {metadata.ETag}");
}
```

**Resilience Handling:**

- Gracefully skips inaccessible directories during recursive listing
- Handles concurrent file deletions
- Skips symbolic links/junctions in recursive traversal

## Common Operations

### Reading Data

All connectors support reading through `OpenReadAsync`:

```csharp
var uri = StorageUri.FromFilePath("data.csv");
var source = new CsvSourceNode<MyData>(
    uri,
    row => new MyData(
        row.Get<string>("Id") ?? string.Empty,
        row.Get<string>("Value") ?? string.Empty)
);

// When the pipeline runs, the CSV connector uses the storage provider
// to open the file for reading
```

### Writing Data

All connectors support writing through `OpenWriteAsync`:

```csharp
var uri = StorageUri.FromFilePath("output.csv");
var sink = new CsvSinkNode<MyResult>(uri);

// When the pipeline runs, the CSV connector uses the storage provider
// to create/overwrite the file
```

### Checking Existence

Use `ExistsAsync` to check if a resource exists:

```csharp
var uri = StorageUri.FromFilePath("config.json");
var provider = new FileSystemStorageProvider();

if (await provider.ExistsAsync(uri))
{
    Console.WriteLine("Configuration file exists");
}
```

### Listing Resources

List resources in a directory using `ListAsync`:

```csharp
var uri = StorageUri.FromFilePath("C:\\data\\csv_files");
var provider = new FileSystemStorageProvider();

// List only top-level files
await foreach (var item in provider.ListAsync(uri, recursive: false))
{
    Console.WriteLine($"{item.Uri}: {item.Size} bytes");
    if (item.IsDirectory)
    {
        Console.WriteLine("  (directory)");
    }
}

// List all files recursively
await foreach (var item in provider.ListAsync(uri, recursive: true))
{
    Console.WriteLine($"{item.Uri}: {item.Size} bytes");
}
```

### Getting Metadata

Retrieve detailed metadata about a resource:

```csharp
var uri = StorageUri.FromFilePath("data.csv");
var provider = new FileSystemStorageProvider();

var metadata = await provider.GetMetadataAsync(uri);
if (metadata != null)
{
    Console.WriteLine($"Size: {metadata.Size} bytes");
    Console.WriteLine($"Modified: {metadata.LastModified}");
    Console.WriteLine($"Content-Type: {metadata.ContentType}");
    Console.WriteLine($"ETag: {metadata.ETag}");
}
```

### Deleting Resources

Delete files and directories:

```csharp
var uri = StorageUri.FromFilePath("C:\\temp\\old_data.csv");
var provider = new FileSystemStorageProvider();

if (metadata.SupportsDelete)
{
    await provider.DeleteAsync(uri);
    Console.WriteLine("File deleted");
}
```

## StorageUri Format

`StorageUri` represents a location in any storage system:

```text
scheme://[host]/path[?param=value]
```

### Examples

```text
// Local filesystem (Windows)
file:///C:/data/users.csv

// Local filesystem (Unix)
file:///home/user/data/users.csv

// UNC path (Windows network share)
file://server/share/data.csv

// AWS S3
s3://my-bucket/data/2024/users.parquet

// Azure Blob Storage
azure://container/logs/app.log?connection-string=...

// Custom backend
custom://my-system/resource?auth=token
```

### Creating URIs

```csharp
// From local file path
var uri = StorageUri.FromFilePath("C:\\data\\file.csv");

// From URI string
var uri = StorageUri.Parse("s3://bucket/key");

// Combining paths
var baseUri = StorageUri.FromFilePath("C:\\data");
var fullUri = baseUri.Combine("subfolder/file.csv");

// Adding parameters
var uri = StorageUri.FromFilePath("data.csv")
    .WithParameter("compression", "gzip")
    .WithParameter("encoding", "utf-8");
```

## Implementation Guide

### Creating a Custom Provider

To implement a custom storage provider (e.g., for S3, Azure, or a custom system):

```csharp
public sealed class MyCustomStorageProvider : IStorageProvider, IStorageProviderMetadataProvider
{
    public StorageScheme Scheme => StorageScheme.Parse("custom");
    
    public bool CanHandle(StorageUri uri) => uri.Scheme == Scheme;
    
    public async Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        // Your implementation here
        throw new NotImplementedException();
    }
    
    public async Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        // Your implementation here
        throw new NotImplementedException();
    }
    
    public async Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        // Your implementation here
        throw new NotImplementedException();
    }
    
    // Implement DeleteAsync if deletion is supported
    public async Task DeleteAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        // Your implementation here
        throw new NotImplementedException();
    }
    
    // Implement ListAsync if listing is supported
    public async IAsyncEnumerable<StorageItem> ListAsync(
        StorageUri prefix,
        bool recursive = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Your implementation here
        throw new NotImplementedException();
        yield break;
    }
    
    // Implement GetMetadataAsync if metadata retrieval is supported
    public async Task<StorageMetadata?> GetMetadataAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        // Your implementation here
        return null;
    }
    
    public StorageProviderMetadata GetMetadata()
    {
        return new StorageProviderMetadata
        {
            Name = "My Custom Provider",
            SupportedSchemes = ["custom"],
            SupportsRead = true,
            SupportsWrite = true,
            SupportsDelete = true,
            SupportsListing = true,
            SupportsMetadata = true,
            SupportsHierarchy = true
        };
    }
}
```

### Best Practices

1. **Normalize Exceptions**: Map provider-specific exceptions to standard .NET exceptions:
   - Resource not found → `FileNotFoundException`
   - Access denied → `UnauthorizedAccessException`
   - Network/timeout issues → `IOException` or `OperationCanceledException`

2. **Document Recursion Semantics**: Clearly explain how `recursive` works in your implementation:

    ```csharp
   /// <remarks>
   /// With recursive=false, returns objects matching the prefix with "/" delimiter applied.
   /// With recursive=true, returns all objects with the prefix (flat list).
   /// </remarks>
   ```

3. **Implement Capabilities Accurately**: Set capability flags to match actual implementation:

    ```csharp
   public StorageProviderMetadata GetMetadata()
   {
       return new StorageProviderMetadata
       {
           SupportsRead = true,
           SupportsWrite = false,    // This provider is read-only
           SupportsDelete = false,
           SupportsListing = true,
           SupportsHierarchy = false // No directory concept
       };
   }
   ```

4. **Respect Cancellation**: Always check the `CancellationToken` during enumeration:

    ```csharp
   public async IAsyncEnumerable<StorageItem> ListAsync(
       StorageUri prefix,
       bool recursive = false,
       [EnumeratorCancellation] CancellationToken cancellationToken = default)
   {
       foreach (var item in GetItems(prefix))
       {
           cancellationToken.ThrowIfCancellationRequested();
           yield return item;
       }
   }
   ```

5. **Populate Metadata Fields**: Provide all available metadata:

    ```csharp
   return new StorageMetadata
   {
       Size = contentLength,
       LastModified = dateModified,
       ContentType = "application/json",        // If available
       CustomMetadata = objectTags,             // If available
       ETag = response.ETag,                    // If available
       IsDirectory = false
   };
   ```

## Using Custom Providers

Register custom providers with dependency injection:

```csharp
services.AddSingleton<IStorageProvider>(new MyCustomStorageProvider());
```

Then use them with connectors:

```csharp
var uri = StorageUri.Parse("custom://bucket/key");
var pipeline = new PipelineBuilder()
    .AddSource(new CsvSourceNode<MyData>(
        uri,
        row => new MyData(
            row.Get<string>("Id") ?? string.Empty,
            row.Get<string>("Value") ?? string.Empty)), "source")
    // ... transforms ...
    .Build();
```

## Error Handling

Storage providers normalize backend-specific errors to standard .NET exceptions:

```csharp
try
{
    using var stream = await provider.OpenReadAsync(uri);
}
catch (FileNotFoundException ex)
{
    Console.WriteLine("Resource not found");
}
catch (UnauthorizedAccessException ex)
{
    Console.WriteLine("Access denied");
}
catch (IOException ex)
{
    Console.WriteLine("I/O error (network, timeout, etc.)");
}
```

### Filesystem-Specific Resilience

The filesystem provider includes automatic resilience handling:

#### Permission Restrictions

During recursive listing, inaccessible directories are automatically skipped:

```csharp
var uri = StorageUri.FromFilePath("C:\\data");

// Even if some subdirectories are restricted, listing completes successfully
await foreach (var item in provider.ListAsync(uri, recursive: true))
{
    // Gets all accessible items, skips restricted directories
}
```

#### Concurrent Modifications

Files deleted during enumeration are automatically skipped:

```csharp
// If a file is deleted during enumeration, it's skipped without error
await foreach (var item in provider.ListAsync(uri, recursive: true))
{
    // Enumeration continues even if files are deleted
}
```

#### Symbolic Links and Junctions

Circular symlinks and junctions are automatically detected and skipped:

```csharp
// If C:\data\link points to C:\data (creating a cycle),
// the link is yielded but not traversed
await foreach (var item in provider.ListAsync(uri, recursive: true))
{
    // Includes link as an item, but doesn't follow it
}
```

## Performance Considerations

### Streaming Large Files

Always use streaming for large files rather than buffering:

```csharp
// Good: Streaming
using var stream = await provider.OpenReadAsync(uri);
using var reader = new StreamReader(stream);
string line;
while ((line = await reader.ReadLineAsync()) != null)
{
    // Process line by line
}

// Bad: Buffering entire file
var content = await File.ReadAllTextAsync(path);
// Uses lots of memory for large files
```

### Batch Operations

When processing multiple files, consider batch operations:

```csharp
var files = new List<StorageUri> { uri1, uri2, uri3 };

// Process concurrently (with appropriate limits)
var semaphore = new SemaphoreSlim(3); // 3 concurrent operations
var tasks = files.Select(async f =>
{
    await semaphore.WaitAsync();
    try
    {
        var metadata = await provider.GetMetadataAsync(f);
        // Process
    }
    finally
    {
        semaphore.Release();
    }
});

await Task.WhenAll(tasks);
```

## Design Considerations

### Recursion Semantics

Different backends have different recursion behaviors:

| Backend | Non-Recursive (recursive=false) | Recursive (recursive=true) |
| --- | --- | --- |
| Filesystem | Direct children only | All descendants |
| S3/Azure | Objects with prefix, "/" delimiter applied | All objects with prefix |
| Database | N/A | Query-based filtering |

### Error Handling Notes

During recursive listing, providers should gracefully handle:

- Permission restrictions on subdirectories
- Concurrent deletions during enumeration
- Circular references (symlinks/junctions)

For filesystem, these are automatically handled by the built-in provider.

### Performance

- Use appropriate buffer sizes for streaming
- Consider pagination for large datasets
- Minimize allocations in hot paths
- Cache provider metadata during pipeline lifetime

## Related Documentation

- **[CSV Connector](./csv.md)** - Example using the filesystem storage provider
