---
title: "CSV Connector"
description: "Read and write CSV files with attribute-based or lambda-based mapping."
order: 2
---

# CSV Connector

The `NPipeline.Connectors.Csv` package reads and writes CSV/TSV files using [CsvHelper](https://joshclose.github.io/CsvHelper/). It supports attribute-based mapping, explicit row mappers, custom delimiters, header validation, and per-row error handling.

## Installation

```bash
dotnet add package NPipeline.Connectors.Csv
```

**Dependencies:** [CsvHelper](https://www.nuget.org/packages/CsvHelper) 33.x, `NPipeline.Connectors`, `NPipeline.StorageProviders`

## Storage Abstraction

The CSV connector uses NPipeline's storage abstraction layer, so the same code works with local files and cloud storage.

### StorageUri

`StorageUri` represents a normalized storage location:

```csharp
// Local files
var local = StorageUri.FromFilePath("data/input.csv");

// Cloud storage
var s3 = StorageUri.Parse("s3://my-bucket/path/to/file.csv");
```

### Storage Resolver

An `IStorageResolver` finds the right storage provider for a given URI. **You don't need one for local files** — the connector creates a default resolver with file system support automatically.

Provide an explicit resolver when working with cloud storage:

```csharp
var resolver = StorageProviderFactory.CreateResolver(
    new StorageResolverOptions
    {
        IncludeFileSystem = true,
        AdditionalProviders = new[] { new S3StorageProvider() }
    });

var source = new CsvSourceNode<User>(
    StorageUri.Parse("s3://my-bucket/users.csv"),
    resolver: resolver);
```

You can also pass an `IStorageProvider` directly to bypass resolution:

```csharp
var provider = new FileSystemStorageProvider();
var source = new CsvSourceNode<User>(provider, StorageUri.FromFilePath("users.csv"));
```

## Column Mapping

### Attribute-Based Mapping

Use `[Column]` and `[IgnoreColumn]` from `NPipeline.Connectors.Attributes` to control how properties map to CSV columns:

```csharp
using NPipeline.Connectors.Attributes;

public class Customer
{
    [Column("customer_id")]
    public int Id { get; set; }

    [Column("first_name")]
    public string FirstName { get; set; } = string.Empty;

    [Column("last_name")]
    public string LastName { get; set; } = string.Empty;

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}
```

When no `[Column]` attribute is present, the mapper falls back to convention matching (PascalCase property → lowercase column name).

### Lambda-Based Mapping

For explicit control, provide a `Func<CsvRow, T>` mapper:

```csharp
var source = new CsvSourceNode<User>(
    StorageUri.FromFilePath("users.csv"),
    row => new User(
        row.Get<int>("Id") ?? 0,
        row.Get<string>("Name") ?? string.Empty,
        row.Get<string>("Email") ?? string.Empty));
```

`CsvRow` provides these methods:

| Method | Description |
|--------|-------------|
| `Get<T>(string name, T defaultValue)` | Read a field by header name, return converted value or default |
| `GetByIndex<T>(int index, T defaultValue)` | Read a field by column index (for headerless files) |
| `TryGet<T>(string name, out T value, T defaultValue)` | Try to read and convert; returns `false` if missing or unconvertible |
| `HasColumn(string name)` | Check whether a column exists in the row |

## Source Node — `CsvSourceNode<T>`

Reads a CSV file and emits each row as an item of type `T`.

### Constructors

```csharp
// Attribute-based mapping with optional resolver (default: file system)
public CsvSourceNode(
    StorageUri uri,
    IStorageResolver? resolver = null,
    CsvConfiguration? configuration = null,
    Encoding? encoding = null)

// Attribute-based mapping with explicit provider
public CsvSourceNode(
    IStorageProvider provider,
    StorageUri uri,
    CsvConfiguration? configuration = null,
    Encoding? encoding = null)

// Lambda-based mapping with optional resolver
public CsvSourceNode(
    StorageUri uri,
    Func<CsvRow, T> rowMapper,
    IStorageResolver? resolver = null,
    CsvConfiguration? configuration = null,
    Encoding? encoding = null)

// Lambda-based mapping with explicit provider
public CsvSourceNode(
    IStorageProvider provider,
    StorageUri uri,
    Func<CsvRow, T> rowMapper,
    CsvConfiguration? configuration = null,
    Encoding? encoding = null)
```

### Example: Reading a CSV File

```csharp
public sealed record User(int Id, string Name, string Email);

public sealed class CsvReaderPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            new CsvSourceNode<User>(
                StorageUri.FromFilePath("users.csv"),
                row => new User(
                    row.Get<int>("Id") ?? 0,
                    row.Get<string>("Name") ?? string.Empty,
                    row.Get<string>("Email") ?? string.Empty)),
            "csv-source");

        var sink = builder.AddSink<ConsoleSinkNode, User>("console-sink");
        builder.Connect(source, sink);
    }
}
```

## Sink Node — `CsvSinkNode<T>`

Writes items from the pipeline to a CSV file. Uses attribute-based mapping by default (via `CsvWriterMapperBuilder`).

### Constructors

```csharp
// Attribute-based mapping with optional resolver (default: file system)
public CsvSinkNode(
    StorageUri uri,
    IStorageResolver? resolver = null,
    CsvConfiguration? configuration = null,
    Encoding? encoding = null)

// Attribute-based mapping with explicit provider
public CsvSinkNode(
    IStorageProvider provider,
    StorageUri uri,
    CsvConfiguration? configuration = null,
    Encoding? encoding = null)
```

### Example: Writing to a CSV File

```csharp
public sealed class CsvWriterPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<InMemorySourceNode<ProcessedUser>, ProcessedUser>("source");
        var sink = builder.AddSink(
            new CsvSinkNode<ProcessedUser>(StorageUri.FromFilePath("output.csv")),
            "csv-sink");
        builder.Connect(source, sink);
    }
}
```

## Configuration

Both source and sink accept an optional `CsvConfiguration`:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `BufferSize` | `int` | `4096` | I/O buffer size in bytes. Increase for large files. |
| `HasHeaderRecord` | `bool` | `true` | First row contains column names |
| `HeaderValidated` | `HeaderValidated?` | `null` | Custom header validation callback |
| `HelperConfiguration` | `CsvHelper.Configuration.CsvConfiguration` | (auto) | Pass-through to CsvHelper for full control (delimiter, culture, quoting, etc.) |
| `RowErrorHandler` | `Func<Exception, CsvRow, bool>?` | `null` | Per-row error handler. Return `true` to skip the row, `false` to throw. |

### Custom Delimiter (TSV)

```csharp
var config = new CsvConfiguration(CultureInfo.InvariantCulture) { HasHeaderRecord = false };
config.HelperConfiguration.Delimiter = "\t";

var source = new CsvSourceNode<User>(
    StorageUri.FromFilePath("users.tsv"),
    row => new User(
        row.GetByIndex<int>(0) ?? 0,
        row.GetByIndex<string>(1) ?? string.Empty,
        row.GetByIndex<string>(2) ?? string.Empty),
    configuration: config);
```

### Per-Row Error Handling

```csharp
var config = new CsvConfiguration
{
    RowErrorHandler = (ex, row) =>
    {
        Console.WriteLine($"Skipping bad row: {ex.Message}");
        return true; // skip this row and continue
    }
};
```

## Example: Transform Pipeline (CSV → CSV)

```csharp
public sealed record UserSummary(string Name, string Domain);

public sealed class Summarizer : TransformNode<User, UserSummary>
{
    public override Task<UserSummary> TransformAsync(
        User item, PipelineContext context, CancellationToken cancellationToken)
    {
        var domain = item.Email.Split('@')[1];
        return Task.FromResult(new UserSummary(item.Name, domain));
    }
}

public sealed class CsvTransformPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            new CsvSourceNode<User>(
                StorageUri.FromFilePath("users.csv"),
                row => new User(
                    row.Get<int>("Id") ?? 0,
                    row.Get<string>("Name") ?? string.Empty,
                    row.Get<string>("Email") ?? string.Empty)),
            "csv-source");

        var transform = builder.AddTransform<Summarizer, User, UserSummary>("summarizer");
        var sink = builder.AddSink(
            new CsvSinkNode<UserSummary>(StorageUri.FromFilePath("summaries.csv")),
            "csv-sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}
```

## Next Steps

- [JSON Connector](json.md) — alternative format for structured data
- [Parquet Connector](parquet.md) — columnar format for large datasets
- [Storage Providers](../storage-providers/index.md) — read CSV from S3, Azure Blob, or GCS

## Storage Abstraction

All file connectors use `StorageUri` + `IStorageResolver` for pluggable storage:

```csharp
// Local file
var source = new CsvSourceNode<User>(StorageUri.FromFilePath("data/users.csv"), ...);

// Cloud storage via IStorageResolver
var source = new CsvSourceNode<User>(
    StorageUri.Parse("s3://my-bucket/data/users.csv"),
    resolver: myStorageResolver, ...);

// Azure Blob
var source = new CsvSourceNode<User>(
    StorageUri.Parse("az://container/data/users.csv"),
    resolver: myStorageResolver, ...);
```

See [Storage Providers](../storage-providers/index.md) for configuring S3, Azure Blob, GCS, SFTP, and ADLS Gen2.

## Attribute Mapping

The CSV connector supports `[Column]` and `[IgnoreColumn]` attributes for automatic mapping (when not using a custom row mapper):

```csharp
public class User
{
    [Column("user_id")]
    public int Id { get; set; }

    [Column("full_name")]
    public string Name { get; set; } = "";

    [IgnoreColumn]
    public string DisplayName => Name.ToUpper();
}
```

## Best Practices

1. **Use custom row mappers** for complex mappings or when header names don't match properties
2. **Increase `BufferSize`** (e.g., 65536) for large files — reduces I/O calls
3. **Use `RowErrorHandler`** to skip bad rows instead of failing the pipeline
4. **Use `StorageUri`** with `IStorageResolver` for cloud storage portability
5. **Set culture via `HelperConfiguration`** when parsing locale-sensitive numbers/dates
