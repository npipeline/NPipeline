---
title: Parquet Connector
description: Read from and write to Apache Parquet files with NPipeline using the Parquet connector.
sidebar_position: 7
---

## Parquet Connector

The `NPipeline.Connectors.Parquet` package provides specialized source and sink nodes for working with Apache Parquet files. Parquet is a columnar storage format optimised for analytical workloads, offering compact file sizes through column-level compression and efficient columnar scans.

This connector uses the [Parquet.Net](https://github.com/aloneguid/parquet-dotnet) library for efficient streaming row-group I/O.

## Installation

To use the Parquet connector, install the `NPipeline.Connectors.Parquet` NuGet package:

```bash
dotnet add package NPipeline.Connectors.Parquet
```

For the core NPipeline package and other available extensions, see the [Installation Guide](../getting-started/installation.md).

## Storage Abstraction Layer

The Parquet connector uses NPipeline's storage abstraction layer, which provides a unified way to work with different storage systems.

> **Note:** The storage abstraction layer is provided by the `NPipeline.StorageProviders` namespace/assembly.

### StorageUri

The `StorageUri` class represents a normalised storage location URI. It supports both absolute URIs (e.g., `s3://bucket/key`) and local file paths:

```csharp
// For local files
var localFile = StorageUri.FromFilePath("data/transactions.parquet");

// For a local directory (reads all Parquet files in the directory)
var localDir = StorageUri.FromFilePath("data/sales/");

// For cloud storage
var s3Uri = StorageUri.Parse("s3://my-bucket/warehouse/sales.parquet");
var azureUri = StorageUri.Parse("azure://my-container/warehouse/sales.parquet");
```

### IStorageResolver

The `IStorageResolver` interface resolves the storage provider to use for a given `StorageUri`. When no resolver is provided, a default resolver backed by the local file system is used automatically.

You only need to supply an explicit resolver when working with cloud storage or custom providers:

```csharp
using NPipeline.StorageProviders;

var resolver = StorageProviderFactory.CreateResolver();

// Or with additional cloud providers:
var resolver = StorageProviderFactory.CreateResolver(
    new StorageResolverOptions
    {
        AdditionalProviders = new[] { new S3StorageProvider() }
    });
```

## Attributes

### `[ParquetColumn]`

The `[ParquetColumn]` attribute (from `NPipeline.Connectors.Parquet.Attributes`) maps a CLR property to a Parquet column. Without it, the property name is used verbatim (Parquet column names are case-sensitive).

```csharp
using NPipeline.Connectors.Parquet.Attributes;

public class Transaction
{
    [ParquetColumn("transaction_id")]   // explicit column name
    public long Id { get; set; }

    [ParquetColumn]                     // uses property name "CustomerName"
    public string CustomerName { get; set; } = string.Empty;

    [ParquetColumn(Ignore = true)]      // excluded from mapping
    public string InternalCode { get; set; } = string.Empty;
}
```

### `[ParquetDecimal]`

The `[ParquetDecimal]` attribute is **required** on every `decimal` property when writing. Parquet's decimal type requires explicit precision and scale at schema-definition time.

```csharp
public class FinancialRecord
{
    [ParquetDecimal(precision: 18, scale: 2)]   // e.g. 1234567890123456.78
    public decimal Amount { get; set; }

    [ParquetDecimal(precision: 28, scale: 8)]
    public decimal ExchangeRate { get; set; }
}
```

Omitting this attribute on a decimal write property will throw a `SchemaBuilderException` at pipeline startup.

## `ParquetSourceNode<T>`

`ParquetSourceNode<T>` reads one or more Parquet files and emits each row as an item of type `T`. Files are read row-group by row-group to bound memory usage.

### Constructors

```csharp
// Attribute-based mapping — resolver is optional for local files
public ParquetSourceNode(
    StorageUri uri,
    IStorageResolver? resolver = null,
    ParquetConfiguration? configuration = null)

// Explicit row mapping — full control over type conversions
public ParquetSourceNode(
    StorageUri uri,
    Func<ParquetRow, T> rowMapper,
    IStorageResolver? resolver = null,
    ParquetConfiguration? configuration = null)

// Direct provider injection
public ParquetSourceNode(
    IStorageProvider provider,
    StorageUri uri,
    ParquetConfiguration? configuration = null)
```

### Example: Attribute-Based Mapping

```csharp
using NPipeline;
using NPipeline.Connectors.Parquet;
using NPipeline.Connectors.Parquet.Attributes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;

public class Transaction
{
    [ParquetColumn("transaction_id")]
    public long Id { get; set; }

    [ParquetColumn("customer_name")]
    public string CustomerName { get; set; } = string.Empty;

    [ParquetDecimal(18, 2)]
    public decimal Amount { get; set; }

    public DateTime TransactionDate { get; set; }
}

public sealed class ParquetReaderPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            new ParquetSourceNode<Transaction>(
                StorageUri.FromFilePath("transactions.parquet")),
            "parquet_source");

        var sink = builder.AddSink<ConsoleSinkNode, Transaction>("console_sink");

        builder.Connect(source, sink);
    }
}
```

### Example: Explicit Row Mapping

```csharp
var source = new ParquetSourceNode<Transaction>(
    StorageUri.FromFilePath("transactions.parquet"),
    row => new Transaction
    {
        Id           = row.Get<long>("transaction_id"),
        CustomerName = row.GetOrDefault("customer_name", string.Empty),
        Amount       = row.Get<decimal>("amount"),
        TransactionDate = row.Get<DateTime>("transaction_date")
    });
```

Use explicit mapping when you need custom type conversions, conditional null handling, or column renaming without additional attributes.

### Reading a Directory

When the URI points to a directory, the source discovers all Parquet files within it (optionally recursive):

```csharp
var source = new ParquetSourceNode<Transaction>(
    StorageUri.FromFilePath("data/sales/"),
    configuration: new ParquetConfiguration
    {
        RecursiveDiscovery = true,
        FileReadParallelism = 4,
        ProjectedColumns = ["transaction_id", "amount"]
    });
```

## `ParquetSinkNode<T>`

`ParquetSinkNode<T>` writes items from the pipeline into a Parquet file, buffering rows into row groups before flushing.

### Constructors

```csharp
// Resolver-based (default resolver used when null)
public ParquetSinkNode(
    StorageUri uri,
    IStorageResolver? resolver = null,
    ParquetConfiguration? configuration = null)

// Direct provider injection
public ParquetSinkNode(
    IStorageProvider provider,
    StorageUri uri,
    ParquetConfiguration? configuration = null)
```

### Example: Writing a Parquet File

```csharp
public sealed class ParquetWriterPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<TransactionSourceNode, Transaction>("source");

        var sink = builder.AddSink(
            new ParquetSinkNode<Transaction>(
                StorageUri.FromFilePath("output/transactions.parquet"),
                configuration: new ParquetConfiguration
                {
                    RowGroupSize  = 100_000,
                    Compression   = Parquet.CompressionMethod.Gzip,
                    UseAtomicWrite = true
                }),
            "parquet_sink");

        builder.Connect(source, sink);
    }
}
```

## `ParquetConfiguration`

`ParquetConfiguration` controls both read and write behaviour.

### Write options

| Property              | Type                | Default   | Description |
|-----------------------|---------------------|-----------|-------------|
| `RowGroupSize`        | `int`               | `50,000`  | Rows buffered before flushing a row group. Larger values improve scan speed; smaller values reduce peak memory. |
| `Compression`         | `CompressionMethod` | `Snappy`  | Compression codec: `Snappy`, `Gzip`, or `None`. |
| `TargetFileSizeBytes` | `long?`             | `256 MB`  | Rotate to a new file when this size is reached (best-effort). Set `null` to disable rotation. |
| `UseAtomicWrite`      | `bool`              | `true`    | Write to a temp file then rename on success, preventing partial-file visibility on crash. |
| `MaxBufferedRows`     | `int`               | `250,000` | Maximum rows held across all partition buffers. Guards against memory exhaustion during high-cardinality fan-out. |

### Read options

| Property              | Type                            | Default  | Description |
|-----------------------|---------------------------------|----------|-------------|
| `ProjectedColumns`    | `IReadOnlyList<string>?`        | `null`   | Column whitelist. Only these columns are materialised, reducing I/O. |
| `SchemaValidator`     | `Func<ParquetSchema, bool>?`    | `null`   | Called before reading begins. Return `false` to abort with an exception. |
| `SchemaCompatibility` | `SchemaCompatibilityMode`       | `Strict` | How to handle mismatches between the file schema and the CLR model. |
| `RecursiveDiscovery`  | `bool`                          | `false`  | Scan sub-directories when resolving Parquet files from a directory URI. |
| `FileReadParallelism` | `int`                           | `1`      | Files read in parallel per source node. `1` gives deterministic ordering. |
| `RowFilter`           | `Func<ParquetRow, bool>?`       | `null`   | Row-level predicate applied after row-group metadata filtering. |

### Error handling

| Property          | Type                                 | Default | Description |
|-------------------|--------------------------------------|---------|-------------|
| `RowErrorHandler` | `Func<Exception, ParquetRow, bool>?` | `null`  | Called when row mapping throws. Return `true` to skip the row; `false` or rethrow to fail the pipeline. |
| `Observer`        | `IParquetConnectorObserver?`         | `null`  | Listener for file and row-group lifecycle events. |

### Example

```csharp
var config = new ParquetConfiguration
{
    // Write
    RowGroupSize          = 100_000,
    Compression           = CompressionMethod.Gzip,
    TargetFileSizeBytes   = 512L * 1024 * 1024,
    UseAtomicWrite        = true,

    // Read
    ProjectedColumns      = ["id", "amount", "event_date"],
    SchemaCompatibility   = SchemaCompatibilityMode.Additive,
    RecursiveDiscovery    = true,
    FileReadParallelism   = 4,

    // Error handling
    RowErrorHandler = (ex, row) =>
    {
        Console.WriteLine($"Skipping bad row: {ex.Message}");
        return true; // skip
    }
};
```

## Schema Compatibility Modes

| Mode       | Behaviour |
|------------|-----------|
| `Strict`   | All mapped CLR properties must exist in the file with matching types. Any divergence throws. |
| `Additive` | Missing columns map to CLR default values; nullable properties become `null`. Extra columns are ignored. |
| `NameOnly`  | Columns matched by name only; compatible type coercions are applied (e.g., `int` → `long`). |

Use `Strict` for critical pipelines where schema drift is an error. Use `Additive` when adding columns to an existing table. Use `NameOnly` when integrating with external systems using slightly different numeric types.

## `ParquetRow`

`ParquetRow` provides typed access to a row's column values when using explicit row mapping.

```csharp
// Typed get — throws if column is missing or type is incompatible
long id       = row.Get<long>("transaction_id");
decimal amount = row.Get<decimal>("amount");

// Get with fallback default
string name   = row.GetOrDefault("customer_name", "Unknown");
decimal? disc = row.GetOrDefault<decimal?>("discount", null);

// Null check
if (row.IsNull("optional_field")) { /* handle null */ }

// Existence check
if (row.HasColumn("legacy_field")) { /* backward compat */ }

// TryGet pattern
if (row.TryGet("discount", out decimal? discount))
    ApplyDiscount(discount!.Value);

// Schema introspection
Console.WriteLine($"Columns: {string.Join(", ", row.ColumnNames)}");
```

## Observability

Implement `IParquetConnectorObserver` to bridge connector events into your logging or metrics infrastructure:

```csharp
public sealed class LoggingParquetObserver : IParquetConnectorObserver
{
    public void OnFileReadStarted(StorageUri uri)
        => logger.LogInformation("Reading {Path}", uri.Path);

    public void OnFileReadCompleted(StorageUri uri, long rows, long bytes, TimeSpan elapsed)
        => logger.LogInformation("Read {Rows:N0} rows ({Bytes:N0} B) in {Ms:N0} ms", rows, bytes, elapsed.TotalMilliseconds);

    public void OnFileWriteCompleted(StorageUri uri, long rows, long bytes, TimeSpan elapsed)
        => logger.LogInformation("Wrote {Rows:N0} rows in {Ms:N0} ms", rows, elapsed.TotalMilliseconds);

    public void OnRowGroupRead(StorageUri uri, int index, long count)
        => logger.LogDebug("Row group {Index}: {Count:N0} rows", index, count);

    public void OnRowGroupWritten(StorageUri uri, int index, long count)
        => logger.LogDebug("Flushed row group {Index}: {Count:N0} rows", index, count);

    public void OnRowMappingError(StorageUri uri, Exception ex)
        => logger.LogWarning(ex, "Mapping error in {Path}", uri.Path);
}
```

Then pass it via `ParquetConfiguration.Observer`.

## Supported Types

| CLR Type                        | Parquet Type        | Notes |
|---------------------------------|---------------------|-------|
| `string`                        | `STRING`            | UTF-8 encoded |
| `int` / `short` / `byte`        | `INT32`             | |
| `long`                          | `INT64`             | |
| `float`                         | `FLOAT`             | IEEE 754 single |
| `double`                        | `DOUBLE`            | IEEE 754 double |
| `bool`                          | `BOOLEAN`           | |
| `decimal`                       | `DECIMAL`           | Requires `[ParquetDecimal]` |
| `DateTime`                      | `INT64` (timestamp) | Stored as UTC |
| `DateTimeOffset`                | `INT64` (timestamp) | Converted to UTC |
| `byte[]`                        | `BYTE_ARRAY`        | Binary data |
| `Guid`                          | `STRING`            | Formatted string |
| `int?`, `long?`, `decimal?` … | Optional            | Any value type can be nullable |

## Performance Considerations

**Row-group size.** The default of 50,000 rows balances memory pressure with scan efficiency. For analytics workloads with ample memory, raise to 100,000–1,000,000. For streaming pipelines with strict memory budgets, lower to 10,000–25,000.

**Compression.** Snappy (default) is fast with moderate ratios — suitable for most workloads. Gzip provides higher compression at the cost of CPU; prefer it for cold storage or bandwidth-constrained environments.

**Column projection.** Use `ProjectedColumns` to read only what you need. For wide schemas (50+ columns) this can reduce I/O by an order of magnitude.

**Atomic writes.** `UseAtomicWrite = true` (default) writes to a temp file and atomically renames on completion. Disable only if the storage backend does not support atomic moves and write latency is critical.

**Parallel reads.** Increase `FileReadParallelism` when reading many small files from a low-latency store such as local NVMe or a nearby S3 bucket. Keep it at `1` for ordered processing requirements.

## Complete Pipeline Example

```csharp
using NPipeline.Connectors.Parquet;
using NPipeline.Connectors.Parquet.Attributes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Models;
using Parquet;

public class SalesRecord
{
    [ParquetColumn("sale_id")]
    public long Id { get; set; }

    [ParquetColumn("product_name")]
    public string ProductName { get; set; } = string.Empty;

    [ParquetDecimal(18, 2)]
    public decimal Amount { get; set; }

    public DateTime SaleDate { get; set; }
}

public sealed class SalesPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var resolver = StorageProviderFactory.CreateResolver();

        var source = builder.AddSource(
            new ParquetSourceNode<SalesRecord>(
                StorageUri.FromFilePath("data/sales/"),
                resolver,
                new ParquetConfiguration
                {
                    RecursiveDiscovery  = true,
                    SchemaCompatibility = SchemaCompatibilityMode.Additive
                }),
            "parquet_source");

        var transform = builder.AddTransform<SalesTransform, SalesRecord, SalesRecord>("transform");

        var sink = builder.AddSink(
            new ParquetSinkNode<SalesRecord>(
                StorageUri.FromFilePath("output/processed_sales.parquet"),
                resolver,
                new ParquetConfiguration
                {
                    RowGroupSize = 100_000,
                    Compression  = CompressionMethod.Gzip
                }),
            "parquet_sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}
```
