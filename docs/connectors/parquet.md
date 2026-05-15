---
title: "Parquet Connector"
description: "Read and write Apache Parquet files with column projection, compression, and schema validation."
order: 4
---

# Parquet Connector

The `NPipeline.Connectors.Parquet` package reads and writes Apache Parquet files using [Parquet.Net](https://github.com/aloneguid/parquet-dotnet). Optimized for large analytical datasets with column projection pushdown, multi-file parallel reads, configurable compression, atomic writes, and schema compatibility modes.

## Installation

```bash
dotnet add package NPipeline.Connectors.Parquet
```

**Dependencies:** [Parquet.Net](https://www.nuget.org/packages/Parquet.Net) 6.x, `NPipeline.Connectors`, `NPipeline.StorageProviders`

## Storage Abstraction

The Parquet connector uses NPipeline's storage abstraction layer. See the [CSV Connector — Storage Abstraction](csv.md#storage-abstraction) section for full details on `StorageUri`, `IStorageResolver`, and when you need an explicit resolver.

```csharp
// Local file (no resolver needed)
var source = new ParquetSourceNode<Order>(StorageUri.FromFilePath("orders.parquet"));

// Cloud storage (explicit resolver)
var source = new ParquetSourceNode<Order>(
    StorageUri.Parse("s3://bucket/orders.parquet"),
    resolver: myResolver);
```

## Column Mapping

### Attribute-Based Mapping

Use `[ParquetColumn]` to map properties to Parquet columns:

```csharp
using NPipeline.Connectors.Parquet;

public class Order
{
    [ParquetColumn("order_id")]
    public int Id { get; set; }

    public string CustomerName { get; set; } = string.Empty;

    [ParquetDecimal(18, 2)]  // Required for decimal properties
    public decimal Amount { get; set; }

    [ParquetColumn(Ignore = true)]
    public string InternalNote { get; set; } = string.Empty;
}
```

> ⚠️ **Important:** Decimal properties **must** have a `[ParquetDecimal(precision, scale)]` attribute. Parquet requires explicit precision and scale for decimal types.

The generic `[Column]` and `[IgnoreColumn]` attributes from `NPipeline.Connectors.Attributes` are also supported.

### Supported Type Mappings

| .NET Type | Parquet Type |
|-----------|-------------|
| `string` | UTF8 string |
| `int`, `short`, `byte` | INT32 |
| `long` | INT64 |
| `float` | FLOAT |
| `double` | DOUBLE |
| `bool` | BOOLEAN |
| `decimal` | DECIMAL (requires `[ParquetDecimal]`) |
| `DateTime`, `DateTimeOffset` | TIMESTAMP |
| `DateOnly` | DATE |
| `Guid` | FIXED_LEN_BYTE_ARRAY |
| `byte[]` | BYTE_ARRAY |
| `enum` | INT32 |
| `List<T>`, `T[]` | Repeated group |

### Lambda-Based Mapping

```csharp
var source = new ParquetSourceNode<Order>(
    StorageUri.FromFilePath("orders.parquet"),
    row => new Order
    {
        Id = row.Get<int>("order_id"),
        CustomerName = row.Get<string>("customer_name") ?? string.Empty,
        Amount = row.Get<decimal>("amount")
    });
```

## Source Node — `ParquetSourceNode<T>`

Reads Parquet files and emits each row as an item of type `T`. Streams row groups one at a time to limit memory usage.

### Constructors

```csharp
// Attribute-based mapping with optional resolver
public ParquetSourceNode(
    StorageUri uri,
    IStorageResolver? resolver = null,
    ParquetConfiguration? configuration = null)

// Lambda-based mapping with optional resolver
public ParquetSourceNode(
    StorageUri uri,
    Func<ParquetRow, T> rowMapper,
    IStorageResolver? resolver = null,
    ParquetConfiguration? configuration = null)

// Attribute-based mapping with explicit provider
public ParquetSourceNode(
    IStorageProvider provider,
    StorageUri uri,
    ParquetConfiguration? configuration = null)

// Lambda-based mapping with explicit provider
public ParquetSourceNode(
    IStorageProvider provider,
    StorageUri uri,
    Func<ParquetRow, T> rowMapper,
    ParquetConfiguration? configuration = null)
```

### Example: Column Projection

Read only the columns you need to reduce I/O:

```csharp
var config = new ParquetConfiguration
{
    ProjectedColumns = new[] { "OrderId", "Amount", "Date" },
    FileReadParallelism = 4
};

var source = new ParquetSourceNode<OrderSummary>(
    StorageUri.FromFilePath("orders.parquet"),
    configuration: config);
```

## Sink Node — `ParquetSinkNode<T>`

Writes items to a Parquet file with configurable row groups, compression, and atomic writes.

### Constructors

```csharp
// Attribute-based mapping with optional resolver
public ParquetSinkNode(
    StorageUri uri,
    IStorageResolver? resolver = null,
    ParquetConfiguration? configuration = null)

// Attribute-based mapping with explicit provider
public ParquetSinkNode(
    IStorageProvider provider,
    StorageUri uri,
    ParquetConfiguration? configuration = null)
```

### Example: Compressed Output

```csharp
var config = new ParquetConfiguration
{
    RowGroupSize = 100_000,
    Compression = CompressionMethod.Snappy,
    UseAtomicWrite = true
};

var sink = new ParquetSinkNode<Order>(
    StorageUri.FromFilePath("output.parquet"),
    configuration: config);
```

## Configuration

### Write Options

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `RowGroupSize` | `int` | `50,000` | Rows buffered before flushing a row group |
| `Compression` | `CompressionMethod` | `Snappy` | Codec: `Snappy`, `Gzip`, or `None` |
| `TargetFileSizeBytes` | `long?` | `256 MB` | Rotate files at this size; `null` disables |
| `UseAtomicWrite` | `bool` | `true` | Write to temp file then rename on success |
| `MaxBufferedRows` | `int` | `250,000` | Max rows across all partition buffers before flush |

### Read Options

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ProjectedColumns` | `IReadOnlyList<string>?` | `null` | Column whitelist — only read these columns |
| `SchemaCompatibility` | `SchemaCompatibilityMode` | `Strict` | Schema validation mode (see below) |
| `RecursiveDiscovery` | `bool` | `false` | Scan subdirectories for Parquet files |
| `FileReadParallelism` | `int` | `1` | Number of files to read in parallel |
| `RowFilter` | `Func<ParquetRow, bool>?` | `null` | Predicate to filter rows during read |
| `SchemaValidator` | `Func<ParquetSchema, bool>?` | `null` | Validate schema before reading |

### Error Handling

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `RowErrorHandler` | `Func<Exception, ParquetRow, bool>?` | `null` | Per-row error handler. Return `true` to skip, `false` to throw. |
| `Observer` | `IParquetConnectorObserver?` | `null` | Lifecycle events listener for metrics and diagnostics |

### Schema Compatibility Modes

| Mode | Behavior |
|------|----------|
| `Strict` | File schema must exactly match the target type. Extra or missing columns cause an error. |
| `Additive` | File may have extra columns (ignored) or missing columns (use defaults). |
| `NameOnly` | Match by column name only, ignoring type differences where safe conversion exists. |

## Performance Tips

- **Column projection** (`ProjectedColumns`): reduces I/O significantly for wide tables — only the requested columns are read from disk.
- **Parallel reads** (`FileReadParallelism`): set > 1 when reading multiple files or when storage supports concurrent access.
- **Row group sizing**: larger groups improve compression ratio but increase memory. 50K–100K rows is a good starting point.
- **Compression**: Snappy (default) balances speed and ratio. Use Gzip for better compression at the cost of CPU.

## Example: Full Pipeline (CSV → Parquet)

```csharp
public sealed class CsvToParquetPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            new CsvSourceNode<Order>(StorageUri.FromFilePath("orders.csv")),
            "csv-source");

        var config = new ParquetConfiguration
        {
            Compression = CompressionMethod.Snappy,
            UseAtomicWrite = true
        };
        var sink = builder.AddSink(
            new ParquetSinkNode<Order>(
                StorageUri.FromFilePath("orders.parquet"),
                configuration: config),
            "parquet-sink");

        builder.Connect(source, sink);
    }
}
```

## Next Steps

- [Data Lake Connector](datalake.md) — Hive-partitioned Parquet tables with time travel
- [CSV Connector](csv.md) — simpler text-based format
- [Storage Providers](../storage-providers/index.md) — read Parquet from cloud storage

## Storage Abstraction

All file connectors use `StorageUri` + `IStorageResolver`:

```csharp
// Local file
var source = new ParquetSourceNode<Order>(StorageUri.FromFilePath("orders.parquet"));

// Cloud storage
var source = new ParquetSourceNode<Order>(
    StorageUri.Parse("s3://my-bucket/data/orders.parquet"),
    resolver: myStorageResolver);
```

## Compression Codecs

| Codec | Ratio | Speed | Best For |
|-------|-------|-------|----------|
| `Snappy` (default) | Good | Fast | Most workloads |
| `Gzip` | Better | Slower | Storage-optimized, archival |
| `None` | — | Fastest | Already-compressed data, debugging |

## Supported .NET Types

| .NET Type | Parquet Type |
|-----------|-------------|
| `bool` | `BOOLEAN` |
| `int`, `long` | `INT32`, `INT64` |
| `float`, `double` | `FLOAT`, `DOUBLE` |
| `decimal` | `FIXED_LEN_BYTE_ARRAY` (Decimal) |
| `string` | `BYTE_ARRAY` (UTF8) |
| `DateTime`, `DateTimeOffset` | `INT96` or `INT64` (timestamp) |
| `byte[]` | `BYTE_ARRAY` |
| `Guid` | `FIXED_LEN_BYTE_ARRAY` |
| Nullable variants | Same with optional repetition |

## Atomic Writes

```csharp
var config = new ParquetConfiguration { UseAtomicWrite = true };
```

With atomic writes enabled, data is written to a temporary file and renamed on success. This prevents partial files on failure — critical for data lake scenarios.

## Best Practices

1. **Use column projection** — only read what you need for wide tables
2. **Use `Snappy` compression** (default) — best speed/ratio tradeoff
3. **Enable atomic writes** for production — prevents partial files
4. **Set `RowGroupSize = 50,000–100,000`** — balances compression and memory
5. **Use `RecursiveDiscovery`** when reading partitioned directories
6. **Use `FileReadParallelism > 1`** for multi-file reads on fast storage
