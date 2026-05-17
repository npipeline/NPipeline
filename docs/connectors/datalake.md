---
title: "Data Lake Connector"
description: "Read and write Hive-partitioned Parquet tables with manifest-based snapshots and time travel."
order: 7
---

# Data Lake Connector

The `NPipeline.Connectors.DataLake` package provides source and sink nodes for Hive-style partitioned Parquet tables. It manages manifest files for snapshot tracking, enabling time-travel queries and atomic writes across partitions. Works with any storage backend (local, S3, Azure Blob, GCS) via the storage abstraction layer.

## Installation

```bash
dotnet add package NPipeline.Connectors.DataLake
```

**Dependencies:** `NPipeline.Connectors.Parquet`, `NPipeline.StorageProviders`

## Relationship to Parquet Connector

| | Parquet Connector | Data Lake Connector |
|---|---|---|
| **Scope** | Single Parquet file | Multi-file partitioned table |
| **Partitioning** | None | Hive-style (`year=2024/month=01/`) |
| **Snapshots** | None | Manifest-tracked snapshots |
| **Time travel** | None | Read historical snapshots with `asOf` |
| **Use case** | Simple file I/O | Analytical data lake tables |

## Source Node - `DataLakeTableSourceNode<T>`

Reads all Parquet files in a table, following the manifest to resolve the current (or historical) snapshot.

### Constructors

```csharp
// Current snapshot with optional resolver
public DataLakeTableSourceNode(
    StorageUri tableBasePath,
    IStorageResolver? resolver = null,
    ParquetConfiguration? configuration = null)

// Current snapshot with explicit provider
public DataLakeTableSourceNode(
    IStorageProvider provider,
    StorageUri tableBasePath,
    ParquetConfiguration? configuration = null)

// Historical snapshot (time travel)
public DataLakeTableSourceNode(
    StorageUri tableBasePath,
    DateTimeOffset asOf,
    IStorageResolver? resolver = null,
    ParquetConfiguration? configuration = null)

// Historical snapshot with explicit provider
public DataLakeTableSourceNode(
    IStorageProvider provider,
    StorageUri tableBasePath,
    DateTimeOffset asOf,
    ParquetConfiguration? configuration = null)
```

### Example: Time Travel

```csharp
// Read the table as it was yesterday
var source = new DataLakeTableSourceNode<SalesRecord>(
    StorageUri.Parse("s3://data-lake/sales"),
    asOf: DateTimeOffset.UtcNow.AddDays(-1),
    resolver: myResolver);
```

## Sink Node - `DataLakePartitionedSinkNode<T>`

Writes items to a partitioned table, routing each item to the appropriate partition directory based on a `PartitionSpec`.

### Constructors

```csharp
// With optional resolver
public DataLakePartitionedSinkNode(
    StorageUri tableBasePath,
    PartitionSpec<T>? partitionSpec = null,
    IStorageResolver? resolver = null,
    ParquetConfiguration? configuration = null)

// With explicit provider
public DataLakePartitionedSinkNode(
    IStorageProvider provider,
    StorageUri tableBasePath,
    PartitionSpec<T>? partitionSpec = null,
    ParquetConfiguration? configuration = null)
```

### Example: Partitioned Write

```csharp
var partitionSpec = new PartitionSpec<SalesRecord>()
    .AddColumn("year", r => r.OrderDate.Year.ToString())
    .AddColumn("region", r => r.Region);

var sink = new DataLakePartitionedSinkNode<SalesRecord>(
    StorageUri.Parse("s3://data-lake/sales"),
    partitionSpec: partitionSpec,
    resolver: myResolver,
    configuration: new ParquetConfiguration
    {
        Compression = CompressionMethod.Snappy,
        UseAtomicWrite = true
    });
```

This produces a directory structure like:

```
s3://data-lake/sales/
  year=2024/region=US/part-00001.parquet
  year=2024/region=EU/part-00001.parquet
  year=2025/region=US/part-00001.parquet
  _manifest/
    snapshot-20240115T120000Z.json
```

## Configuration

The Data Lake connector uses `ParquetConfiguration` for all Parquet-specific settings (compression, row group size, etc.). See [Parquet Connector - Configuration](parquet.md#configuration) for the full property reference.

Key options for data lake use:

| Property | Recommendation | Why |
|----------|---------------|-----|
| `UseAtomicWrite` | `true` | Prevents partial files on failure |
| `Compression` | `Snappy` | Fast compression for analytical queries |
| `RowGroupSize` | `50,000–100,000` | Balance between compression and memory |
| `MaxBufferedRows` | `250,000` | Controls memory across partition buffers |

## Example: Full Pipeline

```csharp
public sealed class SalesIngestionPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            new CsvSourceNode<SalesRecord>(StorageUri.FromFilePath("daily-sales.csv")),
            "csv-source");

        var partitionSpec = new PartitionSpec<SalesRecord>()
            .AddColumn("year", r => r.Date.Year.ToString())
            .AddColumn("month", r => r.Date.Month.ToString("D2"));

        var sink = builder.AddSink(
            new DataLakePartitionedSinkNode<SalesRecord>(
                StorageUri.Parse("s3://data-lake/sales"),
                partitionSpec: partitionSpec,
                resolver: myResolver),
            "lake-sink");

        builder.Connect(source, sink);
    }
}
```

## Next Steps

- [Parquet Connector](parquet.md) - single-file Parquet I/O
- [Storage Providers](../storage-providers/index.md) - configure S3, Azure Blob, or GCS
- [DuckDB Connector](duckdb.md) - query data lake files with SQL

## Partitioning

### Hive-Style Partitions

The `PartitionSpec<T>` defines how records are partitioned into directories:

```csharp
var spec = new PartitionSpec<SalesRecord>()
    .AddColumn("year", r => r.Date.Year.ToString())
    .AddColumn("month", r => r.Date.Month.ToString("D2"))
    .AddColumn("region", r => r.Region);
```

Produces: `year=2024/month=01/region=US/part-00001.parquet`

### Reading Partitioned Data

```csharp
var source = new DataLakeSourceNode<SalesRecord>(
    StorageUri.Parse("s3://data-lake/sales"),
    resolver: myResolver,
    configuration: new ParquetConfiguration
    {
        RecursiveDiscovery = true,
        FileReadParallelism = 4
    });
```

With `RecursiveDiscovery = true`, the connector reads all `.parquet` files in subdirectories.

## Manifest / Snapshots

The connector writes a `_manifest/` directory with snapshot metadata:

```
_manifest/
  snapshot-20240115T120000Z.json
```

Snapshots track which partition files were written in each pipeline run, enabling time-travel queries and incremental processing.

## Schema Evolution

When reading, use `SchemaCompatibilityMode.Additive` to handle schema drift:

```csharp
var config = new ParquetConfiguration
{
    SchemaCompatibility = SchemaCompatibilityMode.Additive
};
```

- **Strict**: File must exactly match target type
- **Additive**: Extra columns ignored, missing columns use defaults

## Best Practices

1. **Choose partition keys carefully** - high-cardinality keys create too many small files
2. **Use `UseAtomicWrite = true`** - prevents partial files on failure
3. **Use `Snappy` compression** - fast and widely supported by query engines
4. **Set `MaxBufferedRows`** to control memory across partition buffers
5. **Use `RecursiveDiscovery`** for reading - handles partition subdirectories automatically
6. **Query with DuckDB** - use the DuckDB connector to query data lake files with SQL
