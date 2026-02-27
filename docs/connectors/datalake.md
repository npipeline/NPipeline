---
title: Data Lake Connector
description: Write partitioned Parquet tables, read historical snapshots, and compact small files with NPipeline using the Data Lake connector.
sidebar_position: 8
---

## Data Lake Connector

The `NPipeline.Connectors.DataLake` package adds table-level semantics on top of the [Parquet connector](./parquet.md). It provides:

- **Hive-style partitioning** — automatically routes records into `column=value/` directory hierarchies
- **Manifest tracking** — an NDJSON-based file inventory that records snapshot IDs, row counts, and partition metadata
- **Time travel** — read the table as of any past timestamp or snapshot ID
- **Small-file compaction** — merge many small files into fewer, query-optimised files
- **Format adapter interface** — extend to Iceberg, Delta Lake, or custom table formats

Parquet is the default storage format. All file I/O uses the NPipeline storage abstraction so the same connector works with local files, S3, Azure Blob Storage, and any other `IStorageProvider`.

## Installation

```bash
dotnet add package NPipeline.Connectors.DataLake
```

This automatically pulls in `NPipeline.Connectors.Parquet`, `NPipeline.Connectors`, and `NPipeline.StorageProviders`.

For the core NPipeline package and other available extensions, see the [Installation Guide](../getting-started/installation.md).

## Relationship to the Parquet Connector

`NPipeline.Connectors.Parquet` handles single-file columnar I/O. `NPipeline.Connectors.DataLake` builds on top of it to add table-level concerns:

| Concern                  | Parquet Connector | Data Lake Connector |
|--------------------------|:-----------------:|:-------------------:|
| Read / write Parquet I/O | ✓                 | ✓ (via Parquet)     |
| Column projection        | ✓                 | ✓                   |
| Hive-style partitioning  |                   | ✓                   |
| Snapshot / manifest      |                   | ✓                   |
| Time travel              |                   | ✓                   |
| Small-file compaction    |                   | ✓                   |

You can use each package independently or together.

## Partition Specifications

A `PartitionSpec<T>` defines which properties determine the partition path. The fluent API compiles property expressions into efficient delegates at startup.

```csharp
using NPipeline.Connectors.DataLake.Partitioning;

public class SalesRecord
{
    public long Id { get; set; }
    public string ProductName { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateTime EventDate { get; set; }
    public string Region { get; set; } = string.Empty;
}

// Single-level — produces: event_date=2025-01-15/
var spec = PartitionSpec<SalesRecord>.By(x => x.EventDate);

// Multi-level — produces: event_date=2025-01-15/region=EU/
var spec = PartitionSpec<SalesRecord>
    .By(x => x.EventDate)
    .ThenBy(x => x.Region);

// Custom column names
var spec = PartitionSpec<SalesRecord>
    .By(x => x.EventDate, "date")
    .ThenBy(x => x.Region, "geo");

// No partitioning — all files written to the table root
var spec = PartitionSpec<SalesRecord>.None();
```

Column names default to `snake_case` (e.g., `EventDate` → `event_date`), matching Hive conventions and ensuring compatibility with Spark, Athena, Trino, and similar query engines.

### Hive-Style Path Formatting

| CLR Type         | Path Format           | Example |
|------------------|-----------------------|---------|
| `DateOnly`       | `yyyy-MM-dd`          | `2025-01-15` |
| `DateTime`       | `yyyy-MM-dd-HH-mm-ss` | `2025-01-15-14-30-00` |
| `DateTimeOffset` | `yyyy-MM-dd-HH-mm-ss` | `2025-01-15-14-30-00` |
| `string`         | URL-encoded           | `Hello%20World` |
| `enum`           | Lowercase name        | `active` |
| `Guid`           | Lowercase D format    | `a1b2c3d4-...` |
| Numeric types    | Invariant culture     | `12345`, `3.14` |

## `DataLakePartitionedSinkNode<T>`

`DataLakePartitionedSinkNode<T>` receives a stream of records and writes them to a partitioned table, flushing row groups as buffers fill.

### Constructors

```csharp
// Resolver-based (default resolver used when null)
public DataLakePartitionedSinkNode(
    StorageUri tableBasePath,
    PartitionSpec<T>? partitionSpec = null,
    IStorageResolver? resolver = null,
    ParquetConfiguration? configuration = null)

// Direct provider injection
public DataLakePartitionedSinkNode(
    IStorageProvider provider,
    StorageUri tableBasePath,
    PartitionSpec<T>? partitionSpec = null,
    ParquetConfiguration? configuration = null)
```

### Example: Partitioned Write

```csharp
using NPipeline.Connectors.DataLake;
using NPipeline.Connectors.DataLake.Partitioning;
using NPipeline.Connectors.Parquet;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Models;

public sealed class SalesWritePipeline : IPipelineDefinition
{
    private static readonly StorageUri TableUri =
        StorageUri.Parse("s3://warehouse/sales_table/");

    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var resolver     = StorageProviderFactory.CreateResolver();
        var partitionSpec = PartitionSpec<SalesRecord>
            .By(x => x.EventDate)
            .ThenBy(x => x.Region);

        var source = builder.AddSource<SalesSourceNode, SalesRecord>("source");

        var sink = builder.AddSink(
            new DataLakePartitionedSinkNode<SalesRecord>(
                StorageProviderFactory.GetProviderOrThrow(resolver, TableUri),
                TableUri,
                partitionSpec,
                new ParquetConfiguration
                {
                    RowGroupSize        = 100_000,
                    TargetFileSizeBytes = 512L * 1024 * 1024
                }),
            "lake_sink");

        builder.Connect(source, sink);
    }
}
```

**Generated directory structure:**

```
sales_table/
├── _manifest/
│   ├── manifest.ndjson
│   └── snapshots/
│       └── 20250215093045abcd1234.ndjson
├── event_date=2025-01-15/
│   ├── region=EU/
│   │   └── part-0001.parquet
│   └── region=US/
│       └── part-0001.parquet
└── event_date=2025-01-16/
    └── region=APAC/
        └── part-0001.parquet
```

Each write operation creates a new snapshot ID and appends entries to the manifest.

## `DataLakeTableSourceNode<T>`

`DataLakeTableSourceNode<T>` reads a table by consulting the manifest to discover data files, then streams all rows.

### Constructors

```csharp
// Latest snapshot
public DataLakeTableSourceNode(
    StorageUri tableBasePath,
    IStorageResolver? resolver = null,
    ParquetConfiguration? configuration = null)

// Direct provider
public DataLakeTableSourceNode(
    IStorageProvider provider,
    StorageUri tableBasePath,
    ParquetConfiguration? configuration = null)

// Time travel — as of a timestamp
public DataLakeTableSourceNode(
    StorageUri tableBasePath,
    DateTimeOffset asOf,
    IStorageResolver? resolver = null,
    ParquetConfiguration? configuration = null)

// Time travel — specific snapshot ID
public DataLakeTableSourceNode(
    StorageUri tableBasePath,
    string snapshotId,
    IStorageResolver? resolver = null,
    ParquetConfiguration? configuration = null)
```

### Example: Reading the Latest Snapshot

```csharp
var source = builder.AddSource(
    new DataLakeTableSourceNode<SalesRecord>(provider, TableUri),
    "lake_source");
```

### Example: Time Travel

```csharp
// By timestamp — returns data as it existed at that moment
var asOf   = new DateTimeOffset(2025, 1, 15, 12, 0, 0, TimeSpan.Zero);
var source = new DataLakeTableSourceNode<SalesRecord>(provider, TableUri, asOf);

// By snapshot ID — pinpoints an exact write operation
var source = new DataLakeTableSourceNode<SalesRecord>(
    provider,
    TableUri,
    snapshotId: "20250215093045abcd1234");
```

Time travel is useful for debugging, point-in-time reporting, and auditing data at earlier states.

## `DataLakeTableWriter<T>`

`DataLakeTableWriter<T>` is a lower-level type used when you need explicit control over snapshot lifecycle outside of a pipeline node. It exposes `AppendAsync` and implements `IAsyncDisposable` to flush the manifest on completion.

```csharp
using NPipeline.Connectors.DataLake;

await using var writer = new DataLakeTableWriter<SalesRecord>(
    provider,
    tableUri,
    partitionSpec,
    new ParquetConfiguration { RowGroupSize = 100_000 });

Console.WriteLine($"Snapshot ID: {writer.SnapshotId}");

var dataPipe = new InMemoryDataPipe<SalesRecord>(records, "SalesData");
await writer.AppendAsync(dataPipe, CancellationToken.None);
// Manifest is flushed when the writer is disposed
```

## Manifest

Every write appends entries to `_manifest/manifest.ndjson` (NDJSON format). Each `ManifestEntry` contains:

| Field              | Description |
|--------------------|-------------|
| `path`             | Relative path from the table base |
| `row_count`        | Number of rows in this file |
| `written_at`       | UTC timestamp of the write |
| `file_size_bytes`  | File size in bytes |
| `partition_values` | Map of partition column names to values |
| `snapshot_id`      | ID of the containing snapshot |
| `content_hash`     | Optional SHA-256 hash for integrity |
| `file_format`      | `"parquet"` |
| `compression`      | Codec used (e.g., `"snappy"`) |

NDJSON is used because it supports append-only writes and allows streaming reads — no full-file rewrite is required when adding new entries.

### Inspecting the Manifest Programmatically

```csharp
using NPipeline.Connectors.DataLake.Manifest;

var reader  = new ManifestReader(provider, tableUri);
var entries = await reader.ReadAllAsync(cancellationToken);

foreach (var entry in entries)
{
    Console.WriteLine($"{entry.Path}: {entry.RowCount:N0} rows, {entry.FileSizeBytes:N0} B");
    Console.WriteLine($"  Snapshot: {entry.SnapshotId}  Written: {entry.WrittenAt:u}");
}

var snapshotIds = await reader.GetSnapshotIdsAsync(cancellationToken);
```

## Compaction

Small files reduce query performance because each file incurs metadata overhead and network round trips. `DataLakeCompactor` consolidates files below a size threshold into larger files while updating the manifest.

```csharp
using NPipeline.Connectors.DataLake;
using NPipeline.Connectors.DataLake.FormatAdapters;

var compactor = new DataLakeCompactor(provider, tableUri);

// Dry run first to see impact
var request = new TableCompactRequest
{
    SmallFileThresholdBytes = 32L * 1024 * 1024,   // compact files < 32 MB
    TargetFileSizeBytes     = 256L * 1024 * 1024,  // target 256 MB output files
    MinFilesToCompact       = 5,
    MaxFilesToCompact       = 100,
    DeleteOriginalFiles     = true,
    DryRun                  = true
};

var preview = await compactor.CompactAsync(request, cancellationToken);
Console.WriteLine($"Would compact {preview.FilesCompacted} → {preview.FilesCreated} files");

// Apply for real
var result = await compactor.CompactAsync(request with { DryRun = false }, cancellationToken);
Console.WriteLine($"Done in {result.Duration.TotalSeconds:N1}s: {result.BytesBefore:N0} → {result.BytesAfter:N0} B");
```

Run compaction as a scheduled background job, triggered when the manifest contains more than a threshold number of small files.

## `ParquetConfiguration` for Data Lake

`DataLakePartitionedSinkNode` and `DataLakeTableSourceNode` accept the same `ParquetConfiguration` as the Parquet connector. The most relevant options are:

| Property              | Recommended Value | Reason |
|-----------------------|-------------------|--------|
| `RowGroupSize`        | `100,000`+        | Larger groups improve query engine scan speed |
| `TargetFileSizeBytes` | `256 MB`–`1 GB`   | Fewer files means lower metadata overhead |
| `MaxBufferedRows`     | `500,000`         | Prevents OOM during high-cardinality partition fan-out |
| `Compression`         | `Snappy`          | Fast codec compatible with all major query engines |

See the [Parquet connector docs](./parquet.md#parquetconfiguration) for the full reference.

## Format Adapter Interface

Implement `ITableFormatAdapter` to target alternative table formats such as Apache Iceberg or Delta Lake:

```csharp
using NPipeline.Connectors.DataLake.FormatAdapters;

public class IcebergFormatAdapter : ITableFormatAdapter
{
    public string Name => "iceberg";

    public Task AppendAsync(TableAppendRequest request, CancellationToken ct)
    {
        // Write Iceberg metadata files and update snapshot log
    }

    public Task<TableSnapshot> GetSnapshotAsync(TableSnapshotRequest request, CancellationToken ct)
    {
        // Resolve snapshot from Iceberg metadata
    }

    public Task<TableCompactResult> CompactAsync(TableCompactRequest request, CancellationToken ct)
    {
        // Compact with Iceberg metadata updates
    }

    public Task<IReadOnlyList<SnapshotSummary>> ListSnapshotsAsync(StorageUri tableBasePath, CancellationToken ct)
    {
        // Read Iceberg snapshot log
    }

    public Task<bool> TableExistsAsync(StorageUri tableBasePath, CancellationToken ct)
    {
        // Check for Iceberg metadata files
    }

    public Task CreateTableAsync(StorageUri tableBasePath, CancellationToken ct)
    {
        // Initialise Iceberg table metadata
    }
}
```

## Production Considerations

**File sizing.** Target 256 MB–1 GB per output file. Use `TargetFileSizeBytes` on `ParquetConfiguration` and run the compactor periodically to recover from streaming ingestion that naturally produces many small files.

**Memory during fan-out.** High-cardinality partition keys (e.g., `user_id`) can create thousands of open partition buffers. Set `MaxBufferedRows` to a value your heap can accommodate, and reduce `RowGroupSize` to increase flush frequency.

**Idempotent writes.** Pass an `IdempotencyKey` on the append request to prevent duplicate entries when a pipeline is retried after failure.

**Manifest growth.** The main manifest file grows with each write. Once it is large enough to affect read latency, snapshot files provide isolation — each snapshot is stored separately under `_manifest/snapshots/` and referenced from the top-level manifest.

**Compaction scheduling.** Run compaction when the manifest contains more than ~10–20 small files per partition. A lightweight cron or Azure Function / AWS Lambda reading `ManifestReader.ReadAllAsync` to count small files is sufficient.

## Complete Pipeline Example

```csharp
using NPipeline.Connectors.DataLake;
using NPipeline.Connectors.DataLake.Partitioning;
using NPipeline.Connectors.Parquet;
using NPipeline.Connectors.Parquet.Attributes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Models;

public class SalesRecord
{
    [ParquetColumn("sale_id")]
    public long Id { get; set; }

    [ParquetColumn("product_name")]
    public string ProductName { get; set; } = string.Empty;

    [ParquetDecimal(18, 2)]
    public decimal Amount { get; set; }

    public DateTime EventDate { get; set; }    // Partition key 1
    public string Region { get; set; } = string.Empty;  // Partition key 2
}

public sealed class DataLakePipeline : IPipelineDefinition
{
    private static readonly StorageUri TableUri =
        StorageUri.Parse("s3://warehouse/sales_table/");

    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var resolver  = StorageProviderFactory.CreateResolver();
        var provider  = StorageProviderFactory.GetProviderOrThrow(resolver, TableUri);

        var partitionSpec = PartitionSpec<SalesRecord>
            .By(x => x.EventDate)
            .ThenBy(x => x.Region);

        var config = new ParquetConfiguration
        {
            RowGroupSize        = 100_000,
            TargetFileSizeBytes = 512L * 1024 * 1024
        };

        // Read the table as of a specific date for reprocessing
        var asOf   = new DateTimeOffset(2025, 1, 15, 0, 0, 0, TimeSpan.Zero);
        var source = builder.AddSource(
            new DataLakeTableSourceNode<SalesRecord>(provider, TableUri, asOf),
            "lake_source");

        var transform = builder.AddTransform<SalesTransform, SalesRecord, SalesRecord>("transform");

        // Write the results back to the same table (new snapshot)
        var sink = builder.AddSink(
            new DataLakePartitionedSinkNode<SalesRecord>(
                provider,
                TableUri,
                partitionSpec,
                config),
            "lake_sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}
```

## Related Packages

- **[NPipeline.Connectors.Parquet](./parquet.md)** — Underlying Parquet file I/O
- **[NPipeline.StorageProviders.Aws](../storage-providers/aws.md)** — S3 storage provider for writing to AWS Data Lake environments
- **[NPipeline.StorageProviders.Azure](../storage-providers/azure.md)** — Azure Blob storage provider for Azure Data Lake Storage Gen2
