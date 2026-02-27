# NPipeline Data Lake Connector

NPipeline Data Lake Connector provides table abstractions for building data lakes on top of the Parquet connector. This package enables partitioned writes,
manifest-based table management, time travel queries, and small-file compaction with Parquet as the default storage format.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based
applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for
resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Connectors.DataLake
```

## Requirements

- **.NET 8.0, 9.0, or 10.0**
- **NPipeline.Connectors.Parquet** (automatically included as a dependency)
- **NPipeline.Connectors** (automatically included as a dependency)
- **NPipeline.StorageProviders** (automatically included as a dependency)

## Relationship to Parquet Connector

This package builds on [`NPipeline.Connectors.Parquet`](../NPipeline.Connectors.Parquet/README.md) and uses Parquet as its default file format. The Data Lake
connector adds:

- **Hive-style partitioning**: Automatic directory structure with `column=value/` patterns
- **Manifest tracking**: NDJSON-based file inventory with snapshot IDs
- **Time travel**: Read table state as of a specific timestamp or snapshot
- **Compaction**: Merge small files into larger, query-optimized files
- **Format adapters**: Extensibility for Iceberg, Delta, or custom table formats

**Why this separation:** The Parquet connector handles single-file I/O with full Parquet feature support. The Data Lake connector adds table-level semantics (
partitioning, snapshots, time travel) without duplicating the Parquet implementation. This allows using either package independently or together.

## Features

- **Partition Specification**: Fluent API for defining multi-level partition schemes
- **Hive-Style Paths**: Standard `column=value/` directory structure for query engine compatibility
- **DataLakeTableWriter**: Append and snapshot APIs for writing partitioned data
- **DataLakeTableSourceNode**: Read all files in a table via manifest lookup
- **Time Travel**: Query historical table state by timestamp or snapshot ID
- **Manifest Tracking**: NDJSON manifest with per-snapshot files for auditability
- **Small-File Compaction**: Consolidate files below size thresholds
- **Format Adapter Interface**: Extend to Iceberg, Delta Lake, or custom formats

## Usage

### Defining Partition Specifications

Use [`PartitionSpec<T>`](Partitioning/PartitionSpec.cs:12) to define how records map to partition directories:

```csharp
using NPipeline.Connectors.DataLake.Partitioning;

public class SalesRecord
{
    public long Id { get; set; }
    public string ProductName { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateTime EventDate { get; set; }  // Partition column
    public string Region { get; set; } = string.Empty;  // Partition column
}

// Single-level partitioning
var spec = PartitionSpec<SalesRecord>.By(x => x.EventDate);

// Multi-level partitioning (produces: event_date=2025-01-15/region=EU/)
var spec = PartitionSpec<SalesRecord>
    .By(x => x.EventDate)
    .ThenBy(x => x.Region);

// Custom column names
var spec = PartitionSpec<SalesRecord>
    .By(x => x.EventDate, "date")
    .ThenBy(x => x.Region, "geo_region");

// No partitioning (all files in table root)
var spec = PartitionSpec<SalesRecord>.None();
```

**Why fluent builder:** The fluent API makes partition schemes readable and discoverable. Property expressions are compiled to delegates for efficient runtime
evaluation. Column names default to snake_case (e.g., `EventDate` → `event_date`) following Hive conventions.

### Writing Partitioned Data

Use [`DataLakeTableWriter<T>`](DataLakeTableWriter.cs) to write partitioned Parquet files:

```csharp
using NPipeline.Connectors.DataLake;
using NPipeline.Connectors.DataLake.Partitioning;
using NPipeline.Connectors.Parquet;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Models;
using NPipeline.DataFlow.DataPipes;

var resolver = StorageProviderFactory.CreateResolver();
var provider = StorageProviderFactory.GetProviderOrThrow(
    resolver,
    StorageUri.Parse("file:///data/warehouse"));

var tableUri = StorageUri.Parse("file:///data/warehouse/sales_table");
var partitionSpec = PartitionSpec<SalesRecord>
    .By(x => x.EventDate)
    .ThenBy(x => x.Region);

var config = new ParquetConfiguration
{
    RowGroupSize = 100_000,
    Compression = Parquet.CompressionMethod.Snappy
};

// Write data
await using var writer = new DataLakeTableWriter<SalesRecord>(
    provider,
    tableUri,
    partitionSpec,
    config);

Console.WriteLine($"Snapshot ID: {writer.SnapshotId}");

var dataPipe = new InMemoryDataPipe<SalesRecord>(records, "SalesData");
await writer.AppendAsync(dataPipe, CancellationToken.None);
```

**Generated directory structure:**

```
sales_table/
├── _manifest/
│   ├── manifest.ndjson
│   └── snapshots/
│       └── 20250215093045abcd1234.ndjson
├── event_date=2025-01-15-00-00-00/
│   ├── region=EU/
│   │   └── part-001.parquet
│   └── region=US/
│       └── part-001.parquet
└── event_date=2025-01-16-00-00-00/
    └── region=APAC/
        └── part-001.parquet
```

### Reading Table Data

Use [`DataLakeTableSourceNode<T>`](DataLakeTableSourceNode.cs:20) to read all data in a table:

```csharp
using NPipeline.Connectors.DataLake;

// Read latest snapshot
var sourceNode = new DataLakeTableSourceNode<SalesRecord>(
    provider,
    tableUri);

// Use in a pipeline
var builder = new PipelineBuilder();
var source = builder.AddSource(() => sourceNode, "table-source");
```

The source node:

1. Reads the manifest file to discover all data files
2. Deduplicates entries by path (keeps latest version)
3. Streams data from each file using `ParquetSourceNode<T>`

### Time Travel Queries

Read table state as of a specific point in time:

```csharp
using NPipeline.Connectors.DataLake;

// Read as of a specific timestamp
var asOfTimestamp = new DateTimeOffset(2025, 1, 15, 12, 0, 0, TimeSpan.Zero);
var timeTravelSource = new DataLakeTableSourceNode<SalesRecord>(
    provider,
    tableUri,
    asOfTimestamp);

// Read a specific snapshot by ID
var snapshotSource = new DataLakeTableSourceNode<SalesRecord>(
    provider,
    tableUri,
    snapshotId: "20250215093045abcd1234");
```

**Why time travel:** Time travel enables:

- **Debugging**: Reproduce issues by querying historical state
- **Audit**: Track data changes over time
- **Rollback analysis**: Compare current vs. previous snapshots
- **Point-in-time reporting**: Generate reports as of specific dates

### Manifest Format

The manifest is stored as NDJSON at `_manifest/manifest.ndjson`:

```json
{"path":"event_date=2025-01-15/region=EU/part-001.parquet","row_count":5000,"written_at":"2025-01-15T10:30:45Z","file_size_bytes":245678,"partition_values":{"event_date":"2025-01-15","region":"EU"},"snapshot_id":"20250215103045abcd1234","format_version":"v1","file_format":"parquet","compression":"snappy"}
{"path":"event_date=2025-01-15/region=US/part-001.parquet","row_count":3200,"written_at":"2025-01-15T10:30:46Z","file_size_bytes":198234,"partition_values":{"event_date":"2025-01-15","region":"US"},"snapshot_id":"20250215103045abcd1234","format_version":"v1","file_format":"parquet"}
```

Each [`ManifestEntry`](Manifest/ManifestEntry.cs:9) tracks:

| Field              | Description                              |
|--------------------|------------------------------------------|
| `path`             | Relative path from table base            |
| `row_count`        | Number of rows in the file               |
| `written_at`       | Timestamp when file was written          |
| `file_size_bytes`  | File size in bytes                       |
| `partition_values` | Partition key/value pairs                |
| `snapshot_id`      | ID of the snapshot containing this file  |
| `content_hash`     | Optional hash for integrity verification |
| `file_format`      | Format (e.g., "parquet")                 |
| `compression`      | Compression codec used                   |

**Why NDJSON:** Newline-delimited JSON allows:

- **Append-only writes**: New entries added without rewriting the entire file
- **Streaming reads**: Process entries line-by-line without loading entire manifest
- **Human readability**: Easy inspection with standard tools
- **Per-snapshot files**: Each snapshot gets its own manifest file for isolation

### Inspecting the Manifest

```csharp
using NPipeline.Connectors.DataLake.Manifest;

var manifestReader = new ManifestReader(provider, tableUri);

// Read all entries
var entries = await manifestReader.ReadAllAsync(cancellationToken);

foreach (var entry in entries)
{
    Console.WriteLine($"Path: {entry.Path}");
    Console.WriteLine($"  Rows: {entry.RowCount}, Size: {entry.FileSizeBytes:N0} bytes");
    Console.WriteLine($"  Snapshot: {entry.SnapshotId}");
    Console.WriteLine($"  Written: {entry.WrittenAt:yyyy-MM-dd HH:mm:ss}");

    if (entry.PartitionValues is not null)
    {
        var partitions = string.Join(", ",
            entry.PartitionValues.Select(kv => $"{kv.Key}={kv.Value}"));
        Console.WriteLine($"  Partitions: {partitions}");
    }
}

// Get available snapshot IDs
var snapshotIds = await manifestReader.GetSnapshotIdsAsync(cancellationToken);
```

### Compaction

Use [`DataLakeCompactor`](DataLakeCompactor.cs) to consolidate small files:

```csharp
using NPipeline.Connectors.DataLake;
using NPipeline.Connectors.DataLake.FormatAdapters;

var compactor = new DataLakeCompactor(provider, tableUri, new ParquetConfiguration());

// Dry run to see what would be compacted
var dryRunRequest = new TableCompactRequest
{
    TableBasePath = tableUri,
    Provider = provider,
    SmallFileThresholdBytes = 32L * 1024 * 1024, // 32 MB
    MinFilesToCompact = 5,
    MaxFilesToCompact = 100,
    TargetFileSizeBytes = 256L * 1024 * 1024, // 256 MB
    DryRun = true,
    DeleteOriginalFiles = true
};

var dryRunResult = await compactor.CompactAsync(dryRunRequest, cancellationToken);
Console.WriteLine($"Would compact {dryRunResult.FilesCompacted} files into {dryRunResult.FilesCreated}");

// Perform actual compaction
var actualRequest = dryRunRequest with { DryRun = false };
var result = await compactor.CompactAsync(actualRequest, cancellationToken);

Console.WriteLine($"Compacted {result.FilesCompacted} files in {result.Duration.TotalSeconds:N1}s");
Console.WriteLine($"Bytes: {result.BytesBefore:N0} → {result.BytesAfter:N0}");
```

**Why compaction:** Small files degrade query performance because:

- Query engines must open and close many file handles
- Metadata overhead (footers, schemas) is repeated per file
- Network latency compounds with many S3/Blob Storage requests

Compaction merges small files into fewer, larger files while preserving data and updating the manifest.

## Format Adapter Interface

Implement [`ITableFormatAdapter`](FormatAdapters/ITableFormatAdapter.cs:11) to support alternative table formats:

```csharp
using NPipeline.Connectors.DataLake.FormatAdapters;

public class IcebergFormatAdapter : ITableFormatAdapter
{
    public string Name => "iceberg";

    public Task AppendAsync(TableAppendRequest request, CancellationToken cancellationToken = default)
    {
        // Implement Iceberg-specific commit protocol
        // Write metadata files, update snapshot log, etc.
    }

    public Task<TableSnapshot> GetSnapshotAsync(TableSnapshotRequest request, CancellationToken cancellationToken = default)
    {
        // Read Iceberg metadata to resolve snapshot
    }

    public Task<TableCompactResult> CompactAsync(TableCompactRequest request, CancellationToken cancellationToken = default)
    {
        // Implement compaction with Iceberg metadata updates
    }

    public Task<IReadOnlyList<SnapshotSummary>> ListSnapshotsAsync(StorageUri tableBasePath, CancellationToken cancellationToken = default)
    {
        // Read Iceberg snapshot log
    }

    public Task<bool> TableExistsAsync(StorageUri tableBasePath, CancellationToken cancellationToken = default)
    {
        // Check for Iceberg metadata files
    }

    public Task CreateTableAsync(StorageUri tableBasePath, CancellationToken cancellationToken = default)
    {
        // Initialize Iceberg table metadata
    }
}
```

**Why format adapters:** Different table formats (Iceberg, Delta Lake, Hudi) have different:

- Metadata file formats and locations
- Commit protocols and concurrency models
- Time travel and snapshot semantics

The adapter interface isolates format-specific logic while reusing the core partitioning and file I/O infrastructure.

## Hive-Style Partition Paths

The connector generates standard Hive-style partition paths for compatibility with query engines:

```
event_date=2025-01-15/region=EU/
```

**Path format rules:**

| CLR Type         | Path Format           | Example                                |
|------------------|-----------------------|----------------------------------------|
| `DateOnly`       | `yyyy-MM-dd`          | `2025-01-15`                           |
| `DateTime`       | `yyyy-MM-dd-HH-mm-ss` | `2025-01-15-14-30-00`                  |
| `DateTimeOffset` | `yyyy-MM-dd-HH-mm-ss` | `2025-01-15-14-30-00`                  |
| `string`         | URL-encoded           | `Hello%20World`                        |
| `enum`           | Lowercase name        | `active`                               |
| `Guid`           | Lowercase D format    | `a1b2c3d4-e5f6-7890-abcd-ef1234567890` |
| Numeric types    | Invariant culture     | `12345`, `3.14`                        |

**Why Hive-style:** Hive-style partitioning is supported by:

- Apache Spark / PySpark
- AWS Athena / Glue
- Azure Synapse / Data Lake Analytics
- Google BigQuery
- Trino / Presto
- DuckDB

This allows the same data files to be queried by multiple engines without ETL.

## Production Considerations

### File Sizing

Target file sizes between 256 MB and 1 GB for optimal query performance:

```csharp
var config = new ParquetConfiguration
{
    RowGroupSize = 100_000,
    TargetFileSizeBytes = 512L * 1024 * 1024 // 512 MB
};
```

### Memory Management

Control memory during high-cardinality partition writes:

```csharp
var config = new ParquetConfiguration
{
    MaxBufferedRows = 500_000,  // Total rows across all partition buffers
    RowGroupSize = 50_000       // Rows per row group
};
```

### Idempotent Writes

Use idempotency keys to prevent duplicate data after retries:

```csharp
var request = new TableAppendRequest<SalesRecord>
{
    TableBasePath = tableUri,
    Provider = provider,
    PartitionSpec = partitionSpec,
    IdempotencyKey = $"batch-{batchId}"  // Deduplicates on retry
};
```

### Manifest Backup

The manifest is the source of truth for table contents. Consider:

- Versioning the `_manifest/` directory in object storage
- Periodic exports to a backup location
- Monitoring manifest file size (split if too large)

### Compaction Strategy

Run compaction as a scheduled job:

```csharp
// Compact files smaller than 32 MB into 256 MB files
var request = new TableCompactRequest
{
    TableBasePath = tableUri,
    Provider = provider,
    SmallFileThresholdBytes = 32L * 1024 * 1024,
    TargetFileSizeBytes = 256L * 1024 * 1024,
    MinFilesToCompact = 10,  // Only compact when enough small files exist
    DeleteOriginalFiles = true
};
```

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

    public DateTime EventDate { get; set; }  // Partition column

    public string Region { get; set; } = string.Empty;  // Partition column
}

public class DataLakePipeline : IPipelineDefinition
{
    private readonly StorageUri _tableUri = StorageUri.Parse("s3://warehouse/sales_table/");

    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var resolver = StorageProviderFactory.CreateResolver();
        var provider = StorageProviderFactory.GetProviderOrThrow(resolver, _tableUri);

        var partitionSpec = PartitionSpec<SalesRecord>
            .By(x => x.EventDate)
            .ThenBy(x => x.Region);

        var config = new ParquetConfiguration
        {
            RowGroupSize = 100_000,
            Compression = Parquet.CompressionMethod.Snappy,
            TargetFileSizeBytes = 512L * 1024 * 1024
        };

        // Source: Read from Data Lake table with time travel
        var asOfDate = new DateTimeOffset(2025, 1, 15, 0, 0, 0, TimeSpan.Zero);
        var source = builder.AddSource(
            new DataLakeTableSourceNode<SalesRecord>(provider, _tableUri, asOfDate),
            "lake-source");

        // Transform: Process records
        var transform = builder.AddTransform<SalesTransform, SalesRecord, SalesRecord>("transform");

        // Sink: Write back to Data Lake with partitioning
        var sink = builder.AddSink(
            new DataLakePartitionedSinkNode<SalesRecord>(
                provider,
                _tableUri,
                partitionSpec,
                config),
            "lake-sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}
```

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Connectors.Parquet](https://www.nuget.org/packages/NPipeline.Connectors.Parquet)** - Parquet file I/O (used internally)
- **[NPipeline.Connectors](https://www.nuget.org/packages/NPipeline.Connectors)** - Storage abstractions and base connectors
- **[NPipeline.Extensions.DependencyInjection](https://www.nuget.org/packages/NPipeline.Extensions.DependencyInjection)** - Dependency injection integration

## License

MIT License - see LICENSE file for details.
