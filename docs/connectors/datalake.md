---
title: "Data Lake Connector"
description: "Read and write Hive-partitioned Parquet tables with manifest-based snapshots and time travel."
order: 7
---

# Data Lake Connector

The `NPipeline.Connectors.DataLake` package provides source and sink nodes for Hive-style partitioned Parquet tables. It manages manifest files for snapshot tracking, enabling time-travel queries across partitions. Works with any storage backend (local, S3, Azure Blob, GCS) via the storage abstraction layer.

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

Reads every data file of a table's current snapshot, or of an earlier one, following the manifest. Files are read with
the [Parquet connector](parquet.md), so records map to columns as described there.

```csharp
// The current snapshot
new DataLakeTableSourceNode<SalesRecord>(StorageUri tableBasePath, IStorageResolver? resolver = null)
new DataLakeTableSourceNode<SalesRecord>(IStorageProvider provider, StorageUri tableBasePath)

// The table as it was at a point in time
new DataLakeTableSourceNode<SalesRecord>(StorageUri tableBasePath, DateTimeOffset asOf, IStorageResolver? resolver = null)
new DataLakeTableSourceNode<SalesRecord>(IStorageProvider provider, StorageUri tableBasePath, DateTimeOffset asOf)

// A specific snapshot
new DataLakeTableSourceNode<SalesRecord>(StorageUri tableBasePath, string snapshotId, IStorageResolver? resolver = null)
new DataLakeTableSourceNode<SalesRecord>(IStorageProvider provider, StorageUri tableBasePath, string snapshotId)
```

### Example: Time Travel

```csharp
// Read the table as it was yesterday
var source = new DataLakeTableSourceNode<SalesRecord>(
    StorageUri.Parse("s3://data-lake/sales"),
    asOf: DateTimeOffset.UtcNow.AddDays(-1),
    resolver: myResolver);
```

Query parameters on the table URI (credentials, a region) are kept on every data file's URI.

## Sink Node - `DataLakePartitionedSinkNode<T>`

Writes items to a partitioned table, routing each item to the partition directory its `PartitionSpec` gives it, and
records the files in the manifest once they are written.

```csharp
new DataLakePartitionedSinkNode<SalesRecord>(
    StorageUri tableBasePath,
    PartitionSpec<SalesRecord>? partitionSpec = null,
    IStorageResolver? resolver = null,
    DataLakeParquetOptions? options = null)

new DataLakePartitionedSinkNode<SalesRecord>(
    IStorageProvider provider,
    StorageUri tableBasePath,
    PartitionSpec<SalesRecord>? partitionSpec = null,
    DataLakeParquetOptions? options = null)
```

`DataLakeTableWriter<T>` offers the same writes outside a pipeline, with `AppendAsync` and snapshot queries.

### Example: Partitioned Write

```csharp
var partitionSpec = PartitionSpec<SalesRecord>.By(r => r.Year).ThenBy(r => r.Region);

var sink = new DataLakePartitionedSinkNode<SalesRecord>(
    StorageUri.Parse("s3://data-lake/sales"),
    partitionSpec,
    myResolver,
    new DataLakeParquetOptions { Codec = CompressionMethod.Zstd });
```

This produces a directory structure like:

```
s3://data-lake/sales/
  year=2024/region=US/part-00001-1a2b3c4d.parquet
  year=2024/region=EU/part-00002-5e6f7a8b.parquet
  year=2025/region=US/part-00003-9c0d1e2f.parquet
  _manifest/
    manifest.ndjson
    snapshots/
```

## Options

`DataLakeParquetOptions` sets how the table's data files are written and buffered. The writer, the partitioned sink and
the compactor take it; readers need none.

| Option | Default | Description |
| --- | --- | --- |
| `RowGroupSize` | `50_000` | Rows buffered per partition before a data file is written, and the most rows per row group |
| `RowGroupBytes` | 128 MB | A row group is also flushed once its buffered values reach about this size |
| `Codec` | `Snappy` | The compression codec |
| `MaxBufferedRows` | `250_000` | The most rows buffered across all partitions; beyond it the largest buffers are written early |

Data files have unique names and only become part of the table when the manifest records them. Each data file is
committed when it's complete, so a failed write leaves no partial file at its final name.

## Example: Full Pipeline

```csharp
public sealed class SalesIngestionPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            CsvConnector.Source<SalesRecord>(StorageUri.FromFilePath("daily-sales.csv")),
            "csv-source");

        var partitionSpec = PartitionSpec<SalesRecord>.By(r => r.Year).ThenBy(r => r.Month);

        var sink = builder.AddSink(
            new DataLakePartitionedSinkNode<SalesRecord>(
                StorageUri.Parse("s3://data-lake/sales"),
                partitionSpec,
                myResolver),
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
var spec = PartitionSpec<SalesRecord>.By(r => r.Year)
    .ThenBy(r => r.Month)
    .ThenBy(r => r.Region, "region_code");
```

Produces: `year=2024/month=1/region_code=US/part-00001-1a2b3c4d.parquet`

### Reading Partitioned Data

`DataLakeTableSourceNode<T>` reads the files the manifest lists, in every partition. To read the files of a partitioned
directory without a manifest, use the Parquet connector with a glob; it fills members from the `key=value` directories:

```csharp
var source = ParquetConnector.Source<SalesRecord>(
    StorageUri.Parse("s3://data-lake/sales/**/*.parquet"),
    o => o with { Provider = s3Provider, FileReadParallelism = 4 });
```

## Compaction

`DataLakeCompactor` merges a partition's small files into one, writing the rows back with their files' schema, and
records the swap in the manifest:

```csharp
var result = await new DataLakeCompactor(provider, table).CompactAsync(new TableCompactRequest
{
    TableBasePath = table,
    Provider = provider,
    SmallFileThresholdBytes = 32L * 1024 * 1024,
    MinFilesToCompact = 5,
});
```

## Manifest / Snapshots

The connector writes a `_manifest/` directory with snapshot metadata:

```
_manifest/
  snapshot-20240115T120000Z.json
```

Snapshots track which partition files were written in each pipeline run, enabling time-travel queries and incremental processing.

## Resilience

The manifest writer appends to `_manifest/manifest.ndjson` through an NResilience policy,
`DataLakeConnectorResilience.ManifestWrite`: three attempts (two retries) with exponential backoff and full jitter from
100 ms, no attempt timeout, and no deadline. Only transient storage errors are retried (`IOException`,
`TimeoutException`, socket errors). Access denied (`UnauthorizedAccessException`), missing paths, and invalid arguments
fail at once. Pass a different policy to the `ManifestWriter` constructor to change this.

A retry recovers from a transient error, not from a conflict with a concurrent writer. Each attempt re-reads the
manifest and skips the append when its entries are already present, so a retry after an attempt that committed doesn't
duplicate entries.

The cloud storage providers' SDKs (Azure, AWS, Google) retry individual requests natively; this policy retries the whole
append.

### Concurrent writers

When the storage provider declares `ConditionalWrite` (Azure Blob, ADLS Gen2 and AWS S3), the manifest writer replaces
`manifest.ndjson` conditionally. It reads the manifest and its ETag, adds its entries, and commits the new content with
`IfMatch` set to that ETag. If another writer changed the manifest in the meantime, the commit fails with
`StoragePreconditionFailedException`. The writer then reads the manifest again and retries, up to 16 times. Two
concurrent writers therefore lose no entries.

On providers without `ConditionalWrite` (S3-compatible stores, GCS, SFTP and the file system), the main manifest is
**last-writer-wins**. An append reads `manifest.ndjson`, adds its entries, and replaces the file (by atomic rename on
providers that declare `AtomicMove`, such as ADLS Gen2 and the local file system, and by overwriting it on the others).
When two writers append at the same time, one writer's entries can be missing from the main manifest.

On providers without `ConditionalWrite`, readers still see every entry. Before appending, each flush writes the writer's per-snapshot manifest,
`_manifest/snapshots/{snapshotId}.ndjson`, holding every entry that writer has flushed. Only that writer writes this
file. `ManifestReader` (and so `DataLakeTableSourceNode` and time travel) merges all snapshot manifests into the main
manifest, so entries lost from the main manifest come back. Two conditions apply:

- Give each writer its own snapshot ID. The built-in writers generate one with `ManifestWriter.GenerateSnapshotId()`.
- Tools that read `manifest.ndjson` directly, without the snapshot files, can miss entries on providers without
  `ConditionalWrite`.

The connector writes manifests as UTF-8 without a byte-order mark. Readers accept files with or without one.

Reads use `DataLakeConnectorResilience.ManifestRead` (the same attempts, backoff, and classifier as writes). A missing
manifest or snapshot directory reads as empty. Any other failure to list or read the manifest or a snapshot file is
retried and then thrown, so a read never silently returns fewer entries than the table has. Pass a different policy to
the `ManifestReader` constructor to change this.

## Schema Evolution

Each data file is read with the [Parquet connector's mapping](parquet.md#missing-columns-and-partitions): columns bind by
name, columns the record does not map are ignored, and a member missing from older files keeps its initialiser unless
it is `required`. Adding a nullable member to the record is therefore safe for tables written before it existed.

## Best Practices

1. **Choose partition keys carefully**: high-cardinality keys create too many small files.
2. **Compact regularly**: `DataLakeCompactor` merges small files into larger ones.
3. **Set `MaxBufferedRows`** to bound memory when records fan out to many partitions.
4. **Use `Snappy` or `Zstd`**: both are fast and widely supported by query engines.
5. **Query with DuckDB**: use the DuckDB connector to query data lake files with SQL.
