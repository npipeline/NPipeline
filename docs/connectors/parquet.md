---
title: "Parquet Connector"
description: "Read and write Apache Parquet files with typed columnar mapping, projection, row-group pruning and list columns."
order: 4
---

# Parquet Connector

The `NPipeline.Connectors.Parquet` package reads and writes Apache Parquet files with
[Parquet.Net](https://github.com/aloneguid/parquet-dotnet). Records map to columns through a mapper compiled once per
file layout: the source reads each row group's mapped columns into typed arrays and builds records from them without
boxing or name lookups, and the sink writes members straight into typed column buffers. Columns a record does not map
are never read.

Globs, commit behavior, row errors, parallel reads and metrics work the same in every file connector; see
[File Connectors: Shared Behaviour](file-connectors.md). Parquet compresses inside the file, so the stream compression
option does not apply.

## Installation

```bash
dotnet add package NPipeline.Connectors.Parquet
```

**Dependencies:** [Parquet.Net](https://www.nuget.org/packages/Parquet.Net) 6.x, `NPipeline.Connectors`,
`NPipeline.StorageProviders`

## Reading

```csharp
public sealed record Order(int Id, string Customer, decimal Total, DateTimeOffset PlacedAt);

var source = ParquetConnector.Source<Order>(StorageUri.FromFilePath("orders.parquet"));
builder.AddSource(source, "orders");
```

Columns bind to `Order`'s members case-insensitively, and only those four columns are read. A value that does not
convert (a `null` for an `int`, say) fails the read with a `RecordMappingException` naming the row and column, unless a
`RowErrorHandler` skips or dead-letters it. The row error's excerpt lists the row's values, since a Parquet row has no
raw text.

To read several files, give a directory (`s3://bucket/orders/`, which reads its `.parquet` files) or a glob
(`s3://bucket/orders/**/*.parquet`). Files are read in path order; set `FileReadParallelism` to read ahead several
files at once while keeping that order. Parquet needs to seek, so a stream that cannot (S3, SFTP) is first copied to a
temporary file.

### Mapping

| Type shape | How it maps |
| --- | --- |
| Class with setters or `init` accessors | Members set by column name |
| Positional record or primary constructor | Constructor parameters matched to columns by name |
| `required` members | Must have a column, or the file fails to bind |
| `[Column("order_id")]` or `[ParquetColumn("order_id")]` | Sets the column name; `[Column]` wins when both are present |
| `[IgnoreColumn]` or `[ParquetColumn(Ignore = true)]` | Leaves the member out |
| `[ParquetDecimal(18, 2)]` | Sets a `decimal` column's precision and scale; the default is `DECIMAL(38, 18)` |
| `int`, `string` and other single values | The file's first column |

### Types

| .NET type | Parquet column | Notes |
| --- | --- | --- |
| `bool`, `sbyte`, `byte`, `short`, `ushort`, `int`, `uint`, `long`, `ulong`, `float`, `double` | The matching physical and logical type | |
| `decimal` | `DECIMAL(38, 18)`, or as `[ParquetDecimal]` says | |
| `string`, `char` | `STRING` | Always nullable |
| `byte[]` | `BINARY` | Always nullable |
| `DateTime` | `TIMESTAMP(MICROS, UTC)` | Read back with `Kind = Utc`; a local time is converted |
| `DateTimeOffset` | `TIMESTAMP(MICROS, UTC)` | Stored as its UTC instant; read back with a zero offset |
| `DateOnly` | `DATE` | |
| `TimeOnly` | `TIME(MICROS)` | |
| `TimeSpan` | `INT64` ticks | |
| `Guid` | `UUID` | |
| Enums | `STRING` names | Read case-insensitively; numbers of defined values are accepted |
| `T?` for any of the above | The same column, nullable | |
| `T[]`, `List<T>`, `IList<T>`, `IReadOnlyList<T>`, `ICollection<T>`, `IReadOnlyCollection<T>`, `IEnumerable<T>` of the above | `LIST` | A `null` list, an empty list and `null` elements all round-trip |

Timestamps and times keep microseconds, so a value's last tick digit is lost. A member of any other type (a nested
class, a dictionary) fails when the node is created, naming the member; mark it `[IgnoreColumn]` or flatten it. Files
with struct or map columns can still be read: those columns are skipped.

Columns written by other tools convert to the member's type where no information is lost or the text parses: an `INT32`
column reads into a `long`, `FLOAT` into `double`, `DATE` into `DateTime`, a timestamp into `DateTimeOffset`, `STRING`
into a number, date or enum, and anything into `string`. Timestamps without a time zone, such as Impala's `INT96`, read
as UTC.

### Missing columns and partitions

A member without a column keeps its initialiser, unless it is `required` (see
[column binding](file-connectors.md#column-binding)). That makes adding a nullable member to a record safe for old files.

Files under `key=value` directories (Hive partitioning) fill members named after the keys when the file has no such
column, so `sales/region=EU/day=2026-01-02/part-0.parquet` sets `Region` and `Day`. A value of
`__HIVE_DEFAULT_PARTITION__` is `null`. Set `PartitionColumns = false` to turn this off.

### Skipping row groups and rows

A `RowGroupFilter` sees each row group before its columns are read, with the range of values the writer recorded for
each column, so a query for one day of a large file reads only the groups that can hold it:

```csharp
var day = new DateTime(2026, 1, 2, 0, 0, 0, DateTimeKind.Utc);

var source = ParquetConnector.Source<Event>(uri, o => o with
{
    RowGroupFilter = group => !group.TryGetRange<DateTime>("At", out var min, out var max) || (min < day.AddDays(1) && max >= day),
});
```

`TryGetRange` returns `false` when the writer recorded no statistics or the column is unsigned (Parquet.Net reports
unsigned statistics as signed), so keep the group in that case. A `RowFilter` sees each row as a `ParquetRow` before
it is mapped.

### Manual mapping

For full control, pass a mapper. It receives a `ParquetRow` holding every column, or only `ProjectedColumns` when set:

```csharp
var source = ParquetConnector.Source(
    StorageUri.FromFilePath("orders.parquet"),
    row => new Order(row.Get<int>("id"), row.Get<string>("customer"), row.GetOrDefault("total", 0m), row.Get<DateTimeOffset>("placed_at")),
    o => o with { ProjectedColumns = ["id", "customer", "total", "placed_at"] });
```

| `ParquetRow` member | Description |
| --- | --- |
| `Get<T>(string name)`, `Get<T>(int index)` | Read and convert a value; throws `FieldMappingException` naming the column |
| `TryGet<T>(string name, out T value)` | Read and convert, returning `false` instead of throwing |
| `GetOrDefault<T>(string name, T defaultValue)` | The value, or `defaultValue` when it is missing, `null` or does not convert |
| `this[string name]`, `this[int index]` | The stored value (a list as an array), or `null` |
| `HasColumn(string name)`, `IsNull(string name)` | Whether the row has the column, and whether its value is `null` or missing |
| `ColumnNames`, `ColumnCount`, `Schema`, `RecordNumber` | The row's columns, the file's schema and the row's position |

A `ParquetRow` is a snapshot and can be kept after the mapper returns. An exception the mapper throws is a row error.

`ParquetRowWriter.WriteAsync(stream, schema, rows)` writes rows back with the schema they were read with, for code
that moves rows whose type is not known in advance, such as the [Data Lake](datalake.md) compactor.

### Read options

| Option | Default | Description |
| --- | --- | --- |
| `ProjectedColumns` | `null` | Extra columns to read; with a manual mapper, the only columns read |
| `SchemaValidator` | `null` | Rejects a file by its schema, failing the read with `ParquetSchemaException` |
| `RowGroupFilter` | `null` | Skips row groups before their columns are read |
| `RowFilter` | `null` | Skips rows before they are mapped |
| `PartitionColumns` | `true` | Fill members from `key=value` directories |
| `Naming` | `AsIs` | How member names become column names, such as `ColumnNamingPolicy.SnakeCaseLower` |
| `MissingColumns` | `ThrowForRequired` | What a member without a column means |

Plus the [shared source options](file-connectors.md#options-every-file-source-and-sink-has): `Provider`, `Resolver`,
`BufferSize`, `Recursive`, `RowErrorHandler`, `RawExcerptLength` and `FileReadParallelism`.

## Writing

```csharp
var sink = ParquetConnector.Sink<Order>(StorageUri.FromFilePath("orders.parquet"));
builder.AddSink(sink, "orders-out");
```

The sink writes each readable member as a column (see [Types](#types)) and flushes a row group every `RowGroupSize`
rows or once the buffered values reach about `RowGroupBytes`, whichever comes first, so wide rows do not hold hundreds
of megabytes in memory. `sink.Schema` is the schema it writes. The file appears only when the write is
complete; see [Commit behavior](file-connectors.md#commit-behavior).

A `null` item fails the write by default; set `NullItems = NullItemHandling.Skip` to drop them.

### Write options

| Option | Default | Description |
| --- | --- | --- |
| `RowGroupSize` | `50_000` | The most rows per row group |
| `RowGroupBytes` | 128 MB | A row group is also flushed once its buffered values reach about this size |
| `Codec` | `Snappy` | The compression codec: `None`, `Snappy`, `Gzip`, `Lz4Raw` or `Zstd` |
| `Naming` | `AsIs` | How member names become column names |

Plus the [shared sink options](file-connectors.md#options-every-file-source-and-sink-has): `Provider`, `Resolver`,
`BufferSize` and `NullItems`.

Row groups of 50,000 to 1,000,000 rows suit most query engines: larger groups compress better and prune coarser.
`Zstd` gives smaller files than `Snappy` for a little more CPU.

## Examples

### CSV to Parquet

```csharp
public sealed class CsvToParquetPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(CsvConnector.Source<Order>(StorageUri.FromFilePath("orders.csv")), "csv");
        var sink = builder.AddSink(ParquetConnector.Sink<Order>(StorageUri.FromFilePath("orders.parquet"), o => o with { Codec = CompressionMethod.Zstd }), "parquet");

        builder.Connect(source, sink);
    }
}
```

### Reading a partitioned dataset from S3

```csharp
public sealed class Sale
{
    public int Id { get; set; }
    public decimal Amount { get; set; }
    public string Region { get; set; } = "";   // from region=... directories
    public DateOnly Day { get; set; }          // from day=... directories
}

var source = ParquetConnector.Source<Sale>(
    StorageUri.Parse("s3://my-bucket/sales/**/*.parquet"),
    o => o with { Provider = new AwsS3StorageProvider(s3Options), FileReadParallelism = 4 });
```

### Skip bad rows and send them to the dead-letter sink

```csharp
var source = ParquetConnector.Source<Order>(uri, o => o with { RowErrorHandler = _ => RowErrorAction.DeadLetter });
builder.AddSource(source, "orders");
builder.AddDeadLetterSink(new MyDeadLetterSink()); // receives ConnectorRecordFailure items
```

## Next Steps

- [File Connectors: Shared Behaviour](file-connectors.md): globs, commit behavior, row errors, parallel reads, metrics
- [Data Lake Connector](datalake.md): partitioned Parquet tables with snapshots and time travel
- [CSV Connector](csv.md): delimited text
- [Storage Providers](../storage-providers/index.md): read Parquet from S3, Azure Blob, GCS or SFTP
