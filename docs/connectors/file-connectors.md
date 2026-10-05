---
title: "File Connectors: Shared Behaviour"
description: "Options, globs, compression, commit behavior, row errors and metrics shared by the file connectors."
order: 1
---

# File Connectors: Shared Behaviour

The file connectors are built on one pair of base classes in `NPipeline.Connectors`, so they share their options,
storage handling, error handling and metrics. This page describes that shared behaviour; each connector's page covers
its format. The [CSV](csv.md), [JSON](json.md), [Excel](excel.md) and [Parquet](parquet.md) connectors use these bases.

## Creating nodes

Each connector has a factory class and an options record per direction:

```csharp
var source = CsvConnector.Source<Order>(StorageUri.Parse("s3://bucket/orders/*.csv"));
var sink = CsvConnector.Sink<Order>(StorageUri.FromFilePath("orders.csv"), o => o with { Delimiter = ";" });
```

The second argument adjusts the default options with a `with` expression. You can also build the options yourself and
call the node's constructor: `new CsvSourceNode<Order>(new CsvReadOptions { Uri = uri, Delimiter = ";" })`. Options
are immutable records, validated when the node is created.

## Options every file source and sink has

| Option | Default | Description |
| --- | --- | --- |
| `Uri` | required | The file to read or write. A source also accepts a directory or a glob (see below). |
| `Provider` | `null` | The storage provider. When `null`, it is resolved from `Resolver`. |
| `Resolver` | `null` | Resolves the provider from the URI. When `null`, a default resolver with the file system is used. |
| `Compression` | `Auto` | `Auto` picks by suffix: `.gz` (gzip), `.br` (Brotli), `.zz` or `.zlib` (zlib), `.deflate`. Or `None`, `Gzip`, `Brotli`, `ZLib`, `Deflate`. |
| `BufferSize` | 64 KB | The buffer for the format's reader or writer. |

Excel and Parquet compress inside the file, so they do not take a stream `Compression`.

Local files need neither a provider nor a resolver. For cloud storage, pass a provider, or a resolver that knows the
scheme; see [Storage Providers](../storage-providers/index.md).

## Sources

### Directories and globs

A source's `Uri` can name more than one file:

| `Uri` | Reads |
| --- | --- |
| `file:///data/orders.csv` | One file |
| `file:///data/orders/` | The connector's files in the directory (`.csv` for CSV); set `Recursive = true` to include subdirectories |
| `s3://bucket/2026/*.csv` | Files matching the glob: `*` matches within one path segment |
| `s3://bucket/**/orders.csv.gz` | `**` matches across segments |

Files are read one after another, in ordinal path order. There is no `?` wildcard, because `?` starts a URI's query
string. Listing needs a provider that supports it.

Each file listed keeps the original URI's query parameters, so credentials and regions set there apply to every file.

### Reading files in parallel

Set `FileReadParallelism` above 1 to read that many files at once. Records still come out in path order: while the
pipeline consumes one file, the next ones are read into bounded buffers, so a slow store's latency overlaps with work.
Stopping early (a `Take`, a failure or cancellation) stops the readers and releases their files. It helps most with
many small or medium files on object storage.

### Formats that need to seek

Some formats (XLSX, which is a zip) must seek. When the provider's stream cannot seek (S3, SFTP, HTTP), the source first
copies it to a temporary file, which is deleted when the file has been read.

### Row errors

Values convert strictly: a value that does not fit its member (`abc` for an `int`) is an error, never a silent
default. By default the read fails with a `RecordMappingException`, which names the file, the record number, the field
and the start of the raw record.

`RowErrorHandler` decides per record instead:

```csharp
var source = CsvConnector.Source<Order>(uri, o => o with
{
    RowErrorHandler = error =>
    {
        logger.LogWarning(error.Exception, "Bad row {Row} in {File}, field {Field}", error.RecordNumber, error.Source, error.Field);
        return RowErrorAction.Skip; // or Fail, or DeadLetter
    },
});
```

| Action | Effect |
| --- | --- |
| `Fail` | The read fails with `RecordMappingException`. |
| `Skip` | The record is dropped and the read continues. |
| `DeadLetter` | A `ConnectorRecordFailure` (source, record number, field, raw excerpt) goes to the pipeline's dead-letter sink, attributed to the source node, and the read continues. Without a dead-letter sink the read fails with `DeadLetterSinkNotConfiguredException`. |

`RowError.RawExcerpt` holds the first 256 characters of the raw record. Set `RawExcerptLength` to change that, or to `0`
to leave raw data out of errors and dead letters (for sensitive feeds).

A record type that cannot bind to the file at all (a required column is missing) fails the file with a
`RecordBindingException` before any record is read.

### Column binding

CSV and Excel bind columns to members by name, case-insensitively, once per file (JSON uses System.Text.Json's own
binding, with the same attributes). `[Column("name")]` sets a member's column name and
`[IgnoreColumn]` leaves it out. A naming policy (`ColumnNamingPolicy.SnakeCaseLower` and others) converts member names
for columns without an attribute.

Types can use setters, `init` accessors, `required` members, positional records or primary constructors.
`MissingColumns` decides what a member without a column means:

| `MissingColumns` | Effect |
| --- | --- |
| `ThrowForRequired` (default) | Fail when a `required` member, a constructor parameter or a `[Column]` member has no column. Other members keep their initial values. |
| `Throw` | Fail when any mapped member has no column. |
| `Ignore` | Never fail. |

## Sinks

### Commit behavior

A file appears at its target only when the write has finished. The sink writes through the storage provider and
commits after the format writer and the compressor have written their last bytes (the compression trailer included).
A write that fails or is cancelled leaves the target as it was: a new file doesn't exist and an existing file keeps its
old content. There is no partial file and nothing to clean up. Each provider does this in its own way; see
[Writes commit explicitly](../storage-providers/index.md#writes-commit-explicitly).

### Null items

`NullItems` decides what happens to a `null` item: `Throw` (the default) fails the write, `Skip` drops it, and `Write`
lets the format write its own null where it has one.

## Metrics and traces

The file connectors report through `System.Diagnostics.Metrics` and `ActivitySource`, both named
`NPipeline.Connectors`:

| Instrument | Unit | Tags |
| --- | --- | --- |
| `npipeline.connector.rows_read`, `rows_written` | `{row}` | `connector`, `storage.scheme` |
| `npipeline.connector.bytes_read`, `bytes_written` | `By` | `connector`, `storage.scheme` |
| `npipeline.connector.files_read`, `files_written` | `{file}` | `connector`, `storage.scheme` |
| `npipeline.connector.row_errors` | `{error}` | `connector`, `storage.scheme`, `action` |

Each file is read or written inside a `connector.file.read` or `connector.file.write` activity, tagged with the file's
path without its query string. Subscribe with OpenTelemetry:

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(m => m.AddMeter("NPipeline.Connectors"))
    .WithTracing(t => t.AddSource("NPipeline.Connectors"));
```
