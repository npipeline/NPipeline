---
title: DuckDB Connector
description: Read from and write to DuckDB databases and files (Parquet/CSV) with NPipeline using the DuckDB connector.
sidebar_position: 12
---

## DuckDB Connector

The `NPipeline.Connectors.DuckDB` package provides source and sink nodes for working with [DuckDB](https://duckdb.org/), an in-process analytical database engine. DuckDB excels at analytical queries over local data files (Parquet, CSV, JSON) and in-memory datasets without requiring a separate server process.

This connector uses the [DuckDB.NET](https://github.com/Giorgi/DuckDB.NET) ADO.NET provider.
The `NPipeline.Connectors.DuckDB` package depends on `DuckDB.NET.Data.Full`, so native DuckDB binaries are included transitively.

## Installation

```bash
dotnet add package NPipeline.Connectors.DuckDB
```

For the core NPipeline package and other available extensions, see the [Installation Guide](../getting-started/installation.md).

## Why DuckDB?

| Feature | DuckDB | SQLite | PostgreSQL |
|---|---|---|---|
| Deployment | In-process, zero-config | In-process | External server |
| Workload | Analytical (OLAP) | Transactional (OLTP) | General-purpose |
| Columnar storage | Yes | No | No |
| Direct file queries | Parquet, CSV, JSON, S3 | No | Via extensions |
| Vectorised execution | Yes | No | Partial |
| Ideal pipeline use | ETL, analytics, local dev | Metadata, small lookups | Production OLTP |

## Quick Start

### Write to DuckDB

```csharp
var sink = new DuckDBSinkNode<SalesRecord>("analytics.duckdb", "sales");

await sink.ExecuteAsync(inputPipe, PipelineContext.Default, CancellationToken.None);
```

### Read from DuckDB

```csharp
var source = new DuckDBSourceNode<SalesRecord>(
    "analytics.duckdb",
    "SELECT * FROM sales WHERE region = 'EU'");

await foreach (var record in source.Initialize(PipelineContext.Default, CancellationToken.None))
{
    Console.WriteLine($"{record.Product}: {record.Amount:C}");
}
```

### Query Parquet/CSV Files Directly

```csharp
// DuckDB auto-detects the file format
var source = DuckDBSourceNode<SalesRecord>.FromFile("data/sales/*.parquet");

// Export pipeline data to CSV
var sink = DuckDBSinkNode<SalesRecord>.ToFile("output/summary.csv");
```

## Attributes

### `[DuckDBColumn]`

Maps a CLR property to a DuckDB column. Without it, the property name is matched using case-insensitive and snake_case conventions.

```csharp
using NPipeline.Connectors.DuckDB.Attributes;

public class Transaction
{
    [DuckDBColumn("transaction_id", PrimaryKey = true)]
    public int Id { get; set; }

    [DuckDBColumn("customer_name")]
    public string CustomerName { get; set; } = string.Empty;

    [DuckDBColumn(Ignore = true)]
    public string InternalNote { get; set; } = string.Empty;
}
```

| Parameter | Type | Description |
|---|---|---|
| `Name` | `string` | Explicit column name in DuckDB. |
| `PrimaryKey` | `bool` | Include in PRIMARY KEY when auto-creating the table. |
| `Ignore` | `bool` | Exclude from reading and writing. |

The connector also recognises `[Column("name")]` and `[IgnoreColumn]` from `NPipeline.Connectors.Attributes`.

## Configuration

### `DuckDBConfiguration`

```csharp
var config = new DuckDBConfiguration
{
    // Connection
    AccessMode = DuckDBAccessMode.ReadWrite,
    MemoryLimit = "4GB",
    Threads = 4,
    Extensions = ["httpfs"],
    Settings = new Dictionary<string, string>
    {
        ["s3_region"] = "us-east-1",
        ["s3_access_key_id"] = "...",
        ["s3_secret_access_key"] = "..."
    },

    // Read
    StreamResults = true,
    FetchSize = 2048,
    ProjectedColumns = ["Id", "Name", "Amount"],
    CommandTimeout = 60,

    // Write
    WriteStrategy = DuckDBWriteStrategy.Appender,  // or Sql
    BatchSize = 5000,
    AutoCreateTable = true,
    TruncateBeforeWrite = false,
    UseTransaction = true,

    // Mapping
    CaseInsensitiveMapping = true,
    CacheMappingMetadata = true,

    // Error handling
    ContinueOnError = false,
    RowErrorHandler = (ex, row) =>
    {
        Console.WriteLine($"Skipping row: {ex.Message}");
        return true; // true = skip and continue
    },

    // Observability
    Observer = new MyDuckDBObserver()
};
```

### Access Modes

| Mode | Description |
|---|---|
| `Automatic` | DuckDB chooses the best mode (default). |
| `ReadOnly` | Open in read-only mode; prevents accidental writes. |
| `ReadWrite` | Explicit read-write mode. |

### Write Strategies

| Strategy | Description | Best For |
|---|---|---|
| `Appender` | Uses DuckDB's native Appender API (default). Highest throughput. | Bulk inserts, ETL |
| `Sql` | Batched INSERT statements inside transactions. | Complex data, compatibility |

## Source Node

### From Database

```csharp
// Auto-mapped via conventions
var source = new DuckDBSourceNode<Record>("analytics.duckdb", "SELECT * FROM events");

// With explicit configuration
var source = new DuckDBSourceNode<Record>("analytics.duckdb", query, config);

// With custom row mapper
var source = new DuckDBSourceNode<Record>(
    "analytics.duckdb",
    "SELECT * FROM events",
    row => new Record
    {
        Id = row.Get<int>("id"),
        Name = row.Get<string>("name"),
        Score = row.GetOrDefault<double>("score")
    });
```

### From File

DuckDB can query Parquet, CSV, and JSON files directly:

```csharp
// Single file
var source = DuckDBSourceNode<T>.FromFile("data/sales.parquet");

// Glob pattern
var source = DuckDBSourceNode<T>.FromFile("data/2024/*.parquet");

// Custom query template ({file} is replaced with the path)
var source = DuckDBSourceNode<T>.FromFile(
    "data/sales.parquet",
    "SELECT *, filename AS source_file FROM read_parquet('{file}')");
```

### In-Memory

```csharp
// null or empty path = in-memory database
var source = new DuckDBSourceNode<T>(null, "SELECT 1 AS id, 'test' AS name");
```

## Sink Node

### To Database

```csharp
// Write with Appender (default, fastest)
var sink = new DuckDBSinkNode<T>("analytics.duckdb", "events");

// Write with SQL strategy
var sink = new DuckDBSinkNode<T>("analytics.duckdb", "events",
    new DuckDBConfiguration { WriteStrategy = DuckDBWriteStrategy.Sql });
```

### To File

```csharp
// Export to Parquet
var sink = DuckDBSinkNode<T>.ToFile("output/events.parquet");

// Export to CSV with options
var sink = DuckDBSinkNode<T>.ToFile("output/events.csv",
    new DuckDBConfiguration
    {
        FileExportOptions = new DuckDBFileExportOptions
        {
            CsvDelimiter = '|',
            CsvHeader = true,
            Compression = "gzip"
        }
    });
```

## Dependency Injection

```csharp
using NPipeline.Connectors.DuckDB.DependencyInjection;

services.AddDuckDBConnector(options =>
{
    options.DefaultConfiguration = new DuckDBConfiguration
    {
        DatabasePath = "analytics.duckdb",
        AccessMode = DuckDBAccessMode.ReadWrite,
        MemoryLimit = "2GB"
    };
});

// Named databases
services.AddDuckDBDatabase("analytics", "analytics.duckdb", config =>
{
    config.AccessMode = DuckDBAccessMode.ReadOnly;
});

// Default in-memory database
services.AddDuckDBInMemory();
```

Then inject and use the factories:

```csharp
public class MyPipeline : IPipelineDefinition
{
    private readonly DuckDBSourceNodeFactory _sourceFactory;
    private readonly DuckDBSinkNodeFactory _sinkFactory;

    public MyPipeline(DuckDBSourceNodeFactory sourceFactory, DuckDBSinkNodeFactory sinkFactory)
    {
        _sourceFactory = sourceFactory;
        _sinkFactory = sinkFactory;
    }

    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = _sourceFactory.CreateSource<MyRecord>("SELECT * FROM events");
        var sink = _sinkFactory.CreateSink<MyRecord>("processed_events");

        builder.AddSource(source, "read-events");
        builder.AddSink(sink, "write-events");
    }
}
```

## Observability

Implement `IDuckDBConnectorObserver` to track progress:

```csharp
public class MyObserver : IDuckDBConnectorObserver
{
    public void OnRowRead(long rowIndex) => Console.Write(".");
    public void OnReadCompleted(long totalRows) => Console.WriteLine($"\nRead {totalRows} rows");
    public void OnRowWritten(long rowIndex) { }
    public void OnWriteCompleted(long totalRows) => Console.WriteLine($"Wrote {totalRows} rows");
    public void OnBatchFlushed(int batchSize, long totalRows) { }
    public void OnExtensionLoaded(string name) => Console.WriteLine($"Loaded: {name}");
    public void OnQueryStarted(string query) { }
    public void OnQueryProgress(double percentage) { }
}
```

## Type Mapping

| C# Type | DuckDB Type |
|---|---|
| `bool` | BOOLEAN |
| `byte` | UTINYINT |
| `short` | SMALLINT |
| `int` | INTEGER |
| `long` | BIGINT |
| `float` | FLOAT |
| `double` | DOUBLE |
| `decimal` | DECIMAL(18,6) |
| `string` | VARCHAR |
| `DateTime` | TIMESTAMP |
| `DateOnly` | DATE |
| `TimeOnly` | TIME |
| `DateTimeOffset` | TIMESTAMPTZ |
| `Guid` | UUID |
| `byte[]` | BLOB |
| Enums | VARCHAR (stored as name) |

## Error Handling

```csharp
var config = new DuckDBConfiguration
{
    ContinueOnError = true,
    RowErrorHandler = (exception, row) =>
    {
        Console.Error.WriteLine($"Row error: {exception.Message}");
        return true; // true = skip and continue, false = abort
    }
};
```

Custom exceptions provide additional context:

| Exception | Additional Context |
|---|---|
| `DuckDBConnectorException` | Base type for all connector errors |
| `DuckDBMappingException` | `RowIndex` of the failing row |
| `DuckDBQueryException` | `Query` (truncated to 500 chars) |
| `DuckDBConnectionException` | `DatabasePath` that failed |

## Remote File Access

DuckDB supports querying files from S3, Azure, and GCS via extensions:

```csharp
var config = new DuckDBConfiguration
{
    Extensions = ["httpfs"],
    Settings = new Dictionary<string, string>
    {
        ["s3_region"] = "us-east-1",
        ["s3_access_key_id"] = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID")!,
        ["s3_secret_access_key"] = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY")!
    }
};

var source = DuckDBSourceNode<T>.FromFile(
    "s3://my-bucket/data/events.parquet",
    config: config);
```

## Sample

See the [DuckDB Connector Sample](../../samples/Sample_DuckDBConnector/) for a complete working example that:

1. Generates 1,000 synthetic sensor readings
2. Writes them to a local DuckDB database using the Appender API
3. Queries aggregate statistics using analytical SQL
4. Exports a subset to CSV using COPY TO

## Limitations

- **Single-process**: DuckDB databases can only be opened by one process at a time. Use `AccessMode.ReadOnly` when multiple readers are needed.
- **No connection pooling**: DuckDB is embedded; each `DuckDBSourceNode`/`DuckDBSinkNode` opens and closes its own connection.
- **Appender constraints**: The Appender API is append-only and does not support upsert/conflict resolution. Use `DuckDBWriteStrategy.Sql` when upsert-like semantics are required.
