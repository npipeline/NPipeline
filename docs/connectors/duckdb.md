---
title: "DuckDB Connector"
description: "Read from and write to DuckDB databases with Appender API, file import/export, and in-memory analytics."
order: 13
---

# DuckDB Connector

The `NPipeline.Connectors.DuckDB` package provides source and sink nodes for [DuckDB](https://duckdb.org/), an in-process analytical database. Ideal for local analytics, file format conversion, and ad-hoc queries over Parquet/CSV files. Supports the high-performance Appender API, automatic table creation, and direct file import/export.

## Installation

```bash
dotnet add package NPipeline.Connectors.DuckDB
```

**Dependencies:** [DuckDB.NET.Data.Full](https://www.nuget.org/packages/DuckDB.NET.Data.Full) 1.x

## Why DuckDB?

| | DuckDB | SQLite | PostgreSQL |
|---|--------|--------|-----------|
| **Best for** | Analytics, OLAP | OLTP, embedded | General-purpose server |
| **Deployment** | In-process (no server) | In-process (no server) | Requires server |
| **Columnar storage** | Yes | No | No |
| **Parquet/CSV queries** | Native (`read_parquet()`) | No | Via extensions |
| **Concurrent writers** | No | Limited | Yes |

## Source Node — `DuckDBSourceNode<T>`

### Constructors

```csharp
// Database path + query (null path = in-memory)
public DuckDBSourceNode(
    string? databasePath, string query,
    DuckDBConfiguration? configuration = null)

// With custom mapper
public DuckDBSourceNode(
    string? databasePath, string query,
    Func<DuckDBRow, T> rowMapper,
    DuckDBConfiguration? configuration = null)

// Connection factory (recommended for DI)
public DuckDBSourceNode(
    IDuckDBConnectionFactory connectionFactory, string query,
    DuckDBConfiguration? configuration = null)

// Direct file import (Parquet, CSV)
public static DuckDBSourceNode<T> FromFile(
    string filePath,
    DuckDBConfiguration? configuration = null)
```

### Example: Query Parquet Files Directly

```csharp
var source = new DuckDBSourceNode<SalesRecord>(
    null, // in-memory
    "SELECT region, SUM(amount) as total FROM read_parquet('sales/*.parquet') GROUP BY region");
```

## Sink Node — `DuckDBSinkNode<T>`

| Strategy | Description | Best For |
|----------|-------------|----------|
| `Appender` (default) | DuckDB Appender API | Maximum throughput (fastest) |
| `Sql` | Standard `INSERT` statements | Upserts, complex logic |

### Constructors

```csharp
// Database path + table name
public DuckDBSinkNode(
    string? databasePath, string tableName,
    DuckDBConfiguration? configuration = null)

// Connection factory (recommended for DI)
public DuckDBSinkNode(
    IDuckDBConnectionFactory connectionFactory, string tableName,
    DuckDBConfiguration? configuration = null)

// Direct file export (Parquet, CSV)
public static DuckDBSinkNode<T> ToFile(
    string filePath,
    DuckDBConfiguration? configuration = null)
```

### Example: Auto-Create Table

```csharp
var config = new DuckDBConfiguration
{
    WriteStrategy = DuckDBWriteStrategy.Appender,
    AutoCreateTable = true
};

var sink = new DuckDBSinkNode<Order>("analytics.duckdb", "orders", configuration: config);
```

## Configuration

### Connection

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `DatabasePath` | `string?` | `null` | `.duckdb` file path. `null` = in-memory. |
| `AccessMode` | `DuckDBAccessMode` | `Automatic` | `Automatic`, `ReadOnly`, or `ReadWrite` |
| `MemoryLimit` | `string?` | `null` | Maximum memory (e.g., `"4GB"`) |
| `Threads` | `int` | `0` | Thread count (`0` = auto-detect) |
| `TempDirectory` | `string?` | `null` | Spill-to-disk directory |
| `Extensions` | `string[]?` | `null` | Extensions to load (e.g., `"httpfs"`, `"spatial"`) |
| `Settings` | `Dictionary<string, string>?` | `null` | Additional DuckDB session settings |

### Read

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `StreamResults` | `bool` | `true` | Stream results row-by-row |
| `FetchSize` | `int` | `2048` | Rows per fetch batch |
| `ProjectedColumns` | `string[]?` | `null` | Column projection |
| `CommandTimeout` | `int` | `30` | Command timeout (seconds) |

### Write

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `WriteStrategy` | `DuckDBWriteStrategy` | `Appender` | `Appender` (fastest) or `Sql` |
| `AutoCreateTable` | `bool` | `true` | Create table if it doesn't exist |
| `TruncateBeforeWrite` | `bool` | `false` | Truncate table before writing |
| `UseTransaction` | `bool` | `true` | Wrap writes in a transaction |
| `BatchSize` | `int` | `1000` | Batch size (for `Sql` strategy) |

### Error Handling

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ContinueOnError` | `bool` | `false` | Continue on row-level errors |
| `RowErrorHandler` | `Func<Exception, long, bool>?` | `null` | Error handler (receives row index) |
| `Observer` | `IDuckDBConnectorObserver?` | `null` | Lifecycle observer for metrics |

## Dependency Injection

```csharp
services.AddDuckDBConnector(options =>
{
    options.DefaultConfiguration = new DuckDBConfiguration
    {
        DatabasePath = "analytics.duckdb",
        MemoryLimit = "4GB"
    };
});

services.AddDuckDBDatabase("reporting", "reporting.duckdb", config =>
{
    config.AccessMode = DuckDBAccessMode.ReadOnly;
});
```

## Write Strategy Comparison

| Strategy | Description | Best For |
|----------|-------------|----------|
| `Appender` (default) | DuckDB native appender — fastest path | Bulk loads, ETL |
| `Sql` | Standard SQL `INSERT` statements | Small volumes, complex logic |

The `Appender` strategy bypasses SQL parsing entirely and writes directly to DuckDB's storage engine.

## DuckDB Extensions

Load extensions for additional capabilities:

```csharp
var config = new DuckDBConfiguration
{
    Extensions = ["httpfs", "spatial", "json"],
    Settings = new Dictionary<string, string>
    {
        ["s3_region"] = "us-east-1",
        ["s3_access_key_id"] = "...",
        ["s3_secret_access_key"] = "..."
    }
};
```

| Extension | Description |
|-----------|-------------|
| `httpfs` | Read from HTTP/S3 URLs |
| `spatial` | Spatial data types and functions |
| `json` | JSON file reading/writing |
| `parquet` | Parquet file support (built-in) |

## Performance

### Memory & Spill-to-Disk

```csharp
var config = new DuckDBConfiguration
{
    MemoryLimit = "4GB",
    TempDirectory = "/tmp/duckdb-spill",
    Threads = 8
};
```

When memory is exhausted, DuckDB spills intermediate results to the `TempDirectory`.

### Auto-Create Table

```csharp
var sink = new DuckDBSinkNode<SalesRecord>(
    "analytics.duckdb", "sales",
    new DuckDBConfiguration
    {
        AutoCreateTable = true,        // infer schema from T
        TruncateBeforeWrite = false    // append mode
    });
```

## Best Practices

1. **Use `Appender` strategy** (default) — significantly faster than SQL inserts
2. **Set `MemoryLimit`** to prevent unbounded memory growth
3. **Configure `TempDirectory`** for large datasets that exceed memory
4. **Use `ReadOnly` access mode** for concurrent read pipelines
5. **Use in-memory mode** (`DatabasePath = null`) for ephemeral analytical pipelines
6. **Load extensions** at configuration time — not mid-pipeline

## Next Steps

- [Parquet Connector](parquet.md) — read/write Parquet files directly
- [Data Lake Connector](datalake.md) — partitioned Parquet tables with time travel
