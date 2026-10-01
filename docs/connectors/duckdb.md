---
title: "DuckDB Connector"
description: "Query DuckDB databases and Parquet, CSV and JSON files in process, and write with DuckDB's appender or export to files."
order: 13
---

# DuckDB Connector

The `NPipeline.Connectors.DuckDB` package runs DuckDB in process. Sources run a query against a database file, an
in-memory database, or straight over Parquet, CSV and JSON files; sinks write with DuckDB's appender or `INSERT`
statements, can create the table from the record, and can export it to a file. Mapping, row errors, transactions,
failed batches and checkpoints work as in every SQL connector; see [SQL Connectors: Shared Behaviour](sql-connectors.md).

## Installation

```bash
dotnet add package NPipeline.Connectors.DuckDB
```

**Dependencies:** [DuckDB.NET.Data.Full](https://www.nuget.org/packages/DuckDB.NET.Data.Full), `NPipeline.Connectors`

## Querying files

```csharp
var orders = DuckDBConnector.FromFile<Order>("data/orders/*.parquet");
var totals = DuckDBConnector.Source<RegionTotal>(DuckDBDatabase.InMemory,
    "SELECT region, sum(amount) AS total FROM read_parquet('data/orders/*.parquet') GROUP BY region",
    o => o with { Naming = ColumnNamingPolicy.SnakeCaseLower });
```

`FromFile` picks `read_parquet`, `read_csv` or `read_json` from the extension (`.parquet`, `.csv`, `.tsv`, `.json`,
`.ndjson`, `.jsonl`) and accepts globs. For files on S3 or HTTP, load the `httpfs` extension:
`DuckDBDatabase.InMemory with { Extensions = ["httpfs"] }`.

## Databases

A `DuckDBDatabase` names the database and how each connection is set up:

```csharp
var database = DuckDBDatabase.File("warehouse.duckdb") with { MemoryLimit = "4GB", Threads = 8 };
```

| Property | Default | Description |
| --- | --- | --- |
| `Path` | `null` | The database file; `null` is an in-memory database |
| `AccessMode` | `Automatic` | `ReadOnly` or `ReadWrite` |
| `MemoryLimit`, `Threads`, `TempDirectory` | DuckDB's | Applied with `SET` on each connection |
| `Extensions` | none | Installed and loaded on each connection |
| `Settings` | none | Other `SET name = 'value'` settings; names must be identifiers |

Each connection to an in-memory database opens a database of its own, so a source cannot read what a sink wrote to
one; use a file to share data between nodes. Only one process can open a database file for writing; other processes
can open it with `AccessMode = ReadOnly`.

When a query needs more memory than `MemoryLimit`, DuckDB spills intermediate results to `TempDirectory`, so set both
for large sorts, joins and aggregations. Extensions and their settings go on the database, so every connection has
them:

```csharp
var lake = DuckDBDatabase.InMemory with
{
    Extensions = ["httpfs"],
    Settings = new Dictionary<string, string> { ["s3_region"] = "us-east-1" },
};
var source = DuckDBConnector.Source<Order>(lake, "SELECT * FROM read_parquet('s3://bucket/orders/*.parquet')");
```

`s3_access_key_id` and `s3_secret_access_key` are set the same way; read them from configuration, not source code.

## Writing

```csharp
var sink = DuckDBConnector.Sink<SensorReading>(database, "readings", o => o with { TruncateBeforeWrite = true });
var export = DuckDBConnector.ToFile<SensorReading>("out/readings.parquet");
```

| Option | Default | Description |
| --- | --- | --- |
| `WriteStrategy` | `Appender` | DuckDB's appender (fastest, typed values), or `Sql` for `INSERT` statements, which can upsert |
| `AutoCreateTable` | `true` | Creates the table from the record when it is missing: members' types, and `NOT NULL` for non-nullable members |
| `TruncateBeforeWrite` | `false` | Deletes the table's rows before writing |
| `ExportTo` | `null` | After writing, exports the table with `COPY … TO`; `ToFile` sets it on a staging table in an in-memory database |
| `Export` | CSV header on | Format, compression, CSV delimiter and header, Parquet row group size |

Plus the [options every SQL sink has](sql-connectors.md#options-every-sink-has). Upserts (`Sql` strategy) use
`INSERT … ON CONFLICT`, which needs a primary key or unique constraint on exactly the keys. A table the sink creates
gets one: its primary key is the members marked `[DuckDBColumn(PrimaryKey = true)]`, or else the upsert keys, and upsert
keys other than the primary key get a `UNIQUE` constraint. A table that already exists is left as it is. DuckDB sinks don't retry: the database is in process, with no network to
fail.

## Attributes

`[Column]` and `[IgnoreColumn]` work as in every connector, and so does `[DuckDBColumn]` (`Name`, `Ignore`). Its
`PrimaryKey` makes the column part of the primary key of a table the sink creates.

A sink writes every readable member, so mark computed and generated members `[IgnoreColumn]`; see
[Which members are written](sql-connectors.md#which-members-are-written).

## Dependency Injection

```csharp
services.AddDuckDBConnector()
    .AddDefaultDuckDBDatabase(DuckDBDatabase.File("warehouse.duckdb"))
    .AddDuckDBDatabase("scratch", DuckDBDatabase.InMemory);
```

Registers `DuckDBSourceNodeFactory` and `DuckDBSinkNodeFactory`, whose `CreateSource(query, databaseName)` and
`CreateSink(table, databaseName)` build nodes on the registered databases.

## Next Steps

- [SQL Connectors: Shared Behaviour](sql-connectors.md)
- [Parquet Connector](parquet.md): Parquet files without a query engine
