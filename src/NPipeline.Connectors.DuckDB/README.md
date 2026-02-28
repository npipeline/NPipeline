# NPipeline.Connectors.DuckDB

DuckDB source and sink nodes for NPipeline — an embedded analytical database connector with native Parquet/CSV/JSON support, SQL queries, and high-performance
Appender API.

## Features

- **SQL-over-files** — Query Parquet, CSV, and JSON files with full SQL (aggregations, joins, window functions)
- **Streaming-first** — Source nodes stream rows via `IAsyncEnumerable<T>` with zero full-result buffering
- **High-performance writes** — Sink nodes use DuckDB's native Appender API for ~1M+ rows/sec throughput
- **File export** — Export pipeline data to Parquet/CSV via DuckDB's optimized `COPY TO`
- **Zero infrastructure** — Embedded in-process database; no server, no containers
- **Auto-create tables** — Tables are automatically created from your CLR types
- **Attribute mapping** — Use `[DuckDBColumn]` for property-to-column mapping
- **DI support** — Full `IServiceCollection` integration with named databases

## Runtime Notes

- `NPipeline.Connectors.DuckDB` depends on `DuckDB.NET.Data.Full`, so native DuckDB runtime binaries are included transitively.

## Quick Start

```csharp
// Read from a Parquet file with SQL
var source = DuckDBSourceNode<SalesRecord>.FromFile("data/sales.parquet");

// Query with SQL
var source = new DuckDBSourceNode<Metric>(null, """
    SELECT region, SUM(amount) as Total
    FROM read_parquet('sales/*.parquet')
    GROUP BY region
    """);

// Write with high-performance Appender
var sink = new DuckDBSinkNode<Order>(null, "orders");

// Export to Parquet
var sink = DuckDBSinkNode<Report>.ToFile("output/report.parquet");
```

## Documentation

See the [full documentation](https://github.com/NPipeline/NPipeline/blob/main/docs/connectors/duckdb.md) for detailed usage, configuration options, and
examples.
