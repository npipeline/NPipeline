---
title: "PostgreSQL Connector"
description: "Read from and write to PostgreSQL with multi-row inserts, binary COPY and ON CONFLICT upserts."
order: 10
---

# PostgreSQL Connector

The `NPipeline.Connectors.Postgres` package reads PostgreSQL queries into records and writes records to tables, with
multi-row `INSERT` statements, one statement per row, or binary `COPY`, and upserts with `INSERT … ON CONFLICT`.
Mapping, row errors, transactions, failed batches and checkpoints work as in every SQL connector; see
[SQL Connectors: Shared Behaviour](sql-connectors.md).

## Installation

```bash
dotnet add package NPipeline.Connectors.Postgres
```

**Dependencies:** [Npgsql](https://www.nuget.org/packages/Npgsql), `NPipeline.Connectors`

## Reading

```csharp
public sealed record Order(int OrderId, string Customer, decimal Total, DateTimeOffset PlacedAt);

var source = PostgresConnector.Source<Order>(connectionString, "SELECT order_id, customer, total, placed_at FROM orders ORDER BY order_id");
builder.AddSource(source, "orders");
```

Members map to snake_case columns (`OrderId` to `order_id`), PostgreSQL's convention; set
`Naming = ColumnNamingPolicy.AsIs` for columns named like the members. Acronyms stay one word (`HTTPStatus` to
`http_status`). Parameters are bound by name (`@since`).

## Writing

```csharp
var sink = PostgresConnector.Sink<Order>(connectionString, "orders", o => o with { WriteStrategy = PostgresWriteStrategy.Copy });
builder.AddSink(sink, "orders-out");
```

| `WriteStrategy` | Writes | Notes |
| --- | --- | --- |
| `Batch` (default) | Multi-row `INSERT` statements with positional parameters | Npgsql sends them without rewriting |
| `PerRow` | One statement per row | Slowest |
| `Copy` | Binary `COPY … FROM STDIN` | Fastest; values go in PostgreSQL's binary format, so no text formatting is involved; inserts only |

The sink reads the table's column types once per write, so dates and times are sent as the column's type:

- A `DateTime` into `timestamp` stores its UTC wall-clock time; into `timestamptz`, the instant (an unspecified kind
  counts as UTC). Either way the value does not depend on the session's time zone.
- A `DateTimeOffset` is sent as its UTC instant, since PostgreSQL stores no offset.
- Unsigned integers and `byte` are widened to the next signed type, which PostgreSQL has.

### Upserts

`Upsert = SqlUpsert.On("order_id")` writes `INSERT … ON CONFLICT (order_id) DO UPDATE SET …`; the keys need a unique
index or constraint. `SqlUpsertAction.Ignore` writes `DO NOTHING`.

### Write options

| Option | Default | Description |
| --- | --- | --- |
| `WriteStrategy` | `Batch` | See above |
| `Resilience` | `PostgresConnectorResilience.Default` | Retries a batch after a transient failure; see [Resilience](#resilience) |

Plus the [options every SQL sink has](sql-connectors.md#options-every-sink-has).

## Attributes

`[Column]` and `[IgnoreColumn]` work as in every connector. `[PostgresColumn]` adds `DbType` (an `NpgsqlDbType`) to
type the parameter explicitly:

```csharp
public sealed class Event
{
    [PostgresColumn("payload", DbType = NpgsqlDbType.Jsonb)]
    public string Payload { get; set; } = "{}";
}
```

A sink writes every readable member, so mark computed and generated members `[IgnoreColumn]`; see
[Which members are written](sql-connectors.md#which-members-are-written).

## Connections

A source or sink takes a connection string, a `postgres://` storage URI, or an `IPostgresConnectionPool` of named data
sources (set `ConnectionName` to pick one). Connection strings are used as they are, so pool sizes, timeouts and TLS
(`SslMode=VerifyFull`) belong in them; Npgsql pools connections per connection string, so many sources share one
pool. `postgres://` URIs are described in [Connections](sql-connectors.md#connections).

## Resilience

Sources retry connecting and running the query (no row has been read yet, so a retry cannot emit one twice); sinks
retry a batch whose `PerBatch` transaction was rolled back. Both use `Resilience`, whose default
`PostgresConnectorResilience.Default`:

- Makes up to four attempts (three retries).
- Retries connection failures (08xxx), serialization failures (40001), deadlocks (40P01), resource errors (53xxx),
  server shutdowns (57P01-57P03), and client-side network errors that Npgsql reports as transient.
- Treats too many connections (SQLSTATE 53300) as throttling, which waits longer: backoff starts at 5 seconds.
- Doesn't retry other errors, such as a constraint violation or a missing table.
- Waits with exponential backoff and full jitter, from 1 second up to 30 seconds.
- Has no attempt timeout and no deadline; the command timeout bounds each attempt.

Npgsql doesn't retry commands itself, so the connector is the only retrying layer.

## Dependency Injection

```csharp
services.AddPostgresConnector(options => options.DefaultConnectionString = "Host=localhost;Database=sales;...");
services.AddPostgresConnection("analytics", "Host=analytics-db;...");
```

Registers `IPostgresConnectionPool`, `IPostgresSourceNodeFactory` and `IPostgresSinkNodeFactory`, whose
`CreateSourceNode(query, configure)` and `CreateSinkNode(table, configure)` build nodes on the pool.

## Analyzer

The `NPipeline.Connectors.Postgres.Analyzers` package reports **NP9501** when a source checkpoints with a query that has
no `ORDER BY`; see [Checkpoints](sql-connectors.md#checkpoints).

## Next Steps

- [SQL Connectors: Shared Behaviour](sql-connectors.md)
- [DuckDB Connector](duckdb.md): query Parquet and CSV files with SQL
- [Storage Providers](../storage-providers/index.md): `postgres://` URIs
