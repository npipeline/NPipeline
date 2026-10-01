---
title: "MySQL Connector"
description: "Read from and write to MySQL and MariaDB with multi-row inserts, bulk loads and ON DUPLICATE KEY upserts."
order: 10
---

# MySQL Connector

The `NPipeline.Connectors.MySQL` package reads MySQL and MariaDB queries into records and writes records to tables,
with multi-row `INSERT` statements, one statement per row, or `LOAD DATA LOCAL INFILE`, and upserts with
`ON DUPLICATE KEY UPDATE`. Mapping, row errors, transactions, failed batches and checkpoints work as in every SQL
connector; see [SQL Connectors: Shared Behaviour](sql-connectors.md).

## Installation

```bash
dotnet add package NPipeline.Connectors.MySQL
```

**Dependencies:** [MySqlConnector](https://www.nuget.org/packages/MySqlConnector), `NPipeline.Connectors`

## Reading

The factory is `MySqlNodes`, not `MySqlConnector`, because the driver already uses that name for its namespace.

```csharp
public sealed record Product(int Id, string Name, decimal Price);

var source = MySqlNodes.Source<Product>(connectionString, "SELECT Id, Name, Price FROM products ORDER BY Id");
builder.AddSource(source, "products");
```

Members map to columns of the same name, case-insensitively. Parameters are bound by name (`@since`).

## Writing

```csharp
var sink = MySqlNodes.Sink<Product>(connectionString, "products", o => o with { WriteStrategy = MySqlWriteStrategy.BulkLoad });
builder.AddSink(sink, "products-out");
```

| `WriteStrategy` | Writes | Notes |
| --- | --- | --- |
| `Batch` (default) | Multi-row `INSERT` statements | |
| `PerRow` | One statement per row | Slowest |
| `BulkLoad` | `LOAD DATA LOCAL INFILE` through `MySqlBulkCopy` | Fastest; needs `AllowLoadLocalInfile=true` in the connection string and `local_infile` on the server; inserts only |

`LOAD DATA` turns a value it cannot store (a string too long for its column, a date out of range) into a warning and
stores something else. The bulk writer fails the batch when the load reports a warning, so a changed value never lands
silently.

### Upserts

`Upsert = SqlUpsert.On("Id")` writes `INSERT … ON DUPLICATE KEY UPDATE col = VALUES(col)`, which works on MySQL 8 and
MariaDB. MySQL matches a row on the table's primary key and unique indexes, whichever it collides with; the keys decide
which columns are left alone on update. `SqlUpsertAction.Ignore` keeps existing rows with a no-op update (not
`INSERT IGNORE`, which would also hide other errors).

### Write options

| Option | Default | Description |
| --- | --- | --- |
| `WriteStrategy` | `Batch` | See above |
| `Resilience` | `MySqlConnectorResilience.Default` | Retries a batch after a transient failure; see [Resilience](#resilience) |

Plus the [options every SQL sink has](sql-connectors.md#options-every-sink-has).

## Attributes

`[Column]` and `[IgnoreColumn]` work as in every connector. `[MySqlColumn(AutoIncrement = true)]` marks a column the
database generates: it is read, but never written.

A sink writes every readable member, so mark computed and generated members `[IgnoreColumn]`; see
[Which members are written](sql-connectors.md#which-members-are-written).

## Connections

A source or sink takes a connection string, a `mysql://` or `mariadb://` storage URI (see
[Connections](sql-connectors.md#connections)), or an `IMySqlConnectionPool` of named connection strings (set
`ConnectionName` to pick one). Connection strings are used as they are. MySqlConnector works with MySQL 8 and
MariaDB 10.5 and later.

MySQL can store the zero date `0000-00-00`, which has no .NET value, so reading one is a row error. Add
`ConvertZeroDateTime=True` to the connection string to read it as `DateTime.MinValue`, or `AllowZeroDateTime=True` to
read it as a `MySqlDateTime`.

## Resilience

Sources retry connecting and running the query; sinks retry a batch whose `PerBatch` transaction was rolled back. Both
use `Resilience`, whose default `MySqlConnectorResilience.Default`:

- Makes up to four attempts (three retries).
- Retries lock wait timeouts (1205), deadlocks (1213), and lost connections (2006, 2013).
- Treats too many connections (1040 and 1203) as throttling, which waits longer: backoff starts at 5 seconds.
- Doesn't retry other errors, such as a duplicate key or a missing table.
- Waits with exponential backoff and full jitter, from 2 seconds up to 30 seconds.
- Has no attempt timeout and no deadline; the command timeout bounds each attempt.

A retry is safe because the batch's transaction rolled back, which only a transactional engine such as InnoDB does.
MyISAM and other non-transactional engines keep the rows a failed batch wrote, so a retry can write them twice, and a
batch split into several statements can land partly. For such tables, turn retries off with `Resilience.None`.

## Dependency Injection

```csharp
services.AddMySqlConnector(options => options.DefaultConnectionString = "Server=localhost;Database=shop;...");
services.AddMySqlConnection("reporting", "Server=reporting-db;...");
```

Registers `IMySqlConnectionPool`, `IMySqlSourceNodeFactory` and `IMySqlSinkNodeFactory`.

## Next Steps

- [SQL Connectors: Shared Behaviour](sql-connectors.md)
- [Storage Providers](../storage-providers/index.md): `mysql://` URIs
