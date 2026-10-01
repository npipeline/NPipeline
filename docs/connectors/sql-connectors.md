---
title: "SQL Connectors: Shared Behaviour"
description: "Connections, mapping, row errors, transactions, failed batches, upserts and checkpoints shared by the SQL connectors."
order: 8
---

# SQL Connectors: Shared Behaviour

The [SQL Server](sqlserver.md), [PostgreSQL](postgres.md), [MySQL](mysql.md), [Snowflake](snowflake.md) and
[DuckDB](duckdb.md) connectors are built on one pair of base classes in `NPipeline.Connectors.Sql`, so they read,
write, convert and handle errors the same way. This page describes that shared behaviour; each connector's page covers
its connection, write strategies and upsert syntax.

## Creating nodes

Each connector has a factory class and an options record per direction:

```csharp
var source = SqlServerConnector.Source<Order>(connectionString, "SELECT * FROM dbo.Orders ORDER BY Id");
var sink = PostgresConnector.Sink<Order>(connectionString, "orders", o => o with { BatchSize = 5_000 });
```

The optional last argument adjusts the default options, usually with a `with` expression. The factories take a
connection string, a storage URI (`mssql://`, `postgres://`, `mysql://`, `snowflake://`) or the connector's
connection pool; DuckDB takes a `DuckDBDatabase`. MySQL's factory is `MySqlNodes`, because the MySqlConnector driver
already uses the name `MySqlConnector` for its namespace.

## Connections

Connection strings are passed to the driver as they are, so pool sizes, timeouts, encryption and TLS settings belong
in them (`Encrypt=Strict` for SQL Server, `SslMode=VerifyFull` for PostgreSQL, `SslMode=VerifyFull` for MySQL).

A storage URI names the server, the database and, optionally, the credentials, so the same pipeline code can switch
environments by configuration:

```csharp
var uri = StorageUri.Parse(Environment.GetEnvironmentVariable("ORDERS_DB")!); // e.g. mssql://db.example.com:1433/sales?encrypt=true
var source = SqlServerConnector.Source<Order>(uri, "SELECT * FROM dbo.Orders ORDER BY Id");
```

| Scheme | Connector | Host | Path |
| --- | --- | --- | --- |
| `mssql://`, `sqlserver://` | SQL Server | Server and optional port | Database |
| `postgres://`, `postgresql://` | PostgreSQL | Host and optional port | Database |
| `mysql://`, `mariadb://` | MySQL | Server and optional port | Database |
| `snowflake://` | Snowflake | Account identifier | Database |

Credentials come from the `username` (or `user`) and `password` (or `pwd`) query parameters, or from
`user:password@` before the host. Every other query parameter is passed through as a connection-string keyword, such
as `?encrypt=true&trustServerCertificate=false` for SQL Server, `?sslmode=require` for PostgreSQL, or
`?warehouse=ETL_WH&role=LOADER&schema=PUBLIC` for Snowflake. Keep passwords out of URIs that end up in logs or source
control.

## Reading

A source runs its query and streams the rows; nothing is buffered. The result's columns bind to the record's members by
name once per query (case-insensitively), through a mapper compiled for that result's layout, and each value is read
typed at its ordinal:

| Type shape | How it maps |
| --- | --- |
| Class with setters or `init` accessors | Members set by column name |
| Positional record or primary constructor | Constructor parameters matched to columns by name |
| `required` members | Must have a column, or the query fails before its first row |
| `[Column("order_id")]` or the connector's column attribute | Sets the column name; the shared `[Column]` wins when a member has both |
| `[IgnoreColumn]` | Leaves the member out, when reading and when writing; see [Which members are written](#which-members-are-written) |
| `int`, `string` and other single values | The result's first column |

A value the column already holds as the member's type is read as it is. Otherwise it converts strictly and
culture-invariantly: a `bigint` into an `int` (failing if it does not fit), text into a number, date, `Guid` or enum,
`DATE` into `DateOnly`, `TIME` into `TimeOnly`, integers into enums. A `NULL` for a non-nullable member, or a value
that does not convert, is a row error.

### Options every source has

| Option | Default | Description |
| --- | --- | --- |
| `Query` | required | The query |
| `Parameters` | none | `DatabaseParameter`s, bound by name |
| `CommandTimeout` | 30 s | The command timeout; 0 waits indefinitely |
| `Naming` | the connector's convention | How member names become column names: `AsIs` for SQL Server, MySQL and DuckDB, `SnakeCaseLower` for PostgreSQL, `SnakeCaseUpper` for Snowflake |
| `MissingColumns` | `ThrowForRequired` | What a member without a column means: fail only for `required` members, fail for any, or never |
| `RowErrorHandler` | `null` (fail) | Decides what happens to a row that fails to map: `Fail`, `Skip` or `DeadLetter` |
| `RawExcerptLength` | 256 | Characters of the failed row's values (`id=2, score=abc`) kept in the row error |
| `CheckpointStrategy` | `None` | `InMemory` or `Offset`; see [Checkpoints](#checkpoints) |
| `CheckpointStorage` | `null` | Where `Offset` checkpoints are kept; required for `Offset` |
| `CheckpointInterval` | every 100 rows or 10 s | How often a checkpoint is saved (`RowCountInterval`, `TimeInterval`) |
| `Resilience` | the connector's default | Retries connecting and running the query; see [Retries](#retries). Not on DuckDB, which runs in process |

A row sent to the dead-letter sink arrives as a `ConnectorRecordFailure` with the source, record number, column and
excerpt; the pipeline needs a dead-letter sink (`builder.AddDeadLetterSink(...)`).

### Manual mapping

For full control, pass a mapper. `SqlRow.Get<T>` converts as above and names the column when it fails:

```csharp
var source = SqlServerConnector.Source(connectionString, "SELECT id, total, placed_at FROM orders",
    row => new Order(row.Get<int>("id"), row.GetOrDefault("total", 0m), row.Get<DateTimeOffset>(2)));
```

| `SqlRow` member | Description |
| --- | --- |
| `Get<T>(string name)`, `Get<T>(int ordinal)` | Read and convert a value; throws `FieldMappingException` naming the column |
| `TryGet<T>(string name, out T value)` | Read and convert, returning `false` instead of throwing |
| `GetOrDefault<T>(string name, T defaultValue)` | The value, or `defaultValue` when it is missing, `NULL` or does not convert |
| `this[string name]`, `this[int ordinal]` | The provider's value, or `null` for `NULL` |
| `HasColumn`, `IsNull`, `GetOrdinal` | The result's columns and nulls |
| `ColumnNames`, `FieldCount`, `RecordNumber` | The result's columns and the row's 1-based position |

The same `SqlRow` is reused for every row, so read what you need inside the mapper. An exception the mapper throws is a
row error.

### Checkpoints

`CheckpointStrategy.Offset` (with a `CheckpointStorage`) and `InMemory` record how many rows the source has handed on
and, when the read restarts, skip that many:

```csharp
var source = PostgresConnector.Source<Order>(connectionString, "SELECT * FROM orders ORDER BY id",
    o => o with
    {
        CheckpointStrategy = CheckpointStrategy.Offset,
        CheckpointStorage = new FileCheckpointStorage("checkpoints"),
        CheckpointInterval = new CheckpointIntervalConfiguration { RowCountInterval = 10_000, TimeInterval = TimeSpan.FromMinutes(1) },
    });
```

- **Order.** Skipping lands on the same rows only when the query returns them in a stable order, so give it an
  `ORDER BY` on a unique, increasing key (an identity column, or a timestamp plus the key). Without one, a restart can
  skip rows it never read or read rows twice. The SQL Server and PostgreSQL analyzer packages warn when a
  checkpointing query has no `ORDER BY`.
- **At least once.** A checkpoint is saved every `CheckpointInterval` and when the read ends, fails or is cancelled.
  After a crash, the rows read since the last save are read again.
- **Later runs.** A completed read keeps its checkpoint, so the next run skips the rows already read and emits only
  rows that sort after them: with `ORDER BY id` on an append-only table, each run reads what is new. To read from the
  start, delete the checkpoint (`CheckpointStorage.DeleteAsync`).
- **Cost.** Skipped rows are still read from the server and discarded. For a large table, a query that filters on the
  key (`WHERE id > @since`) reads less.
- **Storage.** `FileCheckpointStorage` keeps checkpoints in a directory; `SqlServerCheckpointStorage`,
  `PostgresCheckpointStorage`, `MySqlCheckpointStorage` and `SnowflakeCheckpointStorage` keep them in a table
  (`pipeline_checkpoints` by default). Checkpoints are keyed by the node's id, so a renamed node starts again.
- In-memory checkpoints belong to the source node, so the same node must be reused to resume.

## Writing

A sink writes every readable member of the record as a column, through a plan compiled once per type, in batches of
`BatchSize` rows. How a batch is written depends on the connector's write strategy: one statement per row, multi-row
`INSERT` statements (split to stay under the database's parameter limit), or the database's bulk API. Enums are
written as their underlying integer and `char` as text.

### Which members are written

**Every public property with a getter, and every public field, is written, including read-only and computed
properties.** The table needs a column for each, or the write fails. Mark anything that has no column with `[IgnoreColumn]`:

```csharp
public class Customer
{
    public int Id { get; set; }
    public string FirstName { get; set; } = "";
    public string LastName { get; set; } = "";

    [IgnoreColumn]                                  // computed: the table has no full_name column
    public string FullName => $"{FirstName} {LastName}";
}
```

Without the attribute, the sink writes a `full_name` column, and the database rejects it, for example PostgreSQL's
`column "full_name" of relation "customers" does not exist`. This differs from reading, where a column with no member
is simply left out.

The same applies to columns the database fills in:

| Column | How to leave it out of writes |
| --- | --- |
| SQL Server `IDENTITY`, Snowflake `AUTOINCREMENT` | `Identity = true` on `[SqlServerColumn]` or `[SnowflakeColumn]`: the member is read but never written |
| MySQL `AUTO_INCREMENT` | `AutoIncrement = true` on `[MySqlColumn]`: read but never written |
| PostgreSQL `SERIAL` or identity, DuckDB sequences | `[IgnoreColumn]`, since the member would otherwise write its default value (`0`) into every row and collide |

A member you ignore is not read back either. To read a generated column but not write it, use two types: one with the
member for the query, and one without it for the sink. The same goes for a column with a default the database should
fill (a `created_at`): the sink writes the member whenever it exists, so a write type without the member lets the
default apply.

### Transactions

| `Transaction` | Behaviour |
| --- | --- |
| `PerBatch` (default) | Each batch commits in its own transaction, so it lands whole or not at all, and a transient failure is retried with the connector's `Resilience` policy (DuckDB doesn't retry) |
| `WholeRun` | One transaction for the whole write, committed after the last batch and rolled back if anything fails. A failure can abort the whole transaction, so batches are not retried |
| `None` | No transaction of the sink's own; a batch split into several statements can land partly, so failed batches are not retried |

A rollback runs even when the write was cancelled. `WholeRun` holds its locks, and the transaction log, until the last
batch, so it suits loads that must appear all at once more than long-running streams.

### Failed batches

By default a batch that fails (after any retries) fails the write. With `FailedBatches = FailedBatchAction.DeadLetter`,
the batch goes to the pipeline's dead-letter sink as a `SqlBatchFailure<T>` (the table and the batch's items) and the
write continues. This cannot be combined with `WholeRun`, whose transaction cannot continue after a failure.

### Upserts

Set `Upsert` to write an upsert on key columns instead of a plain insert:

```csharp
var sink = PostgresConnector.Sink<Order>(connectionString, "orders", o => o with { Upsert = SqlUpsert.On("id") });
var keep = SqlServerConnector.Sink<Order>(connectionString, "Orders", o => o with { Upsert = new SqlUpsert(["Id"], SqlUpsertAction.Ignore) });
```

`SqlUpsertAction.Update` (the default) updates the other columns of a row whose key exists; `Ignore` keeps it. The
keys are column names after the naming policy, and must be columns the record writes. A batch must not repeat a key.
Bulk strategies only insert.

### Identifiers

Table, schema and column names are always quoted, with the closing quote escaped, so no name can break out of the
statement. With `ValidateIdentifiers` (the default), the table, schema and upsert keys must also be plain identifiers
(letters, digits and underscores).

### Options every sink has

| Option | Default | Description |
| --- | --- | --- |
| `Table` | required | The table |
| `Schema` | `null` | The table's schema; `null` uses the connection's default |
| `BatchSize` | 1,000 | Rows per batch |
| `BatchLinger` | 1 s | A partial batch is written once this long has passed since its first row, so a slow stream is written promptly |
| `Transaction` | `PerBatch` | See [Transactions](#transactions) |
| `FailedBatches` | `Fail` | See [Failed batches](#failed-batches) |
| `Upsert` | `null` | See [Upserts](#upserts) |
| `ValidateIdentifiers` | `true` | See [Identifiers](#identifiers) |
| `CommandTimeout`, `Naming` | 30 s, the connector's convention | As for sources |
| `Resilience` | the connector's default | Retries a failed `PerBatch` batch; see [Retries](#retries). Not on DuckDB |

## Retries

The SQL Server, PostgreSQL, MySQL and Snowflake connectors retry with [NResilience](https://github.com/nresilience/NResilience).
Each connector's `Resilience` default (for example `SqlServerConnectorResilience.Default`) retries only the errors
that connector classifies as transient, such as deadlocks, timeouts and lost connections, with exponential backoff; the
connector pages list them.

- **Sources** retry connecting and running the query, before the first row. Once rows are flowing, a failure is not
  retried, because the rows already emitted would be emitted again; restart from a [checkpoint](#checkpoints) instead.
- **Sinks** retry a batch only with `PerBatch` transactions, whose rollback leaves nothing behind. A connection that
  the failure closed is reopened first.
- **Changing the policy.** Derive one with `with`, for example
  `o => o with { Resilience = SqlServerConnectorResilience.Default with { Attempts = 6 } }`, or turn retries off with
  `Resilience.None`.
- **Observing retries.** The connectors don't log retries. To see them, attach a listener:
  `Resilience = SqlServerConnectorResilience.Default.WithListener(e => ...)`.
- **One retrying layer.** Leave the driver's own command retries off (SqlClient's `SqlConfigurableRetryFactory`, for
  example), because two retrying layers multiply attempts.

## Acknowledging messages

A SQL sink reports each batch once it is committed, so messages from a message queue written through `Acknowledging()`
are acknowledged then: `SqlServerConnector.Sink<Order>(connectionString, "Orders").Acknowledging()`. With `WholeRun`
they are acknowledged after the final commit. See [Message Queues: Shared Behaviour](message-queues.md#writing-messages-through-any-sink).

## Metrics

Sources and sinks report rows read, rows written and row errors through `System.Diagnostics.Metrics`
(`NPipeline.Connectors`), tagged with the connector's name, as the file connectors do.

## Next Steps

- [SQL Server](sqlserver.md), [PostgreSQL](postgres.md), [MySQL](mysql.md), [Snowflake](snowflake.md), [DuckDB](duckdb.md)
- [File Connectors: Shared Behaviour](file-connectors.md): the same mapping and row errors for files
