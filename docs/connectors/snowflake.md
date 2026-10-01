---
title: "Snowflake Connector"
description: "Read from and write to Snowflake with multi-row inserts, staged COPY INTO and MERGE upserts."
order: 14
---

# Snowflake Connector

The `NPipeline.Connectors.Snowflake` package reads Snowflake queries into records and writes records to tables, with
multi-row `INSERT` statements, one statement per row, or a staged file loaded with `COPY INTO`, and upserts with
`MERGE`. Mapping, row errors, transactions, failed batches and checkpoints work as in every SQL connector; see
[SQL Connectors: Shared Behaviour](sql-connectors.md).

## Installation

```bash
dotnet add package NPipeline.Connectors.Snowflake
```

**Dependencies:** [Snowflake.Data](https://www.nuget.org/packages/Snowflake.Data), `NPipeline.Connectors`

## Reading

```csharp
public sealed record Order(int OrderId, string CustomerName, decimal Total);

var source = SnowflakeConnector.Source<Order>(connectionString, "SELECT ORDER_ID, CUSTOMER_NAME, TOTAL FROM PUBLIC.ORDERS ORDER BY ORDER_ID");
```

Members map to UPPER_SNAKE columns (`OrderId` to `ORDER_ID`), as Snowflake stores unquoted names; set `Naming` for
another convention. Authentication, account, warehouse, role and database are set in the connection string, including
key pair authentication (`authenticator=snowflake_jwt;private_key_file=…`).

## Writing

```csharp
var sink = SnowflakeConnector.Sink<Order>(connectionString, "ORDERS", o => o with { Schema = "PUBLIC", WriteStrategy = SnowflakeWriteStrategy.StagedCopy });
```

| `WriteStrategy` | Writes | Notes |
| --- | --- | --- |
| `Batch` (default) | Multi-row `INSERT` statements with positional parameters | |
| `PerRow` | One statement per row | Slowest |
| `StagedCopy` | A gzipped CSV file per batch, `PUT` to a stage and loaded with `COPY INTO` | Fastest for large loads; inserts only |

The staged file writes `NULL` as an unquoted empty field and text quoted, so an empty string stays an empty string; it
writes numbers and dates culture-invariantly, timestamps to the tick, and binary as hex. `COPY INTO` runs with
`ON_ERROR = ABORT_STATEMENT`, so a row Snowflake cannot load fails the batch. A retried batch uploads and loads the same
file name, so Snowflake's load metadata skips a file it has already loaded; a first attempt that loads no files fails.

### Upserts

`Upsert = SqlUpsert.On("ORDER_ID")` writes `MERGE INTO … USING (SELECT … FROM VALUES …)`. `SqlUpsertAction.Ignore`
inserts only rows whose key is new.

### Write options

| Option | Default | Description |
| --- | --- | --- |
| `WriteStrategy` | `Batch` | See above |
| `Stage` | `~` | The internal stage for staged copies: `~` (the user stage) or a named stage |
| `StageFilePrefix` | `npipeline_` | The prefix of staged file names |
| `PurgeStagedFiles` | `true` | Whether `COPY INTO` removes a staged file once loaded |
| `Resilience` | `SnowflakeConnectorResilience.Default` | Retries a batch after a transient failure; see [Resilience](#resilience) |

Plus the [options every SQL sink has](sql-connectors.md#options-every-sink-has).

## Attributes

`[Column]` and `[IgnoreColumn]` work as in every connector. `[SnowflakeColumn]` adds `Identity` (read, never written)
and `DbType`/`Size` to type the parameter explicitly.

A sink writes every readable member, so mark computed and generated members `[IgnoreColumn]`; see
[Which members are written](sql-connectors.md#which-members-are-written).

## Connections

A source or sink takes a connection string, a `snowflake://` storage URI (the account as the host and the database as
the path; see [Connections](sql-connectors.md#connections)), or an `ISnowflakeConnectionPool` of named connection
strings (set `ConnectionName` to pick one, for example one per warehouse). Opening a Snowflake session takes seconds
rather than milliseconds, so reuse one connection string, which Snowflake.Data pools sessions for, and size the
warehouse for the load.

A named stage for `StagedCopy` must exist and be one the connection's role can write to; the user stage (`~`) always
exists.

## Resilience

Sources retry connecting and running the query; sinks retry a batch whose `PerBatch` transaction was rolled back. Both
use `Resilience`, whose default `SnowflakeConnectorResilience.Default`:

- Makes up to four attempts (three retries).
- Retries errors the server returns for a statement: network errors (200002), service unavailability (390144),
  statement timeouts (625), and internal errors (604).
- Treats a statement error with a throttling message as throttling, which waits longer: backoff starts at 10 seconds.
- Doesn't retry failures the driver has already retried, or other errors, such as a missing object or a permission error.
- Waits with exponential backoff and full jitter, from 2 seconds up to 60 seconds.

The driver retries every HTTP request of a statement on its own (up to `MAXHTTPRETRIES`, default 7, within
`RETRY_TIMEOUT`, default 300 seconds), including polling for a running query's result. What it reports after giving
up is permanent to the connector: an `HttpRequestException`, a request timeout (270007), an I/O error on `PUT`
(270058), any other driver error in the 270000 range, and a lost session (390111). With `DISABLERETRY=true` in the
connection string, failed HTTP responses aren't retried by either layer.

## Dependency Injection

```csharp
services.AddSnowflakeConnector(options => options.DefaultConnectionString = "account=myaccount;user=...;db=SALES;...");
services.AddSnowflakeConnection("etl", "account=myaccount;warehouse=ETL_WH;...");
```

Registers `ISnowflakeConnectionPool`, `ISnowflakeSourceNodeFactory` and `ISnowflakeSinkNodeFactory`.

## Next Steps

- [SQL Connectors: Shared Behaviour](sql-connectors.md)
- [Storage Providers](../storage-providers/index.md): `snowflake://` URIs
