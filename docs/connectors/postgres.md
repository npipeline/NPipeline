---
title: "PostgreSQL Connector"
description: "Read from and write to PostgreSQL databases with connection pooling, batch writes, and COPY support."
order: 8
---

# PostgreSQL Connector

The `NPipeline.Connectors.Postgres` package provides source and sink nodes for PostgreSQL. Supports connection pooling, parameterized queries, batch inserts, high-performance `COPY` operations, and upserts with `ON CONFLICT`.

## Installation

```bash
dotnet add package NPipeline.Connectors.Postgres
```

**Dependencies:** [Npgsql](https://www.nuget.org/packages/Npgsql) 10.x

## Source Node - `PostgresSourceNode<T>`

Reads rows from a SQL query and emits each as an item of type `T`.

### Constructors

```csharp
// Connection string + SQL query
public PostgresSourceNode(
    string sql,
    PostgresConfiguration configuration,
    Func<PostgresRow, T>? rowMapper = null)

// NpgsqlDataSource (recommended for DI)
public PostgresSourceNode(
    string sql,
    NpgsqlDataSource dataSource,
    Func<PostgresRow, T>? rowMapper = null)

// StorageUri-based (multi-tenant scenarios)
public PostgresSourceNode(
    StorageUri uri, string query,
    IStorageResolver? resolver = null,
    Func<PostgresRow, T>? rowMapper = null,
    PostgresConfiguration? configuration = null)
```

### Example

```csharp
var config = new PostgresConfiguration
{
    ConnectionString = "Host=localhost;Database=orders;Username=app;Password=secret"
};

var source = new PostgresSourceNode<Order>(
    "SELECT id, customer, amount FROM orders WHERE status = 'pending'",
    config,
    row => new Order(
        row.Get<int>("id"),
        row.Get<string>("customer") ?? "",
        row.Get<decimal>("amount")));
```

## Sink Node - `PostgresSinkNode<T>`

Writes items to a PostgreSQL table. Supports three write strategies:

| Strategy | Description | Best For |
|----------|-------------|----------|
| `PerRow` | Individual `INSERT` per item | Small volumes, maximum control |
| `Batch` (default) | Batched `INSERT` statements | Most workloads |
| `Copy` | PostgreSQL `COPY` protocol | Maximum throughput (bulk loads) |

### Constructors

```csharp
// Connection string
public PostgresSinkNode(
    string connectionString, string tableName,
    PostgresWriteStrategy writeStrategy = PostgresWriteStrategy.Batch,
    Func<T, IEnumerable<DatabaseParameter>>? parameterMapper = null,
    PostgresConfiguration? configuration = null,
    string? schema = null)

// Connection pool (recommended for DI)
public PostgresSinkNode(
    IPostgresConnectionPool connectionPool, string tableName,
    PostgresWriteStrategy writeStrategy = PostgresWriteStrategy.Batch,
    Func<T, IEnumerable<DatabaseParameter>>? parameterMapper = null,
    PostgresConfiguration? configuration = null,
    string? schema = null,
    string? connectionName = null)
```

### Example: Batch Upsert

```csharp
var config = new PostgresConfiguration
{
    ConnectionString = "Host=localhost;Database=orders;Username=app;Password=secret",
    WriteStrategy = PostgresWriteStrategy.Batch,
    BatchSize = 1000,
    UseUpsert = true,
    UpsertConflictColumns = new[] { "id" },
    OnConflictAction = OnConflictAction.Update
};

var sink = new PostgresSinkNode<Order>("connection-string", "orders", configuration: config);
```

## Configuration

### Connection

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ConnectionString` | `string` | `""` | PostgreSQL connection string |
| `Schema` | `string` | `"public"` | Default schema |
| `CommandTimeout` | `int` | `30` | Command timeout (seconds) |
| `ConnectionTimeout` | `int` | `15` | Connection timeout (seconds) |
| `CopyTimeout` | `int` | `300` | COPY operation timeout (seconds) |
| `MinPoolSize` | `int` | `5` | Minimum connection pool size |
| `MaxPoolSize` | `int` | `50` | Maximum connection pool size |
| `UseSslMode` | `bool` | `false` | Enable SSL |
| `ReadBufferSize` | `int` | `8192` | Read buffer size (bytes) |

### Write

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `WriteStrategy` | `PostgresWriteStrategy` | `Batch` | `PerRow`, `Batch`, or `Copy` |
| `BatchSize` | `int` | `1000` | Items per batch |
| `MaxBatchSize` | `int` | `5000` | Maximum batch size |
| `UseTransaction` | `bool` | `true` | Wrap writes in a transaction |
| `UseBinaryCopy` | `bool` | `false` | Use binary format for COPY |

### Upsert

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `UseUpsert` | `bool` | `false` | Enable `INSERT ... ON CONFLICT` |
| `UpsertConflictColumns` | `string[]?` | `null` | Conflict target columns |
| `OnConflictAction` | `OnConflictAction` | `Update` | `Update` or `Ignore` |

### Error Handling

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ContinueOnError` | `bool` | `false` | Continue on row-level errors |
| `Resilience` | `Resilience` | `PostgresConnectorResilience.Default` | How transient failures are retried; see [Resilience](#resilience) |
| `RowErrorHandler` | `Func<Exception, PostgresRow?, bool>?` | `null` | Custom error handler |

## Resilience

The sink retries transient failures with [NResilience](https://github.com/nresilience/NResilience). The `Resilience`
property on `PostgresConfiguration` configures it. The default, `PostgresConnectorResilience.Default`, does the following:

- Makes up to four attempts (three retries).
- Retries connection failures (08xxx), serialization failures (40001), deadlocks (40P01), resource errors (53xxx), server shutdowns (57P01-57P03), and client-side network errors that Npgsql reports as transient.
- Treats too many connections (SQLSTATE 53300) as throttling, which waits longer: backoff starts at 5 seconds.
- Doesn't retry other errors, such as a constraint violation or a missing table.
- Waits with exponential backoff and full jitter, from 1 second up to 30 seconds.
- Has no attempt timeout and no deadline. The driver's own timeout bounds each attempt: `CommandTimeout`.
  A long bulk write isn't cut off by a retry policy's timeout.

Each write strategy retries one unit of work that commits all or nothing, so a retry never inserts rows that an
earlier attempt committed:

- `PerRow`: one `INSERT` per row.
- `Batch`: one multi-row `INSERT` statement per flush.
- `Copy`: one `COPY ... FROM STDIN` per flush, which PostgreSQL commits all or nothing.

The source node retries connecting and opening the query reader using the same `Resilience`. Once rows are flowing, a failure is not retried, because the rows already emitted would be emitted again.

With `DeliverySemantic.ExactlyOnce`, the sink wraps all writes in one transaction. A failure can abort that whole
transaction, so the writers make one attempt and the sink rolls the transaction back. A batch that fails isn't
written again when the writer is disposed.

To change a setting, derive a policy with a `with` expression:

```csharp
var config = new PostgresConfiguration
{
    Resilience = PostgresConnectorResilience.Default with { Attempts = 6 },
};
```

To turn retries off, use `Resilience.None`.

The connector is the only layer that retries; Npgsql doesn't retry commands. Retries aren't logged by the connector. To observe them, attach a listener: `PostgresConnectorResilience.Default.WithListener(e => ...)`.

## Dependency Injection

```csharp
services.AddPostgresConnector(options =>
{
    options.DefaultConnectionString = "Host=localhost;Database=mydb;...";
    options.DefaultConfiguration = new PostgresConfiguration
    {
        WriteStrategy = PostgresWriteStrategy.Batch,
        BatchSize = 1000
    };
});

// Named connections for multi-database scenarios
services.AddPostgresConnection("analytics", "Host=analytics-db;...");
services.AddPostgresConnection("operational", "Host=ops-db;...");
```

Registers `IPostgresConnectionPool`, `PostgresSourceNodeFactory`, and `PostgresSinkNodeFactory`.

## Attribute Mapping

### Convention-Based

C# `PascalCase` property names are automatically converted to PostgreSQL `snake_case` column names:

- `CustomerId` → `customer_id`
- `TotalAmount` → `total_amount`

### `[Column]` / `[IgnoreColumn]` (Cross-Connector)

```csharp
using NPipeline.Connectors.Attributes;

public class Customer
{
    [Column("customer_id")]
    public int CustomerId { get; set; }

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}
```

### `[PostgresColumn]` (Connector-Specific)

Extends `[Column]` with PostgreSQL features:

```csharp
using NPipeline.Connectors.Postgres.Mapping;

public class Customer
{
    [PostgresColumn("customer_id", PrimaryKey = true)]
    public int CustomerId { get; set; }

    [PostgresColumn("first_name", DbType = NpgsqlDbType.Varchar, Size = 100)]
    public string FirstName { get; set; } = "";

    [PostgresColumn("email", DbType = NpgsqlDbType.Varchar, Size = 255)]
    public string Email { get; set; } = "";
}
```

| Property | Description |
|----------|-------------|
| `Name` | Column name in the database |
| `DbType` | PostgreSQL data type (`NpgsqlDbType`) |
| `Size` | Size/length for character types |
| `PrimaryKey` | Primary key (used for checkpointing) |
| `Ignore` | Skip mapping this property |

Use common attributes for portable code; use `[PostgresColumn]` when you need type or PK control.

## Delivery Semantics

| Semantic | Data Loss | Duplicates | Overhead | Use Case |
|----------|-----------|------------|----------|----------|
| `AtLeastOnce` (default) | No | Possible | Low | Idempotent operations |
| `AtMostOnce` | Possible | No | Low | Telemetry, metrics |
| `ExactlyOnce` | No | No | High | Financial transactions |

```csharp
var config = new PostgresConfiguration
{
    DeliverySemantic = DeliverySemantic.ExactlyOnce,
    UseTransaction = true,
    CheckpointStrategy = CheckpointStrategy.Offset,
    CheckpointStorage = new FileCheckpointStorage("checkpoints.json")
};
```

## Checkpointing

| Strategy | Persistence | Description |
|----------|-------------|-------------|
| `None` (default) | - | No checkpointing; restart from beginning |
| `InMemory` | Process lifetime | Transient failure recovery within a single run |
| `Offset` | External storage | Track position via monotonically increasing column |
| `KeyBased` | External storage | Track processed items by composite keys |
| `Cursor` | External storage | Track cursor position |
| `CDC` | External storage | Track WAL position for logical replication |

### Offset Example

```csharp
var config = new PostgresConfiguration
{
    CheckpointStrategy = CheckpointStrategy.Offset,
    CheckpointOffsetColumn = "id",
    CheckpointStorage = new FileCheckpointStorage("checkpoints/orders.json")
};

var source = new PostgresSourceNode<Order>(connectionString,
    "SELECT * FROM orders WHERE id > @lastCheckpoint ORDER BY id",
    configuration: config);
```

### CDC Example (Logical Replication)

```csharp
var config = new PostgresConfiguration
{
    CheckpointStrategy = CheckpointStrategy.CDC,
    CdcSlotName = "my_pipeline_slot",
    CdcPublicationName = "my_publication",
    CheckpointStorage = new FileCheckpointStorage("checkpoints/cdc.json")
};
```

### Checkpoint Intervals

```csharp
config.CheckpointInterval = new CheckpointIntervalConfiguration
{
    RowCountInterval = 10_000,
    TimeInterval = TimeSpan.FromMinutes(5)
};
```

## Mapping

| Property | Default | Description |
|----------|---------|-------------|
| `CaseInsensitiveMapping` | `true` | Match `Id`, `id`, `ID` to same property |
| `CacheMappingMetadata` | `true` | Cache mapping delegates per type |
| `ValidateIdentifiers` | `true` | Validate SQL identifiers to prevent injection |
| `UsePreparedStatements` | `true` | Reduce query parsing overhead |

## Performance

### Streaming

```csharp
var config = new PostgresConfiguration { StreamResults = true, FetchSize = 1_000 };
```

Without streaming, Npgsql loads the entire result set into memory.

| FetchSize | Best For |
|-----------|----------|
| 100–500 | Memory-constrained, wide rows |
| 1,000–5,000 | Most workloads |
| 5,000–10,000 | Maximum throughput |

### Write Strategy Comparison

| Strategy | Throughput | Latency | Error Isolation | Use Case |
|----------|-----------|---------|-----------------|----------|
| `PerRow` | Low | Low | High | Real-time, per-row errors |
| `Batch` | High | Medium | Medium | ETL, balanced |
| `Copy` | Very High | High | Low | Bulk loads, data warehouse |

### COPY Binary Format

```csharp
config.UseBinaryCopy = true; // 20–30% faster than text format
```

### Batch Size Guidelines

| Range | Best For |
|-------|----------|
| 100–500 | Real-time processing, low latency |
| 500–1,000 | Balanced throughput and latency |
| 1,000–5,000 | Bulk loading |

## Best Practices

1. **Use DI** with `AddPostgresConnector` for production
2. **Enable streaming** (`StreamResults = true`) for large result sets
3. **Use COPY** for bulk loading - highest throughput
4. **Enable binary COPY** for additional 20–30% performance gain
5. **Enable upsert** for idempotent writes
6. **Validate identifiers** - never disable in production
7. **Use prepared statements** for repeated query patterns
8. **Configure checkpointing** for long-running pipelines

## Next Steps

- [SQL Server Connector](sqlserver.md) - similar patterns for SQL Server
- [Error Handling](../error-handling/index.md) - retry strategies for database errors
- [Dependency Injection](../guides/dependency-injection.md) - full DI integration guide
