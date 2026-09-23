---
title: "MySQL Connector"
description: "Read from and write to MySQL and MariaDB databases with batch inserts, bulk load, and upserts."
order: 10
---

# MySQL Connector

The `NPipeline.Connectors.MySQL` package provides source and sink nodes for MySQL and MariaDB. Supports connection pooling, batch inserts, high-performance `LOAD DATA LOCAL INFILE`, and `ON DUPLICATE KEY` upserts.

## Installation

```bash
dotnet add package NPipeline.Connectors.MySQL
```

**Dependencies:** [MySqlConnector](https://www.nuget.org/packages/MySqlConnector) 2.x

## Source Node - `MySqlSourceNode<T>`

### Constructors

```csharp
// Connection string + query
public MySqlSourceNode(
    string connectionString, string query,
    MySqlConfiguration? configuration = null)

// With custom mapper
public MySqlSourceNode(
    string connectionString, string query,
    Func<MySqlRow, T>? customMapper = null,
    MySqlConfiguration? configuration = null)

// Connection pool (recommended for DI)
public MySqlSourceNode(
    IMySqlConnectionPool connectionPool, string query,
    MySqlConfiguration? configuration = null,
    DatabaseParameter[]? parameters = null,
    bool continueOnError = false,
    string? connectionName = null)
```

## Sink Node - `MySqlSinkNode<T>`

| Strategy | Description | Best For |
|----------|-------------|----------|
| `PerRow` | Individual `INSERT` per item | Small volumes |
| `Batch` (default) | Batched `INSERT` statements | Most workloads |
| `BulkLoad` | `LOAD DATA LOCAL INFILE` | Maximum throughput |

### Constructors

```csharp
// Connection string
public MySqlSinkNode(
    string connectionString, string tableName,
    MySqlConfiguration? configuration = null,
    Func<T, IEnumerable<DatabaseParameter>>? customMapper = null)

// Connection pool (recommended for DI)
public MySqlSinkNode(
    IMySqlConnectionPool connectionPool, string tableName,
    MySqlConfiguration? configuration = null,
    Func<T, IEnumerable<DatabaseParameter>>? customMapper = null,
    string? connectionName = null)
```

## Configuration

### Connection

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ConnectionString` | `string` | `""` | MySQL connection string |
| `CommandTimeout` | `int` | `30` | Command timeout (seconds) |
| `ConnectionTimeout` | `int` | `15` | Connection timeout (seconds) |
| `DefaultDatabase` | `string?` | `null` | Default database name |
| `CharacterSet` | `string` | `"utf8mb4"` | Connection character set |
| `ConvertZeroDateTime` | `bool` | `true` | Convert zero dates to `DateTime.MinValue` |
| `MinPoolSize` | `int` | `1` | Minimum pool size |
| `MaxPoolSize` | `int` | `100` | Maximum pool size |

### Write

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `WriteStrategy` | `MySqlWriteStrategy` | `Batch` | `PerRow`, `Batch`, or `BulkLoad` |
| `BatchSize` | `int` | `100` | Items per batch |
| `MaxBatchSize` | `int` | `1000` | Maximum batch size |
| `UseTransaction` | `bool` | `true` | Wrap writes in a transaction |
| `UsePreparedStatements` | `bool` | `true` | Use prepared statements |

### Bulk Load

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `AllowLoadLocalInfile` | `bool` | `false` | Enable `LOAD DATA LOCAL INFILE` (must also be enabled on the MySQL server) |
| `BulkLoadBatchSize` | `int` | `5000` | Rows per bulk load batch |
| `BulkLoadTimeout` | `int` | `300` | Bulk load timeout (seconds) |
| `FieldTerminator` | `char` | `','` | Field separator |
| `LineTerminator` | `char` | `'\n'` | Line separator |

### Upsert

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `UseUpsert` | `bool` | `false` | Enable `ON DUPLICATE KEY` |
| `UpsertKeyColumns` | `string[]` | `[]` | Key columns for conflict detection |
| `OnDuplicateKeyAction` | `OnDuplicateKeyAction` | `Update` | `Update`, `Ignore`, or `Replace` |

### Error Handling

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ContinueOnError` | `bool` | `false` | Continue on row-level errors |
| `Resilience` | `Resilience` | `MySqlConnectorResilience.Default` | How transient failures are retried; see [Resilience](#resilience) |

## Resilience

The sink retries transient failures with [NResilience](https://github.com/nresilience/NResilience). The `Resilience`
property on `MySqlConfiguration` configures it. The default, `MySqlConnectorResilience.Default`, does the following:

- Makes up to four attempts (three retries).
- Retries lock wait timeouts (1205), deadlocks (1213), and lost connections (2006, 2013).
- Treats too many connections (1040 and 1203) as throttling, which waits longer: backoff starts at 5 seconds.
- Doesn't retry other errors, such as a duplicate key or a missing table.
- Waits with exponential backoff and full jitter, from 2 seconds up to 30 seconds.
- Has no attempt timeout and no deadline. The driver's own timeout bounds each attempt: `CommandTimeout` for rows and batches, and `BulkLoadTimeout` for bulk loads.
  A long bulk write isn't cut off by a retry policy's timeout.

Each write strategy retries one unit of work that commits all or nothing, so a retry never inserts rows that an
earlier attempt committed:

- `PerRow`: one `INSERT` per row.
- `Batch`: one multi-row statement per flush.
- `BulkLoad`: one `LOAD DATA LOCAL INFILE` per flush.

Each unit commits all or nothing on a transactional engine such as InnoDB. Non-transactional engines such as MyISAM keep the rows a failed statement wrote, so a retry can insert them again. For such tables, turn retries off with `Resilience.None`.

With `DeliverySemantic.ExactlyOnce`, the sink wraps all writes in one transaction. A failure can abort that whole
transaction, so the writers make one attempt and the sink rolls the transaction back. A batch that fails isn't
written again when the writer is disposed.

To change a setting, derive a policy with a `with` expression:

```csharp
var config = new MySqlConfiguration
{
    Resilience = MySqlConnectorResilience.Default with { Attempts = 6 },
};
```

To turn retries off, use `Resilience.None`.

The connector is the only layer that retries; MySqlConnector doesn't retry commands. Retries aren't logged by the
connector. To observe them, attach a listener: `MySqlConnectorResilience.Default.WithListener(e => ...)`.

## Dependency Injection

```csharp
services.AddMySqlConnector(options =>
{
    options.DefaultConnectionString = "Server=localhost;Database=mydb;User=app;Password=secret;";
    options.DefaultConfiguration = new MySqlConfiguration
    {
        CharacterSet = "utf8mb4",
        WriteStrategy = MySqlWriteStrategy.Batch,
        BatchSize = 500
    };
});

services.AddMySqlConnection("replica", "Server=replica-db;...");
```

## Attribute Mapping

### Convention-Based

C# `PascalCase` maps to MySQL `snake_case` column names:

- `CustomerId` → `customer_id`

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

### `[MySqlColumn]` (Connector-Specific)

```csharp
using NPipeline.Connectors.MySQL.Mapping;

public class Customer
{
    [MySqlColumn("customer_id", PrimaryKey = true)]
    public int CustomerId { get; set; }

    [MySqlColumn("email", DbType = MySqlDbType.VarChar, Size = 255)]
    public string Email { get; set; } = "";
}
```

## Delivery Semantics

| Semantic | Data Loss | Duplicates | Overhead |
|----------|-----------|------------|----------|
| `AtLeastOnce` (default) | No | Possible | Low |
| `AtMostOnce` | Possible | No | Low |
| `ExactlyOnce` | No | No | High |

## Checkpointing

| Strategy | Description |
|----------|-------------|
| `None` (default) | No checkpointing |
| `InMemory` | Transient recovery within single run |
| `Offset` | Track position via monotonic column |
| `KeyBased` | Track by composite keys |
| `Cursor` | Cursor position tracking |

```csharp
var config = new MySqlConfiguration
{
    CheckpointStrategy = CheckpointStrategy.Offset,
    CheckpointOffsetColumn = "id",
    CheckpointStorage = new FileCheckpointStorage("checkpoints/mysql.json")
};
```

## Performance

### Write Strategy Comparison

| Strategy | Throughput | Latency | Use Case |
|----------|-----------|---------|----------|
| `PerRow` | Low | Low | Real-time, per-row errors |
| `Batch` | High | Medium | Most workloads |
| `BulkLoad` | Very High | High | Bulk loads |

### Bulk Load

Requires `AllowLoadLocalInfile = true` both in the connector config and on the MySQL server (`local_infile = ON`).

### Mapping

| Property | Default | Description |
|----------|---------|-------------|
| `CaseInsensitiveMapping` | `true` | Case-insensitive column matching |
| `CacheMappingMetadata` | `true` | Cache mapping delegates per type |
| `ValidateIdentifiers` | `true` | Validate identifiers to prevent injection |
| `UsePreparedStatements` | `true` | Reduce query parsing overhead |

## Best Practices

1. **Use DI** with `AddMySqlConnector` for production
2. **Use `BulkLoad`** for maximum throughput (requires `AllowLoadLocalInfile`)
3. **Enable upsert** with `ON DUPLICATE KEY` for idempotent writes
4. **Set `CharacterSet = "utf8mb4"`** for full Unicode support
5. **Use prepared statements** for repeated query patterns
6. **Configure checkpointing** for long-running pipelines

## Next Steps

- [PostgreSQL Connector](postgres.md) - similar patterns for PostgreSQL
- [SQL Server Connector](sqlserver.md) - similar patterns for SQL Server
- [Dependency Injection](../guides/dependency-injection.md) - full DI integration guide
