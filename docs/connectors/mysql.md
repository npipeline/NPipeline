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

## Source Node — `MySqlSourceNode<T>`

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

## Sink Node — `MySqlSinkNode<T>`

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
| `MaxRetryAttempts` | `int` | `3` | Retry attempts |
| `RetryDelay` | `TimeSpan` | — | Delay between retries |

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

- [PostgreSQL Connector](postgres.md) — similar patterns for PostgreSQL
- [SQL Server Connector](sqlserver.md) — similar patterns for SQL Server
- [Dependency Injection](../guides/dependency-injection.md) — full DI integration guide
