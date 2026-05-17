---
title: "SQL Server Connector"
description: "Read from and write to SQL Server databases with bulk copy, batch inserts, and MERGE upserts."
order: 9
---

# SQL Server Connector

The `NPipeline.Connectors.SqlServer` package provides source and sink nodes for SQL Server. Supports connection pooling, parameterized queries, batch inserts, high-performance `SqlBulkCopy`, and `MERGE` upserts.

## Installation

```bash
dotnet add package NPipeline.Connectors.SqlServer
```

**Dependencies:** [Microsoft.Data.SqlClient](https://www.nuget.org/packages/Microsoft.Data.SqlClient) 7.x

## Source Node - `SqlServerSourceNode<T>`

### Constructors

```csharp
// Connection string + query
public SqlServerSourceNode(
    string connectionString, string query,
    SqlServerConfiguration? configuration = null)

// With custom mapper
public SqlServerSourceNode(
    string connectionString, string query,
    Func<SqlServerRow, T>? customMapper = null,
    SqlServerConfiguration? configuration = null)

// Connection pool (recommended for DI)
public SqlServerSourceNode(
    ISqlServerConnectionPool connectionPool, string query,
    SqlServerConfiguration? configuration = null,
    DatabaseParameter[]? parameters = null,
    bool continueOnError = false,
    string? connectionName = null)
```

### Example

```csharp
var source = new SqlServerSourceNode<Order>(
    "Server=localhost;Database=Sales;Trusted_Connection=true;",
    "SELECT Id, Customer, Amount FROM dbo.Orders WHERE Status = @status",
    configuration: new SqlServerConfiguration
    {
        StreamResults = true,
        FetchSize = 5000
    });
```

## Sink Node - `SqlServerSinkNode<T>`

| Strategy | Description | Best For |
|----------|-------------|----------|
| `PerRow` | Individual `INSERT` per item | Small volumes |
| `Batch` (default) | Batched `INSERT` statements | Most workloads |
| `BulkCopy` | `SqlBulkCopy` | Maximum throughput |

### Constructors

```csharp
// Connection string
public SqlServerSinkNode(
    string connectionString, string tableName,
    SqlServerConfiguration? configuration = null,
    Func<T, IEnumerable<DatabaseParameter>>? customMapper = null)

// Connection pool (recommended for DI)
public SqlServerSinkNode(
    ISqlServerConnectionPool connectionPool, string tableName,
    SqlServerConfiguration? configuration = null,
    Func<T, IEnumerable<DatabaseParameter>>? customMapper = null,
    string? connectionName = null)
```

### Example: Bulk Copy

```csharp
var config = new SqlServerConfiguration
{
    ConnectionString = "Server=localhost;Database=Sales;...",
    WriteStrategy = SqlServerWriteStrategy.BulkCopy,
    BulkCopyBatchSize = 5000,
    EnableStreaming = true
};

var sink = new SqlServerSinkNode<Order>("connection-string", "dbo.Orders", configuration: config);
```

## Configuration

### Connection

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ConnectionString` | `string` | `""` | SQL Server connection string |
| `Schema` | `string` | `"dbo"` | Default schema |
| `CommandTimeout` | `int` | `30` | Command timeout (seconds) |
| `ConnectionTimeout` | `int` | `15` | Connection timeout (seconds) |
| `MinPoolSize` | `int` | `1` | Minimum connection pool size |
| `MaxPoolSize` | `int` | `100` | Maximum connection pool size |

### Write

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `WriteStrategy` | `SqlServerWriteStrategy` | `Batch` | `PerRow`, `Batch`, or `BulkCopy` |
| `BatchSize` | `int` | `100` | Items per batch |
| `MaxBatchSize` | `int` | `1000` | Maximum batch size |
| `UseTransaction` | `bool` | `true` | Wrap writes in a transaction |
| `UsePreparedStatements` | `bool` | `true` | Use prepared statements |

### Bulk Copy

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `BulkCopyBatchSize` | `int` | `5000` | Rows per bulk copy batch |
| `BulkCopyTimeout` | `int` | `300` | Bulk copy timeout (seconds) |
| `BulkCopyNotifyAfter` | `int` | `1000` | Progress notification interval (rows) |
| `EnableStreaming` | `bool` | `true` | Stream bulk copy data |

### Upsert (MERGE)

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `UseUpsert` | `bool` | `false` | Enable `MERGE` upserts |
| `UpsertKeyColumns` | `string[]?` | `null` | Key columns for MERGE matching |
| `OnMergeAction` | `OnMergeAction` | `Update` | `Update`, `Ignore`, or `Delete` |

### Error Handling

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ContinueOnError` | `bool` | `false` | Continue on row-level errors |
| `MaxRetryAttempts` | `int` | `3` | Retry attempts for transient errors |
| `RetryDelay` | `TimeSpan` | - | Delay between retries |

## Dependency Injection

```csharp
services.AddSqlServerConnector(options =>
{
    options.DefaultConnectionString = "Server=localhost;Database=Sales;...";
    options.DefaultConfiguration = new SqlServerConfiguration
    {
        WriteStrategy = SqlServerWriteStrategy.BulkCopy,
        BulkCopyBatchSize = 5000
    };
});

// Named connections for multi-database scenarios
services.AddSqlServerConnection("reporting", "Server=reporting-db;...");
services.AddSqlServerConnection("warehouse", "Server=warehouse-db;...");
```

Registers `ISqlServerConnectionPool`, `SqlServerSourceNodeFactory`, and `SqlServerSinkNodeFactory`.

## Attribute Mapping

### Convention-Based

C# `PascalCase` property names map directly to SQL Server `PascalCase` column names (no conversion).

### `[Column]` / `[IgnoreColumn]` (Cross-Connector)

```csharp
using NPipeline.Connectors.Attributes;

public class Customer
{
    [Column("CustomerID")]
    public int CustomerId { get; set; }

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}
```

### `[SqlServerColumn]` (Connector-Specific)

Extends `[Column]` with SQL Server features:

```csharp
using NPipeline.Connectors.SqlServer.Mapping;

public class Customer
{
    [SqlServerColumn("CustomerID", PrimaryKey = true, Identity = true)]
    public int CustomerId { get; set; }

    [SqlServerColumn("FirstName", DbType = SqlDbType.NVarChar, Size = 100)]
    public string FirstName { get; set; } = "";

    [SqlServerColumn("Email", DbType = SqlDbType.NVarChar, Size = 255)]
    public string Email { get; set; } = "";
}
```

| Property | Description |
|----------|-------------|
| `Name` | Column name in the database |
| `DbType` | SQL Server data type (`SqlDbType`) |
| `Size` | Size/length for character and numeric types |
| `PrimaryKey` | Primary key (used for checkpointing) |
| `Identity` | Auto-increment identity column |
| `Ignore` | Skip mapping this property |

Use common attributes for portable code; use `[SqlServerColumn]` when you need type, PK, or identity control.

## Delivery Semantics

| Semantic | Data Loss | Duplicates | Overhead | Use Case |
|----------|-----------|------------|----------|----------|
| `AtLeastOnce` (default) | No | Possible | Low | Idempotent operations |
| `AtMostOnce` | Possible | No | Low | Telemetry, metrics |
| `ExactlyOnce` | No | No | High | Financial transactions |

```csharp
var config = new SqlServerConfiguration
{
    DeliverySemantic = DeliverySemantic.ExactlyOnce,
    UseTransaction = true,
    CheckpointStrategy = CheckpointStrategy.Offset,
    CheckpointStorage = new FileCheckpointStorage("checkpoints.json")
};
```

## Checkpointing

Checkpointing enables pipelines to resume from where they left off after a failure.

| Strategy | Persistence | Description |
|----------|-------------|-------------|
| `None` (default) | - | No checkpointing; restart from beginning on failure |
| `InMemory` | Process lifetime | Recover from transient failures within a single run |
| `Offset` | External storage | Track position via monotonically increasing column |
| `KeyBased` | External storage | Track processed items by composite keys |
| `Cursor` | External storage | Track cursor position for iteration |
| `CDC` | External storage | Track LSN for SQL Server Change Data Capture |

### Offset Example

```csharp
var config = new SqlServerConfiguration
{
    CheckpointStrategy = CheckpointStrategy.Offset,
    CheckpointOffsetColumn = "OrderId",
    CheckpointStorage = new FileCheckpointStorage("checkpoints/orders.json")
};

var source = new SqlServerSourceNode<Order>(connectionString,
    "SELECT * FROM Orders WHERE OrderId > @lastCheckpoint ORDER BY OrderId",
    configuration: config);
```

### CDC Example

```csharp
var config = new SqlServerConfiguration
{
    CheckpointStrategy = CheckpointStrategy.CDC,
    CdcCaptureInstance = "dbo_orders",
    CheckpointStorage = new FileCheckpointStorage("checkpoints/cdc.json")
};
```

Requires CDC enabled on the database and table:

```sql
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'orders', @role_name = NULL;
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
| `CaseInsensitiveMapping` | `true` | Match `OrderId`, `orderid`, `ORDERID` to same property |
| `CacheMappingMetadata` | `true` | Cache mapping delegates per type (avoid repeated reflection) |
| `ValidateIdentifiers` | `true` | Validate SQL identifiers to prevent injection |

## Performance

### Streaming

```csharp
var config = new SqlServerConfiguration { StreamResults = true, FetchSize = 1_000 };
```

Without streaming, the entire result set is loaded into memory. With streaming, rows are fetched in batches of `FetchSize`.

| FetchSize | Best For |
|-----------|----------|
| 100–500 | Memory-constrained, wide rows |
| 1,000–5,000 | Most workloads |
| 5,000–10,000 | Maximum throughput, high-bandwidth |

### Write Strategy Comparison

| Strategy | Throughput | Latency | Error Isolation | Use Case |
|----------|-----------|---------|-----------------|----------|
| `PerRow` | Low | Low | High | Real-time, per-row errors |
| `Batch` | High | Medium | Medium | ETL, balanced |
| `BulkCopy` | Very High | High | Low | Bulk loads, data warehouse |

### Batch Size Guidelines

| Range | Best For |
|-------|----------|
| 100–500 | Real-time processing, low latency |
| 500–1,000 | Balanced throughput and latency |
| 1,000–5,000 | Bulk loading |

**Note:** Effective batch size is capped by SQL Server's 2,100 parameter limit divided by the number of mapped columns.

### Prepared Statements

Enabled by default (`UsePreparedStatements = true`). Reduces query parsing overhead by 10–30% for repeated inserts.

## Row-Level Error Handling

```csharp
var config = new SqlServerConfiguration
{
    RowErrorHandler = (exception, row) =>
    {
        Console.WriteLine($"Error on row {row?.Get<int>("OrderId")}: {exception.Message}");
        return exception is FormatException; // true = skip row, false = re-throw
    }
};
```

## Best Practices

1. **Use DI** with `AddSqlServerConnector` for production - centralizes connection management
2. **Enable streaming** (`StreamResults = true`) for large result sets
3. **Use BulkCopy** for bulk loading - significantly faster than Batch
4. **Enable upsert** for idempotent writes to avoid duplicate handling
5. **Validate identifiers** - never disable `ValidateIdentifiers` in production
6. **Use prepared statements** for repeated query patterns
7. **Configure checkpointing** for long-running pipelines
8. **Tune batch size** based on latency/throughput requirements
9. **Set `ApplicationName`** for monitoring in SQL Server Activity Monitor

## Next Steps

- [PostgreSQL Connector](postgres.md) - similar patterns for PostgreSQL
- [MySQL Connector](mysql.md) - similar patterns for MySQL/MariaDB
- [Dependency Injection](../guides/dependency-injection.md) - full DI integration guide
