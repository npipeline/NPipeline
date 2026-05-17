---
title: "Snowflake Connector"
description: "Read from and write to Snowflake with batch inserts, staged COPY, and MERGE upserts."
order: 14
---

# Snowflake Connector

The `NPipeline.Connectors.Snowflake` package provides source and sink nodes for [Snowflake](https://www.snowflake.com/). Supports parameterized queries, batch inserts, high-performance staged `COPY`, `MERGE` upserts, and named connections for multi-warehouse scenarios.

## Installation

```bash
dotnet add package NPipeline.Connectors.Snowflake
```

**Dependencies:** [Snowflake.Data](https://www.nuget.org/packages/Snowflake.Data) 5.x

## Source Node - `SnowflakeSourceNode<T>`

### Constructors

```csharp
// Connection string + query
public SnowflakeSourceNode(
    string connectionString, string query,
    SnowflakeConfiguration? configuration = null)

// With custom mapper
public SnowflakeSourceNode(
    string connectionString, string query,
    Func<SnowflakeRow, T>? customMapper = null,
    SnowflakeConfiguration? configuration = null)

// Connection pool (recommended for DI)
public SnowflakeSourceNode(
    ISnowflakeConnectionPool connectionPool, string query,
    SnowflakeConfiguration? configuration = null,
    DatabaseParameter[]? parameters = null,
    bool continueOnError = false,
    string? connectionName = null)
```

### Example

```csharp
var source = new SnowflakeSourceNode<SalesRecord>(
    "account=myaccount;user=app;password=secret;warehouse=COMPUTE_WH;database=SALES;schema=PUBLIC;",
    "SELECT * FROM orders WHERE order_date >= :start_date",
    configuration: new SnowflakeConfiguration { FetchSize = 10000 });
```

## Sink Node - `SnowflakeSinkNode<T>`

| Strategy | Description | Best For |
|----------|-------------|----------|
| `PerRow` | Individual `INSERT` per item | Small volumes |
| `Batch` (default) | Batched `INSERT` statements | Most workloads |
| `StagedCopy` | `PUT` + `COPY INTO` via internal stage | Maximum throughput |

### Constructors

```csharp
// Connection string
public SnowflakeSinkNode(
    string connectionString, string tableName,
    SnowflakeConfiguration? configuration = null,
    Func<T, IEnumerable<DatabaseParameter>>? customMapper = null)

// Connection pool (recommended for DI)
public SnowflakeSinkNode(
    ISnowflakeConnectionPool connectionPool, string tableName,
    SnowflakeConfiguration? configuration = null,
    Func<T, IEnumerable<DatabaseParameter>>? customMapper = null,
    string? connectionName = null)
```

### Example: Staged Copy

```csharp
var config = new SnowflakeConfiguration
{
    ConnectionString = "account=myaccount;user=app;...",
    WriteStrategy = SnowflakeWriteStrategy.StagedCopy,
    StageName = "@my_stage"
};

var sink = new SnowflakeSinkNode<SalesRecord>(
    config.ConnectionString, "SALES.PUBLIC.ORDERS",
    configuration: config);
```

## Configuration

### Connection

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ConnectionString` | `string` | `""` | Snowflake connection string |
| `Account` | `string` | `""` | Account identifier |
| `User` | `string` | `""` | Username |
| `Role` | `string` | `""` | Role |
| `Warehouse` | `string` | `""` | Warehouse |
| `Database` | `string` | `""` | Database |
| `Schema` | `string` | `"PUBLIC"` | Schema |
| `Authenticator` | `string` | `"snowflake"` | Auth type (`snowflake`, `externalbrowser`, `snowflake_jwt`) |
| `PrivateKeyPath` | `string?` | `null` | Path to private key file (key pair auth) |
| `CommandTimeout` | `int` | `300` | Command timeout (seconds) |
| `ConnectionTimeout` | `int` | `30` | Connection timeout (seconds) |
| `MinPoolSize` | `int` | `1` | Minimum pool size |
| `MaxPoolSize` | `int` | `10` | Maximum pool size |

### Read

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `StreamResults` | `bool` | `true` | Stream results |
| `FetchSize` | `int` | `10000` | Rows per fetch |

### Write

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `WriteStrategy` | `SnowflakeWriteStrategy` | `Batch` | `PerRow`, `Batch`, or `StagedCopy` |
| `BatchSize` | `int` | `1000` | Items per batch |
| `MaxBatchSize` | `int` | `16384` | Max batch size (Snowflake limit) |
| `UseTransaction` | `bool` | `true` | Wrap writes in a transaction |
| `StageName` | `string?` | `null` | Stage name for `StagedCopy` |

### Upsert (MERGE)

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `UseUpsert` | `bool` | `false` | Enable `MERGE` |
| `UpsertKeyColumns` | `string[]?` | `null` | Key columns for MERGE matching |
| `OnMergeAction` | `OnMergeAction` | - | `Update`, `Ignore`, or `Delete` |

### Error Handling

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ContinueOnError` | `bool` | `false` | Continue on errors |
| `MaxRetryAttempts` | `int` | `3` | Retry attempts |
| `RetryDelay` | `TimeSpan` | - | Delay between retries |

## Dependency Injection

```csharp
services.AddSnowflakeConnector(options =>
{
    options.DefaultConnectionString = "account=myaccount;user=app;...";
    options.DefaultConfiguration = new SnowflakeConfiguration
    {
        Warehouse = "COMPUTE_WH",
        WriteStrategy = SnowflakeWriteStrategy.Batch
    };
});

services.AddSnowflakeConnection("etl", "account=myaccount;warehouse=ETL_WH;...");
```

## Attribute Mapping

### `[Column]` / `[IgnoreColumn]` (Cross-Connector)

```csharp
using NPipeline.Connectors.Attributes;

public class SalesRecord
{
    [Column("ORDER_ID")]
    public int OrderId { get; set; }

    [IgnoreColumn]
    public decimal CalculatedTotal => Quantity * UnitPrice;
}
```

### `[SnowflakeColumn]` (Connector-Specific)

```csharp
using NPipeline.Connectors.Snowflake.Mapping;

[SnowflakeTable("ORDERS", Schema = "SALES")]
public class SalesRecord
{
    [SnowflakeColumn("ORDER_ID", PrimaryKey = true)]
    public int OrderId { get; set; }

    [SnowflakeColumn("AMOUNT", DbType = "NUMBER(10,2)")]
    public decimal Amount { get; set; }
}
```

| Property | Description |
|----------|-------------|
| `Name` | Column name |
| `DbType` | Snowflake data type string |
| `PrimaryKey` | Used for checkpointing |
| `Ignore` | Skip mapping |

### Convention-Based

C# `PascalCase` maps to Snowflake `UPPER_SNAKE_CASE` by default.

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
| `InMemory` | Transient recovery within a single run |
| `Offset` | Persist position via monotonic column |
| `KeyBased` | Track processed items by composite keys |
| `Cursor` | Cursor position tracking |

```csharp
var config = new SnowflakeConfiguration
{
    CheckpointStrategy = CheckpointStrategy.Offset,
    CheckpointOffsetColumn = "ORDER_ID",
    CheckpointStorage = new FileCheckpointStorage("checkpoints/snowflake.json")
};
```

## Performance

### Write Strategy Comparison

| Strategy | Throughput | Latency | Use Case |
|----------|-----------|---------|----------|
| `PerRow` | Low | Low | Small volumes, debugging |
| `Batch` | Medium | Medium | Most workloads |
| `StagedCopy` | Very High | High | Bulk loads |

### Snowflake-Specific Considerations

- **Connection latency**: Snowflake connections take 2–5s to establish - use connection pooling
- **Identifiers**: Snowflake defaults to uppercase - use `[SnowflakeColumn]` or convention mapping
- **Query tagging**: Set `ApplicationName` for tracking queries in Snowflake history
- **Warehouse sizing**: Match warehouse size to pipeline throughput needs
- **Max batch size**: Snowflake limits multi-value INSERT to 16,384 rows

## Best Practices

1. **Use StagedCopy** for bulk loads - `PUT` + `COPY INTO` is significantly faster
2. **Use connection pooling** - connection establishment is slow
3. **Set `MaxBatchSize = 16384`** - Snowflake's row limit per INSERT
4. **Enable upsert** with `MERGE` for idempotent loads
5. **Use key pair authentication** for service accounts (`Authenticator = "snowflake_jwt"`)
6. **Size your warehouse** appropriately for the load

## Next Steps

- [DuckDB Connector](duckdb.md) - local analytical queries
- [Parquet Connector](parquet.md) - Snowflake-compatible columnar format
