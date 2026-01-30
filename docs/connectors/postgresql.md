---
title: PostgreSQL Connector
description: Read from and write to PostgreSQL databases with NPipeline using the PostgreSQL connector.
sidebar_position: 2
---

## PostgreSQL Connector

The `NPipeline.Connectors.PostgreSQL` package provides source and sink nodes backed by [Npgsql](https://www.npgsql.org/). The connector focuses on reliable streaming reads, per-row/batch writes, and in-memory checkpointing for transient recovery.

## Installation

```bash
dotnet add package NPipeline.Connectors.PostgreSQL
```

## Dependency injection

Use `AddPostgresConnector` to register a shared connection pool and factories for creating nodes:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.PostgreSQL.DependencyInjection;

var services = new ServiceCollection()
    .AddPostgresConnector(options =>
    {
        options.DefaultConnectionString = "Host=localhost;Database=npipeline;Username=postgres;Password=postgres";
        options.AddOrUpdateConnection("analytics", "Host=localhost;Database=analytics;Username=postgres;Password=postgres");
    })
    .BuildServiceProvider();

var pool = services.GetRequiredService<IPostgresConnectionPool>();
var sourceFactory = services.GetRequiredService<PostgresSourceNodeFactory>();
var sinkFactory = services.GetRequiredService<PostgresSinkNodeFactory>();
```

- `DefaultConnectionString` is optional if you only use named connections.
- `DefaultConfiguration` controls connection-level settings (timeouts, pool sizing, SSL) applied when the pool builds `NpgsqlDataSource` instances.
- `AddPostgresConnection`/`AddDefaultPostgresConnection` configure the same `PostgresOptions` and do not replace previously configured values.

## Reading with `PostgresSourceNode<T>`

Construct a source with either a connection string or the shared pool:

```csharp
var configuration = new PostgresConfiguration
{
    ConnectionString = "Host=localhost;Database=npipeline;Username=postgres;Password=postgres",
    StreamResults = true,
    FetchSize = 1_000,
    MaxRetryAttempts = 3,
    RetryDelay = TimeSpan.FromSeconds(2),
};

var source = new PostgresSourceNode<Order>(
    connectionString: configuration.ConnectionString,
    query: "SELECT id, total, status FROM orders ORDER BY id",
    configuration: configuration);

// Or reuse a shared pool with a named connection
var pooledSource = new PostgresSourceNode<Order>(pool, "SELECT * FROM orders", connectionName: "analytics");

// Custom mapper (skips the reflection-based mapper)
var mappedSource = new PostgresSourceNode<Order>(
    pool,
    "SELECT id, total, status FROM orders",
    row => new Order(row.Get<int>("id"), row.Get<decimal>("total"), row.Get<string>("status")));
```

### Source configuration highlights

- `StreamResults` + `FetchSize` keep memory usage low when reading large result sets.
- `CaseInsensitiveMapping` and `CacheMappingMetadata` reduce column lookup overhead.
- `MaxRetryAttempts` / `RetryDelay` only retry before the first row is yielded.
- `ValidateIdentifiers` guards against SQL injection when dynamic identifiers are used.
- `ContinueOnError` (configuration) or the `continueOnError` constructor flag can swallow per-property mapping errors when you prefer partial results.

## Writing with `PostgresSinkNode<T>`

The sink supports per-row and batched inserts.

```csharp
var sinkConfig = new PostgresConfiguration
{
    ConnectionString = "Host=localhost;Database=npipeline;Username=postgres;Password=postgres",
    BatchSize = 500,
    MaxBatchSize = 5_000,
    UseTransaction = true,
    WriteStrategy = PostgresWriteStrategy.Batch
};

var sink = new PostgresSinkNode<Order>(pool, "orders", writeStrategy: PostgresWriteStrategy.Batch, configuration: sinkConfig);

// Custom parameter mapper: return values in the same order as mapped columns
Func<Order, IEnumerable<DatabaseParameter>> mapper = order => new[]
{
    new DatabaseParameter("id", order.Id),
    new DatabaseParameter("customer_id", order.CustomerId),
    new DatabaseParameter("total", order.Total)
};

var perRowSink = new PostgresSinkNode<Order>(
    pool,
    tableName: "orders",
    writeStrategy: PostgresWriteStrategy.PerRow,
    parameterMapper: mapper,
    configuration: sinkConfig);
```

### Sink configuration highlights

- `PostgresWriteStrategy.Batch` buffers rows and issues a single multi-value `INSERT`; parameter names now align with generated SQL to avoid binding errors.
- `BatchSize` is clamped by `MaxBatchSize` to prevent runaway buffers.
- `ValidateIdentifiers` validates schema/table/column identifiers before generating SQL.
- Custom mappers must return values in column order; names are ignored and replaced with generated parameter names.

## Checkpointing

In-memory checkpointing helps recover from transient failures during a single process execution:

```csharp
var config = new PostgresConfiguration
{
    ConnectionString = "Host=localhost;Database=npipeline;Username=postgres;Password=postgres",
    CheckpointStrategy = CheckpointStrategy.InMemory
};
```

## Mapping

- Convention-based mapping converts property names to `snake_case` column names.
- `[PostgresColumn("column_name")]` overrides the column name; set `Ignore = true` to skip a property.
- `[PostgresIgnore]` skips a property entirely.
- Mapping metadata is cached per type when `CacheMappingMetadata` is enabled (default).

```csharp
public record Order(
    [PostgresColumn("order_id", PrimaryKey = true)] int Id,
    [PostgresColumn("customer_id")] int CustomerId,
    decimal Total,
    [PostgresIgnore] string? InternalNotes);
```
