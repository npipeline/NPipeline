---
title: PostgreSQL Connector
description: Read from and write to PostgreSQL databases with NPipeline using the PostgreSQL connector.
sidebar_position: 3
---

## PostgreSQL Connector

The `NPipeline.Connectors.PostgreSQL` package provides specialized source and sink nodes for working with PostgreSQL databases. This allows you to easily integrate PostgreSQL data into your pipelines as an input source or an output destination.

This connector uses the [Npgsql](https://www.npgsql.org/) library under the hood, providing reliable streaming reads, per-row and batched writes, and in-memory checkpointing for transient recovery.

## Installation

To use the PostgreSQL connector, install the `NPipeline.Connectors.PostgreSQL` NuGet package:

```bash
dotnet add package NPipeline.Connectors.PostgreSQL
```

For the core NPipeline package and other available extensions, see the [Installation Guide](../getting-started/installation.md).

## Dependency Injection

The PostgreSQL connector supports dependency injection for managing connection pools and node factories. This is the recommended approach for production applications.

### Registering the Connector

Use `AddPostgresConnector` to register a shared connection pool and factories for creating nodes:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.PostgreSQL.DependencyInjection;

var services = new ServiceCollection()
    .AddPostgresConnector(options =>
    {
        // Set a default connection string (optional if using only named connections)
        options.DefaultConnectionString = "Host=localhost;Database=npipeline;Username=postgres;Password=postgres";

        // Add named connections for different databases
        options.AddOrUpdateConnection("analytics", "Host=localhost;Database=analytics;Username=postgres;Password=postgres");
        options.AddOrUpdateConnection("warehouse", "Host=localhost;Database=warehouse;Username=postgres;Password=postgres");

        // Configure default connection-level settings
        options.DefaultConfiguration = new PostgresConfiguration
        {
            StreamResults = true,
            FetchSize = 1_000,
            MaxRetryAttempts = 3,
            RetryDelay = TimeSpan.FromSeconds(2)
        };
    })
    .BuildServiceProvider();

// Resolve services from the container
var pool = services.GetRequiredService<IPostgresConnectionPool>();
var sourceFactory = services.GetRequiredService<PostgresSourceNodeFactory>();
var sinkFactory = services.GetRequiredService<PostgresSinkNodeFactory>();
```

### Configuration Options

- **`DefaultConnectionString`**: Optional connection string used when no named connection is specified. Can be omitted if you only use named connections.
- **`DefaultConfiguration`**: Controls connection-level settings (timeouts, pool sizing, SSL) applied when the pool builds `NpgsqlDataSource` instances.
- **`AddOrUpdateConnection(name, connectionString)`**: Adds or updates a named connection. Multiple connections can be configured for different databases.
- **`AddPostgresConnection`/`AddDefaultPostgresConnection`**: Configure the same `PostgresOptions` and do not replace previously configured values.

### Why Use Dependency Injection?

Using dependency injection provides several benefits:

- **Connection Pool Management**: The shared connection pool efficiently manages database connections across multiple nodes
- **Configuration Centralization**: All PostgreSQL connections are configured in one place
- **Testability**: Easy to mock or replace dependencies in unit tests
- **Lifetime Management**: Services are properly disposed when the application shuts down

## Common Attributes

The PostgreSQL connector supports common attributes from `NPipeline.Connectors.Attributes` that work across all connectors, as well as PostgreSQL-specific attributes that extend the common attributes with database-specific features.

### `[Column]` Attribute

The `[Column]` attribute (from `NPipeline.Connectors.Attributes`) is a common attribute that allows you to specify column names and control property mapping across all connectors. It provides:

- **`Name`**: The column name in the database
- **`Ignore`**: When `true`, skips mapping this property

This attribute is recommended for simple scenarios where you only need basic column mapping.

```csharp
using NPipeline.Connectors.Attributes;

public class Customer
{
    [Column("customer_id")]
    public int CustomerId { get; set; }

    [Column("first_name")]
    public string FirstName { get; set; } = string.Empty;

    [Column("last_name")]
    public string LastName { get; set; } = string.Empty;

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}
```

### `[IgnoreColumn]` Attribute

The `[IgnoreColumn]` attribute (from `NPipeline.Connectors.Attributes`) is a marker attribute that excludes a property from mapping entirely. This is useful for computed properties or fields that should not be persisted.

```csharp
using NPipeline.Connectors.Attributes;

public class Order
{
    public int OrderId { get; set; }
    public decimal Subtotal { get; set; }
    public decimal TaxAmount { get; set; }
    public decimal ShippingAmount { get; set; }

    [IgnoreColumn]
    public decimal TotalAmount => Subtotal + TaxAmount + ShippingAmount;

    [IgnoreColumn]
    public bool IsTaxable => TaxAmount > 0;
}
```

### PostgreSQL-Specific Attributes

The PostgreSQL connector provides `[PostgresColumn]` attribute that extends the common attributes with database-specific functionality:

- **`[PostgresColumn]`**: Extends `[Column]` with additional properties:
  - **`DbType`**: Specifies the PostgreSQL data type for the column
  - **`Size`**: Sets the size/length for character and numeric types
  - **`PrimaryKey`**: Indicates whether the column is a primary key (used for checkpointing)

The `[IgnoreColumn]` attribute from `NPipeline.Connectors.Attributes` covers all ignore requirements and works identically to a PostgreSQL-specific ignore attribute.

These attributes are useful when you need database-specific features like type specification or primary key marking.

```csharp
using NPipeline.Connectors.PostgreSQL.Mapping;
using NPipeline.Connectors.Attributes;
using NpgsqlTypes;

public class Customer
{
    [PostgresColumn("customer_id", PrimaryKey = true)]
    public int CustomerId { get; set; }

    [PostgresColumn("first_name", DbType = NpgsqlDbType.Varchar, Size = 100)]
    public string FirstName { get; set; } = string.Empty;

    [PostgresColumn("last_name", DbType = NpgsqlDbType.Varchar, Size = 100)]
    public string LastName { get; set; } = string.Empty;

    [PostgresColumn("email", DbType = NpgsqlDbType.Varchar, Size = 255)]
    public string Email { get; set; } = string.Empty;

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}
```

### Choosing Between Common and PostgreSQL-Specific Attributes

**Use common attributes (`[Column]`, `[IgnoreColumn]`) when:**

- You want code that works across multiple connectors
- You only need basic column mapping functionality
- You prefer using standard attributes provided by the core library
- Your database schema follows standard conventions

**Use PostgreSQL-specific attributes (`[PostgresColumn]`) when:**

- You need to specify database types explicitly (e.g., `VARCHAR(255)`, `NUMERIC(10,2)`)
- You need to mark primary key columns for checkpointing
- You want to leverage PostgreSQL-specific features
- You're maintaining existing code that already uses these attributes

Both attribute types are fully supported and will continue to work in future versions. The common attributes are recommended for new code when you don't need database-specific features, while PostgreSQL-specific attributes provide additional control when needed.

## `PostgresSourceNode<T>`

The `PostgresSourceNode<T>` reads data from a PostgreSQL database and emits each row as an item of type `T`.

### Source Configuration

The constructor for `PostgresSourceNode<T>` provides multiple overloads for flexibility:

```csharp
// Using connection string
public PostgresSourceNode<T>(
    string connectionString,
    string query,
    PostgresConfiguration? configuration = null)

// Using connection pool with named connection
public PostgresSourceNode<T>(
    IPostgresConnectionPool pool,
    string query,
    string? connectionName = null,
    PostgresConfiguration? configuration = null)

// Using connection pool with custom row mapper
public PostgresSourceNode<T>(
    IPostgresConnectionPool pool,
    string query,
    Func<NpgsqlDataReader, T> rowMapper,
    string? connectionName = null,
    PostgresConfiguration? configuration = null)
```

- **`connectionString`**: PostgreSQL connection string (e.g., `"Host=localhost;Database=mydb;Username=postgres;Password=postgres"`)
- **`pool`**: Shared connection pool from dependency injection
- **`query`**: SQL query to execute
- **`rowMapper`**: Custom function to map a data reader row to type `T`. When omitted, uses convention-based mapping
- **`connectionName`**: Name of a configured connection from the pool
- **`configuration`**: Optional configuration object for customizing behavior

> **Tip:** When you need to provide custom configuration or a row mapper, instantiate `PostgresSourceNode<T>` yourself and register it via `builder.AddSource(new PostgresSourceNode<Order>(...), "postgres_source")`. The builder handles registration and disposal for you automatically.

### Example: Reading from PostgreSQL

Let's assume you have a PostgreSQL table named `orders` with the following structure:

| id | customer_id | total | status |
| --- | --- | --- | --- |
| 1 | 100 | 150.00 | completed |
| 2 | 101 | 75.50 | pending |
| 3 | 102 | 200.00 | shipped |

And a corresponding C# record:

```csharp
public sealed record Order(int Id, int CustomerId, decimal Total, string Status);
```

You can read this data into your pipeline as follows:

```csharp
using NPipeline;
using NPipeline.Connectors.PostgreSQL;
using NPipeline.DataFlow.DataPipes;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Tracing;

public sealed record Order(int Id, int CustomerId, decimal Total, string Status);

public sealed class PostgresReaderPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Create the PostgreSQL source node
        var sourceNode = new PostgresSourceNode<Order>(
            connectionString: "Host=localhost;Database=npipeline;Username=postgres;Password=postgres",
            query: "SELECT id, customer_id, total, status FROM orders ORDER BY id",
            configuration: new PostgresConfiguration
            {
                StreamResults = true,
                FetchSize = 1_000
            });
        var source = builder.AddSource(sourceNode, "postgres_source");
        var sink = builder.AddSink<ConsoleSinkNode, Order>("console_sink");

        builder.Connect(source, sink);
    }
}

public sealed class ConsoleSinkNode : SinkNode<Order>
{
    public override async Task ExecuteAsync(
        IDataPipe<Order> input,
        PipelineContext context,
        IPipelineActivity parentActivity,
        CancellationToken cancellationToken)
    {
        await foreach (var order in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"Received: {order}");
        }
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        // Create and run the pipeline
        var runner = PipelineRunner.Create();
        await runner.RunAsync<PostgresReaderPipeline>();

        Console.WriteLine("PostgreSQL reading completed");
    }
}
```

**Expected Output:**

```text
Received: Order { Id = 1, CustomerId = 100, Total = 150.00, Status = completed }
Received: Order { Id = 2, CustomerId = 101, Total = 75.50, Status = pending }
Received: Order { Id = 3, CustomerId = 102, Total = 200.00, Status = shipped }
PostgreSQL reading completed
```

### Example: Using a Connection Pool

When using dependency injection, you can leverage the shared connection pool:

```csharp
public sealed class PooledPostgresReaderPipeline : IPipelineDefinition
{
    private readonly IPostgresConnectionPool _pool;

    public PooledPostgresReaderPipeline(IPostgresConnectionPool pool)
    {
        _pool = pool;
    }

    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Use a named connection from the pool
        var sourceNode = new PostgresSourceNode<Order>(
            pool: _pool,
            query: "SELECT id, customer_id, total, status FROM orders ORDER BY id",
            connectionName: "analytics");
        var source = builder.AddSource(sourceNode, "postgres_source");
        var sink = builder.AddSink<ConsoleSinkNode, Order>("console_sink");

        builder.Connect(source, sink);
    }
}
```

### Example: Using a Custom Row Mapper

For complete control over mapping, provide a custom row mapper function:

```csharp
public sealed record Order(
    int Id,
    int CustomerId,
    decimal Total,
    string Status,
    DateTime CreatedAt);

public sealed class CustomMapperPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceNode = new PostgresSourceNode<Order>(
            connectionString: "Host=localhost;Database=npipeline;Username=postgres;Password=postgres",
            query: "SELECT id, customer_id, total, status, created_at FROM orders",
            rowMapper: row => new Order(
                row.GetInt32(row.GetOrdinal("id")),
                row.GetInt32(row.GetOrdinal("customer_id")),
                row.GetDecimal(row.GetOrdinal("total")),
                row.GetString(row.GetOrdinal("status")),
                row.GetDateTime(row.GetOrdinal("created_at"))));
        var source = builder.AddSource(sourceNode, "postgres_source");
        var sink = builder.AddSink<ConsoleSinkNode, Order>("console_sink");

        builder.Connect(source, sink);
    }
}
```

## `PostgresSinkNode<T>`

The `PostgresSinkNode<T>` writes items from the pipeline to a PostgreSQL database table.

### Sink Configuration

The constructor for `PostgresSinkNode<T>` provides multiple overloads:

```csharp
// Using connection string
public PostgresSinkNode<T>(
    string connectionString,
    string tableName,
    PostgresWriteStrategy writeStrategy,
    PostgresConfiguration? configuration = null)

// Using connection pool with named connection
public PostgresSinkNode<T>(
    IPostgresConnectionPool pool,
    string tableName,
    PostgresWriteStrategy writeStrategy,
    string? connectionName = null,
    PostgresConfiguration? configuration = null)

// Using connection pool with custom parameter mapper
public PostgresSinkNode<T>(
    IPostgresConnectionPool pool,
    string tableName,
    PostgresWriteStrategy writeStrategy,
    Func<T, IEnumerable<DatabaseParameter>> parameterMapper,
    string? connectionName = null,
    PostgresConfiguration? configuration = null)
```

- **`connectionString`**: PostgreSQL connection string
- **`pool`**: Shared connection pool from dependency injection
- **`tableName`**: Name of the target table
- **`writeStrategy`**: Strategy for writing data (`PerRow` or `Batch`)
- **`parameterMapper`**: Custom function to map type `T` to database parameters
- **`connectionName`**: Name of a configured connection from the pool
- **`configuration`**: Optional configuration object for customizing behavior

> **Tip:** When you need to pass a custom parameter mapper or configuration, instantiate `PostgresSinkNode<T>` yourself and register it via `builder.AddSink(new PostgresSinkNode<Order>(...), "postgres_sink")`. The builder handles registration and disposal for you automatically.

### Write Strategies

The connector supports two write strategies:

#### PerRow Strategy

Writes each row individually with a separate `INSERT` statement. This provides:

- Immediate visibility of each row
- Better error isolation (one failed insert doesn't affect others)
- Higher overhead for large datasets

#### Batch Strategy

Buffers multiple rows and issues a single multi-value `INSERT` statement. This provides:

- Better performance for large datasets
- Reduced database round-trips
- All-or-nothing semantics within a batch

### Example: Writing to PostgreSQL

Let's take processed order data and write it to an `order_summary` table:

```csharp
using NPipeline.Connectors.PostgreSQL;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed record OrderSummary(int Id, string CustomerName, decimal Total, string Status);

public sealed class PostgresWriterPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<InMemorySourceNode<OrderSummary>, OrderSummary>("source");

        // Create the PostgreSQL sink node with batch strategy
        var sinkNode = new PostgresSinkNode<OrderSummary>(
            connectionString: "Host=localhost;Database=npipeline;Username=postgres;Password=postgres",
            tableName: "order_summary",
            writeStrategy: PostgresWriteStrategy.Batch,
            configuration: new PostgresConfiguration
            {
                BatchSize = 1_000,
                MaxBatchSize = 5_000,
                UseTransaction = true
            });
        var sink = builder.AddSink(sinkNode, "postgres_sink");

        builder.Connect(source, sink);
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        var orders = new List<OrderSummary>
        {
            new(1, "Alice Smith", 150.00m, "completed"),
            new(2, "Bob Johnson", 75.50m, "pending"),
            new(3, "Carol Williams", 200.00m, "shipped")
        };

        // Set up test data
        var context = PipelineContext.Default;
        context.Items[typeof(InMemorySourceNode<OrderSummary>).FullName!] = orders.ToArray();

        var runner = PipelineRunner.Create();
        await runner.RunAsync<PostgresWriterPipeline>(context);

        Console.WriteLine("PostgreSQL write completed");
    }
}
```

**Expected Database Content:**

| id | customer_name | total | status |
| --- | --- | --- | --- |
| 1 | Alice Smith | 150.00 | completed |
| 2 | Bob Johnson | 75.50 | pending |
| 3 | Carol Williams | 200.00 | shipped |

### Example: Using a Custom Parameter Mapper

For complete control over parameter mapping, provide a custom parameter mapper function:

```csharp
public sealed record Order(int Id, int CustomerId, decimal Total, string Status);

public sealed class CustomMapperWriterPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<InMemorySourceNode<Order>, Order>("source");

        // Custom parameter mapper: return values in the same order as mapped columns
        Func<Order, IEnumerable<DatabaseParameter>> mapper = order => new[]
        {
            new DatabaseParameter("id", order.Id),
            new DatabaseParameter("customer_id", order.CustomerId),
            new DatabaseParameter("total", order.Total),
            new DatabaseParameter("status", order.Status)
        };

        var sinkNode = new PostgresSinkNode<Order>(
            connectionString: "Host=localhost;Database=npipeline;Username=postgres;Password=postgres",
            tableName: "orders",
            writeStrategy: PostgresWriteStrategy.PerRow,
            parameterMapper: mapper,
            configuration: new PostgresConfiguration { UseTransaction = true });
        var sink = builder.AddSink(sinkNode, "postgres_sink");

        builder.Connect(source, sink);
    }
}
```

## Configuration Reference

### PostgresConfiguration

The `PostgresConfiguration` class provides comprehensive options for configuring PostgreSQL read and write operations.

#### Properties

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `ConnectionString` | `string?` | `null` | PostgreSQL connection string. Not required when using a connection pool. |
| `StreamResults` | `bool` | `false` | Enables streaming of results to reduce memory usage for large result sets. |
| `FetchSize` | `int` | `100` | Number of rows to fetch per round-trip when streaming. Larger values reduce round-trips but use more memory. |
| `MaxRetryAttempts` | `int` | `0` | Maximum number of retry attempts for transient failures. Only applies before the first row is yielded. |
| `RetryDelay` | `TimeSpan` | `TimeSpan.FromSeconds(1)` | Delay between retry attempts. |
| `CaseInsensitiveMapping` | `bool` | `false` | Enables case-insensitive column name mapping. Useful when database column names have inconsistent casing. |
| `CacheMappingMetadata` | `bool` | `true` | Caches mapping metadata per type to improve performance. Disable if mapping changes at runtime. |
| `ValidateIdentifiers` | `bool` | `true` | Validates SQL identifiers (schema, table, column names) to prevent SQL injection. |
| `ContinueOnError` | `bool` | `false` | Continues processing when per-property mapping errors occur. Properties with errors are set to default values. |
| `CheckpointStrategy` | `CheckpointStrategy` | `CheckpointStrategy.None` | Strategy for checkpointing to recover from transient failures. |
| `BatchSize` | `int` | `1,000` | Target batch size for batch write operations. |
| `MaxBatchSize` | `int` | `5,000` | Maximum batch size to prevent runaway buffers. `BatchSize` is clamped to this value. |
| `UseTransaction` | `bool` | `true` | Wraps write operations in a transaction for atomicity. |
| `CommandTimeout` | `int?` | `null` | Command timeout in seconds. When `null`, uses the default Npgsql timeout. |

### PostgresWriteStrategy

Enum defining write strategies for the sink node.

| Value | Description |
| --- | --- |
| `PerRow` | Writes each row individually with a separate `INSERT` statement. |
| `Batch` | Buffers multiple rows and issues a single multi-value `INSERT` statement. |

### CheckpointStrategy

Enum defining checkpointing strategies for transient recovery.

| Value | Description |
| --- | --- |
| `None` | No checkpointing. Failures require restarting from the beginning. |
| `InMemory` | Stores checkpoint state in memory. Enables recovery from transient failures during a single process execution. |

## Advanced Configuration

### Streaming Large Result Sets

When reading large result sets, enable streaming to keep memory usage low:

```csharp
var config = new PostgresConfiguration
{
    StreamResults = true,
    FetchSize = 1_000  // Adjust based on your data and memory constraints
};

var source = new PostgresSourceNode<Order>(
    connectionString,
    "SELECT * FROM large_table",
    configuration: config);
```

**Why Streaming Matters:**

Without streaming (`StreamResults = false`), Npgsql loads the entire result set into memory. For tables with millions of rows, this can cause out-of-memory exceptions. Streaming fetches rows in batches, allowing you to process data without loading everything at once.

### Batch Writing Configuration

Optimize batch writing based on your workload:

```csharp
var config = new PostgresConfiguration
{
    BatchSize = 500,        // Target batch size
    MaxBatchSize = 5_000,   // Maximum to prevent runaway buffers
    UseTransaction = true   // Wrap in transaction for atomicity
};

var sink = new PostgresSinkNode<Order>(
    pool,
    "orders",
    PostgresWriteStrategy.Batch,
    configuration: config);
```

**Batch Size Guidelines:**

- **Small batches (100-500)**: Better for real-time processing and lower latency
- **Medium batches (500-1,000)**: Good balance between throughput and latency
- **Large batches (1,000-5,000)**: Maximum throughput for bulk loading
- **Very large batches (>5,000)**: May cause memory pressure and longer transaction times

### Retry Configuration

Configure retries to handle transient failures:

```csharp
var config = new PostgresConfiguration
{
    MaxRetryAttempts = 3,
    RetryDelay = TimeSpan.FromSeconds(2)
};

var source = new PostgresSourceNode<Order>(
    connectionString,
    "SELECT * FROM orders",
    configuration: config);
```

**Important:** Retries only occur before the first row is yielded. Once streaming begins, failures are propagated to the pipeline.

### Case-Insensitive Mapping

Enable case-insensitive mapping when database column names have inconsistent casing:

```csharp
var config = new PostgresConfiguration
{
    CaseInsensitiveMapping = true,
    CacheMappingMetadata = true
};

var source = new PostgresSourceNode<Order>(
    connectionString,
    "SELECT Id, CustomerId, Total, Status FROM orders",
    configuration: config);
```

This maps columns like `Id`, `id`, and `ID` to the same property.

### Checkpointing for Transient Recovery

Enable in-memory checkpointing to recover from transient failures:

```csharp
var config = new PostgresConfiguration
{
    CheckpointStrategy = CheckpointStrategy.InMemory,
    StreamResults = true
};

var source = new PostgresSourceNode<Order>(
    connectionString,
    "SELECT * FROM orders ORDER BY id",
    configuration: config);
```

**How Checkpointing Works:**

The source node tracks the last successfully processed row ID. If a transient failure occurs (e.g., network timeout), the node can resume from the last checkpoint rather than restarting from the beginning.

**Limitations:**

- Only works for queries with an ordering column (typically an ID)
- Checkpoint state is lost if the process terminates
- Requires `StreamResults = true` to work correctly

### Example: Transforming and Writing to PostgreSQL

This pipeline reads order data, transforms it, and writes the result to a summary table:

```csharp
using NPipeline.Connectors.PostgreSQL;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed record Order(int Id, int CustomerId, decimal Total, string Status);
public sealed record OrderSummary(int OrderId, string StatusCategory, decimal Total);

public sealed class OrderTransformer : TransformNode<Order, OrderSummary>
{
    public override Task<OrderSummary> ExecuteAsync(
        Order item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var statusCategory = item.Status switch
        {
            "completed" or "shipped" => "fulfilled",
            "pending" or "processing" => "in_progress",
            _ => "other"
        };
        return Task.FromResult(new OrderSummary(item.Id, statusCategory, item.Total));
    }
}

public sealed class PostgresTransformPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Read from orders table
        var source = builder.AddSource(
            new PostgresSourceNode<Order>(
                "Host=localhost;Database=npipeline;Username=postgres;Password=postgres",
                "SELECT id, customer_id, total, status FROM orders",
                configuration: new PostgresConfiguration { StreamResults = true, FetchSize = 1_000 }),
            "postgres_source");

        // Transform data
        var transform = builder.AddTransform<OrderTransformer, Order, OrderSummary>("transformer");

        // Write to order_summary table
        var sink = builder.AddSink(
            new PostgresSinkNode<OrderSummary>(
                "Host=localhost;Database=npipeline;Username=postgres;Password=postgres",
                "order_summary",
                PostgresWriteStrategy.Batch,
                configuration: new PostgresConfiguration { BatchSize = 1_000, UseTransaction = true }),
            "postgres_sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        var runner = PipelineRunner.Create();
        await runner.RunAsync<PostgresTransformPipeline>();
    }
}
```

## Mapping

The PostgreSQL connector provides flexible mapping between database columns and C# properties.

### Convention-Based Mapping

By default, the connector uses convention-based mapping:

- C# property names in `PascalCase` are converted to `snake_case` column names
- Example: `CustomerId` → `customer_id`, `TotalAmount` → `total_amount`

### Attribute-Based Mapping

Override default mapping using attributes:

#### `[PostgresColumn]`

Specifies the column name for a property:

```csharp
public record Order(
    [PostgresColumn("order_id", PrimaryKey = true)] int Id,
    [PostgresColumn("customer_id")] int CustomerId,
    [PostgresColumn("order_total")] decimal Total,
    string Status);
```

Parameters:

- **`Name`**: Column name in the database
- **`PrimaryKey`**: Indicates whether the column is a primary key (used for checkpointing)
- **`Ignore`**: When `true`, skips mapping this property

#### `[PostgresIgnore]`

Skips a property entirely during mapping:

```csharp
public record Order(
    int Id,
    int CustomerId,
    decimal Total,
    [PostgresIgnore] string? InternalNotes,
    [PostgresIgnore] DateTime? LastUpdated);
```

### Mapping Metadata Caching

Mapping metadata is cached per type when `CacheMappingMetadata` is enabled (default). This improves performance by avoiding reflection on every row.

**When to Disable Caching:**

- When mapping changes at runtime (rare)
- When memory is extremely constrained
- When debugging mapping issues

### Example: Complete Mapping Configuration

```csharp
using NPipeline.Connectors.PostgreSQL.Mapping;

public record Order(
    [PostgresColumn("order_id", PrimaryKey = true)] int Id,
    [PostgresColumn("customer_id")] int CustomerId,
    [PostgresColumn("order_total")] decimal Total,
    [PostgresColumn("order_status")] string Status,
    [PostgresIgnore] string? InternalNotes,
    [PostgresIgnore] DateTime? ComputedFields);

public sealed class MappingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceNode = new PostgresSourceNode<Order>(
            "Host=localhost;Database=npipeline;Username=postgres;Password=postgres",
            "SELECT order_id, customer_id, order_total, order_status FROM orders",
            configuration: new PostgresConfiguration
            {
                CacheMappingMetadata = true,
                CaseInsensitiveMapping = true
            });
        var source = builder.AddSource(sourceNode, "postgres_source");
        var sink = builder.AddSink<ConsoleSinkNode, Order>("console_sink");

        builder.Connect(source, sink);
    }
}
```

## Performance Considerations

### Reading Performance

#### Streaming vs. Buffering

- **Streaming (`StreamResults = true`)**: Recommended for large result sets. Fetches rows in batches, keeping memory usage low.
- **Buffering (`StreamResults = false`)**: Loads entire result set into memory. Faster for small result sets but causes memory issues with large datasets.

#### Fetch Size Tuning

Adjust `FetchSize` based on your workload:

```csharp
// Small fetch size: Lower memory usage, more round-trips
var smallFetchConfig = new PostgresConfiguration
{
    StreamResults = true,
    FetchSize = 100
};

// Large fetch size: Fewer round-trips, higher memory usage
var largeFetchConfig = new PostgresConfiguration
{
    StreamResults = true,
    FetchSize = 10_000
};
```

**Guidelines:**

- **100-500**: Good for memory-constrained environments or very wide rows
- **1,000-5,000**: Balanced approach for most workloads
- **5,000-10,000**: Maximum throughput for high-bandwidth networks

#### Mapping Metadata Caching

Enable `CacheMappingMetadata` (default) to avoid reflection overhead:

```csharp
var config = new PostgresConfiguration
{
    CacheMappingMetadata = true  // Default, but explicit for clarity
};
```

### Writing Performance

#### Batch vs. Per-Row Writes

- **Batch strategy**: 10-100x faster than per-row for bulk operations
- **Per-row strategy**: Better for low-latency, real-time scenarios

```csharp
// Batch: Maximum throughput
var batchSink = new PostgresSinkNode<Order>(
    pool,
    "orders",
    PostgresWriteStrategy.Batch,
    configuration: new PostgresConfiguration { BatchSize = 1_000, UseTransaction = true });

// Per-row: Low latency
var perRowSink = new PostgresSinkNode<Order>(
    pool,
    "orders",
    PostgresWriteStrategy.PerRow);
```

#### Batch Size Tuning

Choose batch size based on your requirements:

```csharp
// Small batches: Lower latency, more round-trips
var smallBatchConfig = new PostgresConfiguration
{
    BatchSize = 100,
    MaxBatchSize = 1_000
};

// Large batches: Higher throughput, more memory
var largeBatchConfig = new PostgresConfiguration
{
    BatchSize = 2_000,
    MaxBatchSize = 10_000
};
```

**Guidelines:**

- **100-500**: Near real-time processing
- **500-1,000**: Balanced throughput and latency
- **1,000-5,000**: Bulk loading scenarios

#### Transaction Management

Use transactions (`UseTransaction = true`) for:

- Data integrity (all or nothing)
- Better performance (single commit vs. multiple)

Disable transactions for:

- Very large datasets where transaction log size is a concern
- Scenarios where partial failure is acceptable

### Connection Pool Management

When using dependency injection, the connection pool efficiently manages connections:

- **Default pool size**: Configured via `DefaultConfiguration`
- **Connection reuse**: Connections are reused across operations
- **Automatic cleanup**: Connections are properly disposed

```csharp
services.AddPostgresConnector(options =>
{
    options.DefaultConfiguration = new PostgresConfiguration
    {
        // Connection pool settings are passed to Npgsql
        // See Npgsql documentation for detailed options
    };
});
```

## Limitations

### Checkpointing Limitations

- **In-memory only**: Checkpoint state is lost if the process terminates
- **Single process**: Cannot recover across process restarts
- **Ordered queries required**: Requires queries with an ordering column (typically an ID)
- **No distributed recovery**: Cannot coordinate checkpoints across multiple processes

### Write Strategy Limitations

- **Batch strategy**: All rows in a batch succeed or fail together
- **Per-row strategy**: Higher overhead for large datasets
- **No upsert support**: Only supports `INSERT` operations (no `UPDATE` or `UPSERT`)
- **No bulk copy**: Does not use PostgreSQL's `COPY` command

### Mapping Limitations

- **No complex type mapping**: Complex types must be handled via custom mappers
- **No array type support**: PostgreSQL arrays require custom handling
- **No JSON mapping**: JSON columns require custom mapping
- **Limited enum support**: Enums require explicit configuration

### Connection Limitations

- **No connection string encryption**: Connection strings are stored in plain text
- **No automatic failover**: Requires additional configuration for high availability
- **No read replica support**: All operations go to the primary database

## Prepared Statements

The connector uses prepared statements by default (`UsePreparedStatements = true`). Prepared statements:

- Reduce query parsing overhead on the database server
- Improve performance for repeated query patterns (same query, different parameters)
- Provide automatic SQL injection protection

### When to Disable Prepared Statements

Consider disabling `UsePreparedStatements` only for:

- Ad-hoc queries that are dynamically generated and never repeated
- Very complex queries that may not benefit from preparation
- Testing scenarios where you need to debug query generation

### Performance Impact

| Scenario | Prepared Statements | Performance Impact |
|----------|-------------------|-------------------|
| Repeated inserts (same query pattern) | Enabled | 10-30% faster |
| Ad-hoc queries (different each time) | Enabled | 5-10% overhead |
| One-time bulk operations | Disabled | No impact |

### Configuration

```csharp
var config = new PostgresConfiguration
{
    UsePreparedStatements = true  // Default, keep enabled for production
};
```

## Best Practices

### Configuration

1. **Use dependency injection**: Leverage `AddPostgresConnector` for production applications
2. **Enable streaming for large datasets**: Set `StreamResults = true` to avoid memory issues
3. **Tune fetch size**: Adjust `FetchSize` based on your data size and memory constraints
4. **Use batch writes for bulk operations**: `PostgresWriteStrategy.Batch` provides much better throughput
5. **Validate identifiers**: Keep `ValidateIdentifiers = true` to prevent SQL injection
6. **Cache mapping metadata**: Enable `CacheMappingMetadata` for better performance
7. **Use prepared statements**: Keep `UsePreparedStatements = true` for repeated query patterns

### Data Modeling

1. **Use convention-based mapping**: Leverage `PascalCase` to `snake_case` conversion
2. **Override with attributes**: Use `[PostgresColumn]` for non-standard column names
3. **Skip internal properties**: Use `[PostgresIgnore]` for properties that shouldn't be persisted
4. **Design for streaming**: Order queries by an ID column to enable checkpointing

### Error Handling

1. **Configure retries**: Set `MaxRetryAttempts` and `RetryDelay` for transient failures
2. **Use transactions**: Enable `UseTransaction = true` for atomic writes
3. **Handle mapping errors**: Consider `ContinueOnError = true` for partial results
4. **Log failures**: Implement logging to track connection and query failures

### Performance

1. **Profile your workload**: Test with representative data to identify bottlenecks
2. **Monitor memory usage**: Watch memory consumption with large result sets
3. **Optimize batch size**: Tune `BatchSize` based on your latency and throughput requirements
4. **Use connection pooling**: Leverage the shared connection pool for multiple operations
5. **Index appropriately**: Ensure database indexes support your queries

### Security

1. **Use connection string parameters**: Configure SSL, timeouts, and other security settings
2. **Validate identifiers**: Never disable `ValidateIdentifiers` in production
3. **Limit permissions**: Use database accounts with minimal required permissions
4. **Encrypt at rest**: Ensure database encryption is configured
5. **Use SSL**: Enable SSL for database connections in production

## Advanced Scenarios

### Reading Multiple Tables

Read from multiple tables and merge the data:

```csharp
public sealed class MultiTablePipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var pool = context.GetRequiredService<IPostgresConnectionPool>();

        // Read from orders table
        var ordersSource = builder.AddSource(
            new PostgresSourceNode<Order>(
                pool,
                "SELECT * FROM orders",
                connectionName: "analytics"),
            "orders_source");

        // Read from customers table
        var customersSource = builder.AddSource(
            new PostgresSourceNode<Customer>(
                pool,
                "SELECT * FROM customers",
                connectionName: "analytics"),
            "customers_source");

        // Join data using a merge node
        var join = builder.AddMerge<Order, Customer, OrderCustomerSummary>(
            (orders, customers) => orders.Join(
                customers,
                o => o.CustomerId,
                c => c.Id,
                (o, c) => new OrderCustomerSummary(o.Id, c.Name, o.Total, o.Status)),
            "join");

        var sink = builder.AddSink<ConsoleSinkNode, OrderCustomerSummary>("console_sink");

        builder.Connect(ordersSource, join);
        builder.Connect(customersSource, join);
        builder.Connect(join, sink);
    }
}
```

### Round-Trip Processing

Read from PostgreSQL, process the data, and write back to a different table:

```csharp
public sealed class RoundTripPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var pool = context.GetRequiredService<IPostgresConnectionPool>();

        // Read from raw_orders table
        var source = builder.AddSource(
            new PostgresSourceNode<RawOrder>(
                pool,
                "SELECT * FROM raw_orders WHERE processed = false",
                connectionName: "warehouse"),
            "raw_source");

        // Process and validate data
        var transform = builder.AddTransform<OrderProcessor, RawOrder, ProcessedOrder>("processor");

        // Write to processed_orders table
        var sink = builder.AddSink(
            new PostgresSinkNode<ProcessedOrder>(
                pool,
                "processed_orders",
                PostgresWriteStrategy.Batch,
                connectionName: "warehouse",
                configuration: new PostgresConfiguration { BatchSize = 1_000, UseTransaction = true }),
            "processed_sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}
```

### Handling Large Transactions

For very large datasets, consider splitting into multiple transactions:

```csharp
public sealed class LargeDatasetPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var pool = context.GetRequiredService<IPostgresConnectionPool>();

        var source = builder.AddSource(
            new PostgresSourceNode<LargeRecord>(
                pool,
                "SELECT * FROM large_table",
                connectionName: "warehouse",
                configuration: new PostgresConfiguration { StreamResults = true, FetchSize = 5_000 }),
            "source");

        var sink = builder.AddSink(
            new PostgresSinkNode<LargeRecord>(
                pool,
                "target_table",
                PostgresWriteStrategy.Batch,
                connectionName: "warehouse",
                configuration: new PostgresConfiguration
                {
                    BatchSize = 1_000,
                    MaxBatchSize = 5_000,
                    UseTransaction = true  // Each batch is its own transaction
                }),
            "sink");

        builder.Connect(source, sink);
    }
}
```

### Custom Error Handling

Implement custom error handling for database operations:

```csharp
public sealed class ResilientPostgresSourceNode<T> : PostgresSourceNode<T>
{
    private readonly ILogger<ResilientPostgresSourceNode<T>> _logger;

    public ResilientPostgresSourceNode(
        string connectionString,
        string query,
        PostgresConfiguration? configuration = null,
        ILogger<ResilientPostgresSourceNode<T>>? logger = null)
        : base(connectionString, query, configuration)
    {
        _logger = logger;
    }

    public override async IAsyncEnumerable<T> ExecuteAsync(
        PipelineContext context,
        IPipelineActivity parentActivity,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var item in base.ExecuteAsync(context, parentActivity, cancellationToken))
            {
                yield return item;
            }
        }
        catch (NpgsqlException ex) when (ex.IsTransient)
        {
            _logger.LogWarning(ex, "Transient database error occurred");
            throw; // Re-throw to allow retry logic
        }
        catch (NpgsqlException ex)
        {
            _logger.LogError(ex, "Fatal database error occurred");
            throw;
        }
    }
}
```

## Related Topics

- **[NPipeline Extensions Index](../.)**: Return to the extensions overview.
- **[CSV Connector](./csv.md)**: Learn about working with CSV files.
- **[Excel Connector](./excel.md)**: Learn about working with Excel files.
- **[Storage Provider Interface](../storage-providers/storage-provider.md)**: Understand the storage layer architecture.
- **[Npgsql Documentation](https://www.npgsql.org/doc/)**: Detailed documentation for the underlying PostgreSQL driver.
