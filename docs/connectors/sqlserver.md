---
title: SQL Server Connector
description: Read from and write to Microsoft SQL Server databases with NPipeline using the SQL Server connector.
sidebar_position: 4
---

## SQL Server Connector

The `NPipeline.Connectors.SqlServer` package provides specialized source and sink nodes for working with Microsoft SQL Server databases. This allows you to easily integrate SQL Server data into your pipelines as an input source or an output destination.

This connector uses the [Microsoft.Data.SqlClient](https://learn.microsoft.com/en-us/dotnet/api/microsoft.data.sqlclient) library under the hood, providing reliable streaming reads, per-row and batched writes, and in-memory checkpointing for transient recovery.

## Installation

To use the SQL Server connector, install the `NPipeline.Connectors.SqlServer` NuGet package:

```bash
dotnet add package NPipeline.Connectors.SqlServer
```

For the core NPipeline package and other available extensions, see the [Installation Guide](../getting-started/installation.md).

## Features

The SQL Server connector provides the following capabilities:

- **Source Node**: Read data from SQL Server tables and views
- **Sink Node**: Write data to SQL Server tables
- **Write Strategies**: Support for PerRow and Batch write strategies
- **Connection Pooling**: Efficient connection management with named connections
- **Attribute Mapping**: Support for `[Column]`, `[IgnoreColumn]`, and `[SqlServerColumn]` attributes
- **Common Attributes**: Cross-connector `ColumnAttribute` and `IgnoreColumnAttribute` support
- **Convention Mapping**: Automatic PascalCase mapping (no conversion needed)
- **Custom Mappers**: `Func<T, IEnumerable<DatabaseParameter>>` for complete control
- **Error Handling**: Retry logic for transient SQL Server errors
- **In-Memory Checkpointing**: Basic recovery support for streaming operations
- **Streaming Results**: Fetch data in streams to reduce memory usage
- **MARS Support**: Multiple Active Result Sets for improved concurrency
- **Authentication**: Both Windows Authentication and SQL Server Authentication support

## Dependency Injection

The SQL Server connector supports dependency injection for managing connection pools and node factories. This is the recommended approach for production applications.

### Registering the Connector

Use `AddSqlServerConnector` to register a shared connection pool and factories for creating nodes:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.SqlServer.DependencyInjection;

var services = new ServiceCollection()
    .AddSqlServerConnector(options =>
    {
        // Set a default connection string (optional if using only named connections)
        options.DefaultConnectionString = "Server=localhost;Database=npipeline;Integrated Security=True;";

        // Add named connections for different databases
        options.AddOrUpdateConnection("analytics", "Server=localhost;Database=analytics;Integrated Security=True;");
        options.AddOrUpdateConnection("warehouse", "Server=localhost;Database=warehouse;Integrated Security=True;");

        // Configure default connection-level settings
        options.DefaultConfiguration = new SqlServerConfiguration
        {
            StreamResults = true,
            FetchSize = 1_000,
            MaxRetryAttempts = 3,
            RetryDelay = TimeSpan.FromSeconds(2)
        };
    })
    .BuildServiceProvider();

// Resolve services from the container
var pool = services.GetRequiredService<ISqlServerConnectionPool>();
var sourceFactory = services.GetRequiredService<SqlServerSourceNodeFactory>();
var sinkFactory = services.GetRequiredService<SqlServerSinkNodeFactory>();
```

### Configuration Options

- **`DefaultConnectionString`**: Optional connection string used when no named connection is specified. Can be omitted if you only use named connections.
- **`DefaultConfiguration`**: Controls connection-level settings (timeouts, pool sizing, MARS) applied when the pool builds `SqlConnection` instances.
- **`AddOrUpdateConnection(name, connectionString)`**: Adds or updates a named connection. Multiple connections can be configured for different databases.
- **`AddSqlServerConnection`/`AddDefaultSqlServerConnection`**: Configure the same `SqlServerOptions` and do not replace previously configured values.

### Why Use Dependency Injection?

Using dependency injection provides several benefits:

- **Connection Pool Management**: The shared connection pool efficiently manages database connections across multiple nodes
- **Configuration Centralization**: All SQL Server connections are configured in one place
- **Testability**: Easy to mock or replace dependencies in unit tests
- **Lifetime Management**: Services are properly disposed when the application shuts down

## Common Attributes

The SQL Server connector supports common attributes from `NPipeline.Connectors.Attributes` that work across all connectors, as well as SQL Server-specific attributes that extend the common attributes with database-specific features.

### `[Column]` Attribute

The `[Column]` attribute (from `NPipeline.Connectors.Attributes`) is a common attribute that allows you to specify column names and control property mapping across all connectors. It provides:

- **`Name`**: The column name in the database
- **`Ignore`**: When `true`, skips mapping this property

This attribute is recommended for simple scenarios where you only need basic column mapping.

```csharp
using NPipeline.Connectors.Attributes;

public class Customer
{
    [Column("CustomerID")]
    public int CustomerId { get; set; }

    [Column("FirstName")]
    public string FirstName { get; set; } = string.Empty;

    [Column("LastName")]
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

### SQL Server-Specific Attributes

The SQL Server connector provides `[SqlServerColumn]` attribute that extends the common attributes with database-specific functionality:

- **`[SqlServerColumn]`**: Extends `[Column]` with additional properties:
  - **`DbType`**: Specifies the SQL Server data type for the column
  - **`Size`**: Sets the size/length for character and numeric types
  - **`PrimaryKey`**: Indicates whether the column is a primary key (used for checkpointing)
  - **`Identity`**: Indicates whether the column is an auto-increment identity column

The `[IgnoreColumn]` attribute from `NPipeline.Connectors.Attributes` covers all ignore requirements and works identically to a SQL Server-specific ignore attribute.

These attributes are useful when you need database-specific features like type specification, primary key marking, or identity column handling.

```csharp
using NPipeline.Connectors.SqlServer.Mapping;
using NPipeline.Connectors.Attributes;
using System.Data;

public class Customer
{
    [SqlServerColumn("CustomerID", PrimaryKey = true, Identity = true)]
    public int CustomerId { get; set; }

    [SqlServerColumn("FirstName", DbType = SqlDbType.NVarChar, Size = 100)]
    public string FirstName { get; set; } = string.Empty;

    [SqlServerColumn("LastName", DbType = SqlDbType.NVarChar, Size = 100)]
    public string LastName { get; set; } = string.Empty;

    [SqlServerColumn("Email", DbType = SqlDbType.NVarChar, Size = 255)]
    public string Email { get; set; } = string.Empty;

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}
```

### Choosing Between Common and SQL Server-Specific Attributes

**Use common attributes (`[Column]`, `[IgnoreColumn]`) when:**

- You want code that works across multiple connectors
- You only need basic column mapping functionality
- You prefer using standard attributes provided by the core library
- Your database schema follows standard conventions

**Use SQL Server-specific attributes (`[SqlServerColumn]`) when:**

- You need to specify database types explicitly (e.g., `NVARCHAR(255)`, `DECIMAL(10,2)`)
- You need to mark primary key columns for checkpointing
- You need to mark identity columns for auto-increment
- You want to leverage SQL Server-specific features
- You're maintaining existing code that already uses these attributes

Both attribute types are fully supported and will continue to work in future versions. The common attributes are recommended for new code when you don't need database-specific features, while SQL Server-specific attributes provide additional control when needed.

## `SqlServerSourceNode<T>`

The `SqlServerSourceNode<T>` reads data from a SQL Server database and emits each row as an item of type `T`.

### Source Configuration

The constructor for `SqlServerSourceNode<T>` provides multiple overloads for flexibility:

```csharp
// Using connection string
public SqlServerSourceNode<T>(
    string connectionString,
    string query,
    SqlServerConfiguration? configuration = null)

// Using connection string with custom row mapper
public SqlServerSourceNode<T>(
    string connectionString,
    string query,
    Func<SqlServerRow, T> rowMapper,
    SqlServerConfiguration? configuration = null)

// Using connection pool
public SqlServerSourceNode<T>(
    ISqlServerConnectionPool pool,
    string query,
    SqlServerConfiguration? configuration = null,
    DatabaseParameter[]? parameters = null,
    bool continueOnError = false,
    string? connectionName = null)

// Using connection pool with custom row mapper
public SqlServerSourceNode<T>(
    ISqlServerConnectionPool pool,
    string query,
    Func<SqlServerRow, T> rowMapper,
    SqlServerConfiguration? configuration = null,
    DatabaseParameter[]? parameters = null,
    bool continueOnError = false,
    string? connectionName = null)
```

- **`connectionString`**: SQL Server connection string (e.g., `"Server=localhost;Database=mydb;Integrated Security=True;"`)
- **`pool`**: Shared connection pool from dependency injection
- **`query`**: SQL query to execute
- **`rowMapper`**: Custom function to map a `SqlServerRow` to type `T`. When omitted, uses convention-based mapping
- **`parameters`**: Optional parameters for the SQL query
- **`continueOnError`**: Whether to skip row-level mapping errors
- **`connectionName`**: Name of a configured connection from the pool
- **`configuration`**: Optional configuration object for customizing behavior

> **Tip:** When you need to provide custom configuration or a row mapper, instantiate `SqlServerSourceNode<T>` yourself and register it via `builder.AddSource(new SqlServerSourceNode<Order>(...), "sqlserver_source")`. The builder handles registration and disposal for you automatically.

### Example: Reading from SQL Server

Let's assume you have a SQL Server table named `Orders` with the following structure:

| OrderId | CustomerId | Total | Status |
| --- | --- | --- | --- |
| 1 | 100 | 150.00 | completed |
| 2 | 101 | 75.50 | pending |
| 3 | 102 | 200.00 | shipped |

And a corresponding C# record:

```csharp
public sealed record Order(int OrderId, int CustomerId, decimal Total, string Status);
```

You can read this data into your pipeline as follows:

```csharp
using NPipeline;
using NPipeline.Connectors.SqlServer;
using NPipeline.DataFlow.DataPipes;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Tracing;

public sealed record Order(int OrderId, int CustomerId, decimal Total, string Status);

public sealed class SqlServerReaderPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Create the SQL Server source node
        var sourceNode = new SqlServerSourceNode<Order>(
            connectionString: "Server=localhost;Database=npipeline;Integrated Security=True;",
            query: "SELECT OrderId, CustomerId, Total, Status FROM Orders ORDER BY OrderId",
            configuration: new SqlServerConfiguration
            {
                StreamResults = true,
                FetchSize = 1_000
            });
        var source = builder.AddSource(sourceNode, "sqlserver_source");
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
        await runner.RunAsync<SqlServerReaderPipeline>();

        Console.WriteLine("SQL Server reading completed");
    }
}
```

**Expected Output:**

```text
Received: Order { OrderId = 1, CustomerId = 100, Total = 150.00, Status = completed }
Received: Order { OrderId = 2, CustomerId = 101, Total = 75.50, Status = pending }
Received: Order { OrderId = 3, CustomerId = 102, Total = 200.00, Status = shipped }
SQL Server reading completed
```

### Example: Using a Connection Pool

When using dependency injection, you can leverage the shared connection pool:

```csharp
public sealed class PooledSqlServerReaderPipeline : IPipelineDefinition
{
    private readonly ISqlServerConnectionPool _pool;

    public PooledSqlServerReaderPipeline(ISqlServerConnectionPool pool)
    {
        _pool = pool;
    }

    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Use a named connection from the pool
        var sourceNode = new SqlServerSourceNode<Order>(
            pool: _pool,
            query: "SELECT OrderId, CustomerId, Total, Status FROM Orders ORDER BY OrderId",
            connectionName: "analytics");
        var source = builder.AddSource(sourceNode, "sqlserver_source");
        var sink = builder.AddSink<ConsoleSinkNode, Order>("console_sink");

        builder.Connect(source, sink);
    }
}
```

### Example: Using a Custom Row Mapper

For complete control over mapping, provide a custom row mapper function:

```csharp
public sealed record Order(
    int OrderId,
    int CustomerId,
    decimal Total,
    string Status,
    DateTime CreatedAt);

public sealed class CustomMapperPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceNode = new SqlServerSourceNode<Order>(
            connectionString: "Server=localhost;Database=npipeline;Integrated Security=True;",
            query: "SELECT OrderId, CustomerId, Total, Status, CreatedAt FROM Orders",
            rowMapper: row => new Order(
                row.Get<int>("OrderId"),
                row.Get<int>("CustomerId"),
                row.Get<decimal>("Total"),
                row.Get<string>("Status"),
                row.Get<DateTime>("CreatedAt")));
        var source = builder.AddSource(sourceNode, "sqlserver_source");
        var sink = builder.AddSink<ConsoleSinkNode, Order>("console_sink");

        builder.Connect(source, sink);
    }
}
```

## `SqlServerSinkNode<T>`

The `SqlServerSinkNode<T>` writes items from the pipeline to a SQL Server database table.

### Sink Configuration

The constructor for `SqlServerSinkNode<T>` provides multiple overloads:

```csharp
// Using connection string
public SqlServerSinkNode<T>(
    string connectionString,
    string tableName,
    SqlServerConfiguration? configuration = null,
    Func<T, IEnumerable<DatabaseParameter>>? customMapper = null)

// Using connection pool
public SqlServerSinkNode<T>(
    ISqlServerConnectionPool pool,
    string tableName,
    SqlServerConfiguration? configuration = null,
    Func<T, IEnumerable<DatabaseParameter>>? customMapper = null,
    string? connectionName = null)
```

- **`connectionString`**: SQL Server connection string
- **`pool`**: Shared connection pool from dependency injection
- **`tableName`**: Name of the target table (unqualified)
- **`customMapper`**: Custom function to map type `T` to database parameters
- **`connectionName`**: Name of a configured connection from the pool
- **`configuration`**: Optional configuration object for customizing behavior

> **Note:** Provide the schema via `SqlServerConfiguration.Schema` and pass the unqualified table name.

> **Tip:** When you need to pass a custom parameter mapper or configuration, instantiate `SqlServerSinkNode<T>` yourself and register it via `builder.AddSink(new SqlServerSinkNode<Order>(...), "sqlserver_sink")`. The builder handles registration and disposal for you automatically.

### Write Strategies

The connector supports the following write strategies:

#### PerRow Strategy

Writes each row individually with a separate `INSERT` statement. This provides:

- Immediate visibility of each row
- Better error isolation (one failed insert doesn't affect others)
- Higher overhead for large datasets

#### Batch Strategy

Buffers multiple rows and issues a single multi-row `INSERT` statement. This provides:

- Better performance for large datasets
- Reduced database round-trips
- All-or-nothing semantics within a batch

### Example: Writing to SQL Server

Let's take processed order data and write it to an `OrderSummary` table:

```csharp
using NPipeline.Connectors.SqlServer;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed record OrderSummary(int Id, string CustomerName, decimal Total, string Status);

public sealed class SqlServerWriterPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<InMemorySourceNode<OrderSummary>, OrderSummary>("source");

        // Create the SQL Server sink node with batch strategy
        var sinkNode = new SqlServerSinkNode<OrderSummary>(
            connectionString: "Server=localhost;Database=npipeline;Integrated Security=True;",
            tableName: "OrderSummary",
            configuration: new SqlServerConfiguration
            {
                WriteStrategy = SqlServerWriteStrategy.Batch,
                BatchSize = 1_000,
                MaxBatchSize = 5_000,
                UseTransaction = true,
                Schema = "dbo"
            });
        var sink = builder.AddSink(sinkNode, "sqlserver_sink");

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
        await runner.RunAsync<SqlServerWriterPipeline>(context);

        Console.WriteLine("SQL Server write completed");
    }
}
```

**Expected Database Content:**

| Id | CustomerName | Total | Status |
| --- | --- | --- | --- |
| 1 | Alice Smith | 150.00 | completed |
| 2 | Bob Johnson | 75.50 | pending |
| 3 | Carol Williams | 200.00 | shipped |

### Example: Using a Custom Parameter Mapper

For complete control over parameter mapping, provide a custom parameter mapper function:

```csharp
public sealed record Order(int OrderId, int CustomerId, decimal Total, string Status);

public sealed class CustomMapperWriterPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<InMemorySourceNode<Order>, Order>("source");

        // Custom parameter mapper: return values in the same order as mapped columns
        Func<Order, IEnumerable<DatabaseParameter>> mapper = order => new[]
        {
            new DatabaseParameter("@OrderId", order.OrderId),
            new DatabaseParameter("@CustomerId", order.CustomerId),
            new DatabaseParameter("@Total", order.Total),
            new DatabaseParameter("@Status", order.Status)
        };

        var sinkNode = new SqlServerSinkNode<Order>(
            connectionString: "Server=localhost;Database=npipeline;Integrated Security=True;",
            tableName: "Orders",
            configuration: new SqlServerConfiguration
            {
                WriteStrategy = SqlServerWriteStrategy.PerRow,
                UseTransaction = true,
                Schema = "dbo"
            },
            customMapper: mapper);
        var sink = builder.AddSink(sinkNode, "sqlserver_sink");

        builder.Connect(source, sink);
    }
}
```

## Configuration Reference

### SqlServerConfiguration

The `SqlServerConfiguration` class provides comprehensive options for configuring SQL Server read and write operations.

#### Properties

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `ConnectionString` | `string` | `""` | SQL Server connection string. Not required when using a connection pool. |
| `Schema` | `string` | `"dbo"` | Default schema name for table operations. |
| `StreamResults` | `bool` | `true` | Enables streaming of results to reduce memory usage for large result sets. |
| `FetchSize` | `int` | `1,000` | Number of rows to fetch per round-trip when streaming. Larger values reduce round-trips but use more memory. |
| `MaxRetryAttempts` | `int` | `3` | Maximum number of retry attempts for transient failures. Only applies before the first row is yielded. |
| `RetryDelay` | `TimeSpan` | `TimeSpan.FromSeconds(1)` | Delay between retry attempts. |
| `CaseInsensitiveMapping` | `bool` | `true` | Enables case-insensitive column name mapping. Useful when database column names have inconsistent casing. |
| `CacheMappingMetadata` | `bool` | `true` | Caches mapping metadata per type to improve performance. Disable if mapping changes at runtime. |
| `ValidateIdentifiers` | `bool` | `true` | Validates SQL identifiers (schema, table, column names) to prevent SQL injection. |
| `ContinueOnError` | `bool` | `false` | Continues processing when per-property mapping errors occur. Properties with errors are set to default values. |
| `RowErrorHandler` | `Func<Exception, Mapping.SqlServerRow?, bool>?` | `null` | Custom row error handler for row-level error handling and filtering. Return `true` to skip the row, `false` to re-throw the exception. |
| `CheckpointStrategy` | `CheckpointStrategy` | `CheckpointStrategy.None` | Strategy for checkpointing to recover from transient failures. |
| `BatchSize` | `int` | `100` | Target batch size for batch write operations. Effective size is capped by `MaxBatchSize` and SQL Server's 2,100 parameter limit. |
| `MaxBatchSize` | `int` | `1,000` | Maximum batch size to prevent runaway buffers. `BatchSize` is clamped to this value. |
| `UseTransaction` | `bool` | `true` | Wraps write operations in a transaction for atomicity. |
| `CommandTimeout` | `int` | `30` | Command timeout in seconds. |
| `ConnectionTimeout` | `int` | `15` | Connection timeout in seconds. |
| `MinPoolSize` | `int` | `1` | Minimum connection pool size. |
| `MaxPoolSize` | `int` | `100` | Maximum connection pool size. |
| `EnableMARS` | `bool` | `false` | Enables Multiple Active Result Sets for improved concurrency. |
| `ApplicationName` | `string?` | `null` | Application name for monitoring in SQL Server. |
| `UsePreparedStatements` | `bool` | `true` | Uses prepared statements for improved performance. |

### SqlServerWriteStrategy

Enum defining write strategies for the sink node.

| Value | Description |
| --- | --- |
| `PerRow` | Writes each row individually with a separate `INSERT` statement. |
| `Batch` | Buffers multiple rows and issues a single multi-row `INSERT` statement. |

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
var config = new SqlServerConfiguration
{
    StreamResults = true,
    FetchSize = 1_000  // Adjust based on your data and memory constraints
};

var source = new SqlServerSourceNode<Order>(
    connectionString,
    "SELECT * FROM LargeTable",
    configuration: config);
```

**Why Streaming Matters:**

Without streaming (`StreamResults = false`), SqlClient loads the entire result set into memory. For tables with millions of rows, this can cause out-of-memory exceptions. Streaming fetches rows in batches, allowing you to process data without loading everything at once.

### Batch Writing Configuration

Optimize batch writing based on your workload:

```csharp
var config = new SqlServerConfiguration
{
    BatchSize = 500,        // Target batch size
    MaxBatchSize = 5_000,   // Maximum to prevent runaway buffers
    UseTransaction = true,  // Wrap in transaction for atomicity
    WriteStrategy = SqlServerWriteStrategy.Batch,
    Schema = "dbo"
};

var sink = new SqlServerSinkNode<Order>(
    pool,
    "Orders",
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
var config = new SqlServerConfiguration
{
    MaxRetryAttempts = 3,
    RetryDelay = TimeSpan.FromSeconds(2)
};

var source = new SqlServerSourceNode<Order>(
    connectionString,
    "SELECT * FROM Orders",
    configuration: config);
```

**Important:** Retries only occur before the first row is yielded. Once streaming begins, failures are propagated to the pipeline.

### Case-Insensitive Mapping

Enable case-insensitive mapping when database column names have inconsistent casing:

```csharp
var config = new SqlServerConfiguration
{
    CaseInsensitiveMapping = true,
    CacheMappingMetadata = true
};

var source = new SqlServerSourceNode<Order>(
    connectionString,
    "SELECT OrderId, CustomerId, Total, Status FROM Orders",
    configuration: config);
```

This maps columns like `OrderId`, `orderid`, and `ORDERID` to the same property.

### Checkpointing for Transient Recovery

Enable in-memory checkpointing to recover from transient failures:

```csharp
var config = new SqlServerConfiguration
{
    CheckpointStrategy = CheckpointStrategy.InMemory,
    StreamResults = true
};

var source = new SqlServerSourceNode<Order>(
    connectionString,
    "SELECT * FROM Orders ORDER BY OrderId",
    configuration: config);
```

**How Checkpointing Works:**

The source node tracks the last successfully processed row ID. If a transient failure occurs (e.g., network timeout), the node can resume from the last checkpoint rather than restarting from the beginning.

**Limitations:**

- Only works for queries with an ordering column (typically an ID)
- Checkpoint state is lost if the process terminates
- Requires `StreamResults = true` to work correctly

### Multiple Active Result Sets (MARS)

Enable MARS for improved concurrency when you need to execute multiple commands on a single connection:

```csharp
var config = new SqlServerConfiguration
{
    EnableMARS = true
};

// MARS is enabled via connection string: MultipleActiveResultSets=True
```

**Benefits of MARS:**

- Execute multiple commands on a single connection simultaneously
- Read while writing
- Improved concurrency in complex pipelines

### Row-Level Error Handling

For granular control over row-level mapping errors, use the `RowErrorHandler` property to intercept exceptions and decide whether to skip rows or propagate errors:

```csharp
var config = new SqlServerConfiguration
{
    RowErrorHandler = (exception, row) =>
    {
        // Log the error with row information
        Console.WriteLine($"Error processing row ID {row?.Get<int>("OrderId")}: {exception.Message}");
        
        // Return true to skip this row, false to re-throw the exception
        return exception is FormatException; // Skip format errors, fail on others
    }
};

var source = new SqlServerSourceNode<Order>(
    connectionString,
    "SELECT OrderId, CustomerId, Total, Status FROM Orders",
    configuration: config);
```

**Key Behaviors:**

- **Return `true`**: Skips the row and continues processing
- **Return `false`**: Re-throws the exception, which may be caught by `ContinueOnError`
- **Null row**: Occurs if the error happens before the row is fully populated

This provides fine-grained control for scenarios like:

- Logging problematic rows separately
- Skipping rows with specific data issues
- Collecting statistics on failed rows
- Implementing custom retry logic per row

### Example: Transforming and Writing to SQL Server

This pipeline reads order data, transforms it, and writes the result to a summary table:

```csharp
using NPipeline.Connectors.SqlServer;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed record Order(int OrderId, int CustomerId, decimal Total, string Status);
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
        return Task.FromResult(new OrderSummary(item.OrderId, statusCategory, item.Total));
    }
}

public sealed class SqlServerTransformPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Read from Orders table
        var source = builder.AddSource(
            new SqlServerSourceNode<Order>(
                "Server=localhost;Database=npipeline;Integrated Security=True;",
                "SELECT OrderId, CustomerId, Total, Status FROM Orders",
                configuration: new SqlServerConfiguration { StreamResults = true, FetchSize = 1_000 }),
            "sqlserver_source");

        // Transform data
        var transform = builder.AddTransform<OrderTransformer, Order, OrderSummary>("transformer");

        // Write to OrderSummary table
        var sink = builder.AddSink(
            new SqlServerSinkNode<OrderSummary>(
                "Server=localhost;Database=npipeline;Integrated Security=True;",
                "OrderSummary",
                configuration: new SqlServerConfiguration
                {
                    WriteStrategy = SqlServerWriteStrategy.Batch,
                    BatchSize = 1_000,
                    UseTransaction = true,
                    Schema = "dbo"
                }),
            "sqlserver_sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        var runner = PipelineRunner.Create();
        await runner.RunAsync<SqlServerTransformPipeline>();
    }
}
```

## Mapping

The SQL Server connector provides flexible mapping between database columns and C# properties.

### Convention-Based Mapping

By default, the connector uses convention-based mapping:

- C# property names in `PascalCase` map directly to SQL Server column names in `PascalCase`
- No conversion needed by default (unlike PostgreSQL which uses snake_case)
- Example: `CustomerId` → `CustomerId`, `TotalAmount` → `TotalAmount`

### Attribute-Based Mapping

Override default mapping using attributes:

#### `[SqlServerColumn]`

Specifies the column name for a property with optional SQL Server-specific features:

```csharp
using NPipeline.Connectors.SqlServer.Mapping;
using System.Data;

public record Order(
    [SqlServerColumn("OrderID", PrimaryKey = true, Identity = true)] int OrderId,
    [SqlServerColumn("CustomerID")] int CustomerId,
    [SqlServerColumn("OrderTotal", DbType = SqlDbType.Decimal)] decimal Total,
    [SqlServerColumn("OrderStatus")] string Status);
```

Parameters:

- **`Name`**: Column name in the database
- **`DbType`**: SQL Server data type for the column
- **`Size`**: Size for character and numeric types
- **`PrimaryKey`**: Indicates whether the column is a primary key (used for checkpointing)
- **`Identity`**: Indicates whether the column is an auto-increment identity column
- **`Ignore`**: When `true`, skips mapping this property

#### `[IgnoreColumn]`

Skips a property entirely during mapping:

```csharp
using NPipeline.Connectors.Attributes;

public record Order(
    int OrderId,
    int CustomerId,
    decimal Total,
    [IgnoreColumn] string? InternalNotes,
    [IgnoreColumn] DateTime? ComputedFields);
```

### Mapping Metadata Caching

Mapping metadata is cached per type when `CacheMappingMetadata` is enabled (default). This improves performance by avoiding reflection on every row.

**When to Disable Caching:**

- When mapping changes at runtime (rare)
- When memory is extremely constrained
- When debugging mapping issues

### Example: Complete Mapping Configuration

```csharp
using NPipeline.Connectors.SqlServer.Mapping;
using NPipeline.Connectors.Attributes;
using System.Data;

[SqlServerTable("Orders", Schema = "Sales")]
public record Order(
    [SqlServerColumn("OrderID", PrimaryKey = true, Identity = true)] int OrderId,
    [SqlServerColumn("CustomerID")] int CustomerId,
    [SqlServerColumn("OrderTotal", DbType = SqlDbType.Decimal)] decimal Total,
    [SqlServerColumn("OrderStatus")] string Status,
    [IgnoreColumn] string? InternalNotes,
    [IgnoreColumn] DateTime? ComputedFields);

public sealed class MappingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceNode = new SqlServerSourceNode<Order>(
            "Server=localhost;Database=npipeline;Integrated Security=True;",
            "SELECT OrderID, CustomerID, OrderTotal, OrderStatus FROM Sales.Orders",
            configuration: new SqlServerConfiguration
            {
                CacheMappingMetadata = true,
                CaseInsensitiveMapping = true
            });
        var source = builder.AddSource(sourceNode, "sqlserver_source");
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
var smallFetchConfig = new SqlServerConfiguration
{
    StreamResults = true,
    FetchSize = 100
};

// Large fetch size: Fewer round-trips, higher memory usage
var largeFetchConfig = new SqlServerConfiguration
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
var config = new SqlServerConfiguration
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
var batchSink = new SqlServerSinkNode<Order>(
    pool,
    "Orders",
    configuration: new SqlServerConfiguration
    {
        WriteStrategy = SqlServerWriteStrategy.Batch,
        BatchSize = 1_000,
        UseTransaction = true,
        Schema = "dbo"
    });

// Per-row: Low latency
var perRowSink = new SqlServerSinkNode<Order>(
    pool,
    "Orders",
    configuration: new SqlServerConfiguration
    {
        WriteStrategy = SqlServerWriteStrategy.PerRow,
        Schema = "dbo"
    });
```

#### Batch Size Tuning

Choose batch size based on your requirements:

```csharp
// Small batches: Lower latency, more round-trips
var smallBatchConfig = new SqlServerConfiguration
{
    BatchSize = 100,
    MaxBatchSize = 1_000
};

// Large batches: Higher throughput, more memory
var largeBatchConfig = new SqlServerConfiguration
{
    BatchSize = 2_000,
    MaxBatchSize = 10_000
};
```

**Guidelines:**

- **100-500**: Near real-time processing
- **500-1,000**: Balanced throughput and latency
- **1,000-5,000**: Bulk loading scenarios
- **Note**: Effective batch size is capped by SQL Server's 2,100 parameter limit and the number of mapped columns

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
services.AddSqlServerConnector(options =>
{
    options.DefaultConfiguration = new SqlServerConfiguration
    {
        MinPoolSize = 5,
        MaxPoolSize = 100,
        ConnectionTimeout = 15
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

### Mapping Limitations

- **No complex type mapping**: Complex types must be handled via custom mappers
- **No XML mapping**: XML columns require custom mapping
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
var config = new SqlServerConfiguration
{
    UsePreparedStatements = true  // Default, keep enabled for production
};
```

## Best Practices

### Configuration

1. **Use dependency injection**: Leverage `AddSqlServerConnector` for production applications
2. **Enable streaming for large datasets**: Set `StreamResults = true` to avoid memory issues
3. **Tune fetch size**: Adjust `FetchSize` based on your data size and memory constraints
4. **Use batch writes for bulk operations**: `SqlServerWriteStrategy.Batch` provides much better throughput
5. **Validate identifiers**: Keep `ValidateIdentifiers = true` to prevent SQL injection
6. **Cache mapping metadata**: Enable `CacheMappingMetadata` for better performance
7. **Use prepared statements**: Keep `UsePreparedStatements = true` for repeated query patterns

### Data Modeling

1. **Use convention-based mapping**: Leverage PascalCase to PascalCase mapping (no conversion needed)
2. **Override with attributes**: Use `[Column]` for non-standard column names
3. **Skip internal properties**: Use `[IgnoreColumn]` for properties that shouldn't be persisted
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
        var pool = context.GetRequiredService<ISqlServerConnectionPool>();

        // Read from Orders table
        var ordersSource = builder.AddSource(
            new SqlServerSourceNode<Order>(
                pool,
                "SELECT * FROM Orders",
                connectionName: "analytics"),
            "orders_source");

        // Read from Customers table
        var customersSource = builder.AddSource(
            new SqlServerSourceNode<Customer>(
                pool,
                "SELECT * FROM Customers",
                connectionName: "analytics"),
            "customers_source");

        // Join data using a merge node
        var join = builder.AddMerge<Order, Customer, OrderCustomerSummary>(
            (orders, customers) => orders.Join(
                customers,
                o => o.CustomerId,
                c => c.Id,
                (o, c) => new OrderCustomerSummary(o.OrderId, c.Name, o.Total, o.Status)),
            "join");

        var sink = builder.AddSink<ConsoleSinkNode, OrderCustomerSummary>("console_sink");

        builder.Connect(ordersSource, join);
        builder.Connect(customersSource, join);
        builder.Connect(join, sink);
    }
}
```

### Round-Trip Processing

Read from SQL Server, process the data, and write back to a different table:

```csharp
public sealed class RoundTripPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var pool = context.GetRequiredService<ISqlServerConnectionPool>();

        // Read from RawOrders table
        var source = builder.AddSource(
            new SqlServerSourceNode<RawOrder>(
                pool,
                "SELECT * FROM RawOrders WHERE Processed = 0",
                connectionName: "warehouse"),
            "raw_source");

        // Process and validate data
        var transform = builder.AddTransform<OrderProcessor, RawOrder, ProcessedOrder>("processor");

        // Write to ProcessedOrders table
        var sink = builder.AddSink(
            new SqlServerSinkNode<ProcessedOrder>(
                pool,
                "ProcessedOrders",
                configuration: new SqlServerConfiguration
                {
                    WriteStrategy = SqlServerWriteStrategy.Batch,
                    BatchSize = 1_000,
                    UseTransaction = true,
                    Schema = "dbo"
                },
                connectionName: "warehouse"),
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
        var pool = context.GetRequiredService<ISqlServerConnectionPool>();

        var source = builder.AddSource(
            new SqlServerSourceNode<LargeRecord>(
                pool,
                "SELECT * FROM LargeTable",
                connectionName: "warehouse",
                configuration: new SqlServerConfiguration { StreamResults = true, FetchSize = 5_000 }),
            "source");

        var sink = builder.AddSink(
            new SqlServerSinkNode<LargeRecord>(
                pool,
                "TargetTable",
                configuration: new SqlServerConfiguration
                {
                    WriteStrategy = SqlServerWriteStrategy.Batch,
                    BatchSize = 1_000,
                    MaxBatchSize = 5_000,
                    UseTransaction = true,  // Each batch is its own transaction
                    Schema = "dbo"
                },
                connectionName: "warehouse"),
            "sink");

        builder.Connect(source, sink);
    }
}
```

### Custom Error Handling

Implement custom error handling for database operations:

```csharp
public sealed class ResilientSqlServerSourceNode<T> : SqlServerSourceNode<T>
{
    private readonly ILogger<ResilientSqlServerSourceNode<T>> _logger;

    public ResilientSqlServerSourceNode(
        string connectionString,
        string query,
        SqlServerConfiguration? configuration = null,
        ILogger<ResilientSqlServerSourceNode<T>>? logger = null)
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
        catch (SqlException ex) when (IsTransientError(ex))
        {
            _logger.LogWarning(ex, "Transient database error occurred");
            throw; // Re-throw to allow retry logic
        }
        catch (SqlException ex)
        {
            _logger.LogError(ex, "Fatal database error occurred");
            throw;
        }
    }

    private bool IsTransientError(SqlException ex)
    {
        // SQL Server transient error codes
        int[] transientErrorCodes = { -2, 53, 64, 1205, 40501 };
        return transientErrorCodes.Contains(ex.Number);
    }
}
```

## Migration from PostgreSQL

Migrating from the PostgreSQL connector to the SQL Server connector is straightforward. Both connectors follow the same architectural patterns and support common attributes.

### Namespace Changes

```csharp
// PostgreSQL
using NPipeline.Connectors.PostgreSQL;
using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.Connectors.PostgreSQL.Mapping;
using NPipeline.Connectors.Attributes;  // Common attributes

// SQL Server
using NPipeline.Connectors.SqlServer;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Mapping;
using NPipeline.Connectors.Attributes;  // Common attributes (same!)
```

### Type Changes

```csharp
// PostgreSQL
PostgresSourceNode<T>
PostgresSinkNode<T>
PostgresConfiguration
PostgresWriteStrategy
PostgresColumnAttribute         // DB-specific extension
PostgresTableAttribute
PostgresRow

// SQL Server
SqlServerSourceNode<T>
SqlServerSinkNode<T>
SqlServerConfiguration
SqlServerWriteStrategy
SqlServerColumnAttribute        // DB-specific extension
SqlServerTableAttribute
SqlServerRow

// Common Attributes (Both Connectors)
ColumnAttribute                 // Replace PostgresColumn/SqlServerColumn for basic mappings
IgnoreColumnAttribute           // Replace PostgresIgnore/SqlServerIgnore
```

### Attribute Changes

```csharp
// PostgreSQL - snake_case convention
[PostgresColumn("customer_id")]
public int CustomerId { get; set; }
[PostgresIgnore]
public string ComputedValue { get; set; }

// SQL Server - PascalCase convention (simpler mappings use common attributes)
[Column("CustomerId")]  // For basic mappings - use common attribute
public int CustomerId { get; set; }
[IgnoreColumn]          // Common ignore attribute
public string ComputedValue { get; set; }

// SQL Server - DB-specific features still use SqlServerColumnAttribute
[SqlServerColumn("CustomerId", DbType = SqlDbType.Int, PrimaryKey = true)]
public int CustomerId { get; set; }
```

### Configuration Changes

```csharp
// PostgreSQL
var config = new PostgresConfiguration
{
    Schema = "public",
    WriteStrategy = PostgresWriteStrategy.Batch,
    StreamResults = true,
    FetchSize = 1_000
};

// SQL Server
var config = new SqlServerConfiguration
{
    Schema = "Sales",              // Default schema is dbo instead of public
    WriteStrategy = SqlServerWriteStrategy.Batch,
    StreamResults = true,
    FetchSize = 1_000
};
```

### Write Strategy Mapping

| PostgreSQL | SQL Server | Notes |
|-----------|-------------|-------|
| `PostgresWriteStrategy.PerRow` | `SqlServerWriteStrategy.PerRow` | Direct mapping |
| `PostgresWriteStrategy.Batch` | `SqlServerWriteStrategy.Batch` | Direct mapping |
| `PostgresWriteStrategy.Copy` | `SqlServerWriteStrategy.BulkCopy` | Similar semantics, Pro feature |

### Naming Convention Differences

**PostgreSQL:**

- Uses snake_case by default
- Property `CustomerId` maps to column `customer_id`
- Uses double quotes for identifier quoting: `"customer_id"`

**SQL Server:**

- Uses PascalCase by default
- Property `CustomerId` maps to column `CustomerId`
- Uses square brackets for identifier quoting: `[CustomerId]`

This means SQL Server mappings are often simpler since no case conversion is needed.

### Connection String Differences

```csharp
// PostgreSQL
var postgresConnectionString = "Host=localhost;Database=npipeline;Username=postgres;Password=postgres;";

// SQL Server - Windows Authentication
var sqlServerConnectionString = "Server=localhost;Database=npipeline;Integrated Security=True;";

// SQL Server - SQL Server Authentication
var sqlServerConnectionString = "Server=localhost;Database=npipeline;User Id=sa;Password=yourPassword;";
```

### Example Migration

**Before (PostgreSQL):**

```csharp
using NPipeline.Connectors.PostgreSQL;
using NPipeline.Connectors.PostgreSQL.Mapping;

[PostgresTable("customers")]
public class Customer
{
    [PostgresColumn("customer_id", PrimaryKey = true)]
    public int CustomerId { get; set; }
    
    [PostgresColumn("first_name")]
    public string FirstName { get; set; }
    
    [PostgresColumn("last_name")]
    public string LastName { get; set; }
}

var config = new PostgresConfiguration
{
    Schema = "public",
    WriteStrategy = PostgresWriteStrategy.Batch
};

var source = new PostgresSourceNode<Customer>(
    "Host=localhost;Database=npipeline;Username=postgres;Password=postgres;",
    "SELECT * FROM customers",
    configuration: config);
```

**After (SQL Server):**

```csharp
using NPipeline.Connectors.SqlServer;
using NPipeline.Connectors.SqlServer.Mapping;
using NPipeline.Connectors.Attributes;  // Common attributes

[SqlServerTable("Customers", Schema = "Sales")]
public class Customer
{
    [Column("CustomerId")]  // Use common attribute for basic mappings
    public int CustomerId { get; set; }
    
    [Column("FirstName")]
    public string FirstName { get; set; }
    
    [Column("LastName")]
    public string LastName { get; set; }
}

var config = new SqlServerConfiguration
{
    Schema = "Sales",
    WriteStrategy = SqlServerWriteStrategy.Batch
};

var source = new SqlServerSourceNode<Customer>(
    "Server=localhost;Database=npipeline;Integrated Security=True;",
    "SELECT * FROM Sales.Customers",
    configuration: config);
```

## Troubleshooting

### Common Issues

#### Connection Timeouts

**Symptom:** `SqlException: Timeout expired. The timeout period elapsed prior to completion of the operation or the server is not responding.`

**Solutions:**

1. Increase `ConnectionTimeout` in configuration
2. Increase `CommandTimeout` for long-running queries
3. Check network connectivity to SQL Server
4. Verify SQL Server is running and accepting connections

```csharp
var config = new SqlServerConfiguration
{
    ConnectionTimeout = 30,  // Increase from default 15
    CommandTimeout = 60       // Increase from default 30
};
```

#### Memory Issues with Large Result Sets

**Symptom:** Out of memory exceptions when reading large tables

**Solutions:**

1. Enable streaming: `StreamResults = true`
2. Reduce fetch size: `FetchSize = 500`
3. Process data in batches using checkpointing

```csharp
var config = new SqlServerConfiguration
{
    StreamResults = true,
    FetchSize = 500,
    CheckpointStrategy = CheckpointStrategy.InMemory
};
```

#### Batch Insert Failures

**Symptom:** Some rows fail to insert in batch mode, but succeed in per-row mode

**Solutions:**

1. Reduce batch size: `BatchSize = 100`
2. Enable `ContinueOnError` to skip problematic rows
3. Check for constraint violations or data type mismatches
4. Review error logs for specific failure reasons

```csharp
var config = new SqlServerConfiguration
{
    BatchSize = 100,
    ContinueOnError = true
};
```

#### Mapping Errors

**Symptom:** `SqlServerMappingException: Unable to map column 'ColumnName' to property 'PropertyName'`

**Solutions:**

1. Verify column names match (case-insensitive by default)
2. Use `[Column]` attribute to specify correct column name
3. Enable `CaseInsensitiveMapping = true`
4. Check for ignored properties with `[IgnoreColumn]`

```csharp
public class Order
{
    [Column("OrderID")]  // Specify exact column name
    public int OrderId { get; set; }
}
```

#### Deadlocks

**Symptom:** `SqlException: Transaction (Process ID) was deadlocked on lock resources with another process and has been chosen as the deadlock victim.`

**Solutions:**

1. Configure retry logic: `MaxRetryAttempts = 3`
2. Reduce transaction scope (smaller batches)
3. Review database indexes and query plans
4. Consider using `READ COMMITTED SNAPSHOT` isolation level

```csharp
var config = new SqlServerConfiguration
{
    MaxRetryAttempts = 3,
    RetryDelay = TimeSpan.FromSeconds(2)
};
```

### Debugging Tips

1. **Enable Logging**: Use `ILogger` to capture detailed error information
2. **Test Queries First**: Run queries in SQL Server Management Studio to verify syntax
3. **Check Connection String**: Verify all parameters are correct
4. **Monitor SQL Server**: Use SQL Server Profiler or Extended Events to trace queries
5. **Validate Schema**: Ensure table and column names exist in the database

### Getting Help

If you encounter issues not covered here:

1. Check the [NPipeline GitHub Issues](https://github.com/your-repo/NPipeline/issues)
2. Review the [SQL Server Documentation](https://learn.microsoft.com/en-us/sql/)
3. Consult the [Microsoft.Data.SqlClient Documentation](https://learn.microsoft.com/en-us/dotnet/api/microsoft.data.sqlclient)

## Related Topics

- **[NPipeline Extensions Index](../.)**: Return to the extensions overview.
- **[CSV Connector](./csv.md)**: Learn about working with CSV files.
- **[Excel Connector](./excel.md)**: Learn about working with Excel files.
- **[PostgreSQL Connector](./postgresql.md)**: Learn about PostgreSQL connector for comparison.
- **[Storage Provider Interface](../storage-providers/storage-provider.md)**: Understand the storage layer architecture.
- **[Microsoft.Data.SqlClient Documentation](https://learn.microsoft.com/en-us/dotnet/api/microsoft.data.sqlclient)**: Detailed documentation for the underlying SQL Server driver.
