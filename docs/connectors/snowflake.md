---
title: Snowflake Connector
description: Read from and write to Snowflake databases with NPipeline using the Snowflake connector.
sidebar_position: 6
---

## Snowflake Connector

The `NPipeline.Connectors.Snowflake` package provides specialized source and sink nodes for working with Snowflake cloud data warehouses. This allows you to easily integrate Snowflake data into your pipelines as an input source or an output destination.

This connector uses the [Snowflake.Data](https://github.com/snowflakedb/snowflake-connector-net) official ADO.NET driver under the hood, providing reliable streaming reads, per-row and batched writes, bulk loading via staged copy (PUT + COPY INTO), and in-memory checkpointing for transient recovery.

## Installation

To use the Snowflake connector, install the `NPipeline.Connectors.Snowflake` NuGet package:

```bash
dotnet add package NPipeline.Connectors.Snowflake
```

For the core NPipeline package and other available extensions, see the [Installation Guide](../getting-started/installation.md).

## Features

The Snowflake connector provides the following capabilities:

- **Source Node**: Read data from Snowflake tables and views
- **Sink Node**: Write data to Snowflake tables
- **Write Strategies**: Support for PerRow, Batch, and StagedCopy (PUT + COPY INTO) write strategies
- **Upsert Support**: MERGE-based insert-or-update semantics with configurable key columns
- **Delivery Semantics**: AtLeastOnce, AtMostOnce, and ExactlyOnce delivery guarantees
- **Checkpointing**: Snowflake-based checkpoint storage for resumable pipelines
- **Connection Pooling**: Efficient connection management with named connections
- **Attribute Mapping**: Support for `[Column]`, `[IgnoreColumn]`, and `[SnowflakeColumn]` attributes
- **Common Attributes**: Cross-connector `ColumnAttribute` and `IgnoreColumnAttribute` support
- **Convention Mapping**: Automatic PascalCase to UPPER_SNAKE_CASE convention mapping
- **Custom Mappers**: `Func<T, IEnumerable<DatabaseParameter>>` for complete control
- **Error Handling**: Retry logic for transient Snowflake errors with exponential backoff
- **Streaming Results**: Fetch data in streams to reduce memory usage
- **Query Tagging**: Automatic `QUERY_TAG` integration for observability
- **Authentication**: Password and key-pair (JWT) authentication support

## Dependency Injection

The Snowflake connector supports dependency injection for managing connection pools and node factories. This is the recommended approach for production applications.

### Registering the Connector

Use `AddSnowflakeConnector` to register a shared connection pool and factories for creating nodes:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.Snowflake.DependencyInjection;

var services = new ServiceCollection()
    .AddSnowflakeConnector(options =>
    {
        // Set a default connection string
        options.DefaultConnectionString =
            "account=myaccount;host=myaccount.snowflakecomputing.com;user=myuser;password=mypassword;db=mydb;schema=PUBLIC;warehouse=COMPUTE_WH";

        // Add named connections for different databases
        options.AddOrUpdateConnection("analytics",
            "account=myaccount;host=myaccount.snowflakecomputing.com;user=myuser;password=mypassword;db=analytics;schema=PUBLIC;warehouse=ANALYTICS_WH");
        options.AddOrUpdateConnection("staging",
            "account=myaccount;host=myaccount.snowflakecomputing.com;user=myuser;password=mypassword;db=staging;schema=PUBLIC;warehouse=ETL_WH");

        // Configure default connection-level settings
        options.DefaultConfiguration = new SnowflakeConfiguration
        {
            StreamResults = true,
            FetchSize = 10_000,
            MaxRetryAttempts = 3,
            RetryDelay = TimeSpan.FromSeconds(2),
            CommandTimeout = 300
        };
    })
    .BuildServiceProvider();

// Resolve services from the container
var pool = services.GetRequiredService<ISnowflakeConnectionPool>();
var sourceFactory = services.GetRequiredService<SnowflakeSourceNodeFactory>();
var sinkFactory = services.GetRequiredService<SnowflakeSinkNodeFactory>();
```

### Configuration Options

- **`DefaultConnectionString`**: Optional connection string used when no named connection is specified. Can be omitted if you only use named connections.
- **`DefaultConfiguration`**: Controls connection-level settings (timeouts, pool sizing, fetch size) applied when the pool builds connections.
- **`AddOrUpdateConnection(name, connectionString)`**: Adds or updates a named connection. Multiple connections can be configured for different databases or warehouses.
- **`AddSnowflakeConnection`/`AddDefaultSnowflakeConnection`**: Configure the same `SnowflakeOptions` and do not replace previously configured values.

### Why Use Dependency Injection?

Using dependency injection provides several benefits:

- **Connection Pool Management**: The shared connection pool efficiently manages Snowflake connections across multiple nodes
- **Configuration Centralization**: All Snowflake connections are configured in one place
- **Testability**: Easy to mock or replace dependencies in unit tests
- **Lifetime Management**: Services are properly disposed when the application shuts down

## Common Attributes

The Snowflake connector supports common attributes from `NPipeline.Connectors.Attributes` that work across all connectors, as well as Snowflake-specific attributes that extend the common attributes with database-specific features.

### `[Column]` Attribute

The `[Column]` attribute (from `NPipeline.Connectors.Attributes`) is a common attribute that allows you to specify column names and control property mapping across all connectors. It provides:

- **`Name`**: The column name in the database
- **`Ignore`**: When `true`, skips mapping this property

This attribute is recommended for simple scenarios where you only need basic column mapping.

```csharp
using NPipeline.Connectors.Attributes;

public class Customer
{
    [Column("ID")]
    public int Id { get; set; }

    [Column("FIRST_NAME")]
    public string FirstName { get; set; } = string.Empty;

    [Column("LAST_NAME")]
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

### Snowflake-Specific Attributes

The Snowflake connector provides `[SnowflakeColumn]` and `[SnowflakeTable]` attributes that extend the common attributes with database-specific functionality:

- **`[SnowflakeColumn]`**: Extends `[Column]` with additional properties:
  - **`DbType`**: Specifies the database data type for the column
  - **`NativeTypeName`**: Specifies the Snowflake-native type name (e.g., `TIMESTAMP_NTZ`, `NUMBER(18,2)`, `VARIANT`)
  - **`Size`**: Sets the size/length for character and numeric types
  - **`PrimaryKey`**: Indicates whether the column is a primary key (used for upsert/MERGE)
  - **`Identity`**: Indicates whether the column is an auto-increment identity column

- **`[SnowflakeTable]`**: Specifies the target table:
  - **`Name`**: The table name (typically uppercase in Snowflake)
  - **`Schema`**: The schema name (defaults to `PUBLIC`)
  - **`Database`**: Optional database name

The `[IgnoreColumn]` attribute from `NPipeline.Connectors.Attributes` covers all ignore requirements and works identically across all connectors.

```csharp
using NPipeline.Connectors.Snowflake.Mapping;
using NPipeline.Connectors.Attributes;
using System.Data;

[SnowflakeTable("CUSTOMERS", Schema = "PUBLIC")]
public class Customer
{
    [SnowflakeColumn("ID", PrimaryKey = true)]
    public int Id { get; set; }

    [SnowflakeColumn("FIRST_NAME")]
    public string FirstName { get; set; } = string.Empty;

    [SnowflakeColumn("LAST_NAME")]
    public string LastName { get; set; } = string.Empty;

    [SnowflakeColumn("EMAIL")]
    public string Email { get; set; } = string.Empty;

    [SnowflakeColumn("CREATED_AT", DbType = DbType.DateTime2, NativeTypeName = "TIMESTAMP_NTZ")]
    public DateTime CreatedAt { get; set; }

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}
```

### Choosing Between Common and Snowflake-Specific Attributes

**Use common attributes (`[Column]`, `[IgnoreColumn]`) when:**

- You want code that works across multiple connectors
- You only need basic column mapping functionality
- You prefer using standard attributes provided by the core library
- Your database schema follows standard conventions

**Use Snowflake-specific attributes (`[SnowflakeColumn]`, `[SnowflakeTable]`) when:**

- You need to specify Snowflake-native types explicitly (e.g., `TIMESTAMP_NTZ`, `NUMBER(18,2)`, `VARIANT`)
- You need to mark primary key columns for upsert/MERGE operations
- You need to mark identity columns for auto-increment
- You want to specify the table schema and database explicitly
- You're maintaining existing code that already uses these attributes

Both attribute types are fully supported and will continue to work in future versions.

## `SnowflakeSourceNode<T>`

The `SnowflakeSourceNode<T>` reads data from a Snowflake database and emits each row as an item of type `T`.

### Source Configuration

The constructor for `SnowflakeSourceNode<T>` provides multiple overloads for flexibility:

```csharp
// Using connection string
public SnowflakeSourceNode<T>(
    string connectionString,
    string query,
    SnowflakeConfiguration? configuration = null)

// Using connection string with custom row mapper
public SnowflakeSourceNode<T>(
    string connectionString,
    string query,
    Func<SnowflakeRow, T> rowMapper,
    SnowflakeConfiguration? configuration = null)

// Using connection pool
public SnowflakeSourceNode<T>(
    ISnowflakeConnectionPool pool,
    string query,
    SnowflakeConfiguration? configuration = null,
    DatabaseParameter[]? parameters = null,
    bool continueOnError = false,
    string? connectionName = null)

// Using connection pool with custom row mapper
public SnowflakeSourceNode<T>(
    ISnowflakeConnectionPool pool,
    string query,
    Func<SnowflakeRow, T> rowMapper,
    SnowflakeConfiguration? configuration = null,
    DatabaseParameter[]? parameters = null,
    bool continueOnError = false,
    string? connectionName = null)
```

- **`connectionString`**: Snowflake connection string (e.g., `"account=myaccount;host=myaccount.snowflakecomputing.com;user=myuser;password=mypassword;db=mydb"`)
- **`pool`**: Shared connection pool from dependency injection
- **`query`**: SQL query to execute
- **`rowMapper`**: Custom function to map a `SnowflakeRow` to type `T`. When omitted, uses convention-based mapping
- **`parameters`**: Optional parameters for the SQL query
- **`continueOnError`**: Whether to skip row-level mapping errors
- **`connectionName`**: Name of a configured connection from the pool
- **`configuration`**: Optional configuration object for customizing behavior

> **Tip:** When you need to provide custom configuration or a row mapper, instantiate `SnowflakeSourceNode<T>` yourself and register it via `builder.AddSource(new SnowflakeSourceNode<Order>(...), "snowflake_source")`. The builder handles registration and disposal for you automatically.

### Example: Reading from Snowflake

Let's assume you have a Snowflake table named `ORDERS` with the following structure:

| ORDER_ID | CUSTOMER_ID | TOTAL | STATUS |
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
using NPipeline.Connectors.Snowflake;
using NPipeline.DataFlow.DataPipes;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Tracing;

public sealed record Order(int OrderId, int CustomerId, decimal Total, string Status);

public sealed class SnowflakeReaderPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceNode = new SnowflakeSourceNode<Order>(
            connectionString: "account=myaccount;host=myaccount.snowflakecomputing.com;user=myuser;password=mypassword;db=mydb;schema=PUBLIC;warehouse=COMPUTE_WH",
            query: "SELECT ORDER_ID, CUSTOMER_ID, TOTAL, STATUS FROM PUBLIC.ORDERS ORDER BY ORDER_ID",
            configuration: new SnowflakeConfiguration
            {
                StreamResults = true,
                FetchSize = 10_000
            });
        var source = builder.AddSource(sourceNode, "snowflake_source");
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
        var runner = PipelineRunner.Create();
        await runner.RunAsync<SnowflakeReaderPipeline>();

        Console.WriteLine("Snowflake reading completed");
    }
}
```

**Expected Output:**

```text
Received: Order { OrderId = 1, CustomerId = 100, Total = 150.00, Status = completed }
Received: Order { OrderId = 2, CustomerId = 101, Total = 75.50, Status = pending }
Received: Order { OrderId = 3, CustomerId = 102, Total = 200.00, Status = shipped }
Snowflake reading completed
```

### Example: Using a Connection Pool

When using dependency injection, you can leverage the shared connection pool:

```csharp
public sealed class PooledSnowflakeReaderPipeline : IPipelineDefinition
{
    private readonly ISnowflakeConnectionPool _pool;

    public PooledSnowflakeReaderPipeline(ISnowflakeConnectionPool pool)
    {
        _pool = pool;
    }

    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceNode = new SnowflakeSourceNode<Order>(
            pool: _pool,
            query: "SELECT ORDER_ID, CUSTOMER_ID, TOTAL, STATUS FROM PUBLIC.ORDERS ORDER BY ORDER_ID",
            connectionName: "analytics");
        var source = builder.AddSource(sourceNode, "snowflake_source");
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
        var sourceNode = new SnowflakeSourceNode<Order>(
            connectionString: "account=myaccount;host=myaccount.snowflakecomputing.com;user=myuser;password=mypassword;db=mydb;schema=PUBLIC;warehouse=COMPUTE_WH",
            query: "SELECT ORDER_ID, CUSTOMER_ID, TOTAL, STATUS, CREATED_AT FROM PUBLIC.ORDERS",
            rowMapper: row => new Order(
                row.Get<int>("ORDER_ID"),
                row.Get<int>("CUSTOMER_ID"),
                row.Get<decimal>("TOTAL"),
                row.Get<string>("STATUS"),
                row.Get<DateTime>("CREATED_AT")));
        var source = builder.AddSource(sourceNode, "snowflake_source");
        var sink = builder.AddSink<ConsoleSinkNode, Order>("console_sink");

        builder.Connect(source, sink);
    }
}
```

## `SnowflakeSinkNode<T>`

The `SnowflakeSinkNode<T>` writes items from the pipeline to a Snowflake database table.

### Sink Configuration

The constructor for `SnowflakeSinkNode<T>` provides multiple overloads:

```csharp
// Using connection string
public SnowflakeSinkNode<T>(
    string connectionString,
    string tableName,
    SnowflakeConfiguration? configuration = null,
    Func<T, IEnumerable<DatabaseParameter>>? customMapper = null)

// Using connection pool
public SnowflakeSinkNode<T>(
    ISnowflakeConnectionPool pool,
    string tableName,
    SnowflakeConfiguration? configuration = null,
    Func<T, IEnumerable<DatabaseParameter>>? customMapper = null,
    string? connectionName = null)
```

- **`connectionString`**: Snowflake connection string
- **`pool`**: Shared connection pool from dependency injection
- **`tableName`**: Name of the target table (unqualified, typically uppercase)
- **`customMapper`**: Custom function to map type `T` to database parameters
- **`connectionName`**: Name of a configured connection from the pool
- **`configuration`**: Optional configuration object for customizing behavior

> **Note:** Provide the schema via `SnowflakeConfiguration.Schema` and pass the unqualified table name.

### Write Strategies

The connector supports three write strategies:

#### PerRow Strategy

Writes each row individually with a separate `INSERT` statement. This provides:

- Immediate visibility of each row
- Better error isolation (one failed insert doesn't affect others)
- Higher overhead for large datasets
- Full transaction support

#### Batch Strategy

Buffers multiple rows and issues a single multi-row `INSERT` statement. This provides:

- Better performance for moderate datasets
- Reduced database round-trips
- All-or-nothing semantics within a batch
- Full transaction support
- MERGE-based upsert support

#### StagedCopy Strategy

Uses Snowflake's internal staging for maximum throughput. This provides:

- Highest performance for bulk loading (10K+ rows)
- Data is written to CSV, staged via `PUT`, then loaded via `COPY INTO`
- Configurable compression (GZIP, SNAPPY)
- Automatic file cleanup after copy (`PurgeAfterCopy`)
- **Not transactional** — use PerRow or Batch for ExactlyOnce semantics

```csharp
var configuration = new SnowflakeConfiguration
{
    WriteStrategy = SnowflakeWriteStrategy.StagedCopy,
    StageName = "~",  // User stage (default)
    FileFormat = "CSV",
    CopyCompression = "GZIP",
    PurgeAfterCopy = true,
    CommandTimeout = 300,
    Schema = "PUBLIC"
};

var sink = new SnowflakeSinkNode<Order>(
    connectionString,
    "ORDERS",
    configuration: configuration);
```

**StagedCopy Configuration Options:**

- **`StageName`**: Stage name for file staging (default: `~` for user stage)
- **`StageFilePrefix`**: Prefix for staged files (default: `npipeline_`)
- **`FileFormat`**: File format for staging (default: `CSV`)
- **`CopyCompression`**: Compression format (default: `GZIP`)
- **`PurgeAfterCopy`**: Whether to remove staged files after COPY INTO (default: `true`)
- **`OnErrorAction`**: Error handling for COPY INTO (default: `ABORT_STATEMENT`)

### Write Strategy Comparison

| Strategy | Throughput | Latency | Error Isolation | Transactional | Use Case |
|----------|------------|---------|-----------------|---------------|----------|
| PerRow | Low | Low | High | Yes | Real-time, small batches |
| Batch | Medium | Medium | Medium | Yes | Moderate volumes, ETL |
| StagedCopy | Very High | High | Low | No | Large bulk loads, data warehouse |

### Example: Writing to Snowflake

```csharp
using NPipeline.Connectors.Snowflake;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed record OrderSummary(int Id, string CustomerName, decimal Total, string Status);

public sealed class SnowflakeWriterPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<InMemorySourceNode<OrderSummary>, OrderSummary>("source");

        var sinkNode = new SnowflakeSinkNode<OrderSummary>(
            connectionString: "account=myaccount;host=myaccount.snowflakecomputing.com;user=myuser;password=mypassword;db=mydb;schema=PUBLIC;warehouse=COMPUTE_WH",
            tableName: "ORDER_SUMMARY",
            configuration: new SnowflakeConfiguration
            {
                WriteStrategy = SnowflakeWriteStrategy.Batch,
                BatchSize = 1_000,
                MaxBatchSize = 16_384,
                UseTransaction = true,
                Schema = "PUBLIC"
            });
        var sink = builder.AddSink(sinkNode, "snowflake_sink");

        builder.Connect(source, sink);
    }
}
```

### Example: Using StagedCopy for Bulk Loading

```csharp
var sinkNode = new SnowflakeSinkNode<LargeDataset>(
    connectionString,
    "LARGE_TABLE",
    configuration: new SnowflakeConfiguration
    {
        WriteStrategy = SnowflakeWriteStrategy.StagedCopy,
        StageName = "~",
        FileFormat = "CSV",
        CopyCompression = "GZIP",
        PurgeAfterCopy = true,
        Schema = "PUBLIC"
    });
```

The StagedCopy strategy works as follows:

1. **CSV Generation**: Data is serialized to a temporary CSV file on disk
2. **PUT Upload**: The file is uploaded to Snowflake's internal stage using `PUT file:///path @stage`
3. **COPY INTO**: Data is loaded from the stage into the target table using `COPY INTO`
4. **Cleanup**: Staged files are purged if `PurgeAfterCopy = true`

This is Snowflake's recommended approach for loading large volumes of data and has no equivalent in the SQL Server or PostgreSQL connectors.

### Example: Using a Custom Parameter Mapper

```csharp
Func<Order, IEnumerable<DatabaseParameter>> mapper = order => new[]
{
    new DatabaseParameter("@p0", order.OrderId),
    new DatabaseParameter("@p1", order.CustomerId),
    new DatabaseParameter("@p2", order.Total),
    new DatabaseParameter("@p3", order.Status)
};

var sinkNode = new SnowflakeSinkNode<Order>(
    connectionString,
    "ORDERS",
    configuration: new SnowflakeConfiguration
    {
        WriteStrategy = SnowflakeWriteStrategy.PerRow,
        UseTransaction = true,
        Schema = "PUBLIC"
    },
    customMapper: mapper);
```

## Configuration Reference

### SnowflakeConfiguration

The `SnowflakeConfiguration` class provides comprehensive options for configuring Snowflake read and write operations.

#### Properties

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `ConnectionString` | `string` | `""` | Snowflake connection string. Not required when using a connection pool. |
| `Schema` | `string` | `"PUBLIC"` | Default schema name for table operations. |
| `StreamResults` | `bool` | `true` | Enables streaming of results to reduce memory usage for large result sets. |
| `FetchSize` | `int` | `10,000` | Number of rows to fetch per round-trip when streaming. Larger values reduce round-trips but use more memory. |
| `WriteStrategy` | `SnowflakeWriteStrategy` | `Batch` | Write strategy for sink operations (PerRow, Batch, StagedCopy). |
| `BatchSize` | `int` | `1,000` | Target batch size for batch write operations. |
| `MaxBatchSize` | `int` | `16,384` | Maximum batch size to prevent runaway buffers. `BatchSize` is clamped to this value. |
| `UseTransaction` | `bool` | `true` | Wraps write operations in a transaction for atomicity (PerRow and Batch only). |
| `UseUpsert` | `bool` | `false` | Enables MERGE-based upsert semantics. |
| `UpsertKeyColumns` | `string[]?` | `null` | Columns that form the merge key for upsert. |
| `OnMergeAction` | `OnMergeAction` | `Update` | Merge resolution action (`Update`, `Ignore`, or `Delete`). |
| `CommandTimeout` | `int` | `300` | Command timeout in seconds (5 minutes default, suitable for Snowflake's compute provisioning). |
| `MaxRetryAttempts` | `int` | `3` | Maximum number of retry attempts for transient failures. |
| `RetryDelay` | `TimeSpan` | `TimeSpan.FromSeconds(2)` | Base delay between retry attempts (exponential backoff with jitter, 60s cap). |
| `MaxPoolSize` | `int` | `10` | Maximum connection pool size. Conservative default since Snowflake connections are expensive. |
| `CaseInsensitiveMapping` | `bool` | `true` | Enables case-insensitive column name mapping. Handles uppercase mismatch between C# PascalCase and Snowflake UPPER_CASE. |
| `ContinueOnError` | `bool` | `false` | Continues processing when per-property mapping errors occur. |
| `QueryTag` | `string` | `"NPipeline"` | Query tag for Snowflake observability (`ALTER SESSION SET QUERY_TAG`). |
| `StageName` | `string` | `"~"` | Stage name for StagedCopy strategy (`~` = user stage). |
| `StageFilePrefix` | `string` | `"npipeline_"` | Prefix for staged files. |
| `FileFormat` | `string` | `"CSV"` | File format for StagedCopy (CSV or PARQUET). |
| `CopyCompression` | `string` | `"GZIP"` | Compression for StagedCopy (GZIP or SNAPPY). |
| `PurgeAfterCopy` | `bool` | `true` | Remove staged files after COPY INTO completes. |
| `OnErrorAction` | `string` | `"ABORT_STATEMENT"` | Error handling for COPY INTO. |
| `CheckpointTableName` | `string` | `"PIPELINE_CHECKPOINTS"` | Table name for Snowflake-based checkpoint storage. |

### SnowflakeWriteStrategy

Enum defining write strategies for the sink node.

| Value | Description |
| --- | --- |
| `PerRow` | Writes each row individually with a separate `INSERT` statement. Best for real-time processing and per-row error handling. |
| `Batch` | Buffers multiple rows and issues a single multi-value `INSERT` statement. Best for moderate-volume operations and high throughput. |
| `StagedCopy` | Uses Snowflake's PUT + COPY INTO for maximum throughput. Best for bulk loading large datasets (10K+ rows). |

### OnMergeAction

Enum defining actions to take when a MERGE statement encounters a match.

| Value | Description |
| --- | --- |
| `Update` | Updates non-key columns using values from the incoming row. Generates `WHEN MATCHED THEN UPDATE SET ...`. |
| `Ignore` | Leaves the existing row unchanged when a match is found. Omits the `WHEN MATCHED` clause entirely — only new rows are inserted. |
| `Delete` | Removes the existing row when a match is found. Generates `WHEN MATCHED THEN DELETE`. |

## Upsert Operations

The connector supports Snowflake's `MERGE` statement for upsert operations, allowing you to insert rows or update them if they already exist.

### Basic Upsert Configuration

Enable upsert by setting `UseUpsert = true` and specifying the key columns:

```csharp
var configuration = new SnowflakeConfiguration
{
    UseUpsert = true,
    UpsertKeyColumns = new[] { "CUSTOMER_ID" },
    OnMergeAction = OnMergeAction.Update,
    WriteStrategy = SnowflakeWriteStrategy.Batch,
    Schema = "PUBLIC"
};

var sink = new SnowflakeSinkNode<Customer>(
    connectionString,
    "CUSTOMERS",
    configuration: configuration);
```

### Merge Actions

#### OnMergeAction.Update

Updates non-key columns with values from the incoming row when a match is found:

```sql
MERGE INTO "PUBLIC"."CUSTOMERS" AS target
USING (SELECT @p0 AS "CUSTOMER_ID", @p1 AS "NAME", @p2 AS "EMAIL") AS source
ON target."CUSTOMER_ID" = source."CUSTOMER_ID"
WHEN MATCHED THEN
    UPDATE SET "NAME" = source."NAME", "EMAIL" = source."EMAIL"
WHEN NOT MATCHED THEN
    INSERT ("CUSTOMER_ID", "NAME", "EMAIL") VALUES (source."CUSTOMER_ID", source."NAME", source."EMAIL");
```

#### OnMergeAction.Ignore

Leaves the existing row unchanged. Only new (unmatched) rows are inserted:

```csharp
var configuration = new SnowflakeConfiguration
{
    UseUpsert = true,
    UpsertKeyColumns = new[] { "CUSTOMER_ID" },
    OnMergeAction = OnMergeAction.Ignore
};
```

#### OnMergeAction.Delete

Deletes the matching row when the source row is present:

```csharp
var configuration = new SnowflakeConfiguration
{
    UseUpsert = true,
    UpsertKeyColumns = new[] { "CUSTOMER_ID" },
    OnMergeAction = OnMergeAction.Delete
};
```

### Composite Key Upsert

For tables with composite unique constraints:

```csharp
public record OrderItem(int OrderId, int ProductId, int Quantity, decimal UnitPrice);

var configuration = new SnowflakeConfiguration
{
    UseUpsert = true,
    UpsertKeyColumns = new[] { "ORDER_ID", "PRODUCT_ID" },
    OnMergeAction = OnMergeAction.Update,
    WriteStrategy = SnowflakeWriteStrategy.Batch,
    Schema = "PUBLIC"
};
```

## Checkpointing

The Snowflake connector provides `SnowflakeCheckpointStorage` for persisting checkpoint state to a Snowflake table. This enables resumable pipelines that can recover from failures.

### Configuration

```csharp
var checkpointStorage = new SnowflakeCheckpointStorage(
    connectionString,
    new SnowflakeConfiguration
    {
        CheckpointTableName = "PIPELINE_CHECKPOINTS",
        Schema = "PUBLIC"
    });
```

The checkpoint table is created automatically with the following schema:

```sql
CREATE TABLE IF NOT EXISTS "PUBLIC"."PIPELINE_CHECKPOINTS" (
    "ID" NUMBER AUTOINCREMENT PRIMARY KEY,
    "PIPELINE_ID" VARCHAR(500) NOT NULL,
    "NODE_ID" VARCHAR(500) NOT NULL,
    "CHECKPOINT_KEY" VARCHAR(500) NOT NULL,
    "CHECKPOINT_VALUE" VARCHAR NOT NULL,
    "CHECKPOINT_DATA" VARIANT,
    "CREATED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "UPDATED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
```

Checkpoints are stored and retrieved using Snowflake's `MERGE INTO` for upsert semantics.

## Error Handling

### Transient Error Detection

The connector automatically detects transient Snowflake errors and retries operations using exponential backoff with jitter:

- **Snowflake error codes**: 390114 (session expired), 390144 (login timeout), 200002 (general network), 625 (connection reset), 604 (timeout)
- **Exception types**: `TimeoutException`, `HttpRequestException`, `SocketException`
- **Message-based detection**: "timeout", "connection", "network", "throttl"

### Retry Configuration

```csharp
var configuration = new SnowflakeConfiguration
{
    MaxRetryAttempts = 3,
    RetryDelay = TimeSpan.FromSeconds(2)  // Exponential backoff: 2s, 4s, 8s (capped at 60s)
};
```

The retry delay uses exponential backoff with ±25% jitter and a 60-second cap:

$$\text{delay} = \min(\text{baseDelay} \times 2^{\text{attempt}} \times (0.75 + 0.5 \times \text{random}), 60\text{s})$$

### Row-Level Error Handling

Configure error handling behavior for individual rows:

```csharp
var configuration = new SnowflakeConfiguration
{
    ContinueOnError = true,
    RowErrorHandler = (exception, row, rowIndex) =>
    {
        Console.WriteLine($"Error on row {rowIndex}: {exception.Message}");
        return true;  // Skip the row and continue
    }
};
```

## Snowflake-Specific Considerations

### Identifier Handling

Snowflake uses **uppercase identifiers** by default. Unquoted identifiers are automatically uppercased by Snowflake. The connector:

- Quotes all identifiers with `"double quotes"` to preserve case
- Defaults `CaseInsensitiveMapping = true` to handle uppercase mismatch
- Convention-based mapping maps PascalCase properties to UPPER_CASE columns

### Connection Latency

Snowflake is a cloud service with higher connection latency than on-premises databases:

- Default `FetchSize = 10,000` (vs 1,000 for SqlServer) to reduce round-trips
- Default `RetryDelay = 2s` (vs 1s for local databases)
- Default `CommandTimeout = 300` (5 minutes) since Snowflake queries involve compute provisioning
- Conservative `MaxPoolSize = 10` since each connection is expensive

### Query Tagging

The connector automatically sets `QUERY_TAG` for observability:

```sql
ALTER SESSION SET QUERY_TAG = 'NPipeline';
```

This integrates with Snowflake's query history, making it easy to identify NPipeline operations in the Snowflake UI.

### Semi-Structured Data

Snowflake natively supports `VARIANT`, `OBJECT`, and `ARRAY` types:

- `SnowflakeRow.Get<string>("variant_column")` — returns the raw JSON string
- Write mapping: pass `string` (valid JSON) to write to a `VARIANT` column

### Authentication

The connector supports all authentication methods provided by the `Snowflake.Data` driver:

- **Password authentication**: `user=myuser;password=mypassword`
- **Key-pair authentication**: `authenticator=snowflake_jwt;private_key_file=/path/to/key.p8`
- **External browser (SSO)**: `authenticator=externalbrowser`
- **OAuth**: `authenticator=oauth;token=<oauth_token>`

## Performance Tuning

### Read Performance

- Set `StreamResults = true` for large result sets
- Increase `FetchSize` (default 10,000) for read-heavy workloads
- Use a properly sized Snowflake warehouse for compute-intensive queries
- Consider query result caching for repeated queries

### Write Performance

- Use **StagedCopy** for bulk loads over 10,000 rows (3x+ faster than Batch)
- Use **Batch** with appropriate `BatchSize` for moderate volumes (2x+ faster than PerRow)
- The maximum batch size is 16,384 rows per multi-row INSERT
- Consider warehouse size for write-heavy workloads

### Connection Management

- Use connection pooling via dependency injection for production scenarios
- Keep `MaxPoolSize` conservative (default 10) since Snowflake connections are expensive
- Use named connections to route traffic to appropriate warehouses

## Limitations

- **StagedCopy is not transactional**: The PUT + COPY INTO workflow cannot be wrapped in a single transaction. Use PerRow or Batch for ExactlyOnce semantics.
- **No local testing**: Snowflake is a cloud-only service. Integration tests require a Snowflake account and are gated by environment variables.
- **Connection latency**: Cloud connectivity introduces higher latency compared to on-premises databases. Tune `FetchSize` and `BatchSize` accordingly.
- **Internal stages only**: V1 supports only internal stages (`~` user stage and named internal stages). External stages (S3, Azure, GCS) are planned for future releases.

## Troubleshooting

### Connection Issues

| Problem | Solution |
|---------|----------|
| `SnowflakeDbException: Unable to connect` | Verify account identifier, host, and network connectivity |
| `Login timeout` | Check credentials, verify warehouse is not suspended |
| `Connection refused` | Ensure host includes `.snowflakecomputing.com`, check firewall rules |
| `SSL/TLS errors` | Ensure system certificates are up to date |

### Write Issues

| Problem | Solution |
|---------|----------|
| `StagedCopy fails on PUT` | Verify user has stage access permissions |
| `Batch INSERT slow` | Reduce batch size or switch to StagedCopy |
| `MERGE fails` | Verify `UpsertKeyColumns` match primary key or unique constraint |
| `Identifier not found` | Snowflake uppercases unquoted identifiers; use exact case in attributes |

### Mapping Issues

| Problem | Solution |
|---------|----------|
| `Column not found` | Enable `CaseInsensitiveMapping = true` (default) |
| `Type mismatch` | Use `NativeTypeName` to specify Snowflake-native types |
| `Computed property persisted` | Add `[IgnoreColumn]` attribute to computed properties |

## Next Steps

- **[Sample Application](../../samples/Sample_SnowflakeConnector/README.md)**: See the full working sample
- **[SQL Server Connector](sqlserver.md)**: Compare with the SQL Server connector
- **[PostgreSQL Connector](postgres.md)**: Compare with the PostgreSQL connector
- **[Common Patterns](../core-concepts/common-patterns.md)**: See connectors in practical examples
- **[Installation](../getting-started/installation.md)**: Review installation options for connector packages
