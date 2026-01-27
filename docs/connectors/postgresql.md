---
title: PostgreSQL Connector
description: Read from and write to PostgreSQL databases with NPipeline using the PostgreSQL connector.
sidebar_position: 2
---

## PostgreSQL Connector

The `NPipeline.Connectors.PostgreSQL` package provides specialized source and sink nodes for working with PostgreSQL databases. This allows you to easily integrate PostgreSQL data into your pipelines as an input source or an output destination.

This connector uses the popular [Npgsql](https://www.npgsql.org/) library under the hood, providing a high-performance, fully-featured ADO.NET data provider for PostgreSQL.

## Installation

To use the PostgreSQL connector, install the `NPipeline.Connectors.PostgreSQL` NuGet package:

```bash
dotnet add package NPipeline.Connectors.PostgreSQL
```

For the core NPipeline package and other available extensions, see the [Installation Guide](../getting-started/installation.md).

## Dependency Injection Setup

The PostgreSQL connector provides extension methods for easy integration with Microsoft's dependency injection container:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.PostgreSQL.DependencyInjection;

// Register PostgreSQL connector services
var services = new ServiceCollection()
    .AddPostgresConnector(options =>
    {
        options.DefaultConnectionString = "Host=localhost;Database=mydb;Username=myuser;Password=mypass";
        options.DefaultCommandTimeout = 30;
        options.DefaultUseSslMode = false;
    })
    .BuildServiceProvider();
```

The `AddPostgresConnector()` extension method registers:

- `PostgresOptions` - Global options for PostgreSQL connector
- `PostgresConnectionPool` - Manages PostgreSQL connections
- `PostgresMapperFactory` - Creates type-safe mappers for database operations
- `PostgresSourceNodeFactory` - Factory for creating source nodes
- `PostgresSinkNodeFactory` - Factory for creating sink nodes
- `PostgresCheckpointStorage` - Checkpoint storage for resume capability (requires connection string)
- `NpgsqlDataSource` - Default data source if DefaultConnectionString is provided

### PostgresOptions Properties

The `PostgresOptions` class provides global configuration for the PostgreSQL connector when using dependency injection:

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `DefaultConnectionString` | `string?` | `null` | Default connection string to use when none is specified |
| `DefaultCommandTimeout` | `int` | `30` | Default command timeout in seconds |
| `DefaultUseSslMode` | `bool` | `false` | Whether to use SSL by default |
| `NamedConnections` | `Dictionary<string, string>` | `new()` | Dictionary of named connection strings for multiple databases |

### Named Connections

You can register named connection strings for use with multiple databases:

```csharp
var services = new ServiceCollection()
    .AddPostgresConnector()
    .AddPostgresConnection("source", "Host=localhost;Database=source;...")
    .AddPostgresConnection("destination", "Host=localhost;Database=dest;...")
    .BuildServiceProvider();
```

## `PostgresSourceNode<T>`

The `PostgresSourceNode<T>` reads data from a PostgreSQL table and emits each row as an item of type `T`.

### Source Configuration

The source node uses the shared `PostgresConfiguration`. Key options for reads:

- ConnectionString: PostgreSQL connection string used when no `NpgsqlDataSource` is supplied.
- Schema: Default schema applied to unqualified table names (default `public`).
- FetchSize: Number of rows per round-trip (default `1000`); retained for future use, current driver versions do not expose per-command fetch size.
- StreamResults: Enables sequential, low-memory reads (default `true`).
- CommandTimeout / ConnectionTimeout: Timeouts in seconds for queries and connections.
- MinPoolSize / MaxPoolSize / ReadBufferSize: Connection pool and buffer tuning knobs.
- CaseInsensitiveMapping / CacheMappingMetadata: Controls how column names are matched and cached.
- MaxRetryAttempts / RetryDelay: Retries are only attempted before the first row is yielded.

### Example: Reading from PostgreSQL

```csharp
using NPipeline;
using NPipeline.Connectors.PostgreSQL;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Npgsql;

public sealed record Product(int Id, string Name, decimal Price);

public sealed class ProductSourcePipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Option 1: Using connection string
        var options = new PostgresConfiguration
        {
            ConnectionString = "Host=localhost;Database=mydb;Username=myuser;Password=mypass",
            FetchSize = 500
        };
        
        var sourceNode = new PostgresSourceNode<Product>("SELECT id, name, price FROM products", options);
        
        // Option 2: Using NpgsqlDataSource (for connection pooling)
        var dataSource = NpgsqlDataSource.Create("Host=localhost;Database=mydb;Username=myuser;Password=mypass");
        var sourceNodeWithDataSource = new PostgresSourceNode<Product>("SELECT id, name, price FROM products", dataSource);
        
        // Option 3: Using custom row mapper
        var sourceNodeWithMapper = new PostgresSourceNode<Product>(
            "SELECT id, name, price FROM products",
            options,
            row => new Product(
                row.Get<int>("id"),
                row.Get<string>("name"),
                row.Get<decimal>("price")
            )
        );
        
        var source = builder.AddSource(sourceNode, "postgres_source");
        var sink = builder.AddSink<ConsoleSinkNode<Product>, Product>("console_sink");

        builder.Connect(source, sink);
    }
}

public sealed class ConsoleSinkNode<T> : SinkNode<T>
{
    public override async Task ExecuteAsync(
        IDataPipe<T> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"Received: {item}");
        }
    }
}

### PostgresRow

The `PostgresRow` class provides access to data from a PostgreSQL data reader:

```csharp
// Get value by column name
int id = row.Get<int>("id");
string name = row.Get<string>("name");
decimal price = row.Get<decimal>("price");

// Get nullable value
int? optionalId = row.Get<int?>("optional_id");

// Check if column exists
bool hasColumn = row.HasColumn("name");

// Get column names
var columnNames = row.ColumnNames;
```

### Custom Query Example

```csharp
var options = new PostgresConfiguration
{
    ConnectionString = "Host=localhost;Database=mydb;Username=myuser;Password=mypass",
    Query = "SELECT id, name, price FROM products WHERE active = true ORDER BY created_at DESC"
};

var sourceNode = new PostgresSourceNode<Product>(options);
```

## `PostgresSinkNode<T>`

The `PostgresSinkNode<T>` writes items from the pipeline to a PostgreSQL table.

### Sink Configuration

The same `PostgresConfiguration` type configures writes. Notable options:

- WriteStrategy: `PerRow`, `Batch` (default), or `Copy` bulk loading.
- BatchSize / MaxBatchSize: Bound batch sizes to prevent runaway memory usage.
- UseTransaction: Wrap batches in a transaction (default `true`).
- UseUpsert + UpsertConflictColumns + OnConflictAction: Control `ON CONFLICT` behavior for idempotent writes.
- Schema: Prefixed automatically when the table name is unqualified.
- UseBinaryCopy: Required for the `Copy` strategy; a misconfiguration now fails fast.
- CopyTimeout: Dedicated timeout applied to COPY operations (default `300s`).
- ContinueOnError / RowErrorHandler: Swallow or inspect row-level failures.
- MaxRetryAttempts / RetryDelay: AtLeastOnce retries now replay buffered input to avoid silent drops; expect extra memory use when retries are enabled.

### Write Strategies

The PostgreSQL connector supports multiple write strategies:

- **PerRow**: Write each item individually
- **Batch**: Write items in batches for better performance
- **Copy**: Uses the PostgreSQL COPY command for maximum throughput

### Delivery Semantics Guide

The connector supports different delivery semantics:

- **AtLeastOnce**: Items may be processed multiple times
- **ExactlyOnce**: Items are processed exactly once using transactions or idempotent operations

### Example: Writing to PostgreSQL

```csharp
using NPipeline;
using NPipeline.Connectors.PostgreSQL;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Npgsql;

public sealed record ProcessedProduct(int Id, string Name, decimal Price, string Status);

public sealed class ProductSinkPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Option 1: Using connection string
        var options = new PostgresConfiguration
        {
            ConnectionString = "Host=localhost;Database=mydb;Username=myuser;Password=mypass",
            TableName = "processed_products",
            WriteStrategy = PostgresWriteStrategy.Batch,
            BatchSize = 500,
            UseTransaction = true
        };
        
        var sinkNode = new PostgresSinkNode<ProcessedProduct>("processed_products", options);
        
        // Option 2: Using NpgsqlDataSource (for connection pooling)
        var dataSource = NpgsqlDataSource.Create("Host=localhost;Database=mydb;Username=myuser;Password=mypass");
        var sinkNodeWithDataSource = new PostgresSinkNode<ProcessedProduct>(dataSource, "processed_products");
        
        var source = builder.AddSource<InMemorySourceNode<ProcessedProduct>, ProcessedProduct>("source");
        var sink = builder.AddSink(sinkNode, "postgres_sink");

        builder.Connect(source, sink);
    }
}
```

### Upsert Example

```csharp
var options = new PostgresSinkOptions
{
    ConnectionString = "Host=localhost;Database=mydb;Username=myuser;Password=mypass",
    TableName = "products",
    UseUpsert = true,
    UpsertConflictColumns = new[] { "id" },
    OnConflictAction = OnConflictAction.Update
};

var sinkNode = new PostgresSinkNode<Product>(options);
```

## Attribute-Based Mapping

The PostgreSQL connector supports attribute-based mapping for type-safe configuration:

### PostgresTable Attribute

```csharp
[PostgresTable("products", Schema = "inventory")]
public sealed record Product(int Id, string Name, decimal Price);
```

- **`TableName`**: Table name (defaults to class name)
- **`Schema`**: Schema name (default: "public")

### PostgresColumn Attribute

```csharp
[PostgresTable("products")]
public sealed record Product(
    [PostgresColumn("product_id")] int Id,
    [PostgresColumn("product_name")] string Name,
    [PostgresColumn("unit_price")] decimal Price
);
```

- **`ColumnName`**: Column name (defaults to property name)

## Checkpointing

The PostgreSQL connector includes `PostgresCheckpointStorage` for resume capability:

```csharp
using NPipeline.Connectors.PostgreSQL.Checkpointing;

var checkpointStorage = new PostgresCheckpointStorage(
    "Host=localhost;Database=mydb;Username=myuser;Password=mypass",
    "pipeline_checkpoints",
    "public"
);

// Or use DI to get registered instance
var checkpointStorage = serviceProvider.GetRequiredService<ICheckpointStorage>();
```

## Advanced Topics

### Error Handling

The connector provides detailed error handling with PostgreSQL-specific exception mapping. See [PostgreSQL Error Handling](../postgresql-connector-error-handling.md) for more details.

### Delivery Semantics

Learn about different delivery semantics and when to use each in [PostgreSQL Delivery Semantics](../postgresql-connector-delivery-semantics.md).

### Write Strategies Guide

Understand the different write strategies and their performance characteristics in [PostgreSQL Write Strategies](../postgresql-connector-write-strategies.md).

### Checkpointing Guide

Learn about checkpointing and resume capability in [PostgreSQL Checkpointing](../postgresql-connector-checkpointing.md).

## Related Topics

- **[NPipeline Connectors Index](./index.md)**: Return to connectors overview.
- **[Common Patterns](../core-concepts/common-patterns.md)**: See connectors in practical examples.
- **[Installation](../getting-started/installation.md)**: Review installation options for connector packages.
