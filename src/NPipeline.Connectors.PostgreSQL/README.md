# NPipeline.Connectors.PostgreSQL

A PostgreSQL connector for NPipeline data pipelines. Provides source and sink nodes for reading from and writing to PostgreSQL databases with support for convention-based mapping, custom mappers, connection pooling, and streaming.

## Installation

```bash
dotnet add package NPipeline.Connectors.PostgreSQL
```

## Quick Start

### Reading from PostgreSQL

```csharp
using NPipeline.Connectors.PostgreSQL;
using NPipeline.Pipeline;

// Define your model
public record Customer(int Id, string Name, string Email);

// Create a source node
var connectionString = "Host=localhost;Database=mydb;Username=postgres;Password=password";
var source = new PostgresSourceNode<Customer>(
    connectionString,
    "SELECT id, name, email FROM customers"
);

// Use in a pipeline
var pipeline = new PipelineBuilder()
    .AddSource(source, "customer_source")
    .AddSink<ConsoleSinkNode<Customer>, Customer>("console_sink")
    .Build();

await pipeline.RunAsync();
```

### Writing to PostgreSQL

```csharp
using NPipeline.Connectors.PostgreSQL;
using NPipeline.Pipeline;

// Define your model
public record Customer(int Id, string Name, string Email);

// Create a sink node
var connectionString = "Host=localhost;Database=mydb;Username=postgres;Password=password";
var sink = new PostgresSinkNode<Customer>(
    connectionString,
    "customers",
    writeStrategy: PostgresWriteStrategy.Batch
);

// Use in a pipeline
var pipeline = new PipelineBuilder()
    .AddSource<InMemorySourceNode<Customer>, Customer>("source")
    .AddSink(sink, "customer_sink")
    .Build();

await pipeline.RunAsync();
```

## Key Features

- **Streaming reads** - Process large result sets with minimal memory usage
- **Batch writes** - High-performance bulk inserts with configurable batch sizes
- **Connection pooling** - Efficient connection management via dependency injection
- **Convention-based mapping** - Automatic `PascalCase` to `snake_case` conversion
- **Custom mappers** - Full control over row-to-object mapping
- **Retry logic** - Automatic retry for transient errors
- **Checkpointing** - In-memory recovery from transient failures
- **SSL/TLS support** - Secure database connections
- **SQL injection prevention** - Identifier validation enabled by default

## Configuration

### PostgresConfiguration

Configure connector behavior with `PostgresConfiguration`:

```csharp
var configuration = new PostgresConfiguration
{
    ConnectionString = "Host=localhost;Database=mydb;Username=postgres;Password=password",
    StreamResults = true,
    FetchSize = 1_000,
    BatchSize = 500,
    MaxBatchSize = 5_000,
    UseTransaction = true,
    MaxRetryAttempts = 3,
    RetryDelay = TimeSpan.FromSeconds(2),
    ValidateIdentifiers = true,
    CheckpointStrategy = CheckpointStrategy.InMemory,
    CommandTimeout = 30
};
```

### Connection String

The connection string supports all Npgsql options:

```text
Host=localhost;Port=5432;Database=mydb;Username=postgres;Password=password;Timeout=15;Pooling=true;SslMode=Require
```

## Mapping

### Convention-Based Mapping

Properties are automatically mapped to columns using `snake_case` conversion:

```csharp
public record Customer(
    int CustomerId,      // Maps to customer_id
    string FirstName,     // Maps to first_name
    string EmailAddress    // Maps to email_address
);
```

### Attribute-Based Mapping

Override default mapping with attributes:

```csharp
using NPipeline.Connectors.PostgreSQL.Mapping;

public record Customer(
    [PostgresColumn("cust_id", PrimaryKey = true)] int Id,
    [PostgresColumn("full_name")] string Name,
    [PostgresIgnore] string TemporaryField
);
```

### Custom Mappers

For complete control, provide a custom mapper function:

```csharp
var source = new PostgresSourceNode<Customer>(
    connectionString,
    "SELECT id, name, email FROM customers",
    rowMapper: row => new Customer(
        row.GetInt32(row.GetOrdinal("id")),
        row.GetString(row.GetOrdinal("name")),
        row.GetString(row.GetOrdinal("email"))
    )
);
```

## Write Strategies

### Per-Row Strategy

Writes each row individually. Best for:

- Small batches
- Real-time processing
- Per-row error handling

```csharp
var sink = new PostgresSinkNode<Customer>(
    connectionString,
    "customers",
    writeStrategy: PostgresWriteStrategy.PerRow
);
```

### Batch Strategy

Buffers rows and issues a single multi-value `INSERT`. Best for:

- Large datasets
- Bulk imports
- High-throughput scenarios

```csharp
var configuration = new PostgresConfiguration
{
    BatchSize = 500,
    MaxBatchSize = 5_000,
    UseTransaction = true
};

var sink = new PostgresSinkNode<Customer>(
    connectionString,
    "customers",
    writeStrategy: PostgresWriteStrategy.Batch,
    configuration: configuration
);
```

## Dependency Injection

Register the connector with dependency injection for production applications:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.PostgreSQL.DependencyInjection;

var services = new ServiceCollection()
    .AddPostgresConnector(options =>
    {
        options.DefaultConnectionString = "Host=localhost;Database=mydb;Username=postgres;Password=password";
        options.AddOrUpdateConnection("analytics", "Host=localhost;Database=analytics;Username=postgres;Password=postgres");
        options.DefaultConfiguration = new PostgresConfiguration
        {
            StreamResults = true,
            FetchSize = 1_000,
            BatchSize = 500
        };
    })
    .BuildServiceProvider();

var pool = services.GetRequiredService<IPostgresConnectionPool>();
var sourceFactory = services.GetRequiredService<PostgresSourceNodeFactory>();
var sinkFactory = services.GetRequiredService<PostgresSinkNodeFactory>();
```

### Using Named Connections

```csharp
var source = new PostgresSourceNode<Customer>(
    pool,
    "SELECT * FROM customers",
    connectionName: "analytics"
);
```

## Streaming

Enable streaming for large result sets to reduce memory usage:

```csharp
var configuration = new PostgresConfiguration
{
    StreamResults = true,
    FetchSize = 1_000
};

var source = new PostgresSourceNode<Customer>(
    connectionString,
    "SELECT * FROM large_table",
    configuration: configuration
);
```

**Why streaming matters:** Without streaming, the entire result set is loaded into memory. Streaming fetches rows in batches, allowing you to process millions of rows without memory issues.

## Checkpointing

Enable in-memory checkpointing to recover from transient failures:

```csharp
var configuration = new PostgresConfiguration
{
    CheckpointStrategy = CheckpointStrategy.InMemory,
    StreamResults = true
};
```

The connector tracks the last successfully processed row ID. If a transient failure occurs, processing resumes from the last checkpoint rather than restarting from the beginning.

## Error Handling

### Retry Configuration

Configure retries for transient failures:

```csharp
var configuration = new PostgresConfiguration
{
    MaxRetryAttempts = 3,
    RetryDelay = TimeSpan.FromSeconds(2)
};
```

### Custom Exception Handling

```csharp
try
{
    await pipeline.RunAsync();
}
catch (NpgsqlException ex) when (ex.IsTransient)
{
    // Retry operation
    await Task.Delay(TimeSpan.FromSeconds(5));
    await pipeline.RunAsync();
}
```

## SSL/TLS Configuration

Configure SSL/TLS for secure connections:

```csharp
var configuration = new PostgresConfiguration
{
    // SSL mode is configured via connection string
    ConnectionString = "Host=localhost;Database=mydb;Username=postgres;Password=password;SslMode=Require"
};
```

Available SSL modes: `Disable`, `Allow`, `Prefer`, `Require`, `VerifyCa`, `VerifyFull`

## Performance Tips

1. **Use batch writes** - 10-100x faster than per-row for bulk operations
2. **Enable streaming** - Essential for large result sets
3. **Tune batch size** - 500-1,000 provides good balance between throughput and latency
4. **Adjust fetch size** - 1,000-5,000 rows works well for most workloads
5. **Use connection pooling** - Leverage dependency injection for efficient connection management

## Security

- **Identifier validation** - Enabled by default to prevent SQL injection
- **Parameterized queries** - All queries use parameterized statements
- **SSL/TLS support** - Encrypt connections to database

## Documentation

For comprehensive documentation including advanced scenarios, configuration reference, and best practices, see the [PostgreSQL Connector documentation](https://github.com/npipeline/NPipeline/blob/main/docs/connectors/postgresql.md).

## License

MIT License - see LICENSE file for details.
