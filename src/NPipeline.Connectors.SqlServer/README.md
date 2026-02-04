# NPipeline.Connectors.SqlServer

A SQL Server connector for NPipeline data pipelines. Provides source and sink nodes for reading from and writing to SQL Server databases with support for
convention-based mapping, custom mappers, connection pooling, and streaming.

## Installation

```bash
dotnet add package NPipeline.Connectors.SqlServer
```

## Quick Start

### Reading from SQL Server

```csharp
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Nodes;
using NPipeline.Pipeline;

// Define your model
public record Customer(int Id, string Name, string Email);

// Create a source node
var connectionString = "Server=localhost;Database=mydb;User Id=sa;Password=password";
var source = new SqlServerSourceNode<Customer>(
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

### Writing to SQL Server

```csharp
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Nodes;
using NPipeline.Pipeline;

// Define your model
public record Customer(int Id, string Name, string Email);

// Create a sink node
var connectionString = "Server=localhost;Database=mydb;User Id=sa;Password=password";
var configuration = new SqlServerConfiguration
{
    WriteStrategy = SqlServerWriteStrategy.Batch,
    Schema = "dbo"
};

var sink = new SqlServerSinkNode<Customer>(
    connectionString,
    "customers",
    configuration
);

// Use in a pipeline
var pipeline = new PipelineBuilder()
    .AddSource<InMemorySourceNode<Customer>, Customer>("source")
    .AddSink(sink, "customer_sink")
    .Build();

await pipeline.RunAsync();
```

## Using StorageUri for Environment-Aware Configuration

The SQL Server connector supports URI-based configuration through `StorageUri`, enabling seamless environment switching without code changes.

### Basic Usage

```csharp
using NPipeline.Connectors;
using NPipeline.Connectors.SqlServer;

var uri = StorageUri.Parse("mssql://localhost:1433/mydb?username=sa&password=password");
var source = new SqlServerSourceNode<Customer>(uri, "SELECT * FROM customers");

var sink = new SqlServerSinkNode<Customer>(uri, "customers");
```

### Environment Switching Example

```csharp
// Development (local database)
var devUri = StorageUri.Parse("mssql://localhost:1433/mydb?username=sa&password=devpass");

// Production (Azure SQL)
var prodUri = StorageUri.Parse("mssql://myserver.database.windows.net:1433/mydb?username=produser&password=${DB_PASSWORD}");

// Same pipeline code works in both environments
var uri = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") == "Production" ? prodUri : devUri;
var source = new SqlServerSourceNode<Customer>(uri, "SELECT * FROM customers");
```

### URI Parameters

Supported query parameters for SQL Server URIs:

| Parameter                | Type   | Description                               |
|--------------------------|--------|-------------------------------------------|
| `username`               | string | Database username                         |
| `password`               | string | Database password                         |
| `encrypt`                | bool   | Enable encryption (`true`/`false`)        |
| `trustServerCertificate` | bool   | Trust server certificate (`true`/`false`) |
| `timeout`                | int    | Connection timeout in seconds             |

### Using the Resolver Factory

```csharp
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.SqlServer;

var resolver = SqlServerStorageResolverFactory.CreateResolver();
var uri = StorageUri.Parse("mssql://localhost:1433/mydb?username=sa&password=password");

// Provider is resolved automatically
var provider = resolver.ResolveProvider(uri);
var connectionString = ((IDatabaseStorageProvider)provider).GetConnectionString(uri);
```

## Key Features

- **Streaming reads** - Process large result sets with minimal memory usage
- **Batch writes** - High-performance bulk inserts with configurable batch sizes
- **Connection pooling** - Efficient connection management via dependency injection
- **Convention-based mapping** - Automatic `PascalCase` to `PascalCase` mapping (no conversion)
- **Custom mappers** - Full control over row-to-object mapping
- **Retry logic** - Automatic retry for transient errors
- **Checkpointing** - In-memory recovery from transient failures
- **SSL/TLS support** - Secure database connections
- **SQL injection prevention** - Identifier validation enabled by default

## Configuration

### SqlServerConfiguration

Configure connector behavior with `SqlServerConfiguration`:

```csharp
var configuration = new SqlServerConfiguration
{
    ConnectionString = "Server=localhost;Database=mydb;User Id=sa;Password=password",
    StreamResults = true,
    FetchSize = 1_000,
    BatchSize = 1_000,
    MaxBatchSize = 5_000,
    UseTransaction = true,
    Schema = "dbo",
    MaxRetryAttempts = 3,
    RetryDelay = TimeSpan.FromSeconds(2),
    ValidateIdentifiers = true,
    CheckpointStrategy = CheckpointStrategy.InMemory,
    CommandTimeout = 30
};

// Note: Pass unqualified table names (e.g., "Customers") and set the schema via configuration.
```

### Connection String

The connection string supports all Microsoft.Data.SqlClient options:

```text
Server=localhost;Port=1433;Database=mydb;User Id=sa;Password=password;Timeout=15;Pooling=true;Encrypt=True;TrustServerCertificate=False
```

## Mapping

### Convention-Based Mapping

Properties are automatically mapped to columns using `PascalCase` naming (no conversion):

```csharp
public record Customer(
    int CustomerId,      // Maps to CustomerId
    string FirstName,     // Maps to FirstName
    string EmailAddress  // Maps to EmailAddress
);
```

### Attribute-Based Mapping

Override default mapping with attributes:

```csharp
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.SqlServer.Mapping;

public record Customer(
    [SqlServerColumn("cust_id", PrimaryKey = true)] int Id,
    [SqlServerColumn("full_name")] string Name,
    [IgnoreColumn] string TemporaryField
);
```

### Custom Mappers

For complete control, provide a custom mapper function:

```csharp
var source = new SqlServerSourceNode<Customer>(
    connectionString,
    "SELECT id, name, email FROM customers",
    rowMapper: row => new Customer(
        row.Get<int>("id"),
        row.Get<string>("name"),
        row.Get<string>("email")
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
var configuration = new SqlServerConfiguration
{
    WriteStrategy = SqlServerWriteStrategy.PerRow,
    Schema = "dbo"
};

var sink = new SqlServerSinkNode<Customer>(
    connectionString,
    "customers",
    configuration
);
```

### Batch Strategy

Buffers rows and issues a single multi-value `INSERT`. Best for:

- Large datasets
- Bulk imports
- High-throughput scenarios

```csharp
var configuration = new SqlServerConfiguration
{
    BatchSize = 1_000,
    MaxBatchSize = 5_000,
    UseTransaction = true,
    Schema = "dbo"
};

var sink = new SqlServerSinkNode<Customer>(
    connectionString,
    "customers",
    configuration
);
```

## Dependency Injection

Register the connector with dependency injection for production applications:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.SqlServer.DependencyInjection;

var services = new ServiceCollection()
    .AddSqlServerConnector(options =>
    {
        options.DefaultConnectionString = "Server=localhost;Database=mydb;User Id=sa;Password=password";
        options.AddOrUpdateConnection("analytics", "Server=localhost;Database=analytics;User Id=sa;Password=password");
        options.DefaultConfiguration = new SqlServerConfiguration
        {
            StreamResults = true,
            FetchSize = 1_000,
            BatchSize = 1_000
        };
    })
    .BuildServiceProvider();

var pool = services.GetRequiredService<ISqlServerConnectionPool>();
var sourceFactory = services.GetRequiredService<SqlServerSourceNodeFactory>();
var sinkFactory = services.GetRequiredService<SqlServerSinkNodeFactory>();
```

### Using Named Connections

```csharp
var source = new SqlServerSourceNode<Customer>(
    pool,
    "SELECT * FROM customers",
    connectionName: "analytics"
);
```

## Streaming

Enable streaming for large result sets to reduce memory usage:

```csharp
var configuration = new SqlServerConfiguration
{
    StreamResults = true,
    FetchSize = 1_000
};

var source = new SqlServerSourceNode<Customer>(
    connectionString,
    "SELECT * FROM large_table",
    configuration: configuration
);
```

**Why streaming matters:** Without streaming, the entire result set is loaded into memory. Streaming fetches rows in batches, allowing you to process millions
of rows without memory issues.

## Checkpointing

Enable in-memory checkpointing to recover from transient failures:

```csharp
var configuration = new SqlServerConfiguration
{
    CheckpointStrategy = CheckpointStrategy.InMemory,
    StreamResults = true
};
```

The connector tracks the last successfully processed row ID. If a transient failure occurs, processing resumes from the last checkpoint rather than restarting
from the beginning.

## Analyzers

The SQL Server connector includes a companion analyzer package that provides compile-time diagnostics to help prevent common mistakes when using checkpointing.

### Installation

```bash
dotnet add package NPipeline.Connectors.SqlServer.Analyzers
```

### NP9502: Checkpointing requires ORDER BY clause

**Category:** Reliability
**Default Severity:** Warning

When using checkpointing with SQL Server source nodes, the SQL query must include an `ORDER BY` clause on a unique, monotonically increasing column. This
ensures consistent row ordering across checkpoint restarts. Without proper ordering, checkpointing may skip rows or process duplicates.

#### Example

```csharp
// ❌ Warning: Missing ORDER BY clause
var source = new SqlServerSourceNode<MyRecord>(
    connectionString,
    "SELECT id, name, created_at FROM my_table",
    configuration: new SqlServerConfiguration
    {
        CheckpointStrategy = CheckpointStrategy.Offset
    }
);

// ✅ Correct: Includes ORDER BY clause
var source = new SqlServerSourceNode<MyRecord>(
    connectionString,
    "SELECT id, name, created_at FROM my_table ORDER BY id",
    configuration: new SqlServerConfiguration
    {
        CheckpointStrategy = CheckpointStrategy.Offset
    }
);
```

#### Why This Matters

Checkpointing tracks the position of processed rows to enable recovery from failures. Without a consistent `ORDER BY` clause:

- **Data Loss:** Rows may be skipped during recovery
- **Data Duplication:** Rows may be processed multiple times
- **Inconsistent State:** Checkpoint positions become unreliable

#### Recommended Ordering Columns

Use a unique, monotonically increasing column such as:

- `id` (primary key)
- `created_at` (timestamp)
- `updated_at` (timestamp)
- `timestamp` (timestamp column)
- Any auto-incrementing or sequential column

For more details, see
the [SQL Server Analyzer documentation](https://github.com/npipeline/NPipeline/blob/main/src/NPipeline.Connectors.SqlServer.Analyzers/README.md).

## Error Handling

### Retry Configuration

Configure retries for transient failures:

```csharp
var configuration = new SqlServerConfiguration
{
    MaxRetryAttempts = 3,
    RetryDelay = TimeSpan.FromSeconds(2)
};
```

### Row-Level Error Handling

Handle mapping errors at the row level by providing a custom error handler:

```csharp
var configuration = new SqlServerConfiguration
{
    RowErrorHandler = (exception, row) =>
    {
        // Log the error with row context
        logger.LogWarning(exception, "Failed to map row");

        // Return true to skip the row and continue processing
        return true;
    }
};

var source = new SqlServerSourceNode<Customer>(
    connectionString,
    "SELECT * FROM customers",
    configuration: configuration
);
```

Alternatively, use `ContinueOnError` for a simpler approach that skips all rows with errors:

```csharp
var configuration = new SqlServerConfiguration
{
    ContinueOnError = true  // Skip rows with any mapping errors
};
```

### Connection-Level Error Handling

Handle transient connection and execution errors:

```csharp
try
{
    await pipeline.RunAsync();
}
catch (SqlException ex) when (SqlServerTransientErrorDetector.IsTransient(ex))
{
    // Retry operation for transient failures
    await Task.Delay(TimeSpan.FromSeconds(5));
    await pipeline.RunAsync();
}
```

## SSL/TLS Configuration

Configure SSL/TLS for secure connections:

```csharp
var configuration = new SqlServerConfiguration
{
    // SSL mode is configured via connection string
    ConnectionString = "Server=localhost;Database=mydb;User Id=sa;Password=password;Encrypt=True;TrustServerCertificate=False"
};
```

Available encryption options: `False`, `True`, `Strict`, `Optional`

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

| Scenario                              | Prepared Statements | Performance Impact |
|---------------------------------------|---------------------|--------------------|
| Repeated inserts (same query pattern) | Enabled             | 10-30% faster      |
| Ad-hoc queries (different each time)  | Enabled             | 5-10% overhead     |
| One-time bulk operations              | Disabled            | No impact          |

## Performance Tips

1. **Use batch writes** - 10-100x faster than per-row for bulk operations
2. **Enable streaming** - Essential for large result sets
3. **Tune batch size** - 1,000-5,000 provides good balance between throughput and latency
4. **Adjust fetch size** - 1,000-5,000 rows works well for most workloads
5. **Use connection pooling** - Leverage dependency injection for efficient connection management
6. **Respect parameter limits** - SQL Server caps commands at 2,100 parameters, so effective batch size is automatically limited by column count

## Security

- **Identifier validation** - Enabled by default to prevent SQL injection
- **Parameterized queries** - All queries use parameterized statements
- **SSL/TLS support** - Encrypt connections to database

## Documentation

For comprehensive documentation including advanced scenarios, configuration reference, and best practices, see
the [SQL Server Connector documentation](https://github.com/npipeline/NPipeline/blob/main/docs/connectors/sqlserver.md).

## License

MIT License - see LICENSE file for details.
