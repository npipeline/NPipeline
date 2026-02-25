# NPipeline.Connectors.SqlServer

A SQL Server connector for NPipeline data pipelines. Provides source and sink nodes for reading from and writing to SQL Server databases with support for multiple write strategies (PerRow, Batch, BulkCopy), upsert operations, delivery semantics, checkpointing strategies, convention-based mapping, custom mappers, and connection pooling.

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
- **Multiple write strategies** - PerRow, Batch, and BulkCopy (SqlBulkCopy API) for different performance needs
- **Upsert support** - MERGE-based insert-or-update semantics with configurable key columns
- **Delivery semantics** - AtLeastOnce, AtMostOnce, and ExactlyOnce delivery guarantees
- **Checkpointing strategies** - None, InMemory, Offset, KeyBased, Cursor, and CDC for resumable pipelines
- **Connection pooling** - Efficient connection management via dependency injection
- **Convention-based mapping** - Automatic `PascalCase` to `PascalCase` mapping (no conversion)
- **Custom mappers** - Full control over row-to-object mapping
- **Retry logic** - Automatic retry for transient errors
- **SSL/TLS support** - Secure database connections
- **SQL injection prevention** - Identifier validation enabled by default

## Configuration

### SqlServerConfiguration

Configure connector behavior with `SqlServerConfiguration`:

```csharp
var configuration = new SqlServerConfiguration
{
    // Connection settings
    ConnectionString = "Server=localhost;Database=mydb;User Id=sa;Password=password",
    Schema = "dbo",
    CommandTimeout = 30,
    ConnectionTimeout = 15,
    
    // Write settings
    WriteStrategy = SqlServerWriteStrategy.Batch,
    BatchSize = 1_000,
    MaxBatchSize = 5_000,
    UseTransaction = true,
    UsePreparedStatements = true,
    
    // BulkCopy settings
    BulkCopyBatchSize = 5_000,
    BulkCopyTimeout = 300,
    BulkCopyNotifyAfter = 1_000,
    EnableStreaming = true,
    
    // Upsert settings
    UseUpsert = false,
    UpsertKeyColumns = new[] { "Id" },
    OnMergeAction = OnMergeAction.Update,
    
    // Read settings
    StreamResults = true,
    FetchSize = 1_000,
    
    // Delivery semantics
    DeliverySemantic = DeliverySemantic.AtLeastOnce,
    
    // Checkpointing
    CheckpointStrategy = CheckpointStrategy.None,
    CheckpointStorage = null,
    CheckpointOffsetColumn = "Id",
    CheckpointKeyColumns = null,
    CdcCaptureInstance = null,
    
    // Error handling
    MaxRetryAttempts = 3,
    RetryDelay = TimeSpan.FromSeconds(2),
    ContinueOnError = false,
    
    // Mapping
    ValidateIdentifiers = true,
    CaseInsensitiveMapping = true,
    CacheMappingMetadata = true
};

// Note: Pass unqualified table names (e.g., "Customers") and set the schema via configuration.
```

### Configuration Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ConnectionString` | `string` | `""` | SQL Server connection string |
| `Schema` | `string` | `"dbo"` | Default schema name for table operations |
| `WriteStrategy` | `SqlServerWriteStrategy` | `Batch` | Write strategy (PerRow, Batch, BulkCopy) |
| `BatchSize` | `int` | `100` | Target batch size for batch writes |
| `MaxBatchSize` | `int` | `1,000` | Maximum batch size to prevent runaway buffers |
| `UseTransaction` | `bool` | `true` | Wrap writes in a transaction |
| `UsePreparedStatements` | `bool` | `true` | Use prepared statements for writes |
| `UseUpsert` | `bool` | `false` | Enable MERGE-based upserts |
| `UpsertKeyColumns` | `string[]?` | `null` | Key columns for MERGE matching |
| `OnMergeAction` | `OnMergeAction` | `Update` | Action on MERGE match (`Update`, `Ignore`, or `Delete`) |
| `BulkCopyBatchSize` | `int` | `5,000` | Rows per bulk copy batch |
| `BulkCopyTimeout` | `int` | `300` | Bulk copy timeout in seconds |
| `BulkCopyNotifyAfter` | `int` | `1,000` | Rows before progress notification |
| `EnableStreaming` | `bool` | `true` | Enable streaming for bulk copy |
| `StreamResults` | `bool` | `true` | Enable streaming for reads |
| `FetchSize` | `int` | `1,000` | Rows to fetch per round-trip |
| `DeliverySemantic` | `DeliverySemantic` | `AtLeastOnce` | Delivery guarantee semantic |
| `CheckpointStrategy` | `CheckpointStrategy` | `None` | Checkpointing strategy |
| `CheckpointStorage` | `ICheckpointStorage?` | `null` | Checkpoint storage backend |
| `CheckpointOffsetColumn` | `string?` | `null` | Column for offset checkpointing |
| `CheckpointKeyColumns` | `string[]?` | `null` | Columns for key-based checkpointing |
| `CdcCaptureInstance` | `string?` | `null` | CDC capture instance name |
| `MaxRetryAttempts` | `int` | `3` | Maximum retry attempts |
| `RetryDelay` | `TimeSpan` | `1 second` | Delay between retries |
| `ValidateIdentifiers` | `bool` | `true` | Validate SQL identifiers |
| `CaseInsensitiveMapping` | `bool` | `true` | Case-insensitive column mapping |
| `CacheMappingMetadata` | `bool` | `true` | Cache mapping metadata |

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

The connector supports three write strategies for different performance and reliability requirements.

### PerRow Strategy

Writes each row individually with a separate `INSERT` statement. This provides:

- Immediate visibility of each row
- Better error isolation (one failed insert doesn't affect others)
- Higher overhead for large datasets

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

**Best for:** Small batches, real-time processing, per-row error handling

### Batch Strategy

Buffers rows and issues a single multi-value `INSERT`. This provides:

- Better performance for large datasets
- Reduced database round-trips
- All-or-nothing semantics within a batch

```csharp
var configuration = new SqlServerConfiguration
{
    WriteStrategy = SqlServerWriteStrategy.Batch,
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

**Best for:** Large datasets, bulk imports, high-throughput scenarios

### BulkCopy Strategy

Uses SQL Server's native `SqlBulkCopy` API for maximum throughput. This provides:

- Highest performance for bulk loading
- Support for batch size and notification callbacks
- Table lock options for reduced contention

```csharp
var configuration = new SqlServerConfiguration
{
    WriteStrategy = SqlServerWriteStrategy.BulkCopy,
    BulkCopyBatchSize = 5_000,
    BulkCopyTimeout = 300,  // 5 minutes
    BulkCopyNotifyAfter = 1_000,  // Progress notifications
    EnableStreaming = true,
    Schema = "dbo"
};

var sink = new SqlServerSinkNode<Customer>(
    connectionString,
    "customers",
    configuration
);
```

**Best for:** Very large datasets (millions of rows), data warehouse loading, maximum throughput

### Write Strategy Comparison

| Strategy | Throughput | Latency | Error Isolation | Use Case |
|----------|------------|---------|-----------------|----------|
| PerRow | Low | Low | High | Real-time, small batches |
| Batch | High | Medium | Medium | Bulk loading, ETL |
| BulkCopy | Very High | High | Low | Large bulk loads, data warehouse |

## Upsert Operations

The connector supports SQL Server's `MERGE` statement for upsert operations, allowing you to insert rows or update them if they already exist.

### Basic Upsert Configuration

Enable upsert by setting `UseUpsert = true` and specifying the key columns:

```csharp
var configuration = new SqlServerConfiguration
{
    UseUpsert = true,
    UpsertKeyColumns = new[] { "Id" },  // Primary key or unique constraint columns
    OnMergeAction = OnMergeAction.Update,  // Update on match
    WriteStrategy = SqlServerWriteStrategy.Batch,
    Schema = "dbo"
};

var sink = new SqlServerSinkNode<Customer>(
    connectionString,
    "customers",
    configuration
);
```

### Merge Actions

#### OnMergeAction.Update

Updates non-key columns with values from the incoming row when a match is found:

```sql
MERGE INTO customers AS target
USING (VALUES (@Id, @Name, @Email)) AS source (Id, Name, Email)
ON target.Id = source.Id
WHEN MATCHED THEN
    UPDATE SET Name = source.Name, Email = source.Email
WHEN NOT MATCHED THEN
    INSERT (Id, Name, Email) VALUES (source.Id, source.Name, source.Email);
```

#### OnMergeAction.Ignore

Leaves the existing row unchanged. Only new (unmatched) rows are inserted:

```csharp
var configuration = new SqlServerConfiguration
{
    UseUpsert = true,
    UpsertKeyColumns = new[] { "Id" },
    OnMergeAction = OnMergeAction.Ignore  // Leave existing rows as-is
};
```

#### OnMergeAction.Delete

Deletes the matching row when the source row is present:

```csharp
var configuration = new SqlServerConfiguration
{
    UseUpsert = true,
    UpsertKeyColumns = new[] { "Id" },
    OnMergeAction = OnMergeAction.Delete  // Remove existing rows on match
};
```

### Composite Key Upsert

For tables with composite unique constraints:

```csharp
public record OrderItem(int OrderId, int ProductId, int Quantity, decimal UnitPrice);

var configuration = new SqlServerConfiguration
{
    UseUpsert = true,
    UpsertKeyColumns = new[] { "OrderId", "ProductId" },  // Composite key
    OnMergeAction = OnMergeAction.Update,
    WriteStrategy = SqlServerWriteStrategy.Batch,
    Schema = "dbo"
};

var sink = new SqlServerSinkNode<OrderItem>(
    connectionString,
    "order_items",
    configuration
);
```

**Why use upsert:** Upsert eliminates the need for separate insert/update logic and handles race conditions where a row might be inserted between your check and insert operations.

## Delivery Semantics

The connector supports three delivery semantics to control data consistency guarantees during failures.

### AtLeastOnce (Default)

Guarantees that every item is delivered at least once. Items may be duplicated on retry after a failure.

```csharp
var configuration = new SqlServerConfiguration
{
    DeliverySemantic = DeliverySemantic.AtLeastOnce,
    UseTransaction = true
};
```

**Characteristics:**

- No data loss
- Possible duplicates on retry
- Best for idempotent operations or when duplicates can be tolerated

### AtMostOnce

Guarantees that every item is delivered at most once. Items may be lost on failure, but never duplicated.

```csharp
var configuration = new SqlServerConfiguration
{
    DeliverySemantic = DeliverySemantic.AtMostOnce
};
```

**Characteristics:**

- No duplicates
- Possible data loss on failure
- Best for high-throughput scenarios where occasional data loss is acceptable

### ExactlyOnce

Guarantees that every item is delivered exactly once. Uses transactional semantics to prevent both data loss and duplication.

```csharp
var configuration = new SqlServerConfiguration
{
    DeliverySemantic = DeliverySemantic.ExactlyOnce,
    UseTransaction = true,
    CheckpointStrategy = CheckpointStrategy.Offset,
    CheckpointStorage = new FileCheckpointStorage("checkpoints.json")
};
```

**Characteristics:**

- No data loss
- No duplicates
- Higher overhead due to transaction coordination
- Requires checkpoint storage

**Use when:** Financial transactions, audit logging, or any scenario requiring strict exactly-once guarantees.

### Delivery Semantic Comparison

| Semantic | Data Loss | Duplicates | Overhead | Use Case |
|----------|-----------|------------|----------|----------|
| AtLeastOnce | No | Possible | Low | General purpose, idempotent ops |
| AtMostOnce | Possible | No | Low | Telemetry, metrics |
| ExactlyOnce | No | No | High | Financial, audit |

## Checkpointing Strategies

Checkpointing enables pipelines to resume from where they left off after a failure, rather than restarting from the beginning.

### None (Default)

No checkpointing is performed. Failures require restarting from the beginning.

```csharp
var configuration = new SqlServerConfiguration
{
    CheckpointStrategy = CheckpointStrategy.None
};
```

### InMemory

Stores checkpoint state in memory. Enables recovery from transient failures during a single process execution.

```csharp
var configuration = new SqlServerConfiguration
{
    CheckpointStrategy = CheckpointStrategy.InMemory,
    StreamResults = true
};

var source = new SqlServerSourceNode<Order>(
    connectionString,
    "SELECT * FROM orders ORDER BY OrderId",  // ORDER BY is required for checkpointing
    configuration: configuration
);
```

**Limitations:** Checkpoint state is lost when the process terminates.

### Offset

Persists numeric offset checkpoints to external storage. Tracks position using a monotonically increasing column (e.g., identity column).

```csharp
var configuration = new SqlServerConfiguration
{
    CheckpointStrategy = CheckpointStrategy.Offset,
    CheckpointOffsetColumn = "OrderId",
    CheckpointStorage = new FileCheckpointStorage("checkpoints/order_offset.json")
};

var source = new SqlServerSourceNode<Order>(
    connectionString,
    "SELECT * FROM orders WHERE OrderId > @lastCheckpoint ORDER BY OrderId",
    configuration: configuration
);
```

**Requirements:**

- Requires `CheckpointOffsetColumn` to be specified
- Requires `CheckpointStorage` to persist checkpoints
- Query must include `ORDER BY` on the offset column

### KeyBased

Tracks processed items using composite keys. Useful for tables without a single monotonic column.

```csharp
var configuration = new SqlServerConfiguration
{
    CheckpointStrategy = CheckpointStrategy.KeyBased,
    CheckpointKeyColumns = new[] { "CustomerId", "OrderDate" },
    CheckpointStorage = new FileCheckpointStorage("checkpoints/order_keys.json")
};
```

**Requirements:**

- Requires `CheckpointKeyColumns` to be specified
- Requires `CheckpointStorage` to persist checkpoints

### Cursor

Tracks cursor position for cursor-based iteration.

```csharp
var configuration = new SqlServerConfiguration
{
    CheckpointStrategy = CheckpointStrategy.Cursor,
    CheckpointStorage = new FileCheckpointStorage("checkpoints/cursor.json")
};
```

### CDC (Change Data Capture)

Tracks LSN (Log Sequence Number) for SQL Server Change Data Capture. Enables capturing changes from the database transaction log.

```csharp
var configuration = new SqlServerConfiguration
{
    CheckpointStrategy = CheckpointStrategy.CDC,
    CdcCaptureInstance = "dbo_orders",
    CheckpointStorage = new FileCheckpointStorage("checkpoints/cdc.json")
};
```

**Requirements:**

- Requires `CdcCaptureInstance` to be specified
- Requires SQL Server CDC to be enabled on the database and table
- Requires appropriate SQL Server permissions

### Checkpoint Storage

Implement `ICheckpointStorage` to persist checkpoints to your preferred backend:

```csharp
public interface ICheckpointStorage
{
    Task<Checkpoint?> LoadAsync(string pipelineId, CancellationToken cancellationToken = default);
    Task SaveAsync(string pipelineId, Checkpoint checkpoint, CancellationToken cancellationToken = default);
}
```

**Built-in implementations:**

- `FileCheckpointStorage` - Stores checkpoints in a JSON file
- `InMemoryCheckpointStorage` - Stores checkpoints in memory (for testing)

### Checkpoint Intervals

Configure how frequently checkpoints are saved:

```csharp
var configuration = new SqlServerConfiguration
{
    CheckpointStrategy = CheckpointStrategy.Offset,
    CheckpointInterval = new CheckpointIntervalConfiguration
    {
        RowCount = 10_000,  // Save every 10,000 rows
        TimeInterval = TimeSpan.FromMinutes(5)  // Or every 5 minutes, whichever comes first
    }
};
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

Checkpointing enables pipelines to resume from where they left off after a failure. See the [Checkpointing Strategies](#checkpointing-strategies) section for detailed configuration options for each strategy.

### Quick Example: Offset Checkpointing

```csharp
var configuration = new SqlServerConfiguration
{
    CheckpointStrategy = CheckpointStrategy.Offset,
    CheckpointOffsetColumn = "Id",
    CheckpointStorage = new FileCheckpointStorage("checkpoints.json"),
    StreamResults = true
};

var source = new SqlServerSourceNode<Order>(
    connectionString,
    "SELECT * FROM orders WHERE Id > @lastCheckpoint ORDER BY Id",
    configuration: configuration
);
```

The connector tracks the last successfully processed row ID. If a transient failure occurs, processing resumes from the last checkpoint rather than restarting from the beginning.

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

1. **Choose the right write strategy** - BulkCopy for maximum throughput, Batch for balanced workloads, PerRow for real-time
2. **Enable streaming** - Essential for large result sets
3. **Tune batch size** - 1,000-5,000 provides good balance between throughput and latency
4. **Adjust fetch size** - 1,000-5,000 rows works well for most workloads
5. **Use connection pooling** - Leverage dependency injection for efficient connection management
6. **Use upsert for idempotent writes** - Enable `UseUpsert` when loading data that may already exist
7. **Select appropriate delivery semantics** - Use ExactlyOnce for critical data, AtLeastOnce for general workloads
8. **Configure checkpointing for long-running pipelines** - Use Offset or KeyBased checkpointing for recovery
9. **Respect parameter limits** - SQL Server caps commands at 2,100 parameters, so effective batch size is automatically limited by column count

## Security

- **Identifier validation** - Enabled by default to prevent SQL injection
- **Parameterized queries** - All queries use parameterized statements
- **SSL/TLS support** - Encrypt connections to database

## Documentation

For comprehensive documentation including advanced scenarios, configuration reference, and best practices, see
the [SQL Server Connector documentation](https://github.com/npipeline/NPipeline/blob/main/docs/connectors/sqlserver.md).

## License

MIT License - see LICENSE file for details.
