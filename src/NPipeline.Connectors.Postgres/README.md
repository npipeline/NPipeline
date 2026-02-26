# NPipeline.Connectors.Postgres

A PostgreSQL connector for NPipeline data pipelines. Provides source and sink nodes for reading from and writing to PostgreSQL databases with support for
multiple write strategies, upsert operations, delivery semantics, checkpointing, convention-based mapping, custom mappers, connection pooling, and streaming.

## Installation

```bash
dotnet add package NPipeline.Connectors.Postgres
```

## Quick Start

### Reading from PostgreSQL

```csharp
using NPipeline.Connectors.Postgres;
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
using NPipeline.Connectors.Postgres;
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

## Using StorageUri for Environment-Aware Configuration

The PostgreSQL connector supports URI-based configuration through `StorageUri`, enabling seamless environment switching without code changes.

### Basic Usage

```csharp
using NPipeline.Connectors;
using NPipeline.Connectors.Postgres;

var uri = StorageUri.Parse("postgres://localhost:5432/mydb?username=postgres&password=password");
var source = new PostgresSourceNode<Customer>(uri, "SELECT * FROM customers");

var sink = new PostgresSinkNode<Customer>(uri, "customers");
```

### Environment Switching Example

```csharp
// Development (local database)
var devUri = StorageUri.Parse("postgres://localhost:5432/mydb?username=postgres&password=devpass");

// Production (AWS RDS)
var prodUri = StorageUri.Parse("postgres://mydb.prod.ap-southeast-2.rds.amazonaws.com:5432/mydb?username=produser&password=${DB_PASSWORD}");

// Same pipeline code works in both environments
var uri = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") == "Production" ? prodUri : devUri;
var source = new PostgresSourceNode<Customer>(uri, "SELECT * FROM customers");
```

### URI Parameters

Supported query parameters for PostgreSQL URIs:

| Parameter  | Type   | Description                                                                   |
|------------|--------|-------------------------------------------------------------------------------|
| `username` | string | Database username                                                             |
| `password` | string | Database password                                                             |
| `sslmode`  | string | SSL mode: `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full` |
| `timeout`  | int    | Connection timeout in seconds                                                 |
| `pooling`  | bool   | Enable connection pooling (`true`/`false`)                                    |

### Using the Resolver Factory

```csharp
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Postgres;

var resolver = PostgresStorageResolverFactory.CreateResolver();
var uri = StorageUri.Parse("postgres://localhost:5432/mydb?username=postgres&password=password");

// Provider is resolved automatically
var provider = resolver.ResolveProvider(uri);
var connectionString = ((IDatabaseStorageProvider)provider).GetConnectionString(uri);
```

## Key Features

- **Streaming reads** - Process large result sets with minimal memory usage
- **Multiple write strategies** - Per-row, batched inserts, and high-performance COPY protocol
- **Upsert support** - ON CONFLICT DO UPDATE / ON CONFLICT DO NOTHING SQL generation
- **Delivery semantics** - At-least-once, at-most-once, and exactly-once delivery guarantees
- **Checkpointing strategies** - InMemory, Offset, KeyBased, Cursor, and CDC checkpointing
- **Connection pooling** - Efficient connection management via dependency injection
- **Convention-based mapping** - Automatic `PascalCase` to `snake_case` conversion
- **Custom mappers** - Full control over row-to-object mapping
- **Retry logic** - Automatic retry for transient errors
- **SSL/TLS support** - Secure database connections
- **SQL injection prevention** - Identifier validation enabled by default
- **Binary COPY support** - High-performance binary format for bulk loading

## Configuration

### PostgresConfiguration

Configure connector behavior with `PostgresConfiguration`:

```csharp
var configuration = new PostgresConfiguration
{
    ConnectionString = "Host=localhost;Database=mydb;Username=postgres;Password=password",
    Schema = "public",
    StreamResults = true,
    FetchSize = 1_000,
    BatchSize = 1_000,
    MaxBatchSize = 5_000,
    UseTransaction = true,
    WriteStrategy = PostgresWriteStrategy.Batch,
    UseUpsert = false,
    UseBinaryCopy = false,
    DeliverySemantic = DeliverySemantic.AtLeastOnce,
    CheckpointStrategy = CheckpointStrategy.None,
    MaxRetryAttempts = 3,
    RetryDelay = TimeSpan.FromSeconds(1),
    ValidateIdentifiers = true,
    CommandTimeout = 30,
    CopyTimeout = 300
};
```

### Configuration Properties

| Property                | Type                    | Default       | Description                                    |
|-------------------------|-------------------------|---------------|------------------------------------------------|
| `ConnectionString`      | `string`                | `""`          | PostgreSQL connection string                   |
| `Schema`                | `string`                | `"public"`    | Default schema name                            |
| `WriteStrategy`         | `PostgresWriteStrategy` | `Batch`       | Write strategy (PerRow, Batch, Copy)           |
| `BatchSize`             | `int`                   | `1000`        | Target batch size for batched writes           |
| `MaxBatchSize`          | `int`                   | `5000`        | Maximum batch size to prevent unbounded memory |
| `UseTransaction`        | `bool`                  | `true`        | Wrap writes in a transaction                   |
| `UseUpsert`             | `bool`                  | `false`       | Enable ON CONFLICT upsert semantics            |
| `UpsertConflictColumns` | `string[]?`             | `null`        | Columns that form the conflict target          |
| `OnConflictAction`      | `OnConflictAction`      | `Update`      | Conflict resolution action (Update, Ignore)    |
| `UseBinaryCopy`         | `bool`                  | `false`       | Use binary format for COPY operations          |
| `DeliverySemantic`      | `DeliverySemantic`      | `AtLeastOnce` | Delivery guarantee semantic                    |
| `CheckpointStrategy`    | `CheckpointStrategy`    | `None`        | Checkpointing strategy for recovery            |
| `CheckpointStorage`     | `ICheckpointStorage?`   | `null`        | Storage backend for checkpoints                |
| `StreamResults`         | `bool`                  | `true`        | Enable streaming for large result sets         |
| `FetchSize`             | `int`                   | `1000`        | Rows to fetch per round-trip when streaming    |
| `CommandTimeout`        | `int`                   | `30`          | Command timeout in seconds                     |
| `CopyTimeout`           | `int`                   | `300`         | COPY operation timeout in seconds              |
| `MaxRetryAttempts`      | `int`                   | `3`           | Maximum retry attempts for transient errors    |
| `RetryDelay`            | `TimeSpan`              | `1 second`    | Delay between retry attempts                   |
| `ValidateIdentifiers`   | `bool`                  | `true`        | Validate SQL identifiers to prevent injection  |
| `UsePreparedStatements` | `bool`                  | `true`        | Use prepared statements for writes             |

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
using NPipeline.Connectors.Postgres.Mapping;

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

The PostgreSQL connector supports three write strategies, each optimized for different use cases.

### PerRow Strategy

Writes each row individually with a separate `INSERT` statement.

**Best for:**

- Small batches or real-time processing
- Per-row error handling requirements
- Scenarios where immediate visibility of each row is critical

```csharp
var sink = new PostgresSinkNode<Customer>(
    connectionString,
    "customers",
    writeStrategy: PostgresWriteStrategy.PerRow
);
```

**Trade-offs:** Higher overhead for large datasets due to individual round-trips, but provides better error isolation.

### Batch Strategy

Buffers rows and issues a single multi-value `INSERT` statement (e.g., `INSERT INTO table VALUES (...), (...), (...)`).

**Best for:**

- Large datasets and bulk imports
- High-throughput scenarios
- Balanced performance and reliability

```csharp
var configuration = new PostgresConfiguration
{
    BatchSize = 1_000,
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

**Trade-offs:** 10-100x faster than PerRow for bulk operations, but all rows in a batch succeed or fail together.

### Copy Strategy

Uses PostgreSQL's native `COPY` protocol for maximum throughput. Supports both text and binary formats.

**Best for:**

- Maximum throughput bulk loading
- Very large datasets (millions of rows)
- Data warehouse loading scenarios

```csharp
var configuration = new PostgresConfiguration
{
    CopyTimeout = 600,  // 10 minutes for large loads
    UseBinaryCopy = true  // Use binary format for better performance
};

var sink = new PostgresSinkNode<Customer>(
    connectionString,
    "customers",
    writeStrategy: PostgresWriteStrategy.Copy,
    configuration: configuration
);
```

**Binary vs Text Format:**

- **Text format** (`UseBinaryCopy = false`): More compatible, easier to debug
- **Binary format** (`UseBinaryCopy = true`): 20-30% faster, more efficient encoding

**Trade-offs:** Fastest option for large datasets, but bypasses some PostgreSQL processing layers and has limited error granularity.

### Write Strategy Comparison

| Strategy | Throughput | Latency | Error Isolation | Use Case                         |
|----------|------------|---------|-----------------|----------------------------------|
| PerRow   | Low        | Low     | High            | Real-time, small batches         |
| Batch    | High       | Medium  | Medium          | Bulk loading, ETL                |
| Copy     | Very High  | High    | Low             | Large bulk loads, data warehouse |

## Upsert Operations

The connector supports PostgreSQL's `ON CONFLICT` clause for upsert operations, allowing you to insert rows or update them if they already exist.

### Basic Upsert Configuration

Enable upsert by setting `UseUpsert = true` and specifying the conflict columns:

```csharp
var configuration = new PostgresConfiguration
{
    UseUpsert = true,
    UpsertConflictColumns = new[] { "id" },  // Primary key or unique constraint columns
    OnConflictAction = OnConflictAction.Update  // Update on conflict
};

var sink = new PostgresSinkNode<Customer>(
    connectionString,
    "customers",
    writeStrategy: PostgresWriteStrategy.Batch,
    configuration: configuration
);
```

### Conflict Actions

#### OnConflictAction.Update

Updates non-conflict columns with values from the inserted row:

```sql
INSERT INTO customers (id, name, email)
VALUES (1, 'John Doe', 'john@example.com')
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    email = EXCLUDED.email
```

```csharp
var configuration = new PostgresConfiguration
{
    UseUpsert = true,
    UpsertConflictColumns = new[] { "id" },
    OnConflictAction = OnConflictAction.Update
};
```

#### OnConflictAction.Ignore

Silently skips conflicting rows without raising an error:

```sql
INSERT INTO customers (id, name, email)
VALUES (1, 'John Doe', 'john@example.com')
ON CONFLICT (id) DO NOTHING
```

```csharp
var configuration = new PostgresConfiguration
{
    UseUpsert = true,
    UpsertConflictColumns = new[] { "id" },
    OnConflictAction = OnConflictAction.Ignore
};
```

### Composite Conflict Targets

For tables with composite unique constraints:

```csharp
public record OrderItem(int OrderId, int ProductId, int Quantity, decimal UnitPrice);

var configuration = new PostgresConfiguration
{
    UseUpsert = true,
    UpsertConflictColumns = new[] { "order_id", "product_id" },  // Composite key
    OnConflictAction = OnConflictAction.Update
};

var sink = new PostgresSinkNode<OrderItem>(
    connectionString,
    "order_items",
    writeStrategy: PostgresWriteStrategy.Batch,
    configuration: configuration
);
```

### Upsert with Write Strategies

Upsert works with all write strategies:

- **PerRow**: Each row is processed individually with upsert semantics
- **Batch**: Multi-value INSERT with ON CONFLICT clause
- **Copy**: Not supported for upsert operations (falls back to Batch)

**Why use upsert:** Upsert eliminates the need for separate insert/update logic and handles race conditions where a row might be inserted between your check and
insert operations.

## Delivery Semantics

The connector supports three delivery semantics to control data consistency guarantees during failures.

### AtLeastOnce (Default)

Guarantees that every item is delivered at least once. Items may be duplicated on retry after a failure.

```csharp
var configuration = new PostgresConfiguration
{
    DeliverySemantic = DeliverySemantic.AtLeastOnce,
    UseTransaction = true
};
```

**Characteristics:**

- No data loss
- Possible duplicates on retry
- Best for idempotent operations or when duplicates can be tolerated

**Use when:**

- Processing idempotent operations
- Duplicates can be filtered downstream
- Data loss is unacceptable

### AtMostOnce

Guarantees that every item is delivered at most once. Items may be lost on failure, but never duplicated.

```csharp
var configuration = new PostgresConfiguration
{
    DeliverySemantic = DeliverySemantic.AtMostOnce
};
```

**Characteristics:**

- No duplicates
- Possible data loss on failure
- Best for high-throughput scenarios where occasional data loss is acceptable

**Use when:**

- Processing telemetry or metrics data
- High throughput is critical
- Occasional data loss is acceptable

### ExactlyOnce

Guarantees that every item is delivered exactly once. Uses transactional semantics to prevent both data loss and duplication.

```csharp
var configuration = new PostgresConfiguration
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

**Use when:**

- Financial transactions
- Audit logging
- Any scenario requiring strict exactly-once guarantees

### Delivery Semantic Comparison

| Semantic    | Data Loss | Duplicates | Overhead | Use Case                        |
|-------------|-----------|------------|----------|---------------------------------|
| AtLeastOnce | No        | Possible   | Low      | General purpose, idempotent ops |
| AtMostOnce  | Possible  | No         | Low      | Telemetry, metrics              |
| ExactlyOnce | No        | No         | High     | Financial, audit                |

## Checkpointing

Checkpointing enables pipelines to resume from where they left off after a failure, rather than restarting from the beginning.

### Checkpoint Strategies

#### None (Default)

No checkpointing is performed. Failures require restarting from the beginning.

```csharp
var configuration = new PostgresConfiguration
{
    CheckpointStrategy = CheckpointStrategy.None
};
```

#### InMemory

Stores checkpoint state in memory. Enables recovery from transient failures during a single process execution.

```csharp
var configuration = new PostgresConfiguration
{
    CheckpointStrategy = CheckpointStrategy.InMemory,
    StreamResults = true
};

var source = new PostgresSourceNode<Order>(
    connectionString,
    "SELECT * FROM orders ORDER BY id",  // ORDER BY is required for checkpointing
    configuration: configuration
);
```

**Limitations:** Checkpoint state is lost when the process terminates.

#### Offset

Persists numeric offset checkpoints to external storage. Tracks position using a monotonically increasing column (e.g., auto-increment ID).

```csharp
var configuration = new PostgresConfiguration
{
    CheckpointStrategy = CheckpointStrategy.Offset,
    CheckpointOffsetColumn = "id",
    CheckpointStorage = new FileCheckpointStorage("checkpoints/order_offset.json")
};

var source = new PostgresSourceNode<Order>(
    connectionString,
    "SELECT * FROM orders WHERE id > @lastCheckpoint ORDER BY id",
    configuration: configuration
);
```

**Requirements:**

- Requires `CheckpointOffsetColumn` to be specified
- Requires `CheckpointStorage` to persist checkpoints
- Query must include `ORDER BY` on the offset column

#### KeyBased

Tracks processed items using composite keys. Useful for tables without a single monotonic column.

```csharp
var configuration = new PostgresConfiguration
{
    CheckpointStrategy = CheckpointStrategy.KeyBased,
    CheckpointKeyColumns = new[] { "customer_id", "order_date" },
    CheckpointStorage = new FileCheckpointStorage("checkpoints/order_keys.json")
};
```

**Requirements:**

- Requires `CheckpointKeyColumns` to be specified
- Requires `CheckpointStorage` to persist checkpoints

#### Cursor

Tracks cursor position for cursor-based iteration.

```csharp
var configuration = new PostgresConfiguration
{
    CheckpointStrategy = CheckpointStrategy.Cursor,
    CheckpointStorage = new FileCheckpointStorage("checkpoints/cursor.json")
};
```

#### CDC (Change Data Capture)

Tracks WAL position for PostgreSQL logical replication. Enables capturing changes from the PostgreSQL write-ahead log.

```csharp
var configuration = new PostgresConfiguration
{
    CheckpointStrategy = CheckpointStrategy.CDC,
    CdcSlotName = "my_pipeline_slot",
    CdcPublicationName = "my_publication",
    CheckpointStorage = new FileCheckpointStorage("checkpoints/cdc.json")
};
```

**Requirements:**

- Requires `CdcSlotName` to be specified
- Requires PostgreSQL logical replication to be configured
- Requires appropriate PostgreSQL permissions

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
var configuration = new PostgresConfiguration
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
using NPipeline.Connectors.Postgres.DependencyInjection;

var services = new ServiceCollection()
    .AddPostgresConnector(options =>
    {
        options.DefaultConnectionString = "Host=localhost;Database=mydb;Username=postgres;Password=password";
        options.AddOrUpdateConnection("analytics", "Host=localhost;Database=analytics;Username=postgres;Password=postgres");
        options.DefaultConfiguration = new PostgresConfiguration
        {
            StreamResults = true,
            FetchSize = 1_000,
            BatchSize = 1_000
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

**Why streaming matters:** Without streaming, the entire result set is loaded into memory. Streaming fetches rows in batches, allowing you to process millions
of rows without memory issues.

## Analyzers

The PostgreSQL connector includes a companion analyzer package that provides compile-time diagnostics to help prevent common mistakes when using checkpointing.

### Installation

```bash
dotnet add package NPipeline.Connectors.Postgres.Analyzers
```

### NP9501: Checkpointing requires ORDER BY clause

**Category:** Reliability
**Default Severity:** Warning

When using checkpointing with PostgreSQL source nodes, the SQL query must include an `ORDER BY` clause on a unique, monotonically increasing column. This
ensures consistent row ordering across checkpoint restarts. Without proper ordering, checkpointing may skip rows or process duplicates.

#### Example

```csharp
// ❌ Warning: Missing ORDER BY clause
var source = new PostgresSourceNode<MyRecord>(
    connectionString,
    "SELECT id, name, created_at FROM my_table",
    configuration: new PostgresConfiguration
    {
        CheckpointStrategy = CheckpointStrategy.Offset
    }
);

// ✅ Correct: Includes ORDER BY clause
var source = new PostgresSourceNode<MyRecord>(
    connectionString,
    "SELECT id, name, created_at FROM my_table ORDER BY id",
    configuration: new PostgresConfiguration
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
the [PostgreSQL Analyzer documentation](https://github.com/npipeline/NPipeline/blob/main/src/NPipeline.Connectors.Postgres.Analyzers/README.md).

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

## Security

- **Identifier validation** - Enabled by default to prevent SQL injection
- **Parameterized queries** - All queries use parameterized statements
- **SSL/TLS support** - Encrypt connections to database

## Documentation

For comprehensive documentation including advanced scenarios, configuration reference, and best practices, see
the [PostgreSQL Connector documentation](https://github.com/npipeline/NPipeline/blob/main/docs/connectors/postgres.md).

## License

MIT License - see LICENSE file for details.
