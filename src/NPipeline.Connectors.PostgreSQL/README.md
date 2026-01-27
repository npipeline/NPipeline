# NPipeline.Connectors.PostgreSQL

A PostgreSQL connector for NPipeline data pipelines. This connector provides source and sink nodes for reading from and writing to PostgreSQL databases, with support for convention-based mapping, custom mappers, connection pooling, and more.

## Installation

Install the NuGet package:

```bash
dotnet add package NPipeline.Connectors.PostgreSQL
```

## Features

### Core Connectivity

- Basic connection to PostgreSQL
- Connection string configuration
- Connection pooling support
- SSL/TLS support
- NpgsqlDataSource support
- Connection timeout configuration
- Pool size configuration

### Source Node Features

- `PostgresSourceNode<T>` for reading data
- SQL query execution with parameters
- Streaming support with configurable fetch size
- Custom mapper support with `Func<PostgresRow, T>`
- Convention-based mapping with snake_case conversion
- Retry logic with transient error detection
- Row-level error handling with continue on error option
- Multiple result set support
- Command timeout configuration

### Sink Node Features

- `PostgresSinkNode<T>` for writing data
- Per-row write strategy
- Batch write strategy
- Transaction support
- Batch size configuration
- Error handling with continue on error option

### Mapping Features

- `PostgresMapperBuilder` for building complex mappers
- `PostgresParameterMapper` for parameter mapping
- `PostgresTableAttribute` for table name mapping
- `PostgresColumnAttribute` for column name mapping
- `PostgresIgnoreAttribute` for excluding properties
- Compiled delegate support for performance
- Metadata caching for performance
- Snake_case to PascalCase conversion

### Checkpointing

The PostgreSQL connector supports checkpoint strategies for recovery from failures during pipeline execution.

#### Available Checkpoint Strategies

- **None (Default)**: No checkpointing - data may be lost on failure. Use this when:
  - Data can be easily reprocessed
  - Performance is critical and checkpointing overhead is unacceptable
  - The source data is static and won't change

```csharp
var configuration = new PostgresConfiguration
{
    ConnectionString = connectionString,
    CheckpointStrategy = CheckpointStrategy.None
};
```

- **InMemory**: Checkpoints are stored in memory and lost on process restart. Provides recovery from transient failures during pipeline execution. Use this when:
  - You need to recover from transient failures (network issues, temporary database unavailability)
  - The pipeline runs for a long time and you want to avoid reprocessing all data on failure
  - Process restarts are acceptable to lose checkpoint state
  - You need simple checkpointing without external storage dependencies

```csharp
var configuration = new PostgresConfiguration
{
    ConnectionString = connectionString,
    CheckpointStrategy = CheckpointStrategy.InMemory,
    StreamResults = true
};

var source = new PostgresSourceNode<Customer>(
    connectionString,
    "SELECT id, name, email FROM customers",
    configuration: configuration
);
```

#### How InMemory Checkpointing Works

1. **Progress Tracking**: The connector tracks the row number as it reads from the database
2. **Checkpoint Updates**: For streaming mode, checkpoints are updated after each row. For buffered mode, checkpoints are updated every 100 rows
3. **Failure Recovery**: If a transient failure occurs, the pipeline can be restarted and will resume from the last checkpoint
4. **Checkpoint Clearing**: Checkpoints are automatically cleared when the pipeline completes successfully

#### Example: Using InMemory Checkpointing with Retry Logic

```csharp
using NPipeline.Connectors.PostgreSQL;
using NPipeline.Connectors.PostgreSQL.Nodes;
using NPipeline.Pipeline;
using NPipeline.Connectors.Configuration;

var configuration = new PostgresConfiguration
{
    ConnectionString = connectionString,
    CheckpointStrategy = CheckpointStrategy.InMemory,
    StreamResults = true,
    MaxRetryAttempts = 3,
    RetryDelay = TimeSpan.FromSeconds(5)
};

var source = new PostgresSourceNode<Customer>(
    connectionString,
    "SELECT id, name, email FROM large_customers_table",
    configuration: configuration
);

var pipeline = new PipelineBuilder()
    .AddSource(source)
    .Build();

// The pipeline will automatically retry on transient errors
// and resume from the last checkpoint position
await pipeline.RunAsync();
```

#### Limitations of InMemory Checkpointing

- **Process Restart**: Checkpoints are lost if the process restarts
- **Multiple Instances**: Each process instance maintains its own checkpoint state
- **Memory Usage**: Minimal memory overhead for storing checkpoint state
- **Thread Safety**: Checkpoint storage is thread-safe for concurrent pipeline executions

### Delivery Semantics

- AtLeastOnce delivery
- AtMostOnce delivery

### Error Handling

- `PostgresException` for general PostgreSQL errors
- `PostgresConnectionException` for connection errors
- `PostgresMappingException` for mapping errors
- `PostgresExceptionFactory` for exception creation
- `PostgresTransientErrorDetector` for detecting transient errors
- SQL state-based error classification
- Error code tracking
- IsTransient property on exceptions
- Retry policy support
- Error context preservation
- Detailed error messages
- Stack trace preservation

### Configuration

- `PostgresConfiguration` for connector settings
- Command timeout configuration
- Read buffer size configuration
- Identifier validation (SQL injection prevention)
- DI support with `PostgresOptions`
- Schema configuration
- SSL mode configuration
- Prepared statement configuration

### Dependency Injection

- DI integration with `AddPostgresConnector`
- Connection pool abstractions
- Mapper factory support
- Source node factory support
- Sink node factory support
- Named connections support
- Keyed services support
- Default connection configuration
- Configuration validation
- Service provider integration

### Performance

- Prepared statements support
- Connection pooling
- Streaming for large result sets
- Compiled delegates for mapping
- Metadata caching

### Security

- Identifier validation to prevent SQL injection
- SSL/TLS support for secure connections

## Quick Start

### Reading from PostgreSQL

```csharp
using NPipeline.Connectors.PostgreSQL;
using NPipeline.Connectors.PostgreSQL.Nodes;
using NPipeline.Pipeline;

// Define your model
public class Customer
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string Email { get; set; }
}

// Create a source node
var connectionString = "Host=localhost;Database=mydb;Username=postgres;Password=password";
var source = new PostgresSourceNode<Customer>(
    connectionString,
    "SELECT id, name, email FROM customers WHERE active = @active",
    parameters: new[] { new DatabaseParameter("@active", true) }
);

// Use in a pipeline
var pipeline = new PipelineBuilder()
    .AddSource(source)
    .Build();

await pipeline.RunAsync();
```

### Reading from PostgreSQL with In-Memory Checkpointing

```csharp
using NPipeline.Connectors.PostgreSQL;
using NPipeline.Connectors.PostgreSQL.Nodes;
using NPipeline.Pipeline;
using NPipeline.Connectors.Configuration;

// Define your model
public class Customer
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string Email { get; set; }
}

// Create a source node with in-memory checkpointing
var connectionString = "Host=localhost;Database=mydb;Username=postgres;Password=password";
var configuration = new PostgresConfiguration
{
    ConnectionString = connectionString,
    CheckpointStrategy = CheckpointStrategy.InMemory,
    StreamResults = true
};

var source = new PostgresSourceNode<Customer>(
    connectionString,
    "SELECT id, name, email FROM customers WHERE active = @active",
    configuration: configuration,
    parameters: new[] { new DatabaseParameter("@active", true) }
);

// Use in a pipeline
var pipeline = new PipelineBuilder()
    .AddSource(source)
    .Build();

await pipeline.RunAsync();
```

### Writing to PostgreSQL

```csharp
using NPipeline.Connectors.PostgreSQL;
using NPipeline.Connectors.PostgreSQL.Nodes;
using NPipeline.Pipeline;

// Define your model
public class Customer
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string Email { get; set; }
}

// Create a sink node
var connectionString = "Host=localhost;Database=mydb;Username=postgres;Password=password";
var sink = new PostgresSinkNode<Customer>(
    connectionString,
    "customers",
    writeStrategy: PostgresWriteStrategy.Batch
);

// Use in a pipeline
var pipeline = new PipelineBuilder()
    .AddSource(source)
    .AddSink(sink)
    .Build();

await pipeline.RunAsync();
```

## Configuration

### PostgresConfiguration

```csharp
var configuration = new PostgresConfiguration
{
    ConnectionString = "Host=localhost;Database=mydb;Username=postgres;Password=password",
    Schema = "public",
    CommandTimeout = 30,
    ConnectionTimeout = 15,
    MinPoolSize = 1,
    MaxPoolSize = 100,
    ReadBufferSize = 8192,
    EnableSsl = true,
    SslMode = PostgresSslMode.Require,
    UsePreparedStatements = true,
    ValidateIdentifiers = true,
    DeliverySemantic = DeliverySemantic.AtLeastOnce,
    CheckpointStrategy = CheckpointStrategy.None
};
```

### Connection String Options

The connection string supports all Npgsql connection string options:

```
Host=localhost
Port=5432
Database=mydb
Username=postgres
Password=password
Timeout=15
Pooling=true
MinPoolSize=1
MaxPoolSize=100
SslMode=Require
```

## Mapping

### Convention-Based Mapping

By default, properties are mapped to columns using snake_case conversion:

```csharp
public class Customer
{
    public int CustomerId { get; set; }  // Maps to customer_id
    public string FirstName { get; set; }   // Maps to first_name
    public string EmailAddress { get; set; } // Maps to email_address
}
```

### Attribute-Based Mapping

Use attributes to override convention-based mapping:

```csharp
[PostgresTable("customers", Schema = "sales")]
public class Customer
{
    [PostgresColumn("cust_id", IsPrimaryKey = true)]
    public int Id { get; set; }

    [PostgresColumn("full_name")]
    public string Name { get; set; }

    [PostgresIgnore]
    public string TemporaryField { get; set; }
}
```

### Custom Mappers

For complex mapping scenarios, use a custom mapper:

```csharp
var source = new PostgresSourceNode<Customer>(
    connectionString,
    "SELECT * FROM customers",
    mapper: row => new Customer
    {
        Id = row.Get<int>("id"),
        Name = row.Get<string>("name"),
        Email = row.Get<string>("email")
    }
);
```

## Write Strategies

### Per-Row Write Strategy

Writes each row individually. Good for:

- Small batches
- Scenarios requiring immediate feedback
- Error handling per row

```csharp
var sink = new PostgresSinkNode<Customer>(
    connectionString,
    "customers",
    writeStrategy: PostgresWriteStrategy.PerRow
);
```

### Batch Write Strategy

Writes rows in batches for better performance. Good for:

- Large datasets
- Bulk import scenarios
- Performance-critical operations

```csharp
var configuration = new PostgresConfiguration
{
    BatchSize = 100
};

var sink = new PostgresSinkNode<Customer>(
    connectionString,
    "customers",
    writeStrategy: PostgresWriteStrategy.Batch,
    configuration: configuration
);
```

## Dependency Injection

### Basic Configuration

```csharp
var services = new ServiceCollection();

services.AddPostgresConnector(options =>
{
    options.DefaultConnectionString = "Host=localhost;Database=mydb;Username=postgres;Password=password";
    options.DefaultBatchSize = 100;
    options.DefaultCommandTimeout = 30;
});

var serviceProvider = services.BuildServiceProvider();
```

### Named Connections

```csharp
var services = new ServiceCollection();

services.AddPostgresConnector(options =>
{
    options.NamedConnections["primary"] = "Host=primary;Database=mydb;Username=postgres;Password=password";
    options.NamedConnections["replica"] = "Host=replica;Database=mydb;Username=postgres;Password=password";
});

var serviceProvider = services.BuildServiceProvider();
```

### Adding Connections Individually

```csharp
var services = new ServiceCollection();

services.AddPostgresConnector();
services.AddPostgresConnection("primary", "Host=primary;Database=mydb;Username=postgres;Password=password");
services.AddPostgresConnection("replica", "Host=replica;Database=mydb;Username=postgres;Password=password");

var serviceProvider = services.BuildServiceProvider();
```

### Keyed Services

```csharp
var services = new ServiceCollection();

services.AddPostgresConnector();
services.AddKeyedPostgresConnection("primary", "Host=primary;Database=mydb;Username=postgres;Password=password");
services.AddKeyedPostgresConnection("replica", "Host=replica;Database=mydb;Username=postgres;Password=password");

var serviceProvider = services.BuildServiceProvider();

var connectionPool = serviceProvider.GetRequiredKeyedService<IPostgresConnectionPool>("primary");
```

## Error Handling

### Transient Error Detection

The connector automatically detects transient errors and supports retry logic:

```csharp
// Transient errors include:
// - Connection failures (08006, 08001, 08004)
// - Admin shutdown (57P01, 57P02, 57P03)
// - Serialization failures (40001)
// - Deadlocks (40P01)
// - Resource limits (53000, 53100, 53200, 54000)
```

### Custom Exception Handling

```csharp
try
{
    await pipeline.RunAsync();
}
catch (PostgresException ex) when (ex.IsTransient)
{
    // Retry the operation
    await Task.Delay(TimeSpan.FromSeconds(5));
    await pipeline.RunAsync();
}
catch (PostgresMappingException ex)
{
    // Handle mapping errors
    Console.WriteLine($"Mapping error for property {ex.PropertyName}: {ex.Message}");
}
catch (PostgresConnectionException ex)
{
    // Handle connection errors
    Console.WriteLine($"Connection error: {ex.Message}");
}
```

## Streaming

For large result sets, use streaming to reduce memory usage:

```csharp
var configuration = new PostgresConfiguration
{
    StreamResults = true,
    FetchSize = 1000
};

var source = new PostgresSourceNode<Customer>(
    connectionString,
    "SELECT * FROM large_table",
    configuration: configuration
);
```

## SSL/TLS Configuration

Configure SSL/TLS for secure connections:

```csharp
var configuration = new PostgresConfiguration
{
    EnableSsl = true,
    SslMode = PostgresSslMode.VerifyFull
};
```

Available SSL modes:

- `Disable` - No SSL (default)
- `Allow` - Allow SSL but don't require it
- `Prefer` - Prefer SSL if available
- `Require` - Require SSL
- `VerifyCa` - Require SSL and verify certificate authority
- `VerifyFull` - Require SSL and verify certificate authority and hostname

## Advanced Examples

### Custom Parameter Mapping for Sink

```csharp
var sink = new PostgresSinkNode<Customer>(
    connectionString,
    "customers",
    writeStrategy: PostgresWriteStrategy.Batch,
    parameterMapper: customer => new[]
    {
        new DatabaseParameter("@id", customer.Id),
        new DatabaseParameter("@name", customer.Name),
        new DatabaseParameter("@email", customer.Email),
        new DatabaseParameter("@created_at", DateTime.UtcNow)
    }
);
```

### Using Connection Pool Directly

```csharp
var connectionPool = new PostgresConnectionPool(connectionString);

var source = new PostgresSourceNode<Customer>(
    connectionPool,
    "SELECT * FROM customers"
);

var sink = new PostgresSinkNode<Customer>(
    connectionPool,
    "customers",
    writeStrategy: PostgresWriteStrategy.Batch
);
```

### Named Connection Pool

```csharp
var namedConnections = new Dictionary<string, string>
{
    ["primary"] = "Host=primary;Database=mydb;Username=postgres;Password=password",
    ["replica"] = "Host=replica;Database=mydb;Username=postgres;Password=password"
};

var connectionPool = new PostgresConnectionPool(namedConnections);

var primarySource = new PostgresSourceNode<Customer>(
    connectionPool,
    "SELECT * FROM customers"
);

// Get connection for replica
var replicaConnection = await connectionPool.GetConnectionAsync("replica");
```

## Performance Considerations

1. **Use Batch Write Strategy** - For large datasets, batch writes are significantly faster than per-row writes
2. **Enable Streaming** - For large result sets, streaming reduces memory usage
3. **Configure Pool Size** - Adjust pool size based on your application's concurrency needs
4. **Use Prepared Statements** - Enabled by default for better query performance
5. **Optimize Fetch Size** - Adjust fetch size based on your data size and memory constraints

## Security

The connector includes security features to prevent SQL injection:

1. **Identifier Validation** - Enabled by default, validates table and column names
2. **Parameterized Queries** - All queries use parameterized statements
3. **SSL/TLS Support** - Encrypt connections to the database

## License

This project is part of NPipeline. See the main project for licensing information.

## Contributing

Contributions are welcome! Please see the main NPipeline repository for contribution guidelines.
