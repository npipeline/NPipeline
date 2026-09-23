# NPipeline.Connectors.MongoDB

MongoDB connector for NPipeline data pipelines. Provides source and sink nodes for reading from and writing to MongoDB collections with support for filtering,
sorting, projections, custom mapping, multiple write strategies (InsertMany, Upsert, BulkWrite), and change streams.

## Installation

```bash
dotnet add package NPipeline.Connectors.MongoDB
```

## Quick Start

### Reading from MongoDB

```csharp
using NPipeline.Connectors.MongoDB.Configuration;
using NPipeline.Connectors.MongoDB.Nodes;
using NPipeline.Pipeline;
using MongoDB.Driver;

// Define your model
public record Customer(string Id, string Name, string Email);

// Create a source node
var connectionString = "mongodb://localhost:27017";
var configuration = new MongoConfiguration
{
    DatabaseName = "mydb",
    CollectionName = "customers",
    BatchSize = 1000
};

var source = new MongoSourceNode<Customer>(connectionString, configuration);

// Use in a pipeline
var pipeline = new PipelineBuilder()
    .AddSource(source, "customer_source")
    .AddSink<ConsoleSinkNode<Customer>, Customer>("console_sink")
    .Build();

await pipeline.RunAsync();
```

### Writing to MongoDB

```csharp
using NPipeline.Connectors.MongoDB.Configuration;
using NPipeline.Connectors.MongoDB.Nodes;
using NPipeline.Pipeline;

// Define your model
public record Customer(string Id, string Name, string Email);

// Create a sink node
var connectionString = "mongodb://localhost:27017";
var configuration = new MongoConfiguration
{
    DatabaseName = "mydb",
    CollectionName = "customers",
    WriteStrategy = MongoWriteStrategy.Upsert,
    UpsertKeyFields = ["Id"]
};

var sink = new MongoSinkNode<Customer>(connectionString, configuration);

// Use in a pipeline
var pipeline = new PipelineBuilder()
    .AddSource<InMemorySourceNode<Customer>, Customer>("source")
    .AddSink(sink, "customer_sink")
    .Build();

await pipeline.RunAsync();
```

### Watching MongoDB Changes with Change Streams

```csharp
using NPipeline.Connectors.MongoDB.Configuration;
using NPipeline.Connectors.MongoDB.Nodes;
using NPipeline.Pipeline;

// Define your model
public record CustomerChangeEvent(string Id, string OperationType, Customer FullDocument);

// Create a change stream source node
var connectionString = "mongodb://localhost:27017";
var configuration = new MongoConfiguration
{
    DatabaseName = "mydb",
    CollectionName = "customers"
};

var source = new MongoChangeStreamSourceNode<CustomerChangeEvent>(connectionString, configuration);

// Use in a pipeline
var pipeline = new PipelineBuilder()
    .AddSource(source, "change_stream_source")
    .AddSink<ConsoleSinkNode<CustomerChangeEvent>, CustomerChangeEvent>("console_sink")
    .Build();

await pipeline.RunAsync();
```

## Features

- **Source Node** - Read documents from MongoDB collections with filtering, sorting, and projections
- **Change Stream Source Node** - Monitor collection changes in real-time for CDC (Change Data Capture) scenarios
- **Sink Node** - Write documents to MongoDB collections with multiple strategies
- **Write Strategies** - InsertMany for bulk inserts, Upsert for idempotent updates, BulkWrite for mixed operations
- **Duplicate Key Handling** - Configurable actions (Fail, Skip, Overwrite) when key conflicts occur
- **Custom Mapping** - Custom mappers for complex transformations between BSON and .NET types
- **Batch Operations** - Configurable batch sizes for optimal throughput
- **Filter & Projection** - Native MongoDB filter and projection support
- **Read Preferences** - Configurable read preferences for replica set routing
- **Connection Pooling** - Built-in connection pooling through MongoDB.Driver
- **Streaming** - Memory-efficient streaming for large result sets

## Configuration Options

### MongoConfiguration

The `MongoConfiguration` class provides configuration options:

| Property                | Type                  | Default    | Description                                     |
|-------------------------|-----------------------|------------|-------------------------------------------------|
| `ConnectionString`      | `string`              | *required* | MongoDB connection string                       |
| `DatabaseName`          | `string`              | *required* | MongoDB database name                           |
| `CollectionName`        | `string`              | *required* | MongoDB collection name                         |
| `BatchSize`             | `int`                 | 1000       | Batch size for reading documents                |
| `NoCursorTimeout`       | `bool`                | false      | Disable cursor timeout for long-running queries |
| `ReadPreference`        | `ReadPreferenceMode?` | null       | Read preference mode (Primary, Secondary, etc.) |
| `CommandTimeoutSeconds` | `int`                 | 30         | Command timeout in seconds                      |
| `StreamResults`         | `bool`                | true       | Stream results instead of loading all at once   |
| `WriteStrategy`         | `MongoWriteStrategy`  | BulkWrite  | Write strategy (InsertMany, Upsert, BulkWrite)  |
| `WriteBatchSize`        | `int`                 | 1000       | Batch size for write operations                 |
| `OrderedWrites`         | `bool`                | false      | Execute writes in order                         |
| `OnDuplicate`           | `OnDuplicateAction`   | Fail       | Action on duplicate key (Fail, Skip, Overwrite) |
| `UpsertKeyFields`       | `string[]`            | ["_id"]    | Key fields for upsert operations                |
| `Resilience`            | `Resilience`          | `MongoConnectorResilience.Default` | How the sink retries a batch. See [Resilience](#resilience) |

### MongoWriteStrategy

The `MongoWriteStrategy` enum specifies the write operation mode:

- **InsertMany** - Uses `inserts_many()` for new documents (fastest, fails on duplicates)
- **Upsert** - Uses `replaceOne()` with upsert enabled (idempotent, updates existing documents)
- **BulkWrite** - Uses `bulkWrite()` for mixed operations with fine-grained control

### OnDuplicateAction

The `OnDuplicateAction` enum specifies handling for duplicate key conflicts:

- **Fail** - Throw an exception (default)
- **Skip** - Skip the duplicate document
- **Overwrite** - Replace the existing document

## Resilience

The sink and the change stream source retry through [NResilience](https://github.com/nresilience/NResilience). The
`Resilience` property configures each one.

**Sink.** `MongoConfiguration.Resilience` defaults to `MongoConnectorResilience.Default`, which does the following:

- Makes up to four attempts per batch (three retries), the same count as the old `MaxRetryAttempts = 3`.
- Retries connection failures, server selection timeouts, and server errors that MongoDB marks as retryable (the
  `RetryableWriteError` label, primary step-downs, shutdowns, and network errors). An error labelled
  `SystemOverloadedError` takes the longer throttled backoff. Other errors, including authorization failures and
  duplicate keys, are not retried.
- Waits with exponential backoff and full jitter, from 1 second up to 30 seconds.
- Has no attempt timeout and no overall deadline, because a large batch can take a long time. The driver's own
  server selection and socket timeouts still bound each command.

A retried write never duplicates documents. The sink maps each batch once and gives every inserted document an `_id`
before the first attempt, so each attempt sends the same documents. If an earlier attempt was applied but its reply
was lost, the retry finds those documents already there: with `OnDuplicate = Ignore` they are skipped, and with
`OnDuplicate = Fail` the sink reports a duplicate key instead of writing them twice. A bulk write that reported write
errors, which means part of it may already be applied, is never retried. Upserts replace by key, so a retry of an
upsert is harmless.

**Change stream.** `MongoChangeStreamConfiguration.Resilience` defaults to `MongoConnectorResilience.ChangeStream`: up
to four attempts to open the stream, with backoff from 2 seconds up to 30 seconds. Only the open is retried. Once the
stream is open, the driver resumes it by itself after a resumable error (a network error, a primary step-down, and
similar), from the last change it returned, so nothing is emitted twice. A failure the driver can't resume ends the
stream with that exception. The node's `ResumeToken` then holds the token of the last change it emitted, and opening
the same node again resumes after it.

To change a setting, derive a policy with a `with` expression. To turn retries off, use `Resilience.None`:

```csharp
var config = new MongoConfiguration
{
    DatabaseName = "shop",
    CollectionName = "orders",
    Resilience = MongoConnectorResilience.Default with { Attempts = 6 },
};
```

The driver's retryable writes and reads (`retryWrites` and `retryReads` in the connection string, on by default) stay
on. They retry a single command once, immediately, and the server discards a retried write it already applied, so
they are part of the protocol's exactly-once machinery rather than a second retry policy. The connector's policy
handles what they can't: outages longer than one immediate retry, with backoff in between.

## Advanced Usage

### Custom Filtering and Sorting

```csharp
using MongoDB.Driver;

var filter = Builders<BsonDocument>.Filter.Eq("status", "active");
var sort = Builders<BsonDocument>.Sort.Descending("created_at");

var source = new MongoSourceNode<Customer>(
    connectionString,
    configuration,
    filter: filter,
    sort: sort
);
```

### Custom Mapping

```csharp
// Custom mapper for complex transformations
Func<MongoRow, CustomerDto> mapper = row =>
{
    return new CustomerDto(
        Id: row.GetValue<string>("_id"),
        Name: row.GetValue<string>("name"),
        Email: row.GetValue<string>("email"),
        CreatedAt: row.GetValue<DateTime>("created_at")
    );
};

var source = new MongoSourceNode<CustomerDto>(
    connectionString,
    configuration,
    customMapper: mapper
);
```

### Dependency Injection Integration

```csharp
services.AddMongoDB(options =>
{
    options.ConnectionString = "mongodb://localhost:27017";
});

services.AddMongoSourceNode<Customer>(new MongoConfiguration
{
    DatabaseName = "mydb",
    CollectionName = "customers"
});

services.AddMongoSinkNode<Customer>(new MongoConfiguration
{
    DatabaseName = "mydb",
    CollectionName = "customers_archive",
    WriteStrategy = MongoWriteStrategy.Upsert
});
```

## Requirements

- **.NET 8.0, 9.0, or 10.0**
- **MongoDB 4.0+** (4.4+ for change streams)
- **MongoDB.Driver 3.6.0+** (automatically included as a dependency)
- **NPipeline.Connectors** (automatically included as a dependency)

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Connectors](https://www.nuget.org/packages/NPipeline.Connectors)** - Storage abstractions and base connectors

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.
