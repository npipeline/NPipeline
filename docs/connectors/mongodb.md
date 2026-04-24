---
title: MongoDB Connector
description: Read from and write to MongoDB databases with NPipeline using the MongoDB connector.
order: 15
---

## MongoDB Connector

The `NPipeline.Connectors.MongoDB` package provides specialized source and sink nodes for working with MongoDB databases. This allows you to easily integrate MongoDB data into your pipelines as an input source or an output destination.

This connector uses the official [MongoDB C# Driver](https://www.mongodb.com/docs/drivers/csharp/v2.19/) under the hood, providing reliable streaming reads, multiple write strategies, upsert support, and connection management.

## Installation

To use the MongoDB connector, install the `NPipeline.Connectors.MongoDB` NuGet package:

```bash
dotnet add package NPipeline.Connectors.MongoDB
```

For the core NPipeline package and other available extensions, see the [Installation Guide](../getting-started/installation.md).

## Quick Start

### Reading from MongoDB

```csharp
using NPipeline.Connectors.MongoDB.Configuration;
using NPipeline.Connectors.MongoDB.Nodes;

// Define your model
public sealed record Order
{
    public string Id { get; set; } = string.Empty;
    public string Customer { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public string Status { get; set; } = string.Empty;
}

// Create and configure the source node
var configuration = new MongoConfiguration
{
    DatabaseName = "shop",
    CollectionName = "orders",
    StreamResults = true,
    BatchSize = 1000
};

var sourceNode = new MongoSourceNode<Order>(
    "mongodb://localhost:27017",
    configuration);

// Use in a pipeline
var source = builder.AddSource(sourceNode, "mongo_source");
```

### Writing to MongoDB

```csharp
var sinkConfig = new MongoConfiguration
{
    DatabaseName = "shop",
    CollectionName = "processed_orders",
    WriteStrategy = MongoWriteStrategy.InsertMany,
    WriteBatchSize = 100
};

var sinkNode = new MongoSinkNode<ProcessedOrder>(
    "mongodb://localhost:27017",
    sinkConfig);

var sink = builder.AddSink(sinkNode, "mongo_sink");
```

## Dependency Injection

The MongoDB connector supports dependency injection for managing MongoDB clients and node factories. This is the recommended approach for production applications.

### Registering the Connector

Use `AddMongoConnector` to register shared MongoDB client management:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.MongoDB.DependencyInjection;

var services = new ServiceCollection()
    .AddMongoConnector(options =>
    {
        // Set a default connection string (optional if using only named connections)
        options.DefaultConnectionString = "mongodb://localhost:27017";

        // Add named connections for different databases
        options.AddOrUpdateConnection("analytics", "mongodb://mongo1:27017/analytics");
        options.AddOrUpdateConnection("warehouse", "mongodb://mongo2:27017/warehouse");

        // Configure default settings
        options.DefaultConfiguration = new MongoConfiguration
        {
            BatchSize = 1_000,
            MaxRetryAttempts = 3,
            RetryDelay = TimeSpan.FromSeconds(2)
        };
    })
    .BuildServiceProvider();
```

### Why Use Dependency Injection?

Using dependency injection provides several benefits:

- **Connection Management**: Shared MongoDB clients are efficiently managed across multiple nodes
- **Configuration Centralization**: All MongoDB connections are configured in one place
- **Testability**: Easy to mock or replace dependencies in unit tests
- **Lifetime Management**: Clients are properly disposed when the application shuts down

## `MongoSourceNode<T>`

The `MongoSourceNode<T>` reads documents from a MongoDB collection and emits each document as an item of type `T`.

### Source Configuration

The constructor for `MongoSourceNode<T>` provides multiple overloads for flexibility:

```csharp
// Using connection string
public MongoSourceNode<T>(
    string connectionString,
    MongoConfiguration configuration,
    FilterDefinition<BsonDocument>? filter = null,
    SortDefinition<BsonDocument>? sort = null,
    ProjectionDefinition<BsonDocument>? projection = null,
    Func<MongoRow, T>? customMapper = null)

// Using an existing MongoDB client
public MongoSourceNode<T>(
    IMongoClient client,
    MongoConfiguration configuration,
    FilterDefinition<BsonDocument>? filter = null,
    SortDefinition<BsonDocument>? sort = null,
    ProjectionDefinition<BsonDocument>? projection = null,
    Func<MongoRow, T>? customMapper = null)

// Using a storage URI (with storage provider)
public MongoSourceNode<T>(
    StorageUri uri,
    MongoConfiguration configuration,
    FilterDefinition<BsonDocument>? filter = null,
    SortDefinition<BsonDocument>? sort = null,
    ProjectionDefinition<BsonDocument>? projection = null,
    Func<MongoRow, T>? customMapper = null)

// Using a storage provider
public MongoSourceNode<T>(
    IStorageProvider storageProvider,
    StorageUri uri,
    MongoConfiguration configuration,
    FilterDefinition<BsonDocument>? filter = null,
    SortDefinition<BsonDocument>? sort = null,
    ProjectionDefinition<BsonDocument>? projection = null,
    Func<MongoRow, T>? customMapper = null)
```

**Parameters:**

- **`connectionString`**: MongoDB connection string (e.g., `"mongodb://localhost:27017"`)
- **`client`**: Pre-configured `IMongoClient` instance
- **`configuration`**: Required configuration with `DatabaseName` and `CollectionName`
- **`filter`**: Optional MongoDB filter definition to limit results
- **`sort`**: Optional sort definition for ordering results
- **`projection`**: Optional projection definition to limit returned fields
- **`customMapper`**: Custom function to map a `MongoRow` to type `T`

### Example: Reading with a Filter

```csharp
using MongoDB.Driver;

var filter = Builders<BsonDocument>.Filter.Eq("status", "pending");
var sort = Builders<BsonDocument>.Sort.Ascending("createdAt");

var configuration = new MongoConfiguration
{
    DatabaseName = "shop",
    CollectionName = "orders",
    StreamResults = true,
    BatchSize = 100
};

var sourceNode = new MongoSourceNode<Order>(
    "mongodb://localhost:27017",
    configuration,
    filter: filter,
    sort: sort);
```

### Example: Using a Custom Row Mapper

```csharp
var sourceNode = new MongoSourceNode<Order>(
    "mongodb://localhost:27017",
    new MongoConfiguration
    {
        DatabaseName = "shop",
        CollectionName = "orders"
    },
    customMapper: row => new Order
    {
        Id = row.GetString("_id"),
        Customer = row.GetString("customer"),
        Amount = row.GetDecimal("amount"),
        Status = row.GetString("status"),
        CreatedAt = row.GetDateTime("createdAt")
    });
```

## `MongoSinkNode<T>`

The `MongoSinkNode<T>` writes items from the pipeline to a MongoDB collection.

### Sink Configuration

The constructor for `MongoSinkNode<T>` provides multiple overloads:

```csharp
// Using connection string
public MongoSinkNode<T>(
    string connectionString,
    MongoConfiguration configuration,
    Func<T, BsonDocument>? documentMapper = null,
    Func<T, FilterDefinition<BsonDocument>>? upsertFilterBuilder = null)

// Using an existing MongoDB client
public MongoSinkNode<T>(
    IMongoClient client,
    MongoConfiguration configuration,
    Func<T, BsonDocument>? documentMapper = null,
    Func<T, FilterDefinition<BsonDocument>>? upsertFilterBuilder = null)

// Using a storage URI
public MongoSinkNode<T>(
    StorageUri uri,
    MongoConfiguration configuration,
    Func<T, BsonDocument>? documentMapper = null,
    Func<T, FilterDefinition<BsonDocument>>? upsertFilterBuilder = null)

// Using a storage provider
public MongoSinkNode<T>(
    IStorageProvider storageProvider,
    StorageUri uri,
    MongoConfiguration configuration,
    Func<T, BsonDocument>? documentMapper = null,
    Func<T, FilterDefinition<BsonDocument>>? upsertFilterBuilder = null)
```

**Parameters:**

- **`connectionString`**: MongoDB connection string
- **`client`**: Pre-configured `IMongoClient` instance
- **`configuration`**: Required configuration with `DatabaseName`, `CollectionName`, and `WriteStrategy`
- **`documentMapper`**: Custom function to map type `T` to a `BsonDocument`
- **`upsertFilterBuilder`**: Custom function to build the filter for upsert operations

### Write Strategies

The connector supports three write strategies:

#### InsertMany Strategy

Uses MongoDB's `InsertMany` for batch inserts. This provides:

- Fastest performance for new documents
- Atomic batch inserts within a single command
- Fails on duplicate key errors (use `ContinueOnError = true` to continue)

```csharp
var config = new MongoConfiguration
{
    DatabaseName = "shop",
    CollectionName = "orders",
    WriteStrategy = MongoWriteStrategy.InsertMany,
    WriteBatchSize = 1000,
    OrderedWrites = true
};
```

#### Upsert Strategy

Uses `ReplaceOne` with upsert enabled. This provides:

- Idempotent writes (safe for re-runs)
- Updates existing documents or inserts new ones
- Requires `UpsertKeyFields` to be specified

```csharp
var config = new MongoConfiguration
{
    DatabaseName = "shop",
    CollectionName = "orders",
    WriteStrategy = MongoWriteStrategy.Upsert,
    UpsertKeyFields = new[] { "_id" }
};
```

#### BulkWrite Strategy

Uses MongoDB's `BulkWrite` API for maximum flexibility. This provides:

- Highest throughput for large datasets
- Support for mixed operation types
- Fine-grained error handling with `OrderedWrites = false`

```csharp
var config = new MongoConfiguration
{
    DatabaseName = "shop",
    CollectionName = "orders",
    WriteStrategy = MongoWriteStrategy.BulkWrite,
    WriteBatchSize = 1000,
    OrderedWrites = false // Disable for maximum throughput
};
```

### Write Strategy Comparison

| Strategy | Throughput | Idempotent | Best For |
|----------|------------|------------|----------|
| InsertMany | High | No | Initial data loads, append-only scenarios |
| Upsert | Medium | Yes | Incremental updates, re-runnable pipelines |
| BulkWrite | Very High | No | High-throughput bulk loading |

### Example: Writing with Upsert

```csharp
var config = new MongoConfiguration
{
    DatabaseName = "shop",
    CollectionName = "processed_orders",
    WriteStrategy = MongoWriteStrategy.Upsert,
    UpsertKeyFields = new[] { "_id" },
    WriteBatchSize = 100
};

var sinkNode = new MongoSinkNode<ProcessedOrder>(
    "mongodb://localhost:27017",
    config);
```

### Example: Using a Custom Document Mapper

```csharp
var sinkNode = new MongoSinkNode<Order>(
    "mongodb://localhost:27017",
    new MongoConfiguration
    {
        DatabaseName = "shop",
        CollectionName = "orders",
        WriteStrategy = MongoWriteStrategy.InsertMany
    },
    documentMapper: order => new BsonDocument
    {
        { "_id", order.Id },
        { "customer", order.Customer },
        { "amount", order.Amount },
        { "status", order.Status },
        { "createdAt", DateTime.UtcNow }
    });
```

## Configuration Reference

### MongoConfiguration

The `MongoConfiguration` class provides comprehensive options for configuring MongoDB operations.

#### Connection Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ConnectionString` | `string` | `""` | MongoDB connection string. Not required when using an `IMongoClient`. |
| `DatabaseName` | `string` | `""` | **Required.** The database name. |
| `CollectionName` | `string` | `""` | **Required.** The collection name. |

#### Read Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `BatchSize` | `int` | `1000` | Number of documents to fetch per batch when reading. |
| `NoCursorTimeout` | `bool` | `false` | Disable cursor timeout for long-running queries. |
| `ReadPreference` | `ReadPreferenceMode?` | `null` | Read preference (Primary, PrimaryPreferred, Secondary, etc.). |
| `CommandTimeoutSeconds` | `int` | `30` | Command timeout in seconds. |
| `StreamResults` | `bool` | `true` | Stream results instead of buffering all in memory. |

#### Write Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `WriteStrategy` | `MongoWriteStrategy` | `BulkWrite` | Strategy for writing documents. |
| `WriteBatchSize` | `int` | `1000` | Number of documents per batch write. |
| `OrderedWrites` | `bool` | `false` | Execute writes in order. Set to `true` to stop on first write error. |
| `OnDuplicate` | `OnDuplicateAction` | `Fail` | Action when a duplicate key is encountered. |
| `UpsertKeyFields` | `string[]` | `["_id"]` | Fields to use as the upsert key. Required when `OnDuplicate` is `Overwrite`. |

#### Resilience Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `MaxRetryAttempts` | `int` | `3` | Maximum retry attempts for transient errors. |
| `RetryDelay` | `TimeSpan` | `1 second` | Delay between retry attempts. |
| `ContinueOnError` | `bool` | `false` | Continue processing when a document-level error occurs. |
| `DocumentErrorHandler` | `Func<Exception, BsonDocument?, bool>?` | `null` | Handler for document-level errors. Return `true` to swallow the exception. |

#### Mapping Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `CaseInsensitiveMapping` | `bool` | `true` | Perform case-insensitive field matching. |
| `CacheMappingMetadata` | `bool` | `true` | Cache mapping metadata for performance. |
| `ThrowOnMappingError` | `bool` | `true` | Throw exceptions on mapping errors. |

#### Checkpoint Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `DeliverySemantic` | `DeliverySemantic` | `AtLeastOnce` | Delivery guarantee semantic. |
| `CheckpointStrategy` | `CheckpointStrategy` | `None` | Strategy for checkpointing. |
| `CheckpointStorage` | `ICheckpointStorage?` | `null` | Storage backend for checkpoints. |
| `CheckpointInterval` | `CheckpointIntervalConfiguration` | `new()` | Checkpoint save frequency. |
| `CheckpointFilePath` | `string?` | `null` | File path for file-based checkpoint storage. |
| `CheckpointCollectionName` | `string` | `"pipeline_checkpoints"` | Collection name for database checkpoint storage. |
| `CheckpointOffsetField` | `string?` | `null` | Field name for offset-based checkpointing. |
| `CheckpointKeyFields` | `string[]?` | `null` | Key fields for key-based checkpointing. |

### MongoWriteStrategy

Enum defining write strategies for the sink node.

| Value | Description |
|-------|-------------|
| `InsertMany` | Uses `InsertMany` for batch inserts. Fastest for new documents but fails on duplicate keys. |
| `Upsert` | Uses `ReplaceOne` with upsert enabled. Updates existing documents or inserts new ones. |
| `BulkWrite` | Uses `BulkWrite` for maximum flexibility and throughput. |

### OnDuplicateAction

Enum defining actions when a duplicate key is encountered.

| Value | Description |
|-------|-------------|
| `Fail` | Throw an exception on duplicate keys. |
| `Ignore` | Ignores the duplicate and continues with the next document. |
| `Overwrite` | Update existing documents with new values (requires `UpsertKeyFields`). |

## Attribute-Based Mapping

The MongoDB connector supports attribute-based mapping for mapping C# classes to MongoDB documents.

### `[MongoCollection]` Attribute

Specifies the collection name for a class:

```csharp
[MongoCollection("orders")]
public sealed record Order
{
    // ...
}
```

### `[MongoField]` Attribute

Maps a property to a specific MongoDB field name:

```csharp
[MongoCollection("orders")]
public sealed record Order
{
    [MongoField("_id")]
    public string Id { get; set; } = string.Empty;

    [MongoField("customer_name")]
    public string Customer { get; set; } = string.Empty;

    [MongoField("order_total")]
    public decimal Total { get; set; }
}
```

### Interoperability with `[BsonElement]`

The MongoDB connector also respects the standard MongoDB driver's `[BsonElement]` attribute:

```csharp
using MongoDB.Bson.Serialization.Attributes;

public sealed record Order
{
    [BsonElement("_id")]
    public string Id { get; set; } = string.Empty;

    [BsonElement("customer")]
    public string Customer { get; set; } = string.Empty;
}
```

### Convention-Based Mapping

When no attributes are specified, the connector uses convention-based mapping:

- Property names are mapped directly to field names
- Case-insensitive matching is enabled by default (`CaseInsensitiveMapping = true`)

## Custom Row Mappers

For complete control over mapping, provide custom mapper functions.

### Custom Source Mapper

Use `Func<MongoRow, T>` to map documents to objects:

```csharp
var sourceNode = new MongoSourceNode<Order>(
    connectionString,
    configuration,
    customMapper: row => new Order
    {
        Id = row.GetString("_id"),
        Customer = row.GetString("customer"),
        Amount = row.GetDecimal("amount"),
        Status = row.GetString("status", "pending"), // Default value
        CreatedAt = row.GetDateTime("createdAt", DateTime.UtcNow)
    });
```

### MongoRow Methods

The `MongoRow` class provides typed access to BSON document fields:

| Method | Description |
|--------|-------------|
| `GetString(name, defaultValue)` | Gets a string field value. |
| `GetInt32(name, defaultValue)` | Gets an Int32 field value. |
| `GetInt64(name, defaultValue)` | Gets an Int64 field value. |
| `GetDouble(name, defaultValue)` | Gets a double field value. |
| `GetDecimal(name, defaultValue)` | Gets a decimal field value. |
| `GetBoolean(name, defaultValue)` | Gets a boolean field value. |
| `GetDateTime(name, defaultValue)` | Gets a DateTime field value. |
| `GetGuid(name, defaultValue)` | Gets a Guid field value. |
| `GetDocument(name)` | Gets a nested document as a `MongoRow`. |
| `GetArray(name)` | Gets an array field as a `BsonArray`. |
| `GetBsonValue(name)` | Gets the raw `BsonValue`. |
| `HasField(name)` | Checks if a field exists. |
| `IsNullOrMissing(name)` | Checks if a field is null or missing. |

### Custom Sink Mapper

Use `Func<T, BsonDocument>` to map objects to documents:

```csharp
var sinkNode = new MongoSinkNode<Order>(
    connectionString,
    configuration,
    documentMapper: order => new BsonDocument
    {
        { "_id", order.Id },
        { "customer", order.Customer },
        { "amount", order.Amount },
        { "status", order.Status },
        { "updatedAt", DateTime.UtcNow }
    });
```

## Change Streams (CDC)

MongoDB Change Streams enable capturing changes (inserts, updates, deletes) in real-time. Change streams require a replica set configuration.

### Replica Set Requirement

Change streams only work with replica sets. For local development, use a single-node replica set:

```yaml
# docker-compose.yml
services:
  mongo:
    image: mongo:8
    command: ["--replSet", "rs0", "--bind_ip_all"]
    ports:
      - "27017:27017"
```

Initialize the replica set:

```javascript
// Run in mongosh
rs.initiate({ _id: 'rs0', members: [{ _id: 0, host: 'localhost:27017' }] });
```

### Watching for Changes

Use `MongoChangeStreamSourceNode<T>` from the `NPipeline.Connectors.MongoDB.ChangeStream` namespace:

```csharp
using NPipeline.Connectors.MongoDB.ChangeStream;
using MongoDB.Driver;

var csConfig = new MongoChangeStreamConfiguration
{
    DatabaseName    = "shop",
    CollectionName  = "orders",
    OperationTypes  = [MongoChangeStreamOperationType.Insert, MongoChangeStreamOperationType.Update],
    FullDocumentOption = ChangeStreamFullDocumentOption.UpdateLookup,
    MaxAwaitTime    = TimeSpan.FromSeconds(10),
};

var client = new MongoClient("mongodb://localhost:27017/?replicaSet=rs0");

await using var source = new MongoChangeStreamSourceNode<Order>(
    client,
    databaseName: "shop",
    collectionName: "orders",
    configuration: csConfig);

var context = new PipelineContext();
using var cts = new CancellationTokenSource();

await foreach (var order in source.OpenStream(context, cts.Token))
{
    Console.WriteLine($"Change detected: {order.Id}");
}
```

### Constructor Overloads

```csharp
new MongoChangeStreamSourceNode<T>(
    IMongoClient client,
    string databaseName,
    string? collectionName,                               // null = watch entire database
    IReadOnlyList<MongoChangeStreamOperationType>? operationTypes = null,
    BsonDocument? resumeToken = null,
    Func<MongoRow, T>? customMapper = null,
    MongoChangeStreamConfiguration? configuration = null)
```

### Resume Token

The node exposes a `ResumeToken` property (updated after each delivered event). Persist this value and pass it back to `MongoChangeStreamConfiguration.ResumeToken` to resume without missing events:

```csharp
var csConfig = new MongoChangeStreamConfiguration
{
    // ...
    ResumeToken = savedResumeToken,   // BsonDocument — null starts from current position
};
```

### `MongoChangeStreamConfiguration` Reference

| Property | Default | Description |
|---|---|---|
| `DatabaseName` | `""` | Database to watch |
| `CollectionName` | `null` | Collection to watch; `null` watches the entire database |
| `OperationTypes` | `null` (all) | Filter to specific operation types |
| `ResumeToken` | `null` | Token to resume from; `null` starts from the current oplog position |
| `FullDocumentOption` | `UpdateLookup` | Whether to include the full document on updates |
| `MaxAwaitTime` | `5 s` | Maximum time to wait per poll batch |
| `MaxRetryAttempts` | `3` | Retries on transient stream errors |
| `RetryDelay` | `2 s` | Base delay between retries |
| `ThrowOnMappingError` | `true` | `false` skips events that fail mapping |

## Error Handling & Resilience

### Retry Configuration

Configure retries for transient errors:

```csharp
var config = new MongoConfiguration
{
    DatabaseName = "shop",
    CollectionName = "orders",
    MaxRetryAttempts = 5,
    RetryDelay = TimeSpan.FromSeconds(2)
};
```

### ContinueOnError

Enable `ContinueOnError` to continue processing when individual documents fail:

```csharp
var config = new MongoConfiguration
{
    DatabaseName = "shop",
    CollectionName = "orders",
    WriteStrategy = MongoWriteStrategy.InsertMany,
    ContinueOnError = true // Continue on duplicate key errors
};
```

### Document Error Handler

Use `DocumentErrorHandler` for custom error handling:

```csharp
var config = new MongoConfiguration
{
    DatabaseName = "shop",
    CollectionName = "orders",
    DocumentErrorHandler = (exception, document) =>
    {
        Console.WriteLine($"Error processing document: {exception.Message}");
        // Return true to swallow the exception and continue
        // Return false to propagate the exception
        return true;
    }
};
```

### Exception Types

The connector provides specific exception types:

| Exception | Description |
|-----------|-------------|
| `MongoConnectorException` | Base exception for MongoDB connector errors. |
| `MongoMappingException` | Error mapping between BSON and CLR types. |
| `MongoWriteException` | Error writing documents to MongoDB. |

## Performance Guide

### Batch Size Tuning

Adjust batch sizes based on your workload:

```csharp
// Small batches: Lower latency, more round-trips
var smallBatch = new MongoConfiguration
{
    BatchSize = 100,
    WriteBatchSize = 100
};

// Large batches: Higher throughput, more memory
var largeBatch = new MongoConfiguration
{
    BatchSize = 10_000,
    WriteBatchSize = 5_000
};
```

**Guidelines:**

- **100-500**: Near real-time processing, lower memory usage
- **1,000-5,000**: Balanced throughput and latency (recommended)
- **5,000-10,000**: Maximum throughput for bulk loading

### OrderedWrites

Disable `OrderedWrites` for higher throughput when order doesn't matter:

```csharp
var config = new MongoConfiguration
{
    WriteStrategy = MongoWriteStrategy.BulkWrite,
    OrderedWrites = false // MongoDB may reorder writes for performance
};
```

### Streaming vs. Buffering

Enable `StreamResults` for large result sets:

```csharp
var config = new MongoConfiguration
{
    StreamResults = true, // Stream instead of buffering all in memory
    BatchSize = 1_000
};
```

### Cursor Timeout

For long-running queries, disable cursor timeout:

```csharp
var config = new MongoConfiguration
{
    NoCursorTimeout = true // Prevent cursor timeout for long-running queries
};
```

## Testing Guide

### Using Testcontainers

Use Testcontainers for MongoDB to write integration tests:

```csharp
using Testcontainers.MongoDb;

public class MongoConnectorTests : IAsyncLifetime
{
    private readonly MongoDbContainer _container = new MongoDbBuilder()
        .WithImage("mongo:8")
        .WithCommand("--replSet", "rs0", "--bind_ip_all")
        .Build();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        // Initialize replica set
        // ...
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    [Fact]
    public async Task SourceNode_ReadsDocuments()
    {
        var connectionString = _container.GetConnectionString();
        // Test your pipeline with the test container
    }
}
```

### In-Memory Testing

For unit tests, mock the data stream directly:

```csharp
var testData = new List<Order>
{
    new() { Id = "1", Customer = "Test", Amount = 100m, Status = "pending" }
};

var dataStream = new InMemoryDataStream<Order>(testData);
await sinkNode.ConsumeAsync(dataStream, context, CancellationToken.None);
```

## Best Practices

### Configuration

1. **Use dependency injection**: Register MongoDB clients via `AddMongoConnector` for production applications.
2. **Enable streaming**: Set `StreamResults = true` for large result sets to avoid memory issues.
3. **Tune batch sizes**: Adjust `BatchSize` and `WriteBatchSize` based on your data and latency requirements.
4. **Choose the right write strategy**: Use `InsertMany` for initial loads, `Upsert` for idempotent writes, and `BulkWrite` for maximum throughput.
5. **Disable ordered writes**: Set `OrderedWrites = false` when write order doesn't matter for better performance.

### Data Modeling

1. **Use attribute mapping**: Apply `[MongoCollection]` and `[MongoField]` for clear mapping.
2. **Leverage convention mapping**: Use property names that match MongoDB field names to avoid explicit mapping.
3. **Handle null values**: Use default values in `MongoRow` getters to handle missing fields gracefully.

### Error Handling

1. **Configure retries**: Set `MaxRetryAttempts` and `RetryDelay` for transient failures.
2. **Use ContinueOnError wisely**: Enable for batch loads where individual failures are acceptable.
3. **Implement DocumentErrorHandler**: For custom logging or recovery logic on document errors.

### Security

1. **Use connection string options**: Configure SSL, authentication, and timeouts in the connection string.
2. **Limit permissions**: Use database accounts with minimal required permissions.
3. **Enable TLS**: Always use TLS for production connections.

## Limitations

### Write Strategy Limitations

- **InsertMany**: Fails on duplicate keys unless `ContinueOnError = true` or `OnDuplicate = Skip`.
- **Upsert**: Requires `UpsertKeyFields` to be specified.
- **BulkWrite**: No idempotency guarantees; duplicate inserts will fail.

### Change Stream Limitations

- Requires replica set configuration (not available on standalone MongoDB).
- Resume tokens expire after some time (default 48 hours).

### Mapping Limitations

- Complex nested types require custom mappers.
- Arrays and lists require custom handling.
- Enums require explicit configuration or custom mapping.

## Advanced Scenarios

### Round-Trip Processing

Read from MongoDB, transform, and write back:

```csharp
public sealed class OrderProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            new MongoSourceNode<Order>(
                "mongodb://localhost:27017",
                new MongoConfiguration
                {
                    DatabaseName = "shop",
                    CollectionName = "orders"
                }),
            "order_source");

        var transform = builder.AddTransform<OrderProcessor, Order, ProcessedOrder>("processor");

        var sink = builder.AddSink(
            new MongoSinkNode<ProcessedOrder>(
                "mongodb://localhost:27017",
                new MongoConfiguration
                {
                    DatabaseName = "shop",
                    CollectionName = "processed_orders",
                    WriteStrategy = MongoWriteStrategy.Upsert,
                    UpsertKeyFields = new[] { "_id" }
                }),
            "processed_sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}
```

### Multiple Collections

Read from multiple collections and merge:

```csharp
var ordersSource = builder.AddSource(
    new MongoSourceNode<Order>(
        connectionString,
        new MongoConfiguration { DatabaseName = "shop", CollectionName = "orders" }),
    "orders_source");

var customersSource = builder.AddSource(
    new MongoSourceNode<Customer>(
        connectionString,
        new MongoConfiguration { DatabaseName = "shop", CollectionName = "customers" }),
    "customers_source");
```
