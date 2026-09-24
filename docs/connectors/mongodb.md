---
title: "MongoDB Connector"
description: "Read from and write to MongoDB collections with change streams, bulk writes, and upserts."
order: 11
---

# MongoDB Connector

The `NPipeline.Connectors.MongoDB` package provides source and sink nodes for MongoDB. Supports filtered/sorted reads, change stream sources for real-time processing, bulk writes, and upserts.

## Installation

```bash
dotnet add package NPipeline.Connectors.MongoDB
```

**Dependencies:** [MongoDB.Driver](https://www.nuget.org/packages/MongoDB.Driver) 3.x

## Source Node - `MongoSourceNode<T>`

Reads documents from a collection with optional filter, sort, and projection.

### Constructors

```csharp
// Connection string
public MongoSourceNode(
    string connectionString,
    MongoConfiguration configuration,
    FilterDefinition<BsonDocument>? filter = null,
    SortDefinition<BsonDocument>? sort = null,
    ProjectionDefinition<BsonDocument>? projection = null,
    Func<MongoRow, T>? customMapper = null)

// IMongoClient (recommended for DI)
public MongoSourceNode(
    IMongoClient client,
    MongoConfiguration configuration,
    FilterDefinition<BsonDocument>? filter = null,
    SortDefinition<BsonDocument>? sort = null,
    ProjectionDefinition<BsonDocument>? projection = null,
    Func<MongoRow, T>? customMapper = null)
```

### Example

```csharp
var config = new MongoConfiguration
{
    ConnectionString = "mongodb://localhost:27017",
    DatabaseName = "orders",
    CollectionName = "pending"
};

var source = new MongoSourceNode<Order>(
    config.ConnectionString,
    config,
    filter: Builders<BsonDocument>.Filter.Eq("status", "pending"),
    sort: Builders<BsonDocument>.Sort.Descending("created_at"));
```

## Change Stream Source - `MongoChangeStreamSourceNode<T>`

Listens to real-time changes on a collection (requires a MongoDB replica set).

```csharp
var changeSource = new MongoChangeStreamSourceNode<Order>(
    config.ConnectionString,
    config,
    operationTypes: new[] { ChangeStreamOperationType.Insert, ChangeStreamOperationType.Update });
```

## Sink Node - `MongoSinkNode<T>`

| Strategy | Description | Best For |
|----------|-------------|----------|
| `InsertMany` | Batch `InsertMany` | Append-only |
| `BulkWrite` (default) | Mixed `BulkWrite` operations | Most workloads |
| `Upsert` | `ReplaceOne` with upsert | Idempotent writes |

### Constructors

```csharp
// Connection string
public MongoSinkNode(
    string connectionString,
    MongoConfiguration configuration,
    Func<T, BsonDocument>? documentMapper = null,
    Func<T, FilterDefinition<BsonDocument>>? upsertFilterBuilder = null)

// IMongoClient (recommended for DI)
public MongoSinkNode(
    IMongoClient client,
    MongoConfiguration configuration,
    Func<T, BsonDocument>? documentMapper = null,
    Func<T, FilterDefinition<BsonDocument>>? upsertFilterBuilder = null)
```

### Example: Upsert

```csharp
var config = new MongoConfiguration
{
    ConnectionString = "mongodb://localhost:27017",
    DatabaseName = "orders",
    CollectionName = "processed",
    WriteStrategy = MongoWriteStrategy.Upsert,
    UseUpsert = true,
    UpsertKeyFields = new[] { "orderId" }
};

var sink = new MongoSinkNode<ProcessedOrder>(
    config.ConnectionString,
    config,
    upsertFilterBuilder: item =>
        Builders<BsonDocument>.Filter.Eq("orderId", item.OrderId));
```

## Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ConnectionString` | `string` | `""` | MongoDB connection string |
| `DatabaseName` | `string` | `""` | Database name (required) |
| `CollectionName` | `string` | `""` | Collection name (required) |
| `BatchSize` | `int` | `1000` | Read batch (cursor) size |
| `NoCursorTimeout` | `bool` | `false` | Disable cursor timeout for long reads |
| `ReadPreference` | `ReadPreferenceMode?` | `null` | Read preference (Primary, Secondary, etc.) |
| `WriteStrategy` | `MongoWriteStrategy` | `BulkWrite` | `InsertMany`, `BulkWrite`, or `Upsert` |
| `WriteBatchSize` | `int` | - | Write batch size |
| `UseUpsert` | `bool` | `false` | Enable upsert semantics |
| `UpsertKeyFields` | `string[]` | `[]` | Key fields for upsert matching |
| `OnDuplicate` | `OnDuplicateAction` | `Ignore` | `Ignore`, `Overwrite`, or `Fail` |
| `Resilience` | `Resilience` | `MongoConnectorResilience.Default` | How the sink retries a batch. See [Resilience](#resilience) |
| `ContinueOnError` | `bool` | `false` | Continue on errors |

## Resilience

The sink and the change stream source retry through [NResilience](https://github.com/nresilience/NResilience). The
`Resilience` property configures each one.

**Sink.** `MongoConfiguration.Resilience` defaults to `MongoConnectorResilience.Default`, which does the following:

- Makes up to four attempts per batch (three retries).
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

## Dependency Injection

```csharp
services.AddMongoConnector(options =>
{
    options.DefaultConnectionString = "mongodb://localhost:27017";
    options.DefaultConfiguration = new MongoConfiguration
    {
        DatabaseName = "myapp",
        WriteStrategy = MongoWriteStrategy.BulkWrite
    };
});

services.AddMongoConnection("analytics", "mongodb://analytics-cluster:27017");
```

## Attribute Mapping

### `[MongoCollection]`

Specifies the collection name and database for a type:

```csharp
[MongoCollection("orders", Database = "sales")]
public class Order { ... }
```

### `[MongoField]`

Maps a C# property to a MongoDB field name:

```csharp
public class Customer
{
    [MongoField("_id")]
    public string Id { get; set; } = "";

    [MongoField("first_name")]
    public string FirstName { get; set; } = "";

    [IgnoreColumn]
    public string FullName => $"{FirstName} {LastName}";
}
```

### Custom Row Mappers

For complete control over mapping:

```csharp
var source = new MongoSourceNode<Order>(config.ConnectionString, config,
    customMapper: row => new Order(
        row.Get<string>("_id"),
        row.Get<string>("customer"),
        row.Get<decimal>("amount")));
```

## Performance

### Read Tuning

| Property | Default | Description |
|----------|---------|-------------|
| `BatchSize` | 1000 | Cursor batch size - rows fetched per round-trip |
| `NoCursorTimeout` | `false` | Disable cursor timeout for long-running reads |
| `ReadPreference` | `null` | `Primary`, `Secondary`, `PrimaryPreferred`, etc. |

### Write Strategy Comparison

| Strategy | Throughput | Latency | Best For |
|----------|-----------|---------|----------|
| `InsertMany` | High | Low | Append-only workloads |
| `BulkWrite` | High | Medium | Mixed insert/update/delete |
| `Upsert` | Medium | Medium | Idempotent writes |

### Best Practices

1. **Use `BulkWrite`** as the default - handles mixed operations efficiently
2. **Use `Upsert`** for idempotent pipelines with natural keys
3. **Set `NoCursorTimeout = true`** for long-running reads
4. **Use `ReadPreference.Secondary`** to offload reads from the primary
5. **Index `UpsertKeyFields`** columns for efficient conflict detection
6. **Use `IMongoClient` via DI** - the driver manages connection pooling internally

## Testing with Testcontainers

```csharp
var container = new MongoDbBuilder().Build();
await container.StartAsync();

var config = new MongoConfiguration
{
    ConnectionString = container.GetConnectionString(),
    DatabaseName = "test",
    CollectionName = "orders"
};
```

## Next Steps

- [Cosmos DB Connector](cosmos.md) - Azure Cosmos DB with MongoDB API support
- [Error Handling](../error-handling/index.md) - retry strategies for database errors
