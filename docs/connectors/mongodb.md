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

## Source Node — `MongoSourceNode<T>`

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

## Change Stream Source — `MongoChangeStreamSourceNode<T>`

Listens to real-time changes on a collection (requires a MongoDB replica set).

```csharp
var changeSource = new MongoChangeStreamSourceNode<Order>(
    config.ConnectionString,
    config,
    operationTypes: new[] { ChangeStreamOperationType.Insert, ChangeStreamOperationType.Update });
```

## Sink Node — `MongoSinkNode<T>`

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
| `WriteBatchSize` | `int` | — | Write batch size |
| `UseUpsert` | `bool` | `false` | Enable upsert semantics |
| `UpsertKeyFields` | `string[]` | `[]` | Key fields for upsert matching |
| `OnDuplicate` | `OnDuplicateAction` | `Ignore` | `Ignore`, `Overwrite`, or `Fail` |
| `MaxRetryAttempts` | `int` | `3` | Retry attempts |
| `ContinueOnError` | `bool` | `false` | Continue on errors |

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
| `BatchSize` | 1000 | Cursor batch size — rows fetched per round-trip |
| `NoCursorTimeout` | `false` | Disable cursor timeout for long-running reads |
| `ReadPreference` | `null` | `Primary`, `Secondary`, `PrimaryPreferred`, etc. |

### Write Strategy Comparison

| Strategy | Throughput | Latency | Best For |
|----------|-----------|---------|----------|
| `InsertMany` | High | Low | Append-only workloads |
| `BulkWrite` | High | Medium | Mixed insert/update/delete |
| `Upsert` | Medium | Medium | Idempotent writes |

### Best Practices

1. **Use `BulkWrite`** as the default — handles mixed operations efficiently
2. **Use `Upsert`** for idempotent pipelines with natural keys
3. **Set `NoCursorTimeout = true`** for long-running reads
4. **Use `ReadPreference.Secondary`** to offload reads from the primary
5. **Index `UpsertKeyFields`** columns for efficient conflict detection
6. **Use `IMongoClient` via DI** — the driver manages connection pooling internally

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

- [Cosmos DB Connector](cosmos.md) — Azure Cosmos DB with MongoDB API support
- [Error Handling](../error-handling/index.md) — retry strategies for database errors
