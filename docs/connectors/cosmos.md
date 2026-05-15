---
title: "Cosmos DB Connector"
description: "Read from and write to Azure Cosmos DB with multi-API support (SQL, MongoDB, Cassandra)."
order: 12
---

# Cosmos DB Connector

The `NPipeline.Connectors.Azure.CosmosDb` package provides source and sink nodes for Azure Cosmos DB. Supports all three Cosmos DB APIs: **SQL (Core)**, **MongoDB**, and **Cassandra**. Features include connection pooling, transactional batches, change feed processing, and multiple authentication modes.

## Installation

```bash
dotnet add package NPipeline.Connectors.Azure.CosmosDb
```

**Dependencies:** [Microsoft.Azure.Cosmos](https://www.nuget.org/packages/Microsoft.Azure.Cosmos) 3.x, [Azure.Identity](https://www.nuget.org/packages/Azure.Identity) 1.x, [MongoDB.Driver](https://www.nuget.org/packages/MongoDB.Driver) 3.x, [CassandraCSharpDriver](https://www.nuget.org/packages/CassandraCSharpDriver) 3.x

## API Types

| API | Node Classes | When to Use |
|-----|-------------|-------------|
| **SQL** (default) | `CosmosSourceNode<T>`, `CosmosSinkNode<T>` | Standard Cosmos DB with SQL queries |
| **MongoDB** | `CosmosMongoSourceNode<T>`, `CosmosMongoSinkNode<T>` | Cosmos DB with MongoDB wire protocol |
| **Cassandra** | `CosmosCassandraSourceNode<T>`, `CosmosCassandraSinkNode<T>` | Cosmos DB with Cassandra wire protocol |

## Source Node — `CosmosSourceNode<T>` (SQL API)

### Constructors

```csharp
// Connection string
public CosmosSourceNode(
    string connectionString,
    string databaseId, string containerId,
    string query,
    Func<CosmosRow, T>? mapper = null,
    CosmosConfiguration? configuration = null,
    DatabaseParameter[]? parameters = null,
    bool continueOnError = false)

// Connection pool (recommended for DI)
public CosmosSourceNode(
    ICosmosConnectionPool connectionPool,
    string databaseId, string containerId,
    string query,
    Func<CosmosRow, T>? mapper = null,
    CosmosConfiguration? configuration = null,
    DatabaseParameter[]? parameters = null,
    bool continueOnError = false,
    string? connectionName = null)
```

### Example

```csharp
var source = new CosmosSourceNode<Order>(
    "AccountEndpoint=https://mydb.documents.azure.com:443/;AccountKey=...",
    "orders-db", "orders",
    "SELECT * FROM c WHERE c.status = 'pending'");
```

## Sink Node — `CosmosSinkNode<T>` (SQL API)

| Strategy | Description | Best For |
|----------|-------------|----------|
| `Upsert` (default) | Per-item upsert | Idempotent writes |
| `Insert` | Per-item insert | Append-only |
| `Batch` | Batched operations | Related items in same partition |
| `TransactionalBatch` | Transactional batch | Atomicity within a partition |
| `Bulk` | Bulk execution | Maximum throughput |

### Constructors

```csharp
// Connection string
public CosmosSinkNode(
    string connectionString,
    string databaseId, string containerId,
    CosmosWriteStrategy writeStrategy = CosmosWriteStrategy.Batch,
    Func<T, string>? idSelector = null,
    Func<T, PartitionKey>? partitionKeySelector = null,
    CosmosConfiguration? configuration = null)

// Connection pool (recommended for DI)
public CosmosSinkNode(
    ICosmosConnectionPool connectionPool,
    string databaseId, string containerId,
    CosmosWriteStrategy writeStrategy = CosmosWriteStrategy.Batch,
    Func<T, string>? idSelector = null,
    Func<T, PartitionKey>? partitionKeySelector = null,
    CosmosConfiguration? configuration = null,
    string? connectionName = null)
```

## Authentication

| Mode | Description |
|------|-------------|
| `ConnectionString` (default) | Standard Cosmos DB connection string |
| `AccountEndpointAndKey` | Explicit endpoint + key |
| `AzureAdCredential` | Azure AD / Managed Identity (recommended for production) |

```csharp
// Azure AD authentication
var config = new CosmosConfiguration
{
    AuthenticationMode = CosmosAuthenticationMode.AzureAdCredential,
    AccountEndpoint = "https://mydb.documents.azure.com:443/"
};
```

## Configuration

### Connection

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ApiType` | `CosmosApiType` | `Sql` | `Sql`, `Mongo`, or `Cassandra` |
| `ConnectionString` | `string` | `""` | Connection string |
| `AccountEndpoint` | `string` | `""` | Account endpoint URL |
| `DatabaseId` | `string` | `""` | Database ID (required) |
| `ContainerId` | `string?` | `null` | Container/collection ID |
| `AuthenticationMode` | `CosmosAuthenticationMode` | `ConnectionString` | Authentication mode |
| `ConsistencyLevel` | `ConsistencyLevel?` | `null` | Consistency level override |
| `PreferredRegions` | `List<string>` | `[]` | Preferred regions for geo-replicated accounts |
| `UseGatewayMode` | `bool` | `false` | Use gateway mode (vs direct) |

### Write

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `WriteStrategy` | `CosmosWriteStrategy` | `Upsert` | Write strategy |
| `BatchSize` | `int` | `100` | Batch size |
| `UseTransactionalBatch` | `bool` | `true` | Use transactional batches |
| `AllowBulkExecution` | `bool` | `false` | Enable bulk execution mode |
| `MaxConcurrentOperations` | `int` | `500` | Max concurrent operations |
| `PartitionKeyPath` | `string` | `"/id"` | Partition key path |
| `AutoCreateContainer` | `bool` | `false` | Auto-create container if missing |
| `Throughput` | `int?` | `null` | Provisioned RU/s for auto-created containers |

### Read

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `MaxItemCount` | `int` | `-1` | Max items per page (-1 = server default) |
| `EnableCrossPartitionQuery` | `bool` | `true` | Allow cross-partition queries |
| `StreamResults` | `bool` | `false` | Stream results |

## Dependency Injection

```csharp
// Connection string
services.AddCosmosDbConnector("AccountEndpoint=...;AccountKey=...");

// Azure AD with endpoint
services.AddCosmosDbConnector(
    new Uri("https://mydb.documents.azure.com:443/"),
    new DefaultAzureCredential());

// Full options
services.AddCosmosDbConnector(options =>
{
    options.DefaultConnectionString = "AccountEndpoint=...;AccountKey=...";
    options.DefaultConfiguration = new CosmosConfiguration
    {
        WriteStrategy = CosmosWriteStrategy.Bulk,
        AllowBulkExecution = true
    };
});
```

## Next Steps

- [MongoDB Connector](mongodb.md) — standalone MongoDB (non-Cosmos)
- [Storage Providers](../storage-providers/index.md) — Azure Blob and ADLS Gen2 storage

## API Types

| API | Description |
|-----|-------------|
| `Sql` (default) | Cosmos DB SQL (Core) API |
| `Mongo` | MongoDB API compatibility |
| `Cassandra` | Cassandra API compatibility |

## Partition Keys

Partition key selection is critical for Cosmos DB performance:

```csharp
var config = new CosmosConfiguration
{
    PartitionKeyPath = "/customerId",
    EnableCrossPartitionQuery = true  // required for queries without partition key filter
};
```

Cross-partition queries fan out to all partitions — use partition key filters when possible.

## Consistency Levels

| Level | Latency | Consistency | RU Cost |
|-------|---------|-------------|---------|
| `Strong` | High | Linearizable | High |
| `BoundedStaleness` | Medium | Bounded lag | Medium |
| `Session` (default) | Low | Read-your-writes | Low |
| `ConsistentPrefix` | Low | Ordered | Low |
| `Eventual` | Lowest | No ordering guarantee | Lowest |

```csharp
config.ConsistencyLevel = ConsistencyLevel.Session;
```

## RU/Throughput Management

```csharp
var config = new CosmosConfiguration
{
    AutoCreateContainer = true,
    Throughput = 4000,                  // provisioned RU/s
    AllowBulkExecution = true,          // enable SDK bulk mode
    MaxConcurrentOperations = 500       // concurrent operations limit
};
```

### Bulk Execution

Enable `AllowBulkExecution = true` for high-throughput writes. The Cosmos SDK automatically batches operations by partition key and parallelizes across partitions.

## Change Feed

Read the Cosmos DB change feed for CDC-style processing:

```csharp
var source = new CosmosChangeFeedSourceNode<Order>(new CosmosConfiguration
{
    ConnectionString = "...",
    DatabaseId = "sales",
    ContainerId = "orders",
    ChangeFeedLeaseContainerId = "leases",
    ChangeFeedStartFrom = ChangeFeedStartFrom.Beginning()
});
```

The change feed provides an ordered stream of changes within each partition.

## Best Practices

1. **Use Azure AD auth** in production — avoid connection strings with keys
2. **Choose partition keys** that distribute load evenly and match query patterns
3. **Avoid cross-partition queries** in hot paths — filter by partition key
4. **Use bulk execution** for high-throughput writes
5. **Use `Session` consistency** unless you need stronger guarantees
6. **Set `MaxConcurrentOperations`** to avoid throttling (429 responses)
7. **Use direct mode** (default) — gateway mode adds a network hop
8. **Configure `PreferredRegions`** for geo-replicated accounts
