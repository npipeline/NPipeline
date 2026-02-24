# NPipeline Cosmos DB Connector

A comprehensive Azure Cosmos connector for NPipeline with SQL API support plus Mongo and Cassandra adapter support.

## Features

- **Query Source Node**: Read data using Cosmos DB SQL queries
- **Change Feed Source Node**: Real-time streaming from Cosmos DB Change Feed
- **Sink Node**: Write data with multiple strategies
- **Multiple Write Strategies**: Per-row, Batch, Transactional Batch, and Bulk execution
- **Flexible Partition Key Handling**: Attribute-based, explicit selector, or automatic
- **Azure AD Authentication**: Support for connection strings and Azure Identity
- **StorageUri Support**: Environment-aware configuration via URI scheme
- **Multi-API Adapters**: SQL (`cosmosdb`, `cosmos`), Mongo (`cosmos-mongo`), Cassandra (`cosmos-cassandra`)
- **First-Class API Nodes**: Dedicated Mongo and Cassandra source/sink nodes

## Installation

Add the NuGet package to your project:

```bash
dotnet add package NPipeline.Connectors.CosmosDb
```

## Quick Start

### Reading Data (Query Source)

```csharp
using NPipeline.Connectors.CosmosDb.Nodes;
using NPipeline.Connectors.CosmosDb.Mapping;

// Define your model
public class Customer
{
    public string Id { get; set; }
    [CosmosPartitionKey]
    public string CustomerType { get; set; }
    public string Name { get; set; }
    public string Email { get; set; }
}

// Create a source node
var sourceNode = new CosmosSourceNode<Customer>(
    connectionString: "AccountEndpoint=https://your-account.documents.azure.com:443/;AccountKey=your-key;",
    databaseId: "MyDatabase",
    containerId: "Customers",
    query: "SELECT * FROM c WHERE c.CustomerType = @type",
    parameters: [new DatabaseParameter("type", "Premium")]);

// Use in pipeline
var pipeline = PipelineBuilder.Create<Customer>()
    .Source(sourceNode)
    .Transform(customer => new CustomerDto { ... })
    .Sink(consoleSink)
    .Build();
```

### Real-time Streaming (Change Feed)

```csharp
using NPipeline.Connectors.CosmosDb.ChangeFeed;
using NPipeline.Connectors.CosmosDb.Configuration;

// Configure change feed
var changeFeedConfig = new ChangeFeedConfiguration
{
    StartFrom = ChangeFeedStartFrom.Beginning,
    PollingInterval = TimeSpan.FromSeconds(1),
    MaxItemCount = 100
};

// Create change feed source
var changeFeedSource = new CosmosChangeFeedSourceNode<Order>(
    connectionString: "your-connection-string",
    databaseId: "MyDatabase",
    containerId: "Orders",
    configuration: changeFeedConfig);

// Process changes in real-time
var pipeline = PipelineBuilder.Create<Order>()
    .Source(changeFeedSource)
    .Transform(order => ProcessOrder(order))
    .Sink(orderSink)
    .Build();
```

### Writing Data (Sink)

```csharp
using NPipeline.Connectors.CosmosDb.Nodes;
using NPipeline.Connectors.CosmosDb.Configuration;

// Create sink with batch write strategy
var sinkNode = new CosmosSinkNode<Customer>(
    connectionString: "your-connection-string",
    databaseId: "MyDatabase",
    containerId: "Customers",
    writeStrategy: CosmosWriteStrategy.Batch,
    idSelector: c => c.Id,
    partitionKeySelector: c => new PartitionKey(c.CustomerType));

// Use in pipeline
var pipeline = PipelineBuilder.Create<CustomerDto>()
    .Source(customerSource)
    .Transform(dto => new Customer { ... })
    .Sink(sinkNode)
    .Build();
```

### Mongo API Nodes

```csharp
using NPipeline.Connectors.CosmosDb.Nodes;
using NPipeline.Connectors.CosmosDb.Configuration;

var mongoSource = new CosmosMongoSourceNode<Dictionary<string, object?>>(
    connectionString: "mongodb://user:pass@account.mongo.cosmos.azure.com:10255/?ssl=true",
    databaseId: "MyDatabase",
    containerId: "Customers",
    query: "{ \"status\": \"active\" }");

var mongoSink = new CosmosMongoSinkNode<MyDocument>(
    connectionString: "mongodb://user:pass@account.mongo.cosmos.azure.com:10255/?ssl=true",
    databaseId: "MyDatabase",
    containerId: "Customers",
    writeStrategy: CosmosWriteStrategy.Bulk,
    idSelector: d => d.Id);
```

### Cassandra API Nodes

```csharp
using NPipeline.Connectors.CosmosDb.Api.Cassandra;
using NPipeline.Connectors.CosmosDb.Nodes;
using NPipeline.Connectors.CosmosDb.Configuration;

var cassandraSource = new CosmosCassandraSourceNode<Dictionary<string, object?>>(
    contactPoint: "account.cassandra.cosmos.azure.com",
    keyspace: "my_keyspace",
    query: "SELECT id, status FROM orders WHERE status = 'open';");

var cassandraSink = new CosmosCassandraSinkNode<CassandraStatementRequest>(
    contactPoint: "account.cassandra.cosmos.azure.com",
    keyspace: "my_keyspace",
    writeStrategy: CosmosWriteStrategy.Batch);
```

## Configuration

### CosmosConfiguration

| Property                 | Type   | Default | Description                          |
|--------------------------|--------|---------|--------------------------------------|
| `CommandTimeout`         | `int`  | `30`    | Command timeout in seconds           |
| `FetchSize`              | `int`  | `100`   | Number of items to fetch per request |
| `StreamResults`          | `bool` | `false` | Whether to stream results            |
| `CaseInsensitiveMapping` | `bool` | `true`  | Case-insensitive column mapping      |
| `ContinueOnError`        | `bool` | `false` | Continue on row-level errors         |
| `WriteBatchSize`         | `int`  | `100`   | Batch size for writes                |
| `UseUpsert`              | `bool` | `false` | Use upsert instead of insert         |
| `MaxConcurrency`         | `int?` | `null`  | Max concurrent connections           |

### ChangeFeedConfiguration

| Property          | Type                  | Default     | Description                     |
|-------------------|-----------------------|-------------|---------------------------------|
| `StartFrom`       | `ChangeFeedStartFrom` | `Beginning` | Where to start reading          |
| `StartTime`       | `DateTime?`           | `null`      | Start time for time-based start |
| `PollingInterval` | `TimeSpan`            | `1 second`  | Interval between polls          |
| `MaxItemCount`    | `int`                 | `100`       | Max items per poll              |
| `ContinueOnError` | `bool`                | `false`     | Continue on errors              |

## Write Strategies

### PerRow

Writes items one at a time. Best for:

- Small data volumes
- When you need immediate consistency
- Individual error handling

### Batch

Writes items in parallel batches. Best for:

- High-throughput scenarios
- Items distributed across partitions
- When some failures are acceptable

### TransactionalBatch

Writes items atomically within the same partition. Best for:

- When you need ACID guarantees
- Related items in the same partition
- Financial or critical data

### Bulk

Uses Cosmos DB bulk execution mode. Best for:

- Maximum throughput
- Large data migrations
- When order doesn't matter

## Partition Key Handling

### Attribute-based

```csharp
public class Customer
{
    public string Id { get; set; }

    [CosmosPartitionKey]
    public string CustomerType { get; set; }
}
```

### Explicit Selector

```csharp
var sinkNode = new CosmosSinkNode<Customer>(
    ...,
    partitionKeySelector: c => new PartitionKey(c.Region));
```

### Automatic (None)

If no partition key is specified, `PartitionKey.None` is used for containers without partition key requirements.

## Dependency Injection

```csharp
// Using connection string
services.AddCosmosDbConnector("your-connection-string");

// Using Azure AD
services.AddCosmosDbConnector(
    new Uri("https://your-account.documents.azure.com:443/"),
    new DefaultAzureCredential());

// With full configuration
services.AddCosmosDbConnector(options =>
{
    options.DefaultConnectionString = "your-connection-string";
    options.AddOrUpdateConnection("ReadOnly", "readonly-connection-string");
});

// Custom checkpoint store for Change Feed
services.AddCosmosChangeFeedCheckpointStore<BlobStorageCheckpointStore>();
```

## StorageUri Support

Use URIs for environment-aware configuration:

```csharp
// cosmosdb://account.documents.azure.com/database/container?key=account-key
var uri = StorageUri.Parse("cosmosdb://myaccount.documents.azure.com:443/MyDatabase/MyContainer?key=my-key");

var sourceNode = new CosmosSourceNode<Customer>(
    uri: uri,
    query: "SELECT * FROM c");

// Mongo API URI
var mongoUri = StorageUri.Parse("cosmos-mongo://user:pass@account.mongo.cosmos.azure.com:10255/MyDatabase");

// Cassandra API URI
var cassandraUri = StorageUri.Parse("cosmos-cassandra://account.cassandra.cosmos.azure.com:10350/my_keyspace");
```

## Mongo and Cassandra Support

- Mongo and Cassandra are exposed via the API adapter layer (`ICosmosApiAdapterResolver`).
- SQL source/sink nodes remain SQL-specific.
- First-class nodes are available: `CosmosMongoSourceNode<T>`, `CosmosMongoSinkNode<T>`, `CosmosCassandraSourceNode<T>`, `CosmosCassandraSinkNode<T>`.
- Cassandra change feed remains unsupported as a native feature. `CosmosCassandraChangeFeedSourceNode<T>` intentionally throws `NotSupportedException` with
  guidance to use polling or external CDC.

## Error Handling

```csharp
var config = new CosmosConfiguration
{
    ContinueOnError = true,
    ThrowOnMappingError = false
};
```

## Custom Mapping

```csharp
// Custom mapper function
var sourceNode = new CosmosSourceNode<Customer>(
    ...,
    mapper: row => new Customer
    {
        Id = row.Get<string>("id"),
        Name = row.Get<string>("name"),
        Email = row.GetValue("email")?.ToString() ?? string.Empty
    });
```

## License

This project is licensed under the MIT License.
