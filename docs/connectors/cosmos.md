---
title: Azure Cosmos DB Connector
description: Read from and write to Azure Cosmos DB with NPipeline using the CosmosDB connector with support for SQL, Mongo, and Cassandra APIs.
sidebar_position: 5
---

## Azure Cosmos DB Connector

The `NPipeline.Connectors.Azure.CosmosDb` package provides specialized source and sink nodes for working with Azure Cosmos DB. This allows you to easily integrate Cosmos DB data into your pipelines as an input source or an output destination across multiple API types.

This connector supports the **SQL API** with native change feed capabilities, plus **Mongo API** and **Cassandra API** adapters for multi-model support. It uses the [Azure.Cosmos](https://learn.microsoft.com/en-us/dotnet/api/overview/azure/cosmosdb) SDK for reliable operations.

## Installation

To use the Cosmos DB connector, install the `NPipeline.Connectors.Azure.CosmosDb` NuGet package:

```bash
dotnet add package NPipeline.Connectors.Azure.CosmosDb
```

For the core NPipeline package and other available extensions, see the [Installation Guide](../getting-started/installation.md).

## Features

The Cosmos DB connector provides the following capabilities:

- **SQL API Source Node**: Read data using Cosmos DB SQL queries with parameterization
- **Change Feed Source Node**: Real-time streaming from Cosmos DB Change Feed
- **Sink Node**: Write data with multiple strategies for different use cases
- **Write Strategies**: PerRow, Batch, TransactionalBatch, and Bulk execution modes
- **Partition Key Handling**: Attribute-based, explicit selector, or automatic detection
- **Azure AD Authentication**: Connection strings and managed identity support via `DefaultAzureCredential`
- **Multi-API Support**: SQL, Mongo API, and Cassandra API with dedicated nodes
- **StorageUri Configuration**: Environment-aware setup via URI schemes
- **Connection Pooling**: Efficient resource management through dependency injection

## Dependency Injection

The Cosmos DB connector supports dependency injection for managing connections and factories. This is the recommended approach for production applications.

### Registering the Connector

Use `AddCosmosDbConnector` to register connection management and node factories:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.Azure.CosmosDb.DependencyInjection;

var services = new ServiceCollection()
    .AddCosmosDbConnector(options =>
    {
        // Using connection string
        options.DefaultConnectionString = "AccountEndpoint=https://your-account.documents.azure.com:443/;AccountKey=your-key;";
        
        // Or using Azure AD
        // options.DefaultUri = new Uri("https://your-account.documents.azure.com:443/");
        // options.DefaultCredential = new DefaultAzureCredential();
        
        // Add named connections
        options.AddOrUpdateConnection("secondary", "secondary-connection-string");
    })
    .BuildServiceProvider();

var sourceFactory = services.GetRequiredService<CosmosSourceNodeFactory>();
var sinkFactory = services.GetRequiredService<CosmosSinkNodeFactory>();
```

### Why Use Dependency Injection?

Using dependency injection provides several benefits:

- **Connection Pooling**: Efficiently reuses connections across multiple nodes
- **Configuration Centralization**: All Cosmos DB connections configured in one place
- **Testability**: Easy to mock or replace dependencies in unit tests
- **Lifetime Management**: Services properly disposed when the application shuts down

## Common Attributes

The Cosmos DB connector supports common attributes from `NPipeline.Connectors.Attributes` for consistent data mapping across all connectors.

### `[Column]` Attribute

The `[Column]` attribute allows you to specify property names and control mapping:

```csharp
using NPipeline.Connectors.Attributes;

public class Customer
{
    [Column("customer_id")]
    public string Id { get; set; } = string.Empty;

    [Column("customer_type")]
    public string CustomerType { get; set; } = string.Empty;

    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
}
```

### `[IgnoreColumn]` Attribute

The `[IgnoreColumn]` attribute excludes properties from mapping. Useful for computed properties:

```csharp
using NPipeline.Connectors.Attributes;

public class Order
{
    public string Id { get; set; } = string.Empty;
    public decimal Subtotal { get; set; }
    public decimal Tax { get; set; }

    [IgnoreColumn]
    public decimal Total => Subtotal + Tax;
}
```

### `[CosmosPartitionKey]` Attribute

Cosmos DB-specific attribute to mark partition key properties:

```csharp
using NPipeline.Connectors.Azure.CosmosDb.Mapping;

public class Customer
{
    public string Id { get; set; } = string.Empty;
    
    [CosmosPartitionKey]
    public string Region { get; set; } = string.Empty;
    
    public string Name { get; set; } = string.Empty;
}
```

## SQL API: Query Source Node

The `CosmosSourceNode<T>` reads data using Cosmos DB SQL queries.

### Basic Example

```csharp
using NPipeline.Connectors.Azure.CosmosDb.Nodes;

var sourceNode = new CosmosSourceNode<Customer>(
    connectionString: "your-connection-string",
    databaseId: "MyDatabase",
    containerId: "Customers",
    query: "SELECT * FROM c WHERE c.Status = @status",
    parameters: [new DatabaseParameter("status", "Active")]);

var pipeline = PipelineBuilder.Create<Customer>()
    .Source(sourceNode)
    .Transform(customer => new CustomerDto { /* ... */ })
    .Sink(consoleSink)
    .Build();
```

### Using StorageUri

Configure connections using environment-aware URIs:

```csharp
var uri = StorageUri.Parse("cosmosdb://account.documents.azure.com:443/MyDatabase/Customers?key=account-key");

var sourceNode = new CosmosSourceNode<Customer>(
    uri: uri,
    query: "SELECT * FROM c WHERE c.Region = @region",
    parameters: [new DatabaseParameter("region", "US")]);
```

## Change Feed Source Node

The `CosmosChangeFeedSourceNode<T>` enables real-time streaming from the Cosmos DB Change Feed.

```csharp
using NPipeline.Connectors.Azure.CosmosDb.ChangeFeed;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;

var changeFeedConfig = new ChangeFeedConfiguration
{
    StartFrom = ChangeFeedStartFrom.Beginning,
    PollingInterval = TimeSpan.FromSeconds(1),
    MaxItemCount = 100
};

var changeFeedSource = new CosmosChangeFeedSourceNode<Order>(
    connectionString: "your-connection-string",
    databaseId: "MyDatabase",
    containerId: "Orders",
    configuration: changeFeedConfig);

var pipeline = PipelineBuilder.Create<Order>()
    .Source(changeFeedSource)
    .Transform(order => ProcessOrder(order))
    .Sink(orderSink)
    .Build();
```

## Sink Node

The `CosmosSinkNode<T>` writes data to Cosmos DB with configurable strategies.

```csharp
using NPipeline.Connectors.Azure.CosmosDb.Nodes;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;

var sinkNode = new CosmosSinkNode<Customer>(
    connectionString: "your-connection-string",
    databaseId: "MyDatabase",
    containerId: "Customers",
    writeStrategy: CosmosWriteStrategy.Batch,
    idSelector: c => c.Id,
    partitionKeySelector: c => new PartitionKey(c.Region));

var pipeline = PipelineBuilder.Create<CustomerDto>()
    .Source(customerSource)
    .Transform(dto => new Customer { /* ... */ })
    .Sink(sinkNode)
    .Build();
```

## Write Strategies

Choose the write strategy that matches your requirements:

### PerRow

Writes items one at a time. Best for:

- Small data volumes
- When immediate consistency is required
- Individual error handling per item

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
- When operation order doesn't matter

## Partition Key Handling

Cosmos DB requires proper partition key configuration for scalable operations.

### Attribute-Based

Use `[CosmosPartitionKey]` to automatically detect the partition key:

```csharp
public class Customer
{
    public string Id { get; set; } = string.Empty;
    
    [CosmosPartitionKey]
    public string Region { get; set; } = string.Empty;
}
```

### Explicit Selector

Specify partition key selection in the sink node:

```csharp
var sinkNode = new CosmosSinkNode<Customer>(
    /* ... */
    partitionKeySelector: c => new PartitionKey(c.Region));
```

### No Partition Key

For containers without partition key requirements, `PartitionKey.None` is used automatically.

## Mongo API

Access Cosmos DB Mongo API with dedicated nodes.

```csharp
using NPipeline.Connectors.Azure.CosmosDb.Nodes;

var mongoSource = new CosmosMongoSourceNode<BsonDocument>(
    connectionString: "mongodb://user:pass@account.mongo.cosmos.azure.com:10255/?ssl=true",
    databaseId: "MyDatabase",
    containerId: "Customers",
    query: "{ \"status\": \"active\" }");

var mongoSink = new CosmosMongoSinkNode<BsonDocument>(
    connectionString: "mongodb://user:pass@account.mongo.cosmos.azure.com:10255/?ssl=true",
    databaseId: "MyDatabase",
    containerId: "Customers",
    writeStrategy: CosmosWriteStrategy.Bulk);
```

## Cassandra API

Access Cosmos DB Cassandra API with dedicated nodes.

```csharp
using NPipeline.Connectors.Azure.CosmosDb.Api.Cassandra;
using NPipeline.Connectors.Azure.CosmosDb.Nodes;

var cassandraSource = new CosmosCassandraSourceNode<Dictionary<string, object?>>(
    contactPoint: "account.cassandra.cosmos.azure.com",
    keyspace: "my_keyspace",
    query: "SELECT id, status FROM orders WHERE status = 'open';");

var cassandraSink = new CosmosCassandraSinkNode<Dictionary<string, object?>>(
    contactPoint: "account.cassandra.cosmos.azure.com",
    keyspace: "my_keyspace",
    writeStrategy: CosmosWriteStrategy.Batch);
```

> **Note:** Cassandra change feed is not supported as a native Cosmos DB feature. Use polling or external change data capture (CDC) for streaming requirements.

## Configuration

### CosmosConfiguration

| Property | Type | Default | Description |
| -------- | ---- | ------- | ----------- |
| `CommandTimeout` | `int` | `30` | Command timeout in seconds |
| `FetchSize` | `int` | `100` | Number of items per request |
| `StreamResults` | `bool` | `false` | Stream results vs. buffer |
| `WriteBatchSize` | `int` | `100` | Batch size for writes |
| `MaxConcurrency` | `int?` | `null` | Max concurrent operations |
| `ContinueOnError` | `bool` | `false` | Continue on row-level errors |

### ChangeFeedConfiguration

| Property | Type | Default | Description |
| -------- | ---- | ------- | ----------- |
| `StartFrom` | `ChangeFeedStartFrom` | `Beginning` | Start position (Beginning, Now, Time) |
| `StartTime` | `DateTime?` | `null` | Start time for time-based start |
| `PollingInterval` | `TimeSpan` | `1 second` | Interval between polls |
| `MaxItemCount` | `int` | `100` | Max items per poll |
| `ContinueOnError` | `bool` | `false` | Continue on errors |

## Error Handling

Configure error handling to match your resilience requirements:

```csharp
var config = new CosmosConfiguration
{
    ContinueOnError = true,        // Continue processing on errors
    ThrowOnMappingError = false    // Don't throw on mapping issues
};
```

## Custom Mapping

Provide custom mapping logic for complex transformations:

```csharp
var sourceNode = new CosmosSourceNode<Customer>(
    connectionString: "your-connection-string",
    databaseId: "MyDatabase",
    containerId: "Customers",
    query: "SELECT * FROM c",
    mapper: row => new Customer
    {
        Id = row.Get<string>("id") ?? string.Empty,
        Name = row.Get<string>("name") ?? string.Empty,
        Email = row.GetValue("email")?.ToString() ?? string.Empty
    });
```

## Next Steps

- **[Storage Providers](../storage-providers/storage-provider.md)**: Learn about the abstraction layer
- **[Dependency Injection](../extensions/dependency-injection.md)**: Configure services for your pipelines
- **[Error Handling](../core-concepts/resilience/error-handling.md)**: Handle failures gracefully
