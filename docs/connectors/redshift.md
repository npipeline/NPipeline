# AWS Redshift Connector

The AWS Redshift connector provides source and sink nodes for streaming data to and from Amazon Redshift clusters.

## Installation

```bash
dotnet add package NPipeline.Connectors.Aws.Redshift
```

## Features

- **Streaming reads** with configurable fetch size
- **Three write strategies**: PerRow, Batch, and CopyFromS3
- **Upsert support** via staging table pattern or MERGE syntax
- **Automatic column mapping** with PascalCase to snake_case convention
- **Connection pooling** via NpgsqlDataSource
- **Storage URI support** with `redshift://` scheme
- **Full DI integration** with factory pattern

## Quick Start

### Reading from Redshift

```csharp
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Nodes;

var source = new RedshiftSourceNode<Order>(
    "Host=my-cluster.redshift.amazonaws.com;Port=5439;Database=mydb;Username=user;Password=pass;SSL Mode=Require",
    "SELECT * FROM public.orders WHERE created_at > '2024-01-01'",
    new RedshiftConfiguration
    {
        StreamResults = true,
        FetchSize = 10_000
    });

await foreach (var order in source.ReadAsync())
{
    Console.WriteLine($"Order: {order.Id}");
}
```

### Writing to Redshift

```csharp
var sinkConfig = new RedshiftConfiguration
{
    WriteStrategy = RedshiftWriteStrategy.Batch,
    BatchSize = 1_000,
    UseTransaction = true
};

var sink = new RedshiftSinkNode<Order>(
    connectionString,
    "orders",
    writeStrategy: RedshiftWriteStrategy.Batch,
    configuration: sinkConfig);

await sink.WriteAsync(new Order { Id = 1, Name = "Test" });
await sink.FlushAsync();
```

### Upsert Support

```csharp
var config = new RedshiftConfiguration
{
    WriteStrategy = RedshiftWriteStrategy.Batch,
    UseUpsert = true,
    UpsertKeyColumns = ["id"],
    OnMergeAction = OnMergeAction.Update // or Skip for insert-only
};

var sink = new RedshiftSinkNode<Order>(connectionString, "orders", config);
```

## Configuration Options

### Connection

| Property | Default | Description |
|----------|---------|-------------|
| `ConnectionString` | - | Full Npgsql connection string |
| `Host` | - | Redshift cluster host |
| `Port` | 5439 | Cluster port |
| `Database` | - | Database name |
| `Schema` | public | Default schema |
| `CommandTimeout` | 300 | Command timeout in seconds |
| `ConnectionTimeout` | 30 | Connection timeout in seconds |

### Write Strategies

| Strategy | Description | Use Case |
|----------|-------------|----------|
| `PerRow` | Individual INSERT statements | Development, very low volume |
| `Batch` | Multi-row INSERT with VALUES | Up to ~50k rows per batch |
| `CopyFromS3` | Upload to S3 + COPY command | Large loads (100k+ rows) |

### Upsert Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `UseUpsert` | false | Enable upsert semantics |
| `UpsertKeyColumns` | - | Columns for matching |
| `OnMergeAction` | Update | Update or Skip matched rows |
| `UseMergeSyntax` | false | Use MERGE instead of staging table |
| `StagingTablePrefix` | #npipeline_stage_ | Prefix for staging tables |

## Storage URI Support

```csharp
// redshift://user:password@host:port/database?schema=public&sslmode=Require
var uri = new StorageUri("redshift://etl:secret@my-cluster.redshift.amazonaws.com:5439/analytics");
var provider = new RedshiftDatabaseStorageProvider();
var connectionString = provider.GetConnectionString(uri);
```

## Dependency Injection

```csharp
services.AddRedshiftConnector(options =>
{
    options.DefaultConnectionString = "Host=...;Database=...";
    options.WriteStrategy = RedshiftWriteStrategy.Batch;
    options.BatchSize = 5_000;
});

// Use factories
public class MyPipeline
{
    private readonly IRedshiftSourceNodeFactory _sourceFactory;
    private readonly IRedshiftSinkNodeFactory _sinkFactory;

    public MyPipeline(
        IRedshiftSourceNodeFactory sourceFactory,
        IRedshiftSinkNodeFactory sinkFactory)
    {
        _sourceFactory = sourceFactory;
        _sinkFactory = sinkFactory;
    }

    public async Task RunAsync()
    {
        var source = _sourceFactory.Create<Order>("SELECT * FROM orders");
        var sink = _sinkFactory.Create<Order>("orders");
        // ...
    }
}
```

## Column Mapping

By default, PascalCase property names are converted to snake_case:

| C# Property | Redshift Column |
|-------------|-----------------|
| `OrderId` | `order_id` |
| `CustomerName` | `customer_name` |
| `HTTPStatusCode` | `http_status_code` |

Use attributes for custom mapping:

```csharp
[RedshiftTable("my_table", Schema = "analytics")]
public class MyEntity
{
    [RedshiftColumn("id")]
    public int Id { get; set; }

    [RedshiftColumn("custom_name")]
    public string Name { get; set; }

    [RedshiftColumn("internal", Ignore = true)]
    public string Internal { get; set; }
}
```

## Error Handling

The connector includes retry logic with exponential backoff for transient errors:

```csharp
var config = new RedshiftConfiguration
{
    MaxRetryAttempts = 3,
    RetryDelay = TimeSpan.FromSeconds(2),
    ContinueOnError = false
};
```

## Checkpointing

```csharp
var checkpointStorage = new RedshiftCheckpointStorage(
    connectionPool,
    configuration,
    schema: "public");

// Use with pipeline for exactly-once processing
```

## Requirements

- .NET 8.0 or later
- Amazon Redshift cluster (provisioned or serverless)
- Network access to cluster (port 5439)
- For CopyFromS3: S3 bucket and IAM role with appropriate permissions
