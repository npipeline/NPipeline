# NPipeline.Connectors.Aws.Redshift

AWS Redshift data warehouse connector for NPipeline. Enables streaming reads and high-throughput writes to Redshift using the PostgreSQL wire protocol (Npgsql)
with S3-based bulk loading support.

## Features

- **Streaming reads** with configurable fetch size and checkpoint support
- **Multiple write strategies**: PerRow, Batch (multi-row INSERT), and CopyFromS3 (high-throughput bulk loading)
- **Upsert support** with staging-table pattern and optional `MERGE`
- **Attribute-based mapping** with convention-over-configuration
- **Transient error detection** with exponential backoff and jitter
- **Full DI support** with factory pattern

## Quick start

```csharp
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Nodes;

var source = new RedshiftSourceNode<Order>(
    connectionString,
    "SELECT order_id, customer_id, total_amount FROM public.orders",
    configuration: new RedshiftConfiguration
    {
        StreamResults = true,
        FetchSize = 10_000,
    });

var sink = new RedshiftSinkNode<OrderSummary>(
    connectionString,
    "order_summaries",
    writeStrategy: RedshiftWriteStrategy.CopyFromS3,
    configuration: new RedshiftConfiguration
    {
        WriteStrategy = RedshiftWriteStrategy.CopyFromS3,
        S3BucketName = "my-etl-bucket",
        IamRoleArn = "arn:aws:iam::123456789012:role/RedshiftS3AccessRole",
        AwsRegion = "us-east-1",
        BatchSize = 5_000,
        UseUpsert = true,
        UpsertKeyColumns = ["order_id"],
    });
```

## Storage URI

```csharp
using NPipeline.StorageProviders.Models;

var uri = StorageUri.Parse("redshift://etl_user:secret@cluster.region.redshift.amazonaws.com:5439/analytics?sslmode=require");
var sourceFromUri = new RedshiftSourceNode<Order>(uri, "SELECT * FROM public.orders");
```

## Dependency injection

```csharp
services.AddRedshiftConnector(options =>
{
    options.DefaultConnectionString = connectionString;
    options.DefaultConfiguration = new RedshiftConfiguration
    {
        WriteStrategy = RedshiftWriteStrategy.Batch,
        BatchSize = 1_000,
    };
    options.AddOrUpdateConnection("analytics", analyticsConnectionString);
});
```

See docs/connectors/redshift.md for full examples and operational guidance.

## License

MIT
