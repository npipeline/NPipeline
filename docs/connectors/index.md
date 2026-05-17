---
title: "Connectors"
description: "Choose the right connector for your data source or destination."
order: 4
---

# Connectors

> **Prerequisites:** [Key Concepts](../getting-started/key-concepts.md)

A [connector](../reference/glossary.md#connector) is a NuGet package that provides source and/or sink nodes for a specific data system. Each connector handles serialization, connection management, and system-specific optimizations.

## Choosing a Connector

### File Formats

For reading and writing data files. Pair with a [storage provider](../storage-providers/index.md) for cloud storage.

| Connector | Format | Best For | Package |
|-----------|--------|----------|---------|
| [CSV](csv.md) | CSV/TSV | Tabular data, spreadsheet exports | `NPipeline.Connectors.Csv` |
| [JSON](json.md) | JSON array, NDJSON | API data, config files, document streams | `NPipeline.Connectors.Json` |
| [Parquet](parquet.md) | Apache Parquet | Large analytical datasets, columnar queries | `NPipeline.Connectors.Parquet` |
| [Excel](excel.md) | XLS/XLSX | Spreadsheet data, business reports | `NPipeline.Connectors.Excel` |

### Databases

For reading from and writing to relational and document databases.

| Connector | System | Key Features | Package |
|-----------|--------|-------------|---------|
| [PostgreSQL](postgres.md) | PostgreSQL | COPY protocol, upsert, streaming results | `NPipeline.Connectors.Postgres` |
| [MySQL](mysql.md) | MySQL/MariaDB | Bulk load, upsert, CDC support | `NPipeline.Connectors.MySQL` |
| [SQL Server](sqlserver.md) | SQL Server | BulkCopy, MERGE upsert, streaming | `NPipeline.Connectors.SqlServer` |
| [Snowflake](snowflake.md) | Snowflake | JWT auth, batch write, streaming results | `NPipeline.Connectors.Snowflake` |
| [MongoDB](mongodb.md) | MongoDB | Upsert, change streams, checkpointing | `NPipeline.Connectors.MongoDB` |
| [Cosmos DB](cosmos.md) | Azure Cosmos DB | SQL/Mongo/Cassandra APIs, change feed | `NPipeline.Connectors.Azure.CosmosDb` |
| [DuckDB](duckdb.md) | DuckDB | Appender writes, auto-create tables | `NPipeline.Connectors.DuckDB` |

### Message Queues

For consuming from and publishing to message brokers.

| Connector | System | Key Features | Package |
|-----------|--------|-------------|---------|
| [Kafka](kafka.md) | Apache Kafka | Consumer groups, idempotent writes, transactions | `NPipeline.Connectors.Kafka` |
| [RabbitMQ](rabbitmq.md) | RabbitMQ | Topology management, acknowledgment strategies | `NPipeline.Connectors.RabbitMQ` |
| [AWS SQS](aws-sqs.md) | Amazon SQS | Long polling, batch operations, dead letter | `NPipeline.Connectors.Aws.Sqs` |
| [Azure Service Bus](azure-service-bus.md) | Azure Service Bus | Queues, topics, sessions, batch sending | `NPipeline.Connectors.Azure.ServiceBus` |

### Specialized

| Connector | System | Purpose | Package |
|-----------|--------|---------|---------|
| [HTTP](http.md) | REST APIs | Pagination, auth providers, rate limiting | `NPipeline.Connectors.Http` |
| [Data Lake](datalake.md) | Hive-style tables | Partitioned Parquet, snapshots, compaction | `NPipeline.Connectors.DataLake` |

## Common Patterns

### Installation

```bash
dotnet add package NPipeline.Connectors.Json
```

### Source → Transform → Sink

Most connectors follow the same pattern:

```csharp
public void Define(PipelineBuilder builder, PipelineContext context)
{
    var source = builder.AddSource<JsonSourceNode<Order>, Order>("read");
    var transform = builder.AddTransform<ValidateOrder, Order, ValidatedOrder>("validate");
    var sink = builder.AddSink<PostgresSinkNode<ValidatedOrder>, ValidatedOrder>("write");

    builder.Connect(source, transform);
    builder.Connect(transform, sink);
}
```

### Storage Provider Integration

File-based connectors (CSV, JSON, Parquet, Excel) read from and write to `IStorageProvider`. This means the same connector code works with local files, S3, Azure Blob, GCS, or SFTP:

```csharp
var config = new JsonConfiguration { Format = JsonFormat.NewlineDelimited };
var storageProvider = new AwsS3StorageProvider(s3Options);
var source = new JsonSourceNode<Order>(config, storageProvider, new StorageUri("s3://bucket/orders.ndjson"));
```

> 🔗 **See also:** [Storage Providers](../storage-providers/index.md) for choosing and configuring storage backends.

## Next Steps

- Pick a connector from the tables above to see configuration details
- [Storage Providers](../storage-providers/index.md) - configure where file-based connectors read/write
- [Custom Nodes](../guides/custom-nodes.md) - build your own source or sink
