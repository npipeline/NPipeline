---
title: "Samples"
description: "Categorized index of all sample projects with descriptions and complexity ratings."
order: 101
---

# Samples

All samples are in the [`samples/`](https://github.com/user/npipeline/tree/main/samples) directory. Each is a standalone console application.

## Core Pipeline Patterns

| Sample | Description | Complexity |
|--------|-------------|------------|
| `Sample_BasicPipeline` | Minimal pipeline with source → transform → sink | Beginner |
| `Sample_LambdaNodes` | Inline lambda-based node definitions | Beginner |
| `Sample_SimpleDataTransformation` | Basic data mapping transform | Beginner |
| `Sample_ComplexDataTransformations` | Multi-step transformation chains | Intermediate |
| `Sample_CustomNodeImplementation` | Implementing custom node classes | Intermediate |
| `Sample_Composition` | Composing sub-pipelines into larger pipelines | Advanced |
| `Sample_FileProcessing` | End-to-end file processing pipeline | Intermediate |

## Node Types

| Sample | Description | Complexity |
|--------|-------------|------------|
| `Sample_AggregateNode` | Reduce operations (sum, count, average) | Intermediate |
| `Sample_AdvancedAggregateNode` | Time-windowed aggregation with watermarks | Advanced |
| `Sample_BatchingNode` | Grouping items into fixed-size batches | Intermediate |
| `Sample_UnbatchingNode` | Expanding batches into individual items | Intermediate |
| `Sample_BranchNode` | Fan-out to multiple downstream paths | Intermediate |
| `Sample_TapNode` | Side-effect observation without modifying data | Beginner |
| `Sample_LookupNode` | Enrichment via in-memory lookup table | Intermediate |
| `Sample_KeyedJoinNode` | Inner join of two streams by key | Intermediate |
| `Sample_SelfJoinNode` | Self-join on a single stream | Intermediate |
| `Sample_TimeWindowedJoinNode` | Time-windowed join across streams | Advanced |
| `Sample_CustomMergeNode` | Custom merge node for fan-in | Advanced |

## Windowing and Streaming

| Sample | Description | Complexity |
|--------|-------------|------------|
| `Sample_StreamingAnalytics` | Real-time streaming analytics | Advanced |
| `Sample_WindowingStrategies` | Tumbling, sliding, and session windows | Advanced |
| `Sample_WatermarkHandling` | Late data handling with watermarks | Advanced |
| `Sample_IntentDrivenGrouping` | Intent-driven batching for optimization | Intermediate |

## Error Handling and Resilience

| Sample | Description | Complexity |
|--------|-------------|------------|
| `Sample_BasicErrorHandling` | Try/catch patterns in nodes | Beginner |
| `Sample_AdvancedErrorHandling` | Error handlers and dead letter queues | Intermediate |
| `Sample_FluentErrorHandling` | Fluent API for resilience configuration | Intermediate |
| `Sample_RetryDelay` | Retry with exponential backoff | Intermediate |

## Parallel Execution and Performance

| Sample | Description | Complexity |
|--------|-------------|------------|
| `Sample_ParallelProcessing` | Parallel node execution with backpressure | Intermediate |
| `Sample_ParallelExecution_Simplified` | Simplified parallel execution setup | Beginner |
| `Sample_PerformanceOptimization` | ValueTask fast paths and optimization techniques | Advanced |

## File Connectors

| Sample | Description | Complexity |
|--------|-------------|------------|
| `Sample_CsvConnector` | CSV reading and writing | Beginner |
| `Sample_JsonConnector` | JSON reading and writing | Beginner |
| `Sample_ExcelConnector` | Excel file processing | Beginner |
| `Sample_ParquetConnector` | Parquet columnar format | Intermediate |

## Database Connectors

| Sample | Description | Complexity |
|--------|-------------|------------|
| `Sample_SqlServerConnector` | SQL Server source and sink | Intermediate |
| `Sample_PostgresConnector` | PostgreSQL source and sink | Intermediate |
| `Sample_MySQLConnector` | MySQL source and sink | Intermediate |
| `Sample_MongoDbConnector` | MongoDB document operations | Intermediate |
| `Sample_CosmosDbConnector` | Azure Cosmos DB connector | Intermediate |
| `Sample_DuckDBConnector` | DuckDB analytics connector | Intermediate |
| `Sample_SnowflakeConnector` | Snowflake data warehouse | Intermediate |

## Message Queue Connectors

| Sample | Description | Complexity |
|--------|-------------|------------|
| `Sample_KafkaConnector` | Kafka producer/consumer | Intermediate |
| `Sample_RabbitMqConnector` | RabbitMQ messaging | Intermediate |
| `Sample_AzureServiceBusConnector` | Azure Service Bus queues/topics | Intermediate |
| `Sample_SqsConnector` | AWS SQS messaging | Intermediate |

## Specialized Connectors

| Sample | Description | Complexity |
|--------|-------------|------------|
| `Sample_HttpConnector` | HTTP GET source connector | Beginner |
| `Sample_HttpPost` | HTTP POST sink connector | Beginner |
| `Sample_DataLakeConnector` | Data lake multi-format connector | Advanced |

## Storage Providers

| Sample | Description | Complexity |
|--------|-------------|------------|
| `Sample_S3StorageProvider` | AWS S3 file operations | Beginner |
| `Sample_S3CompatibleStorageProvider` | MinIO, R2, and other S3-compatible stores | Beginner |
| `Sample_AzureStorageProvider` | Azure Blob Storage | Beginner |
| `Sample_AdlsStorageProvider` | Azure Data Lake Storage Gen2 | Beginner |
| `Sample_GcsStorageProvider` | Google Cloud Storage | Beginner |
| `Sample_SftpStorageProvider` | SFTP file transfers | Beginner |

## Extensions

| Sample | Description | Complexity |
|--------|-------------|------------|
| `Sample_NodesExtension` | Utility nodes (filter, map, flatten) | Intermediate |
| `Sample_LineageExtension` | Data lineage and provenance tracking | Intermediate |
| `Sample_ObservabilityExtension` | Metrics, monitoring, and tracing | Intermediate |

## Running a Sample

```bash
cd samples/Sample_BasicPipeline
dotnet run
```

## Next Steps

- [Your First Pipeline](getting-started/your-first-pipeline.md) — step-by-step tutorial
- [Key Concepts](getting-started/key-concepts.md) — understand the core model
