---
title: Sample Applications
description: Index of sample applications demonstrating NPipeline concepts and patterns
sidebar_position: 1
---

# Sample Applications

Sample applications demonstrate NPipeline concepts through practical, runnable code. Each sample includes a README with setup instructions, code walkthrough, and key concepts.

> **All samples are in the `/samples/` directory.** Browse the code on GitHub or clone the repository to run them locally.

## Getting Started

**New to NPipeline?** Start with these foundational samples:

1. **[Sample_BasicPipeline](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_BasicPipeline)** — Source, transform, and sink nodes; pipeline definition and execution
2. **[Sample_FileProcessing](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_FileProcessing)** — File-based sources and sinks; streaming file I/O
3. **[Sample_BasicErrorHandling](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_BasicErrorHandling)** — Try-catch patterns, retry logic, graceful degradation

## All Samples

### Fundamentals

| Sample | Concepts Demonstrated |
|--------|----------------------|
| [Sample_BasicPipeline](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_BasicPipeline) | Source, transform, sink nodes; pipeline definition; DI integration |
| [Sample_FileProcessing](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_FileProcessing) | File-based sources/sinks; streaming I/O; atomic file operations |
| [Sample_BasicErrorHandling](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_BasicErrorHandling) | Try-catch patterns; exponential backoff; fallback mechanisms |
| [Sample_SimpleDataTransformation](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_SimpleDataTransformation) | CSV parsing; validation; filtering; enrichment |
| [Sample_ParallelProcessing](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_ParallelProcessing) | Parallel execution strategies; resource management; thread safety |

### Error Handling & Resilience

| Sample | Concepts Demonstrated |
|--------|----------------------|
| [Sample_AdvancedErrorHandling](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_AdvancedErrorHandling) | Circuit breakers; dead letter queues; Polly integration |
| [Sample_FluentErrorHandling](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_FluentErrorHandling) | Fluent error handling API; error simulation utilities |
| [Sample_RetryDelay](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_RetryDelay) | Retry delay strategies; backoff patterns; jitter |

### Data Connectors

| Sample | Concepts Demonstrated |
|--------|----------------------|
| [Sample_CsvConnector](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_CsvConnector) | CsvSourceNode/CsvSinkNode; attribute-based mapping; validation |
| [Sample_JsonConnector](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_JsonConnector) | JsonSourceNode/JsonSinkNode; JSON array and NDJSON formats |
| [Sample_ExcelConnector](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_ExcelConnector) | Excel source/sink nodes; Excel-specific configuration |
| [Sample_KafkaConnector](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_KafkaConnector) | Kafka integration; consumer/producer configuration |
| [Sample_PostgreSQLConnector](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_PostgreSQLConnector) | PostgreSQL source/sink; database operations |
| [Sample_CosmosDbConnector](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_CosmosDbConnector) | Cosmos DB integration; NoSQL operations |
| [Sample_SqsConnector](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_SqsConnector) | AWS SQS integration; message queue processing |
| [Sample_SqlServerConnector](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_SqlServerConnector) | SQL Server integration; database connectivity |

### Data Enrichment & Lookups

| Sample | Concepts Demonstrated |
|--------|----------------------|
| [Sample_LookupNode](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_LookupNode) | LookupNode for data enrichment; async lookups; missing data handling |
| [Sample_InMemoryLookupNode](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_InMemoryLookupNode) | In-memory lookup patterns; reference data caching |

### Aggregation & Analytics

| Sample | Concepts Demonstrated |
|--------|----------------------|
| [Sample_AggregateNode](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_AggregateNode) | AggregateNode; tumbling/sliding windows; real-time analytics |
| [Sample_AdvancedAggregateNode](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_AdvancedAggregateNode) | AdvancedAggregateNode; complex accumulators; financial risk calculations |
| [Sample_StreamingAnalytics](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_StreamingAnalytics) | Time-based windowing; watermarks; late data handling |

### Batching & Unbatching

| Sample | Concepts Demonstrated |
|--------|----------------------|
| [Sample_BatchingNode](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_BatchingNode) | BatchingNode; size/time-based batching; bulk operations |
| [Sample_UnbatchingNode](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_UnbatchingNode) | UnbatchingNode; batch-to-stream conversion; hybrid processing |

### Joins & Merging

| Sample | Concepts Demonstrated |
|--------|----------------------|
| [Sample_KeyedJoinNode](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_KeyedJoinNode) | KeyedJoinNode; inner/outer joins; multi-stream correlation |
| [Sample_TimeWindowedJoinNode](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_TimeWindowedJoinNode) | TimeWindowedJoinNode; temporal correlation; watermark management |
| [Sample_SelfJoinNode](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_SelfJoinNode) | Self-join patterns; stream self-correlation |
| [Sample_CustomMergeNode](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_CustomMergeNode) | CustomMergeNode; priority-based merging; conflict resolution |

### Branching & Distribution

| Sample | Concepts Demonstrated |
|--------|----------------------|
| [Sample_BranchNode](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_BranchNode) | BranchNode; parallel data distribution; multi-path processing |
| [Sample_TapNode](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_TapNode) | TapNode; non-intrusive monitoring; audit logging; side-effect processing |

### Windowing & Event-Time Processing

| Sample | Concepts Demonstrated |
|--------|----------------------|
| [Sample_WindowingStrategies](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_WindowingStrategies) | Session windows; dynamic windows; custom trigger windows |
| [Sample_WatermarkHandling](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_WatermarkHandling) | Event-time processing; watermark generation; late data handling |

### Performance & Optimization

| Sample | Concepts Demonstrated |
|--------|----------------------|
| [Sample_PerformanceOptimization](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_PerformanceOptimization) | ValueTask optimization; sync fast paths; memory allocation reduction |
| [Sample_ParallelExecution_Simplified](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_ParallelExecution_Simplified) | Simplified parallel execution; degree of parallelism |

### Custom Implementation

| Sample | Concepts Demonstrated |
|--------|----------------------|
| [Sample_CustomNodeImplementation](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_CustomNodeImplementation) | Custom node development; lifecycle management; caching patterns |
| [Sample_LambdaNodes](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_LambdaNodes) | Lambda-based nodes; inline transforms; functional patterns |

### Complex Scenarios

| Sample | Concepts Demonstrated |
|--------|----------------------|
| [Sample_ComplexDataTransformations](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_ComplexDataTransformations) | Multi-stream joins; external lookups; complex aggregations |
| [Sample_IntentDrivenGrouping](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_IntentDrivenGrouping) | Intent-based grouping; semantic data organization |
| [Sample_HttpPost](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_HttpPost) | HTTP sink nodes; REST API integration |

### Storage Providers

| Sample | Concepts Demonstrated |
|--------|----------------------|
| [Sample_S3StorageProvider](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_S3StorageProvider) | AWS S3 integration; cloud storage operations |
| [Sample_AzureStorageProvider](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_AzureStorageProvider) | Azure Blob Storage integration |
| [Sample_GcsStorageProvider](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_GcsStorageProvider) | Google Cloud Storage integration |

### Extensions

| Sample | Concepts Demonstrated |
|--------|----------------------|
| [Sample_LineageExtension](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_LineageExtension) | Data lineage tracking; sampling strategies; custom sinks |
| [Sample_NodesExtension](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_NodesExtension) | Pre-built processing nodes; cleansing; validation |
| [Sample_ObservabilityExtension](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_ObservabilityExtension) | Metrics collection; performance tracking; monitoring |
| [Sample_Composition](https://github.com/chrisjacques/NPipeline/tree/main/samples/Sample_Composition) | Hierarchical pipelines; sub-pipeline composition; context inheritance |

## Running Samples

All samples follow the same pattern:

```bash
cd samples/<SampleName>
dotnet restore
dotnet run
```

Each sample's README contains specific prerequisites, configuration options, and expected output.

## Related Documentation

- **[Core Concepts](../core-concepts/index.md)** — Fundamental NPipeline concepts
- **[Architecture](../architecture/index.md)** — How NPipeline works internally
- **[Extensions](../extensions/index.md)** — Available extension packages
