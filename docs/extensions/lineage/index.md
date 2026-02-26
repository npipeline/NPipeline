---
title: NPipeline Lineage Extension
description: Comprehensive data lineage tracking and provenance capabilities for NPipeline pipelines.
sidebar_position: 3
slug: /extensions/lineage
---

# NPipeline Lineage Extension

The `NPipeline.Extensions.Lineage` extension provides comprehensive data lineage tracking and provenance capabilities for NPipeline pipelines. Track the complete journey of each data item from source to destination, enabling data governance, debugging, audit trails, and data discovery.

## Overview

Data lineage tracking is the process of recording and maintaining information about the origin, transformations, and flow of data through a system. In the context of NPipeline, lineage tracking captures:

- **Origin**: Where each data item entered the pipeline (source node)
- **Transformations**: Which nodes processed or modified the data
- **Path**: The complete sequence of nodes each item traversed
- **Decisions**: Branching decisions and routing outcomes
- **Cardinality**: How many items were produced/consumed at each hop
- **Timing**: When each transformation occurred (optional)

## Key Features

### Item-Level Lineage Tracking

Track individual data items as they flow through the pipeline, recording:

- **Traversal Path**: Complete list of node IDs the item passed through
- **Lineage Hops**: Detailed information about each hop including:
  - Node ID
  - Decision outcome (success, failure, filtered, etc.)
  - Observed cardinality (one-to-one, one-to-many, many-to-one, many-to-many)
  - Input contributor count
  - Output emission count
  - Ancestry input indices (for join operations)
  - Truncation status

### Pipeline-Level Reports

Generate high-level reports showing pipeline structure and data flow patterns:

- **Node Information**: All nodes in the pipeline with their types and configurations
- **Edge Information**: Connections between nodes showing data flow direction
- **Execution Summary**: Overall pipeline execution statistics
- **Run Metadata**: Pipeline ID, run ID, timestamps

### Configurable Sampling

Control lineage collection overhead with configurable sampling strategies:

**Deterministic Sampling**: Sample every Nth item using a hash-based approach. The same items are always sampled across runs, providing consistent behavior for debugging and compliance.

**Random Sampling**: Sample items randomly at the specified rate. Provides a representative sample with minimal overhead, suitable for monitoring and analytics.

### Data Redaction

Optionally exclude actual data from lineage records to:

- Reduce memory usage for large data objects
- Improve security by not storing sensitive information
- Focus on flow patterns rather than data values

### Flexible Sink Architecture

Implement custom lineage sinks to export lineage data to various destinations:

- **Logging**: Built-in [`LoggingPipelineLineageSink`](../../../src/NPipeline.Extensions.Lineage/LoggingPipelineLineageSink.cs) for structured logging
- **Databases**: Store lineage information in SQL or NoSQL databases
- **File Systems**: Export to JSON, CSV, or custom formats
- **External Services**: Send lineage data to monitoring or analytics platforms
- **Message Queues**: Publish lineage events for real-time processing

### Thread-Safe Collection

Lineage data is collected safely across parallel and concurrent pipeline executions using thread-safe data structures. The [`LineageCollector`](../../../src/NPipeline.Extensions.Lineage/LineageCollector.cs) uses `ConcurrentDictionary` for storage and fine-grained locking for individual trail updates.

## When to Use Lineage Tracking

### Production Environments

Lineage tracking is particularly valuable in production scenarios where:

- **Compliance Requirements**: Regulatory mandates require audit trails
- **Data Quality Monitoring**: Need to quickly identify and resolve data quality issues
- **Impact Analysis**: Understanding dependencies before making changes
- **Incident Response**: Tracing problems to their source during outages

### Development and Testing

During development and testing, lineage tracking helps:

- **Validate Pipeline Logic**: Ensure data flows through expected paths
- **Debug Transformations**: Identify which node introduced unexpected changes
- **Test Edge Cases**: Verify behavior for specific data items
- **Performance Analysis**: Understand where time is spent in complex pipelines

### Data Science and Analytics

For data science and analytics workflows:

- **Reproducibility**: Document exactly how datasets were created
- **Version Control**: Track data transformations alongside code changes
- **Data Cataloging**: Build a comprehensive catalog of data sources and transformations
- **Model Training**: Understand the provenance of training data

## Integration with NPipeline Core

The Lineage extension integrates seamlessly with NPipeline core through several extension points:

### PipelineBuilder Extensions

Configure lineage tracking directly on your pipeline builder:

```csharp
var builder = new PipelineBuilder("MyPipeline");

// Enable item-level lineage tracking
builder.EnableItemLevelLineage(options =>
{
    options.SampleEvery = 10;
    options.DeterministicSampling = true;
    options.RedactData = true;
});

// Add pipeline-level lineage sink
builder.UseLoggingPipelineLineageSink();
```

### Dependency Injection Integration

Register lineage services with Microsoft.Extensions.DependencyInjection:

```csharp
services.AddNPipelineLineage();
// Or with custom sink
services.AddNPipelineLineage<DatabaseLineageSink>();
// Or with factory
services.AddNPipelineLineage(sp => new CustomLineageSink(logger));
```

### Automatic Collection

Lineage tracking is automatically integrated into pipeline execution when enabled. No modifications to node logic are required—the extension hooks into the pipeline execution lifecycle to capture lineage information transparently.

## Documentation

- **[Getting Started](./getting-started.md)** - Installation and basic setup guide
- **[Configuration](./configuration.md)** - Configuration options and settings
- **[Architecture](./architecture.md)** - Architecture and implementation details
- **[Performance](./performance.md)** - Performance characteristics and optimization strategies
- **[Use Cases](./use-cases.md)** - Common use cases and examples
- **[Extension Samples](../../samples/extensions.md)** - Sample applications for all extensions including lineage
- **[NPipeline Core Concepts](../../core-concepts/index.md)** - Core pipeline concepts
- **[NPipeline Extensions](../index.md)** - Other available extensions
