---
title: NPipeline Lineage Extension
description: Comprehensive data lineage tracking and provenance capabilities for NPipeline pipelines.
sidebar_position: 6
slug: /extensions/lineage
---

# NPipeline Lineage Extension

The `NPipeline.Extensions.Lineage` extension provides comprehensive data lineage tracking and provenance capabilities for NPipeline pipelines. Track the complete journey of each data item from source to destination, enabling data governance, debugging, audit trails, and data discovery.

## What is Lineage Tracking?

Data lineage tracking is the process of recording and maintaining information about the origin, transformations, and flow of data through a system. In the context of NPipeline, lineage tracking captures:

- **Origin**: Where each data item entered the pipeline (source node)
- **Transformations**: Which nodes processed or modified the data
- **Path**: The complete sequence of nodes each item traversed
- **Decisions**: Branching decisions and routing outcomes
- **Cardinality**: How many items were produced/consumed at each hop
- **Timing**: When each transformation occurred (optional)

### Why Lineage Tracking Matters

In production data pipelines, understanding data provenance is critical for several reasons:

**Data Governance**: Regulatory requirements (GDPR, HIPAA, SOX) often mandate complete audit trails showing how data was processed, transformed, and stored. Lineage tracking provides this documentation automatically.

**Root Cause Analysis**: When data quality issues arise, lineage tracking allows you to trace the problem back to its source. Instead of guessing which transformation introduced an error, you can see exactly which node modified the data and when.

**Debugging Complex Pipelines**: In pipelines with branching, joining, and parallel execution, understanding data flow can be challenging. Lineage tracking provides a complete map of each item's journey through the pipeline.

**Impact Analysis**: When modifying a pipeline, you need to understand which downstream processes depend on a particular data source or transformation. Lineage tracking enables impact analysis by showing data dependencies.

**Compliance and Auditing**: Many industries require proof of data integrity and transformation history. Lineage tracking provides an immutable record of all data movements and transformations.

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

```csharp
public sealed record LineageInfo(
    object? Data,                          // Final data (nullable when redacted)
    Guid LineageId,                         // Unique identifier
    IReadOnlyList<string> TraversalPath,   // Node IDs passed through
    IReadOnlyList<LineageHop> LineageHops   // Per-hop details
);
```

### Pipeline-Level Reports

Generate high-level reports showing pipeline structure and data flow patterns:

- **Node Information**: All nodes in the pipeline with their types and configurations
- **Edge Information**: Connections between nodes showing data flow direction
- **Execution Summary**: Overall pipeline execution statistics
- **Run Metadata**: Pipeline ID, run ID, timestamps

### Configurable Sampling

Control lineage collection overhead with configurable sampling strategies:

**Deterministic Sampling**: Sample every Nth item using a hash-based approach. The same items are always sampled across runs, providing consistent behavior for debugging and compliance.

```csharp
options.SampleEvery = 100; // 1% sampling rate
options.DeterministicSampling = true;
```

**Random Sampling**: Sample items randomly at the specified rate. Provides a representative sample with minimal overhead, suitable for monitoring and analytics.

```csharp
options.SampleEvery = 100;
options.DeterministicSampling = false;
```

### Data Redaction

Optionally exclude actual data from lineage records to:

- Reduce memory usage for large data objects
- Improve security by not storing sensitive information
- Focus on flow patterns rather than data values

```csharp
options.RedactData = true; // Data field will be null in LineageInfo
```

### Flexible Sink Architecture

Implement custom lineage sinks to export lineage data to various destinations:

- **Logging**: Built-in [`LoggingPipelineLineageSink`](../../../src/NPipeline.Extensions.Lineage/LoggingPipelineLineageSink.cs) for structured logging
- **Databases**: Store lineage information in SQL or NoSQL databases
- **File Systems**: Export to JSON, CSV, or custom formats
- **External Services**: Send lineage data to monitoring or analytics platforms
- **Message Queues**: Publish lineage events for real-time processing

```csharp
public interface ILineageSink
{
    Task RecordAsync(LineageInfo lineageInfo, CancellationToken cancellationToken);
}

public interface IPipelineLineageSink
{
    Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken);
}
```

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

## Architecture

```text
┌─────────────────────────────────────────┐
│         Pipeline Execution              │
└──────────────┬──────────────────────────┘
               │
               ├─> Lineage Tracking (per item)
               │   - CreateLineagePacket at source
               │   - RecordHop at each node
               │   - ShouldCollectLineage (sampling)
               │
               ↓
        ┌─────────────────────────┐
        │   ILineageCollector     │
        │  (Thread-safe)          │
        │  - ConcurrentDictionary │
        │  - Per-item LineageTrail│
        └──────────┬──────────────┘
                   │
                   ├─> LineageInfo (per item)
                   │   - TraversalPath
                   │   - LineageHops
                   │   - Data (optional)
                   │
                   ├─> PipelineLineageReport
                   │   - Nodes, Edges
                   │   - Run Metadata
                   │
                   ↓
        ┌──────────────────────┐
        │   Lineage Sinks       │
        └──────────┬───────────┘
                   │
                   ├─> LoggingPipelineLineageSink
                   ├─> Custom ILineageSink
                   ├─> Custom IPipelineLineageSink
                   └─> External Systems
```

## Performance Characteristics

### Overhead

Lineage tracking has a non-zero cost that scales with:

- **Number of items processed**: More items = more lineage records
- **Pipeline complexity**: More nodes = more hops per item
- **Data size**: Larger data objects = more memory when not redacted
- **Sampling rate**: Lower sampling = less overhead

### Actual Performance Impact

Based on typical pipelines:

- **Without sampling (100% tracking)**: ~2-5% pipeline throughput impact, ~800 bytes-2 KB per item
- **With 10% sampling**: ~0.2-0.5% pipeline throughput impact, ~80 bytes-200 bytes per item
- **With 1% sampling**: ~0.02-0.05% pipeline throughput impact, ~8-20 bytes per item
- **With data redaction**: ~30% memory reduction compared to full tracking

These benchmarks assume typical data sizes (100-1000 bytes) and pipelines with 3-5 nodes.

### Mitigation Strategies

Use the following strategies to minimize performance impact:

1. **Sampling**: Use 1-10% sampling in production for most pipelines
   - Deterministic for debugging specific issues (consistent across runs)
   - Random for monitoring and analytics (representative samples)
2. **Data Redaction**: Exclude actual data from lineage records when possible
3. **Materialization Caps**: Limit the number of items materialized in memory
4. **Overflow Policy**: Choose appropriate policy based on your pipeline requirements
5. **Async Sinks**: Use asynchronous lineage sinks to avoid blocking pipeline execution
6. **Selective Tracking**: Enable lineage only for critical pipelines

### Memory Usage

- **Per-item overhead (with sampling)**: 500 bytes to 2 KB per sampled item
- **Per-pipeline overhead**: ~10 KB for the collector itself (negligible)
- **Transient storage**: Lineage data is cleared after pipeline execution unless persisted by sinks

## Understanding Overflow Policies

When the materialization cap is reached, lineage collection behavior depends on the configured overflow policy:

### MaterializationCap Default

The default materialization cap is 10,000 items. Adjust based on your memory constraints:

```csharp
options.MaterializationCap = 10000; // Default
```

### Overflow Policy Options

**Materialize** (Complete tracking, highest memory)

- Continues collecting all lineage data in memory
- Use when memory is available and complete lineage is critical
- Risk: Out-of-memory errors on very large datasets

```csharp
options.OverflowPolicy = LineageOverflowPolicy.Materialize;
```

**Degrade** (Recommended for production)

- Switches to streaming mode when cap is reached
- New items beyond cap are still tracked but streamed to sinks instead of materialized
- Lineage is complete but older items may be removed from in-memory collection
- Best balance of visibility and memory safety

```csharp
options.OverflowPolicy = LineageOverflowPolicy.Degrade; // Default
```

**Drop** (Minimal memory, partial visibility)

- Stops collecting lineage when cap is reached
- Useful for high-volume pipelines where sampling doesn't suffice
- You'll lose lineage for items beyond the cap
- Best for monitoring-only scenarios

```csharp
options.OverflowPolicy = LineageOverflowPolicy.Drop;
```

### Choosing an Overflow Policy

| Scenario | Policy | Reasoning |
| --- | --- | --- |
| Production pipelines with sampling | Degrade | Safe default, maintains visibility |
| Development/debugging | Materialize | Complete information useful for investigation |
| High-volume monitoring | Drop | Prevents memory issues with sampling |
| Memory-constrained environments | Drop | Minimal memory footprint |
| Compliance/audit scenarios | Degrade | Ensures records are persisted to sinks |

### Performance Optimization Strategies

Use the following strategies to minimize performance impact:

1. **Sampling**: Sample 1-10% of items in production
2. **Data Redaction**: Exclude actual data from lineage records
3. **Materialization Caps**: Limit the number of items materialized in memory
4. **Async Sinks**: Use asynchronous lineage sinks to avoid blocking pipeline execution
5. **Selective Tracking**: Enable lineage only for critical pipelines

## Common Use Cases

### Data Governance

Maintain complete audit trails for regulatory compliance:

```csharp
services.AddNPipelineLineage<DatabaseLineageSink>();

// All data transformations are recorded
// with timestamps, decision outcomes, and full provenance
```

### Debugging

Quickly identify which node introduced issues:

```csharp
var lineageInfo = collector.GetLineageInfo(lineageId);
foreach (var hop in lineageInfo.LineageHops)
{
    Console.WriteLine($"Node: {hop.NodeId}, Outcome: {hop.Outcome}");
}
```

### Impact Analysis

Understand dependencies before making changes:

```csharp
// Query lineage to find all items that passed through a specific node
var affectedItems = collector.GetAllLineageInfo()
    .Where(li => li.TraversalPath.Contains("ProblematicNode"));
```

### Performance Monitoring

Identify bottlenecks in complex pipelines:

```csharp
// Analyze hop timestamps to find slow transformations
var slowHops = lineageInfo.LineageHops
    .Where(h => h.DurationMs > threshold);
```

## Getting Started

### Installation

```bash
dotnet add package NPipeline.Extensions.Lineage
```

### Basic Setup

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Lineage.DependencyInjection;

// Add lineage services
services.AddNPipelineLineage();

// Enable in pipeline
builder.EnableItemLevelLineage();
```

### Complete Example

See the [Sample_LineageExtension](../../../samples/Sample_LineageExtension/) project for comprehensive examples demonstrating all features.

## Documentation

- **[Extension README](../../../src/NPipeline.Extensions.Lineage/README.md)** - Complete API reference and configuration guide
- **[Sample Documentation](../samples/lineage.md)** - Sample application walkthrough
- **[NPipeline Core Concepts](../core-concepts/index.md)** - Core pipeline concepts
- **[NPipeline Extensions](index.md)** - Other available extensions
