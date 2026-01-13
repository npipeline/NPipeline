---
title: Metrics Reference
description: Complete reference for all metrics collected by the NPipeline Observability extension, including node and pipeline metrics, calculations, and thread-safety guarantees.
---

# Metrics Reference

This reference provides detailed documentation for all metrics collected by the NPipeline Observability extension, including how metrics are calculated, their data types, and thread-safety considerations.

## Node Metrics

Node metrics capture performance and execution data for individual nodes within a pipeline. Each node execution produces a complete set of metrics.

### INodeMetrics Interface

```csharp
public interface INodeMetrics
{
    string NodeId { get; }
    DateTimeOffset? StartTime { get; }
    DateTimeOffset? EndTime { get; }
    long? DurationMs { get; }
    bool Success { get; }
    long ItemsProcessed { get; }
    long ItemsEmitted { get; }
    Exception? Exception { get; }
    int RetryCount { get; }
    long? PeakMemoryUsageMb { get; }
    long? ProcessorTimeMs { get; }
    double? ThroughputItemsPerSec { get; }
    int? ThreadId { get; }
}
```

### Node Metrics Properties

#### NodeId

- **Type**: `string`
- **Description**: Unique identifier for the node within the pipeline
- **Source**: Assigned during pipeline construction
- **Thread-Safe**: Yes (immutable)
- **Example**: `"TransformNode"` or `"SourceNode_1"`

**Usage**: Use this property to correlate metrics with specific nodes in your pipeline definition. The node ID matches the handle used when connecting nodes.

#### StartTime

- **Type**: `DateTimeOffset?`
- **Description**: Timestamp when the node execution began
- **Source**: Recorded when `OnNodeStarted` event fires
- **Thread-Safe**: Yes (immutable)
- **Resolution**: System clock precision (typically 15-16ms on Windows)

**Calculation**: Captured at the beginning of node execution using `DateTimeOffset.UtcNow`.

**Usage**: Calculate duration by comparing with `EndTime`. Useful for identifying when nodes started relative to each other.

#### EndTime

- **Type**: `DateTimeOffset?`
- **Description**: Timestamp when the node execution completed
- **Source**: Recorded when `OnNodeCompleted` event fires
- **Thread-Safe**: Yes (immutable)
- **Resolution**: System clock precision

**Calculation**: Captured at the end of node execution using `DateTimeOffset.UtcNow`.

**Usage**: Calculate duration and identify when nodes finished. Can be null if the node is still executing or if metrics collection failed.

#### DurationMs

- **Type**: `long?`
- **Description**: Total execution time in milliseconds
- **Source**: Calculated from `StartTime` and `EndTime`
- **Thread-Safe**: Yes (immutable)
- **Precision**: Millisecond

**Calculation**:

```csharp
DurationMs = (long)(EndTime - StartTime).TotalMilliseconds
```

**Usage**: Primary metric for identifying performance bottlenecks. Compare duration across nodes to find slow components.

**Note**: Can be null if either `StartTime` or `EndTime` is null.

#### Success

- **Type**: `bool`
- **Description**: Whether the node execution completed successfully
- **Source**: Determined by exception presence
- **Thread-Safe**: Yes (immutable)

**Calculation**: `true` if no exception occurred, `false` if an exception was thrown.

**Usage**: Filter metrics to analyze successful vs. failed executions. Track success rates over time.

#### ItemsProcessed

- **Type**: `long`
- **Description**: Total number of items processed by the node
- **Source**: Aggregated from item processing events
- **Thread-Safe**: Yes (uses `Interlocked.Add`)
- **Range**: 0 to `long.MaxValue`

**Calculation**: Incremented atomically as items are processed. For parallel execution, this represents the total across all threads.

**Usage**: Measure node throughput and identify processing volume. Compare with `ItemsEmitted` to understand filtering ratios.

**Thread-Safety**: Uses `Interlocked.Add` for atomic increments in concurrent scenarios.

#### ItemsEmitted

- **Type**: `long`
- **Description**: Total number of items emitted by the node
- **Source**: Aggregated from item emission events
- **Thread-Safe**: Yes (uses `Interlocked.Add`)
- **Range**: 0 to `long.MaxValue`

**Calculation**: Incremented atomically as items are emitted to downstream nodes.

**Usage**: Compare with `ItemsProcessed` to understand filtering behavior. For source nodes, `ItemsProcessed` equals `ItemsEmitted`. For filter nodes, `ItemsEmitted` may be less than `ItemsProcessed`.

**Thread-Safety**: Uses `Interlocked.Add` for atomic increments in concurrent scenarios.

#### Exception

- **Type**: `Exception?`
- **Description**: Exception that caused node failure, if any
- **Source**: Captured from execution context
- **Thread-Safe**: Yes (immutable)

**Usage**: Debug failed executions, identify error patterns, and track exception types. Use `Exception.Message` for logging and `Exception.StackTrace` for detailed debugging.

**Note**: Null if the node executed successfully.

#### RetryCount

- **Type**: `int`
- **Description**: Maximum number of retry attempts for this node
- **Source**: Aggregated from retry events
- **Thread-Safe**: Yes (uses `Interlocked.Exchange`)
- **Range**: 0 to `int.MaxValue`

**Calculation**: Tracks the highest retry attempt number observed. If a node retries 3 times, this value will be 3.

**Usage**: Identify unreliable nodes or external dependencies that frequently fail. High retry counts may indicate:

- Unstable external services
- Transient network issues
- Insufficient timeout values
- Resource contention

**Thread-Safety**: Uses `Interlocked.Exchange` to ensure the maximum retry count is retained.

#### PeakMemoryUsageMb

- **Type**: `long?`
- **Description**: Per-node managed memory allocation delta in megabytes during node execution
- **Source**: Calculated as delta between final and initial memory using `GC.GetTotalMemory(false)`
- **Thread-Safe**: Yes (immutable)
- **Granularity**: Per-node managed memory allocation delta

**Calculation**:

```csharp
// Initial memory at node start
var initialMemoryBytes = GC.GetTotalMemory(false);

// ... node execution ...

// Final memory at node end
var finalMemoryBytes = GC.GetTotalMemory(false);

// Calculate delta (memory allocated during node execution)
var deltaBytes = finalMemoryBytes - initialMemoryBytes;
var memoryDeltaMb = deltaBytes / (1024 * 1024);
```

**Usage**: Identify memory-intensive nodes and optimize memory usage. Track memory growth over time to detect leaks.

**Important Notes**:

- This is a **per-node delta** of managed memory allocations, not global process memory
- Memory is measured using `GC.GetTotalMemory(false)` which captures managed memory allocations
- In parallel execution, this reflects the memory allocated during that specific node's execution
- May be null if metrics collection fails or is disabled
- Memory metrics require both extension-level (`EnableMemoryMetrics`) and node-level (`RecordMemoryUsage`) options to be enabled
- Use for relative comparisons between nodes, not absolute memory requirements

#### ProcessorTimeMs

- **Type**: `long?`
- **Description**: Total processor time used in milliseconds
- **Source**: Not available per-node in current implementation
- **Thread-Safe**: Yes (immutable)
- **Granularity**: Process-level (not node-specific)

**Important Notes**:

- This metric is **not available per-node** in the current implementation
- The field is included for future compatibility but will always be `null` for node metrics
- If you need CPU metrics, consider using system-level monitoring tools
- May be null if metrics collection fails or is disabled

#### AverageItemProcessingMs

- **Type**: `double?`
- **Description**: Average time spent processing each item in milliseconds
- **Source**: Calculated from `DurationMs` and `ItemsProcessed`
- **Thread-Safe**: Yes (immutable)
- **Precision**: Double-precision floating point

**Calculation**:

```csharp
AverageItemProcessingMs = DurationMs / ItemsProcessed
```

**Usage**: Measure per-item processing efficiency. Identify nodes with high per-item overhead. Compare processing time across different data volumes.

**Example**: If a node processes 1000 items in 500ms, average per-item time is 0.5ms.

**Note**: Can be null if `DurationMs` is null or `ItemsProcessed` is zero.

#### ThroughputItemsPerSec

- **Type**: `double?`
- **Description**: Processing throughput in items per second
- **Source**: Calculated from `ItemsProcessed` and `DurationMs`
- **Thread-Safe**: Yes (immutable)
- **Precision**: Double-precision floating point

**Calculation**:

```csharp
ThroughputItemsPerSec = ItemsProcessed / (DurationMs / 1000.0)
```

**Usage**: Primary performance metric for comparing node efficiency. Track throughput over time to identify degradation.

**Example**: If a node processes 1000 items in 500ms, throughput is 2000 items/sec.

**Note**: Can be null if `DurationMs` is null or zero.

#### ThreadId

- **Type**: `int?`
- **Description**: Thread ID that primarily processed this node
- **Source**: `Environment.CurrentManagedThreadId`
- **Thread-Safe**: Yes (immutable)

**Usage**: Understand thread assignment in parallel execution scenarios. Identify thread affinity issues or thread pool contention.

**Important Notes**:

- For single-threaded execution, this is the only thread used
- For parallel execution, this is the thread that started execution
- May be null if thread tracking is disabled
- Thread IDs are process-specific and not meaningful across different executions

## Pipeline Metrics

Pipeline metrics provide aggregate data for the entire pipeline execution, including summary statistics and individual node metrics.

### IPipelineMetrics Interface

```csharp
public interface IPipelineMetrics
{
    string PipelineName { get; }
    Guid RunId { get; }
    DateTimeOffset StartTime { get; }
    DateTimeOffset? EndTime { get; }
    long? DurationMs { get; }
    bool Success { get; }
    long TotalItemsProcessed { get; }
    IReadOnlyList<INodeMetrics> NodeMetrics { get; }
    Exception? Exception { get; }
}
```

### Pipeline Metrics Properties

#### PipelineName

- **Type**: `string`
- **Description**: Name of the pipeline definition
- **Source**: Pipeline definition type name or custom name
- **Thread-Safe**: Yes (immutable)
- **Example**: `"DataProcessingPipeline"` or `"ETLPipeline"`

**Usage**: Correlate metrics with specific pipeline definitions. Useful when running multiple pipeline types in the same application.

#### RunId

- **Type**: `Guid`
- **Description**: Unique identifier for this pipeline execution
- **Source**: Generated at pipeline start
- **Thread-Safe**: Yes (immutable)
- **Uniqueness**: Globally unique across all executions

**Usage**: Correlate all metrics from a single pipeline execution. Track executions across distributed systems or log aggregation tools.

**Example**: `123e4567-e89b-12d3-a456-426614174000`

#### StartTime

- **Type**: `DateTimeOffset`
- **Description**: Timestamp when pipeline execution began
- **Source**: Recorded at pipeline start
- **Thread-Safe**: Yes (immutable)
- **Resolution**: System clock precision

**Usage**: Calculate pipeline duration and identify when pipelines started relative to each other.

#### EndTime

- **Type**: `DateTimeOffset?`
- **Description**: Timestamp when pipeline execution completed
- **Source**: Recorded at pipeline completion
- **Thread-Safe**: Yes (immutable)
- **Resolution**: System clock precision

**Usage**: Calculate pipeline duration. Can be null if the pipeline is still executing or if metrics collection failed.

#### DurationMs

- **Type**: `long?`
- **Description**: Total pipeline execution time in milliseconds
- **Source**: Calculated from `StartTime` and `EndTime`
- **Thread-Safe**: Yes (immutable)
- **Precision**: Millisecond

**Calculation**:

```csharp
DurationMs = (long)(EndTime - StartTime).TotalMilliseconds
```

**Usage**: Primary metric for overall pipeline performance. Track duration trends over time to identify degradation or improvements.

**Note**: Can be null if `EndTime` is null.

#### Success

- **Type**: `bool`
- **Description**: Whether the pipeline execution completed successfully
- **Source**: Determined by exception presence
- **Thread-Safe**: Yes (immutable)

**Calculation**: `true` if no exception occurred during pipeline execution, `false` otherwise.

**Usage**: Track pipeline success rates and identify failure patterns. Filter metrics to analyze successful vs. failed executions.

#### TotalItemsProcessed

- **Type**: `long`
- **Description**: Total number of items processed by all nodes in the pipeline
- **Source**: Sum of all node `ItemsProcessed` values
- **Thread-Safe**: Yes (immutable)
- **Range**: 0 to `long.MaxValue`

**Calculation**:

```csharp
TotalItemsProcessed = NodeMetrics.Sum(m => m.ItemsProcessed)
```

**Usage**: Measure pipeline throughput and processing volume. Compare with duration to calculate overall pipeline throughput.

**Important Notes**:

- This is the sum across all nodes, not unique items
- For pipelines with multiple nodes, this may be greater than the actual input count
- Use `NodeMetrics[0].ItemsProcessed` for the actual input count (first node)

#### NodeMetrics

- **Type**: `IReadOnlyList<INodeMetrics>`
- **Description**: Collection of metrics for each node in the pipeline
- **Source**: Collected during pipeline execution
- **Thread-Safe**: Yes (immutable collection)

**Usage**: Analyze individual node performance within the pipeline context. Identify bottlenecks and optimize specific nodes.

**Access Patterns**:

```csharp
// Get all node metrics
var allNodeMetrics = pipelineMetrics.NodeMetrics;

// Find metrics for a specific node
var transformNodeMetrics = pipelineMetrics.NodeMetrics
    .FirstOrDefault(m => m.NodeId == "TransformNode");

// Find slowest node
var slowestNode = pipelineMetrics.NodeMetrics
    .OrderByDescending(m => m.DurationMs)
    .FirstOrDefault();

// Calculate average node duration
var avgDuration = pipelineMetrics.NodeMetrics
    .Where(m => m.DurationMs.HasValue)
    .Average(m => m.DurationMs.Value);
```

#### Exception

- **Type**: `Exception?`
- **Description**: Exception that caused pipeline failure, if any
- **Source**: Captured from execution context
- **Thread-Safe**: Yes (immutable)

**Usage**: Debug failed pipeline executions, identify error patterns, and track exception types. Use `Exception.Message` for logging and `Exception.StackTrace` for detailed debugging.

**Note**: Null if the pipeline executed successfully.

## Metrics Calculation Details

### Throughput Calculation

Throughput is calculated for each node based on items processed and execution duration:

```csharp
ThroughputItemsPerSec = ItemsProcessed / (DurationMs / 1000.0)
```

**Example**:

- ItemsProcessed: 5000
- DurationMs: 2500
- ThroughputItemsPerSec: 5000 / (2500 / 1000) = 2000 items/sec

**Edge Cases**:

- If `DurationMs` is null or zero, throughput is null
- If `ItemsProcessed` is zero, throughput is zero
- Throughput is calculated after node completion, not during execution

### Memory Usage Calculation

Memory usage is measured as **per-node managed memory allocation deltas**:

```csharp
// Initial memory at node start
var initialMemoryBytes = GC.GetTotalMemory(false);

// ... node execution ...

// Final memory at node end
var finalMemoryBytes = GC.GetTotalMemory(false);

// Calculate delta (memory allocated during node execution)
var deltaBytes = finalMemoryBytes - initialMemoryBytes;
var memoryDeltaMb = deltaBytes / (1024 * 1024);
```

**Important Considerations**:

- Memory is measured using `GC.GetTotalMemory(false)` which captures managed memory allocations
- This is a **per-node delta**, not global process memory or peak working set
- In parallel execution, each node gets its own isolated memory measurement
- The delta represents memory allocated during that specific node's execution
- Garbage collection may cause memory usage to fluctuate
- Memory metrics require both extension-level (`EnableMemoryMetrics`) and node-level (`RecordMemoryUsage`) options to be enabled

### Retry Count Calculation

Retry count tracks the maximum retry attempt observed:

```csharp
// Uses Interlocked.Exchange to ensure maximum is retained
RetryCount = Math.Max(currentRetryCount, newRetryCount)
```

**Behavior**:

- If a node retries 3 times (attempts 1, 2, 3), `RetryCount` will be 3
- If multiple retries occur with different reasons, only the count is retained
- Retry reasons are not currently captured in metrics (see TODO in source)

## Thread-Safety Considerations

### Concurrent Metrics Collection

The `ObservabilityCollector` is designed for thread-safe operation:

```csharp
private readonly ConcurrentDictionary<string, NodeMetricsBuilder> _nodeMetrics = new();
```

**Thread-Safety Guarantees**:

- Multiple nodes can record metrics simultaneously without race conditions
- `ConcurrentDictionary` provides atomic operations for adding and updating entries
- Each node has its own `NodeMetricsBuilder` instance

### Atomic Counter Updates

Item counters use atomic operations for thread-safe increments:

```csharp
public void RecordItemMetrics(long itemsProcessed, long itemsEmitted)
{
    Interlocked.Add(ref _itemsProcessed, itemsProcessed);
    Interlocked.Add(ref _itemsEmitted, itemsEmitted);
}
```

**Thread-Safety Guarantees**:

- `Interlocked.Add` ensures atomic addition without locks
- Multiple threads can increment counters simultaneously
- No lost updates or race conditions

### Retry Count Updates

Retry count uses atomic exchange to retain the maximum:

```csharp
public void RecordRetry(int retryCount)
{
    Interlocked.Exchange(ref _retryCount, Math.Max(_retryCount, retryCount));
}
```

**Thread-Safety Guarantees**:

- `Interlocked.Exchange` ensures atomic updates
- Maximum retry count is always retained
- No race conditions when multiple retries occur

### Immutable Metric Records

Once built, metric records are immutable:

```csharp
public sealed record NodeMetrics(
    string NodeId,
    DateTimeOffset? StartTime,
    // ... other properties
) : INodeMetrics;
```

**Thread-Safety Guarantees**:

- Records are immutable and safe to share across threads
- No synchronization needed when reading built metrics
- Safe to store in collections accessed by multiple threads

### Scoped Lifetime

Collectors are scoped to ensure isolation:

```csharp
services.TryAddScoped<IObservabilityCollector, ObservabilityCollector>();
```

**Thread-Safety Guarantees**:

- Each pipeline run gets its own collector instance
- Concurrent pipeline runs don't interfere with each other
- Metrics are automatically disposed when the scope ends

## Performance Metrics Interpretation

### Identifying Bottlenecks

Use node metrics to identify performance bottlenecks:

```csharp
// Find slowest nodes
var bottlenecks = pipelineMetrics.NodeMetrics
    .Where(m => m.DurationMs.HasValue)
    .OrderByDescending(m => m.DurationMs.Value)
    .Take(3);

foreach (var node in bottlenecks)
{
    Console.WriteLine($"{node.NodeId}: {node.DurationMs}ms, {node.ThroughputItemsPerSec} items/sec");
}
```

### Analyzing Throughput

Compare throughput across nodes to understand efficiency:

```csharp
// Calculate throughput ratios
var throughputRatios = pipelineMetrics.NodeMetrics
    .Where(m => m.ThroughputItemsPerSec.HasValue)
    .Select(m => new
    {
        m.NodeId,
        Throughput = m.ThroughputItemsPerSec.Value,
        Ratio = m.ThroughputItemsPerSec.Value / pipelineMetrics.NodeMetrics[0].ThroughputItemsPerSec.Value
    })
    .OrderByDescending(m => m.Ratio);
```

### Memory Efficiency Analysis

Identify memory-intensive nodes:

```csharp
// Find nodes with high memory usage
var memoryIntensive = pipelineMetrics.NodeMetrics
    .Where(m => m.PeakMemoryUsageMb.HasValue)
    .OrderByDescending(m => m.PeakMemoryUsageMb.Value)
    .Take(5);
```

### Retry Pattern Analysis

Identify unreliable nodes:

```csharp
// Find nodes with high retry counts
var unreliableNodes = pipelineMetrics.NodeMetrics
    .Where(m => m.RetryCount > 0)
    .OrderByDescending(m => m.RetryCount);
```

## Related Topics

- **[Observability Overview](./index.md)**: Introduction to observability features
- **[Configuration Guide](./configuration.md)**: Setup and configuration options
- **[Usage Examples](./examples.md)**: Complete code examples
