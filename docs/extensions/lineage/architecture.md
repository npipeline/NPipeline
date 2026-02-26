---
title: Lineage Architecture
description: Internal architecture and design decisions of NPipeline Lineage extension.
---

# Lineage Architecture

This guide covers the internal architecture, design decisions, and implementation details of the NPipeline Lineage extension.

## System Architecture

The Lineage extension follows a layered architecture for separation of concerns:

```text
┌─────────────────────────────────────┐
│         Pipeline Execution          │
└──────────────┬──────────────────────┘
               │
               ├─> Lineage Tracking (per item)
               │   - CreateLineagePacket at source
               │   - RecordHop at each node
               │   - ShouldCollectLineage (sampling)
               │
               ↓
        ┌──────────────────────────┐
        │  ILineageCollector       │
        │  (Thread-safe)           │
        │  - ConcurrentDictionary  │
        │  - Per-item LineageTrail │
        └──────────┬───────────────┘
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
        │   Lineage Sinks      │
        └──────────┬───────────┘
                   │
                   ├─> LoggingPipelineLineageSink
                   ├─> Custom ILineageSink
                   ├─> Custom IPipelineLineageSink
                   └─> External Systems
```

## Core Components

### LineageCollector

The [`LineageCollector`](../../../src/NPipeline.Extensions.Lineage/LineageCollector.cs) is the central component for collecting and storing lineage data:

**Key Responsibilities:**

- Thread-safe collection of lineage data
- Sampling logic implementation
- Materialization cap enforcement
- Overflow policy management
- Query interface for collected data

**Thread-Safety Guarantees:**

- Uses `ConcurrentDictionary<string, LineageTrail>` for storage
- Fine-grained locking for individual trail updates
- Atomic operations for counters
- Safe for parallel and concurrent pipeline executions

**Storage Structure:**

```csharp
// Per-item lineage trail (internal)
// Note: LineageTrail is an internal implementation detail of LineageCollector

// Per-hop lineage information
public sealed record LineageHop(
    string NodeId,
    HopDecisionFlags Outcome,           // Flags enum, not string
    ObservedCardinality Cardinality,    // ObservedCardinality enum
    int? InputContributorCount,         // Nullable int
    int? OutputEmissionCount,           // Nullable int  
    IReadOnlyList<int>? AncestryInputIndices,  // Renamed from AncestryIndices
    bool Truncated
);

// Hop decision flags
[Flags]
public enum HopDecisionFlags
{
    None = 0,
    Emitted = 1 << 0,
    FilteredOut = 1 << 1,
    Joined = 1 << 2,
    Aggregated = 1 << 3,
    Retried = 1 << 4,
    Error = 1 << 5,
    DeadLettered = 1 << 6,
}

// Observed cardinality
public enum ObservedCardinality
{
    Unknown = 0,
    Zero = 1,
    One = 2,
    Many = 3,
}
```

### LineagePacket

The `LineagePacket<T>` is the data structure that flows through the pipeline with each item:

**Purpose:**

- Carries lineage metadata alongside actual data
- Enables automatic lineage tracking without node modifications
- Transparent to pipeline execution

**Structure:**

```csharp
public sealed record LineagePacket<T>(
    T Data,
    Guid LineageId,
    ImmutableList<string> TraversalPath
) : ILineageEnvelope
{
    public ImmutableList<LineageHop> LineageHops { get; init; } = ImmutableList<LineageHop>.Empty;
    public bool Collect { get; init; } = true;
}
```

**Flow:**

1. Created at source node with new `LineageId`
2. Updated at each node with hop information
3. Removed before reaching sink nodes
4. Final lineage stored in collector

### LineageInfo

The public-facing record representing complete lineage for an item:

```csharp
public sealed record LineageInfo(
    object? Data,                           // Final data (nullable when redacted)
    Guid LineageId,                         // Unique identifier
    IReadOnlyList<string> TraversalPath,    // Node IDs passed through
    IReadOnlyList<LineageHop> LineageHops   // Per-hop details
);
```

**Design Rationale:**

- Immutable for thread-safety
- Read-only collections for safe sharing
- Nullable data field for redaction support
- Complete history of item's journey

### PipelineLineageReport

High-level report containing pipeline structure and execution summary:

```csharp
public sealed record PipelineLineageReport(
    string Pipeline,                            // Pipeline name
    Guid RunId,                                 // Unique run identifier
    IReadOnlyList<NodeLineageInfo> Nodes,       // Node information
    IReadOnlyList<EdgeLineageInfo> Edges        // Edge information
);
```

**Components:**

- **Nodes**: All nodes in pipeline with their types and configurations
- **Edges**: Connections between nodes showing data flow
- **Metadata**: Pipeline name and run ID

## Design Decisions

### 1. Transparent Integration

Lineage tracking is designed to be completely transparent to node implementations:

**Approach:**

- Wraps data in `LineagePacket<T>` at source
- Unwraps before reaching sink nodes
- Nodes operate on original data type
- No modifications to node logic required

**Benefits:**

- Zero code changes to existing nodes
- Works with any node implementation
- Easy to enable/disable
- No performance impact when disabled

### 2. Sampling at Collection Time

Sampling decisions are made when lineage data is collected, not when items flow:

**Approach:**

- `ShouldCollectLineage()` called at each hop
- Consistent decision across entire item journey
- Either all hops collected or none
- Simplifies sampling logic

**Benefits:**

- Consistent lineage for sampled items
- No partial lineage records
- Easier to query and analyze
- Predictable memory usage

### 3. Thread-Safe by Default

All lineage operations are designed for concurrent execution:

**Approach:**

- `ConcurrentDictionary` for storage
- `Interlocked` operations for counters
- Immutable data structures
- Scoped collector instances

**Benefits:**

- Safe for parallel pipeline execution
- No race conditions
- No locks for most operations
- Scales with concurrency

### 4. Materialization Cap

Memory usage is controlled through materialization caps:

**Approach:**

- Default cap of 10,000 items
- Configurable overflow policies
- Streaming mode when cap reached
- Automatic cleanup of old data

**Benefits:**

- Prevents out-of-memory errors
- Predictable memory usage
- Configurable for different scenarios
- Graceful degradation

### 5. Sink Abstraction

Separate abstractions for item-level and pipeline-level lineage:

**Interfaces:**

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

**Benefits:**

- Flexible export destinations
- Separate concerns (item vs pipeline)
- Easy to extend
- Multiple sinks supported

## Data Flow

### Item Flow with Lineage

```text
Source Node
  ↓
  Create LineagePacket<T> with new LineageId
  ↓
Transform Node 1
  ↓
  Update LineagePacket with Hop 1
  ↓
Transform Node 2
  ↓
  Update LineagePacket with Hop 2
  ↓
...
  ↓
Sink Node
  ↓
  Unwrap LineagePacket
  ↓
  Store final LineageInfo in collector
```

### Collection Flow

```text
1. Item enters pipeline
   ↓
2. Create LineagePacket with LineageId
   ↓
3. ShouldCollectLineage()? (Sampling check)
   ├─ Yes → Continue tracking
   └─ No → Skip collection
   ↓
4. At each node:
   - Record hop information
   - Update traversal path
   - Check materialization cap
   ↓
5. At sink:
   - Unwrap packet
   - Store LineageInfo
   ↓
6. Pipeline complete:
   - Generate PipelineLineageReport
   - Send to IPipelineLineageSink
```

## Sampling Implementation

### Deterministic Sampling

Uses hash-based approach for consistent item selection:

```csharp
private bool ShouldCollectLineage(Guid lineageId)
{
    if (_options.SampleEvery <= 1)
        return true;  // Collect all
    
    if (_options.DeterministicSampling)
    {
        // Hash-based: same items always selected
        var hash = lineageId.GetHashCode();
        return Math.Abs(hash % _options.SampleEvery) == 0;
    }
    else
    {
        // Random: different items each run
        return Random.Shared.Next(_options.SampleEvery) == 0;
    }
}
```

**Why Hash-Based:**

- Consistent across runs
- No random number generator state issues
- Deterministic for debugging
- Fair distribution

### Sampling Granularity

Sampling is applied at the item level, not hop level:

**Rationale:**

- Complete lineage or no lineage for an item
- Easier to understand item journey
- Consistent memory usage
- Simpler querying

## Overflow Handling

### Materialize Policy

Continues collecting without limits:

```csharp
private void RecordLineageInternal(LineageInfo lineageInfo)
{
    // No cap check
    _lineageData[lineageInfo.LineageId] = lineageInfo;
}
```

**Use Case:** Development, debugging, small datasets

### Degrade Policy (Default)

Switches to streaming mode when cap reached:

```csharp
private void RecordLineageInternal(LineageInfo lineageInfo)
{
    if (_lineageData.Count >= _options.MaterializationCap)
    {
        // Stream to sink instead of storing
        _lineageSink?.RecordAsync(lineageInfo, _cancellationToken);
    }
    else
    {
        _lineageData[lineageInfo.LineageId] = lineageInfo;
    }
}
```

**Use Case:** Production, compliance, most scenarios

### Drop Policy

Stops collecting when cap reached:

```csharp
private void RecordLineageInternal(LineageInfo lineageInfo)
{
    if (_lineageData.Count >= _options.MaterializationCap)
        return;  // Drop
    
    _lineageData[lineageInfo.LineageId] = lineageInfo;
}
```

**Use Case:** High-volume monitoring, memory-constrained

## Sink Architecture

### ILineageSink

Handles item-level lineage export:

**Responsibilities:**

- Receive `LineageInfo` for each collected item
- Export to external systems
- Handle errors gracefully
- Respect cancellation tokens

**Implementation Example:**

```csharp
public sealed class DatabaseLineageSink : ILineageSink
{
    public async Task RecordAsync(LineageInfo lineageInfo, CancellationToken cancellationToken)
    {
        await _repository.SaveLineageAsync(lineageInfo, cancellationToken);
    }
}
```

### IPipelineLineageSink

Handles pipeline-level lineage export:

**Responsibilities:**

- Receive `PipelineLineageReport` at pipeline completion
- Export pipeline structure and summary
- Handle errors gracefully
- Respect cancellation tokens

**Implementation Example:**

```csharp
public sealed class JsonFileLineageSink : IPipelineLineageSink
{
    public async Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken)
    {
        var json = JsonSerializer.Serialize(report);
        await File.WriteAllTextAsync(_filePath, json, cancellationToken);
    }
}
```

### Sink Lifetime

Sinks are created per pipeline run:

**Rationale:**

- Scoped lifetime for isolation
- No state sharing between runs
- Automatic disposal
- Thread-safe by design

## Integration Points

### PipelineBuilder Extensions

Extension methods for enabling lineage:

```csharp
public static class PipelineBuilderLineageExtensions
{
    public static PipelineBuilder EnableItemLevelLineage(
        this PipelineBuilder builder,
        Action<LineageOptions>? configureOptions = null);
    
    public static PipelineBuilder UseLoggingPipelineLineageSink(
        this PipelineBuilder builder);
}
```

### Dependency Injection Integration

Service registration for DI containers:

```csharp
public static class LineageServiceCollectionExtensions
{
    public static IServiceCollection AddNPipelineLineage<TLineageSink>(
        this IServiceCollection services)
        where TLineageSink : class, ILineageSink;
    
    public static IServiceCollection AddNPipelineLineage<TLineageSink, TPipelineLineageSink>(
        this IServiceCollection services)
        where TLineageSink : class, ILineageSink
        where TPipelineLineageSink : class, IPipelineLineageSink;
}
```

## Performance Considerations

### Memory Allocation

- **Per-item overhead**: Memory usage scales with data size and number of hops
- **Per-pipeline overhead**: Fixed overhead for collector initialization
- **Transient storage**: Cleared after pipeline execution

### CPU Impact

- **Hash calculation**: Fast operation per item
- **Dictionary lookup**: Efficient lookup per hop
- **Lock-free operations**: Most operations are lock-free
- **Async sinks**: Non-blocking to pipeline execution

### Scalability

- **Linear scaling**: Memory usage scales with the number of items processed
- **Efficient lookups**: Dictionary operations provide fast access
- **Parallel-safe**: No contention with concurrent execution
- **Configurable overhead**: Sampling reduces overall impact

## Related Topics

- **[Getting Started](./getting-started.md)** - Installation and basic setup
- **[Configuration](./configuration.md)** - Configuration options and settings
- **[Performance](./performance.md)** - Performance characteristics and optimization
- **[Use Cases](./use-cases.md)** - Common use cases and examples
