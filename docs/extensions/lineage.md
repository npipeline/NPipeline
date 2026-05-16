---
title: "Lineage"
description: "Track data provenance with item-level correlation, sampling, redaction, overflow policies, and terminal outcomes."
order: 5
---

# Lineage

The `NPipeline.Extensions.Lineage` package provides data lineage tracking — recording the path each item takes through a pipeline, including transformations, filtering, dead-lettering, and errors. This is critical for compliance (GDPR, HIPAA), debugging, and data quality auditing.

## Installation

```bash
dotnet add package NPipeline.Extensions.Lineage
```

## Quick Start

```csharp
services.AddNPipeline(builder => { ... });
services.AddNPipelineLineage<LoggingPipelineLineageSink>();
```

Items flowing through the pipeline are automatically wrapped in `LineagePacket<T>` with a correlation ID. At each node, the lineage system records the traversal.

## Architecture

```
Source → wraps items in LineagePacket<T> (assigns CorrelationId)
       → each node appends LineageRecord events
       → terminal event per sampled correlation
       → ILineageCollector (thread-safe ConcurrentDictionary)
       → ILineageSink / IPipelineLineageSink (export)
```

### Core Contracts

| Type | Role |
|------|------|
| `ILineage` | Build-time adapter creation, stream wrapping/unwrapping, pipeline report recording |
| `LineageRecord` | Single traversal event (node ID, outcome, correlation ID, optional snapshots) |
| `ILineageCollector` | Thread-safe storage with per-correlation event tracking |
| `ILineageSink` | Item-level export (called per record) |
| `IPipelineLineageSink` | Pipeline-level export (called once at end of run) |
| `LineagePacket<T>` | Wraps items with `Data`, `CorrelationId`, `TraversalPath`, `LineageRecords`, `Collect` |

> **Runtime stream contract:** When item-level lineage is enabled, streams throughout execution carry `LineagePacket<T>` items, not `T` items directly. The `RuntimePipelineBinder` normalizes execution options (such as route predicates) to operate on `LineagePacket<T>` before execution starts. Sinks receive an `IDataStream<LineagePacket<T>>` input; the lineage adapter unwraps each packet to expose the inner `T` to your `ISinkNode<T>.ConsumeAsync` implementation. This unwrapping is strictly typed — the input stream must be `IDataStream<LineagePacket<T>>` or the build fails.

### LineageRecord Fields

| Field | Description |
|-------|-------------|
| `CorrelationId` | Tracks the item across all nodes |
| `NodeId` | Node that produced this record |
| `PipelineId` / `PipelineName` | Pipeline context |
| `OutcomeReason` | Terminal or intermediate outcome |
| `IsTerminal` | Whether this is the final record for this correlation |
| `TraversalPath` | Ordered list of node IDs visited |
| `ContributorCorrelationIds` | For joins/aggregates — IDs of contributing items |
| `InputSnapshot` / `OutputSnapshot` | JSON snapshots (when hop snapshots enabled) |
| `Data` | Item payload (subject to redaction) |

### Terminal Outcomes

| `LineageOutcomeReason` | Description |
|----------------------|-------------|
| `Emitted` | Item successfully reached a sink |
| `FilteredOut` | Item was removed by a filter node |
| `ConsumedWithoutEmission` | Item consumed by an aggregate or reduction |
| `Error` | Item processing failed |
| `DeadLettered` | Item was sent to a dead letter sink |
| `DroppedByBackpressure` | Item dropped by a parallel queue policy |
| `Joined` | Item was part of a join operation |
| `Aggregated` | Item was part of an aggregation |

## Configuration

### LineageOptions Presets

Two built-in presets cover most use cases:

| Preset | Sampling | Intermediate Records | Snapshots | Best For |
|--------|----------|---------------------|-----------|----------|
| `LineageOptions.FastLineage` | Low | Disabled | Disabled | High-volume production |
| `LineageOptions.CompleteLineage` | 100% | Enabled | Enabled | Debugging, compliance |

Customize with the `.With()` method:

```csharp
var options = LineageOptions.FastLineage.With(sampleEvery: 100, redactData: true);
```

### Sampling

**Deterministic sampling** — hash-based, consistent across runs. Items with `hash(correlationId) % SampleEvery == 0` are tracked.

**Random sampling** — probabilistic, different items tracked each run. Probability is `1/SampleEvery`.

| Scenario | Rate | `SampleEvery` |
|----------|------|---------------|
| Compliance / audit trails | 100% | 1 |
| Production monitoring | 1–10% | 10–100 |
| Development / debugging | 10–50% | 2–10 |
| High-volume analytics | 0.1–1% | 100–1000 |

### Data Redaction

When `redactData: true`, the `LineageRecord.Data` field is set to `null`. Traversal context, outcomes, and correlation metadata are preserved. Use for pipelines handling PII or sensitive data.

### Hop Snapshots

When `captureHopSnapshots: true`, each node records JSON snapshots of the item before and after transformation. Handle circular references silently. **Performance impact is high** — enable at conservative sampling rates (≥ 100).

### Overflow Policies

Controls behavior when the materialization cap is exceeded:

| Policy | Behavior | Use Case |
|--------|----------|----------|
| `Degrade` (default) | Switches to streaming positional mapping | Production — memory-safe |
| `Strict` | Throws immediately | Development — fail-fast |
| `WarnContinue` | Logs warning, continues | Testing — see all events |

```csharp
var options = new LineageOptions(
    sampleEvery: 100,
    deterministicSampling: true,
    redactData: true,
    materializationCap: 10_000,
    overflowPolicy: LineageOverflowPolicy.Degrade);
```

### Completeness Guarantees

| Option | Description |
|--------|-------------|
| `EnsurePerInputTerminalRecord` | Every sampled correlation gets a terminal closure |
| `EmitIntermediateNodeRecords` | Emit non-terminal records at each node |
| `EmitBackpressureDropRecords` | Emit terminal records for items dropped by queue policies |

## Querying Lineage

```csharp
// Get full history for a correlation
var history = collector.GetCorrelationHistory(correlationId);

// Get terminal outcome
var terminal = collector.GetTerminalReason(correlationId);

// Find unresolved correlations (items that entered but never got a terminal event)
var unresolved = collector.GetUnresolvedCorrelations();

// All records (for export)
var allRecords = collector.GetAllRecords();
```

## Pipeline Lineage Sinks

### Built-in

| Sink | Description |
|------|-------------|
| `LoggingPipelineLineageSink` | Logs the lineage report via `ILogger` |

### Custom

Implement `ILineageSink` (per-record) or `IPipelineLineageSink` (per-run):

```csharp
public class DatabaseLineageSink : IPipelineLineageSink
{
    public async Task RecordAsync(PipelineLineageReport report, CancellationToken ct)
    {
        // Persist report to database
    }
}
```

## Dependency Injection

```csharp
// With logging sink
services.AddNPipelineLineage<LoggingPipelineLineageSink>();

// With custom sink
services.AddNPipelineLineage<DatabaseLineageSink>();

// With factory
services.AddNPipelineLineage(sp =>
    new DatabaseLineageSink(sp.GetRequiredService<IDbConnection>()));

// With custom collector and sink
services.AddNPipelineLineage<CustomCollector, DatabaseLineageSink>();
```

### Registered Services

| Service | Implementation | Lifetime |
|---------|---------------|----------|
| `ILineage` | `LineageService` | Scoped |
| `ILineageCollector` | `LineageCollector` | Scoped |
| `IPipelineLineageSink` | User-specified | Scoped |
| `ILineageFactory` | `DiLineageFactory` | Scoped |
| `IPipelineLineageSinkProvider` | `DefaultPipelineLineageSinkProvider` | Scoped |

## Configuration Examples

**Production** — low overhead, privacy-safe:

```csharp
var options = new LineageOptions(
    sampleEvery: 100,
    deterministicSampling: true,
    redactData: true,
    materializationCap: 10_000,
    overflowPolicy: LineageOverflowPolicy.Degrade,
    emitBackpressureDropRecords: true);
```

**Development** — full visibility:

```csharp
var options = new LineageOptions(
    sampleEvery: 1,
    deterministicSampling: true,
    redactData: false,
    emitIntermediateNodeRecords: true,
    ensurePerInputTerminalRecord: true,
    captureHopSnapshots: true,
    overflowPolicy: LineageOverflowPolicy.WarnContinue);
```

## Performance

### Option Impact

| Option | Throughput Impact | Memory Impact |
|--------|-------------------|---------------|
| Lower `SampleEvery` | High | High |
| `RedactData = false` | Medium | High |
| `CaptureHopSnapshots = true` | High | High |
| `EmitIntermediateNodeRecords = true` | Medium | Medium |
| `IncludeContributorCorrelationIds = true` | Low–Medium | Medium |
| `EnsurePerInputTerminalRecord = true` | Low | Low |

### Best Practices

1. **Start with `FastLineage`** in production, customize with `.With()`
2. **Use deterministic sampling** for debugging — same items tracked across runs
3. **Enable redaction** for PII/sensitive data
4. **Use `Degrade` overflow policy** in production — memory-safe
5. **Implement async sinks** — avoid blocking I/O in sink implementations
6. **Keep `EmitBackpressureDropRecords = true`** — queryable drop visibility

## Use Cases

| Use Case | Key Options |
|----------|-------------|
| **Compliance audit trails** | 100% sampling, deterministic, not redacted, ensure terminal records |
| **Correlation debugging** | Query `GetCorrelationHistory(id)` to trace item path |
| **Backpressure visibility** | `EmitBackpressureDropRecords = true` for drop dashboards |
| **Join/aggregate provenance** | `IncludeContributorCorrelationIds = true` |
| **Unresolved correlation monitoring** | `GetUnresolvedCorrelations()` for completeness gaps |
| **Privacy-conscious monitoring** | `RedactData = true`, 1–10% sampling |
| **Node health analysis** | Group records by node ID, count errors/dead-letters per node |

## See Also

- [Data Lineage Guide](../observability/data-lineage.md) — walkthrough with examples
- [Extensions Overview](index.md)
