---
title: "Data Lineage"
description: "Track data provenance through pipelines with per-item correlation and pipeline-level reports."
order: 4
---

# Data Lineage

> **Prerequisites:** [Defining Pipelines](../guides/defining-pipelines.md), [Dependency Injection](../guides/dependency-injection.md)

The `NPipeline.Extensions.Lineage` package tracks how data flows through your pipeline - which node produced each [item](../reference/glossary.md#item), what transforms it passed through, and where it ended up.

## Installation

```bash
dotnet add package NPipeline.Extensions.Lineage
```

## Setup

```csharp
services.AddNPipeline(Assembly.GetExecutingAssembly());
services.AddNPipelineLineage(); // logs lineage reports via ILogger
```

Enable item-level lineage in the pipeline definition:

```csharp
public void Define(PipelineBuilder builder, PipelineContext context)
{
    builder.EnableItemLevelLineage();
    // ... add nodes and connections
}
```

## What Gets Tracked

### Pipeline-Level Lineage

After each run, a `PipelineLineageReport` is generated containing:

- Pipeline name, ID, and run ID
- All nodes with type information (`NodeLineageInfo`: ID, type name, input/output types)
- All edges between nodes (`EdgeLineageInfo`: source → target)
- The complete DAG structure

### Item-Level Lineage

When enabled with `EnableItemLevelLineage()`, each item gets a unique correlation ID:

- **Source record** - which node produced the item
- **Transform records** - each transformation the item passed through
- **Sink/terminal record** - where the item was consumed
- **Error records** - if the item failed processing

## Configuration

`LineageOptions` controls all lineage behavior. Two presets are available:

```csharp
// Fast: throughput-oriented, reduced detail (default)
builder.EnableItemLevelLineage(LineageOptions.FastLineage);

// Complete: full detail for compliance and debugging
builder.EnableItemLevelLineage(LineageOptions.CompleteLineage);
```

Or customize individual settings:

```csharp
builder.EnableItemLevelLineage(options => options with
{
    SampleEvery = 100,
    DeterministicSampling = true,
    RedactData = true,
    CaptureHopSnapshots = false,
    OverflowPolicy = LineageOverflowPolicy.Degrade,
    MaterializationCap = 10000
});
```

### Sampling

| Setting | Default | Description |
|---------|---------|-------------|
| `SampleEvery` | `100` | Track lineage for every Nth item (1 = all items) |
| `DeterministicSampling` | `true` | Hash-based (reproducible) vs random (representative) |

**Sampling rate guidelines:**

| Scenario | Recommended Rate |
|----------|-----------------|
| Compliance / audit | `SampleEvery = 1` (100%) |
| Production monitoring | `SampleEvery = 10–100` (1–10%) |
| Development / debugging | `SampleEvery = 2–10` (10–50%) |
| High-volume analytics | `SampleEvery = 100–1000` (0.1–1%) |

### Data Capture Options

| Setting | Default | Description |
|---------|---------|-------------|
| `CaptureHopTimestamps` | `true` | Record timestamps at each node |
| `CaptureDecisions` | `true` | Record resilience decisions (skip, retry, dead-letter) |
| `CaptureObservedCardinality` | `true` | Record input/output counts per node |
| `CaptureAncestryMapping` | `false` | Track full ancestry chain |
| `CaptureHopSnapshots` | `false` | Serialize item before/after each node (expensive) |
| `MaxHopRecordsPerItem` | `256` | Maximum lineage records per correlation ID |
| `RedactData` | `true` | Set `lineageRecord.Data` to null (useful for PII) |

> ⚠️ **Warning:** `CaptureHopSnapshots` serializes items at every node hop. Use with `SampleEvery ≥ 100` to limit overhead.

### Overflow Policies

When the `MaterializationCap` is reached:

| Policy | Behavior | Use When |
|--------|----------|----------|
| `Degrade` (default) | Switches to streaming positional mapping | Production - graceful degradation |
| `Strict` | Throws immediately | Memory limits are critical |
| `WarnContinue` | Logs a warning and continues | Development / debugging |

### Emission Options

| Setting | Default | Description |
|---------|---------|-------------|
| `EnsurePerInputTerminalRecord` | `true` | Guarantee a terminal record per input |
| `EmitBackpressureDropRecords` | `true` | Emit records when items are dropped by backpressure |
| `IncludeContributorCorrelationIds` | `true` | Include contributor IDs in join/aggregate records |
| `EmitIntermediateNodeRecords` | `true` | Emit records for intermediate (non-terminal) nodes |

## Custom Lineage Sinks

Replace the default logging sink:

```csharp
services.AddNPipelineLineage<DatabaseLineageSink>();
```

Implement `IPipelineLineageSink` to store lineage reports:

```csharp
public class DatabaseLineageSink : IPipelineLineageSink
{
    public Task WriteAsync(PipelineLineageReport report, CancellationToken ct)
    {
        // Store report in your lineage database
    }
}
```

## Querying Lineage

Access the `LineageCollector` to query lineage data during or after execution:

```csharp
var collector = context.LineageCollector;
var history = collector.GetCorrelationHistory(correlationId);
var unresolved = collector.GetUnresolvedCorrelations();
var allRecords = collector.GetAllRecords();
```

## Performance Tuning

Lineage adds per-item overhead proportional to the options enabled. To minimize impact:

1. Increase `SampleEvery` - every 100th item is usually sufficient for monitoring
2. Disable `CaptureHopSnapshots` - serialization is the most expensive operation
3. Set `RedactData = true` - avoids storing large payloads
4. Use `LineageOptions.FastLineage` as a starting point
5. Set a `MaterializationCap` to bound memory usage

## Next Steps

- [Lineage Extension Reference](../extensions/lineage.md) - collector, service, sampling, and sink details
- [Metrics and Monitoring](metrics-and-monitoring.md) - operational metrics
- [OpenTelemetry](opentelemetry.md) - distributed tracing
- [Pipeline Context](../guides/pipeline-context.md) - accessing lineage in nodes
