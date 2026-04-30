---
title: Lineage Performance
description: Performance characteristics, benchmarks, and optimization strategies for NPipeline Lineage extension.
order: 4
---

# Lineage Performance

This guide explains where lineage overhead comes from and how to tune it for throughput or completeness.

## What Drives Overhead

Lineage overhead is primarily driven by:

- sampling rate (`SampleEvery`)
- event volume (`EmitIntermediateNodeRecords`, terminal guarantees)
- payload retention (`RedactData`)
- snapshots (`CaptureHopSnapshots`)
- sink implementation latency (`ILineageSink.RecordAsync`)

In practice, lineage cost scales with:

- number of sampled correlations
- number of nodes traversed by sampled correlations
- size/shape of payload data retained in records

## Recommended Profiles

Use profile presets first, then tune:

```csharp
// Throughput-oriented defaults
var fast = LineageOptions.FastLineage;

// Completeness-oriented defaults
var complete = LineageOptions.CompleteLineage;
```

### FastLineage

Best for high-volume production workloads where audit completeness is not required for every node event.

Defaults include lower detail and reduced event count.

### CompleteLineage

Best for debugging, data quality investigations, and strict traceability workflows.

Defaults include rich event coverage, contributor metadata, snapshots, and per-input terminal closure.

## Tuning Patterns

### 1. Throughput First

```csharp
builder.EnableItemLevelLineage(options => options.With(
    sampleEvery: 500,
    deterministicSampling: true,
    redactData: true,
    emitIntermediateNodeRecords: false,
    includeContributorCorrelationIds: false,
    emitBackpressureDropRecords: true,
    captureHopSnapshots: false));
```

### 2. Completeness First

```csharp
builder.EnableItemLevelLineage(options => options.With(
    sampleEvery: 1,
    deterministicSampling: true,
    redactData: false,
    emitIntermediateNodeRecords: true,
    includeContributorCorrelationIds: true,
    ensurePerInputTerminalRecord: true,
    captureHopSnapshots: true));
```

### 3. Memory Guardrails

```csharp
builder.EnableItemLevelLineage(options => options.With(
    materializationCap: 10_000,
    overflowPolicy: LineageOverflowPolicy.Degrade,
    maxHopRecordsPerItem: 256));
```

## Option Impact Matrix

| Option | Throughput Impact | Memory Impact | Notes |
| --- | --- | --- | --- |
| `SampleEvery` lower value | High | High | `1` means capture all sampled events |
| `RedactData = false` | Medium | High | Stores payload in records |
| `CaptureHopSnapshots = true` | High | High | JSON snapshot per sampled hop |
| `EmitIntermediateNodeRecords = true` | Medium | Medium | More events per correlation |
| `IncludeContributorCorrelationIds = true` | Low-Medium | Medium | Useful for many-to-one provenance |
| `EnsurePerInputTerminalRecord = true` | Low | Low | Strong completeness guarantee |

## Sink Performance

A slow sink can dominate lineage overhead. Prefer non-blocking sink implementations:

- buffer writes in memory/channel
- batch persistence to storage
- avoid synchronous I/O
- apply backpressure or bounded queues in sink internals

Example:

```csharp
public sealed class BatchedLineageSink : ILineageSink
{
    public Task RecordAsync(LineageRecord record, CancellationToken cancellationToken)
    {
        // Enqueue record to an internal bounded channel for background batch flush.
        return Task.CompletedTask;
    }
}
```

## Measuring in Your Environment

Use A/B comparisons with representative data:

1. Baseline run without lineage.
2. Run with `FastLineage`.
3. Run with `CompleteLineage`.
4. Tune one option at a time and re-measure.

Track at least:

- throughput (items/sec)
- end-to-end latency
- memory (working set)
- lineage event count (`collector.GetAllRecords().Count`)
- unresolved count (`collector.GetUnresolvedCorrelations().Count`)

### Benchmark Template

Use BenchmarkDotNet for repeatable A/B checks:

```csharp
[MemoryDiagnoser]
public class LineageBenchmarks
{
    [Benchmark(Baseline = true)]
    public Task Baseline()
    {
        return RunPipeline(lineageEnabled: false);
    }

    [Benchmark]
    public Task FastProfile()
    {
        return RunPipeline(lineageOptions: LineageOptions.FastLineage);
    }

    [Benchmark]
    public Task CompleteProfile()
    {
        return RunPipeline(lineageOptions: LineageOptions.CompleteLineage);
    }
}
```

## Practical Defaults

For most production pipelines:

- start from `FastLineage`
- keep `EmitBackpressureDropRecords = true`
- use deterministic sampling
- enable richer options only where diagnostics require them

## Related Topics

- [Getting Started](./getting-started.md)
- [Configuration](./configuration.md)
- [Architecture](./architecture.md)
- [Use Cases](./use-cases.md)
