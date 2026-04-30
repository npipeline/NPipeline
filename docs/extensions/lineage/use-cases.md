---
title: Lineage Use Cases
description: Common use cases and practical examples for NPipeline Lineage extension.
order: 3
---

# Lineage Use Cases

This guide shows practical ways to use event-based lineage in real systems.

## 1. Compliance and Audit Trails

Capture complete lineage for regulated workloads.

```csharp
services.AddNPipelineLineage<DatabaseLineageSink>();

builder.EnableItemLevelLineage(options => options.With(
    sampleEvery: 1,
    deterministicSampling: true,
    redactData: false,
    emitIntermediateNodeRecords: true,
    ensurePerInputTerminalRecord: true,
    includeContributorCorrelationIds: true));
```

Why this works:

- every correlation is sampled
- terminal closure is guaranteed
- each record has explicit `OutcomeReason`
- contributor metadata supports many-to-one provenance analysis

## 2. Correlation-Level Debugging

Trace one problematic item end-to-end.

```csharp
public sealed class CorrelationDebugger
{
    private readonly ILineageCollector _collector;

    public CorrelationDebugger(ILineageCollector collector)
    {
        _collector = collector;
    }

    public void Print(Guid correlationId, ILogger logger)
    {
        var history = _collector.GetCorrelationHistory(correlationId);
        var terminal = _collector.GetTerminalReason(correlationId);

        logger.LogInformation("Correlation {CorrelationId} terminal={Terminal}", correlationId, terminal);

        foreach (var record in history)
        {
            logger.LogInformation(
                "{Timestamp} node={NodeId} reason={Reason} terminal={Terminal}",
                record.TimestampUtc,
                record.NodeId,
                record.OutcomeReason,
                record.IsTerminal);
        }
    }
}
```

## 3. Backpressure Visibility

In high-throughput pipelines, dropped items should be explicit and queryable.

```csharp
builder.EnableItemLevelLineage(options => options.With(
    emitBackpressureDropRecords: true,
    sampleEvery: 1));
```

With queue drop policies (`DropNewest`, `DropOldest`), dropped items emit terminal records with:

- `OutcomeReason = DroppedByBackpressure`
- `IsTerminal = true`

This enables accurate drop-rate dashboards and incident forensics.

## 4. Join/Aggregate Provenance

When outputs are derived from multiple inputs, lineage records can include contributor metadata.

```csharp
builder.EnableItemLevelLineage(options => options.With(
    includeContributorCorrelationIds: true,
    captureAncestryMapping: true,
    sampleEvery: 1));
```

Example query:

```csharp
var all = collector.GetAllRecords();

var joined = all
    .Where(r => r.OutcomeReason == LineageOutcomeReason.Joined ||
                r.OutcomeReason == LineageOutcomeReason.Aggregated)
    .Select(r => new
    {
        r.CorrelationId,
        r.NodeId,
        Contributors = r.ContributorCorrelationIds?.Count ?? 0
    })
    .ToList();
```

## 5. Unresolved Correlation Monitoring

Detect lineage completeness gaps during development and testing.

```csharp
var unresolved = collector.GetUnresolvedCorrelations();

if (unresolved.Count > 0)
{
    logger.LogWarning("Found {Count} unresolved correlations", unresolved.Count);
}
```

This is especially useful when adding custom execution strategies or sinks.

## 6. Privacy-Conscious Observability

Track flow without storing sensitive payload data.

```csharp
builder.EnableItemLevelLineage(options => options.With(
    redactData: true,
    sampleEvery: 10,
    deterministicSampling: true));
```

You still retain:

- node traversal context
- explicit outcomes
- terminal status
- contributor/correlation metadata (if enabled)

## 7. Profile-Driven Environment Setup

Use different presets by environment.

```csharp
LineageOptions profile = env.IsDevelopment()
    ? LineageOptions.CompleteLineage
    : LineageOptions.FastLineage;

builder.EnableItemLevelLineage(_ => profile);
```

Then apply targeted overrides where needed:

```csharp
builder.EnableItemLevelLineage(options => options.With(
    sampleEvery: env.IsDevelopment() ? 1 : 250,
    emitBackpressureDropRecords: true));
```

## 8. Node Health and Hotspot Analysis

Use lineage events to identify nodes with elevated error/dead-letter rates.

```csharp
var records = collector.GetAllRecords();

var nodeHealth = records
    .GroupBy(r => r.NodeId)
    .Select(g => new
    {
        NodeId = g.Key,
        Total = g.Count(),
        Errors = g.Count(r => r.OutcomeReason == LineageOutcomeReason.Error),
        DeadLetters = g.Count(r => r.OutcomeReason == LineageOutcomeReason.DeadLettered),
        Filtered = g.Count(r => r.OutcomeReason == LineageOutcomeReason.FilteredOut)
    })
    .OrderByDescending(x => x.Errors + x.DeadLetters)
    .ToList();
```

This is useful for rollout validation after node implementation changes.

## 9. Change Impact and Downstream Analysis

Estimate blast radius before changing a transform.

```csharp
var affectedCorrelations = collector.GetAllRecords()
    .Where(r => r.NodeId == "ValidationNode")
    .Select(r => r.CorrelationId)
    .Distinct()
    .ToHashSet();

var downstreamTouchCount = collector.GetAllRecords()
    .Where(r => affectedCorrelations.Contains(r.CorrelationId))
    .GroupBy(r => r.NodeId)
    .Select(g => new { NodeId = g.Key, EventCount = g.Count() })
    .OrderByDescending(x => x.EventCount)
    .ToList();
```

Because records are correlation-scoped, this analysis works even across join/aggregate boundaries.

## Related Topics

- [Getting Started](./getting-started.md)
- [Configuration](./configuration.md)
- [Architecture](./architecture.md)
- [Performance](./performance.md)
