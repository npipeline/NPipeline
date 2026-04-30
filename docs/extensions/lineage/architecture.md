---
title: Lineage Architecture
description: Internal architecture and design decisions of NPipeline Lineage extension.
order: 5
---

# Lineage Architecture

This guide describes how lineage is captured, stored, and exported in the current event-based model.

## System Architecture

```text
┌──────────────────────────────────────────────┐
│              Pipeline Execution              │
└─────────────────┬────────────────────────────┘
                  │
                  ├─ Source wraps data in LineagePacket<T>
                  │    - CorrelationId
                  │    - TraversalPath
                  │    - Existing LineageRecord events
                  │
                  ├─ Mapping strategies append LineageRecord events
                  │    - emitted, filtered, error, dead-letter, joined, aggregated
                  │    - contributor metadata for many-to-one paths
                  │
                  └─ Terminal event emitted per sampled correlation
                               │
                               ▼
                    ┌───────────────────────┐
                    │    ILineageCollector  │
                    │   (thread-safe store) │
                    └──────────┬────────────┘
                               │
                               ├─ GetCorrelationHistory(Guid)
                               ├─ GetTerminalReason(Guid)
                               ├─ GetAllRecords()
                               └─ GetUnresolvedCorrelations()
                               │
                               ▼
                    ┌───────────────────────┐
                    │      ILineageSink     │
                    │  RecordAsync(record)  │
                    └───────────────────────┘
```

## Core Contracts

### `LineageRecord`

`LineageRecord` is the canonical lineage event shape. Each record represents one correlation-scoped event at one node.

Key fields include:

- `CorrelationId`
- `NodeId`
- `PipelineId` / `PipelineName`
- `OutcomeReason` (`Emitted`, `FilteredOut`, `ConsumedWithoutEmission`, `Error`, `DeadLettered`, `DroppedByBackpressure`, `Joined`, `Aggregated`)
- `IsTerminal`
- `TraversalPath`
- Optional contributor metadata (`ContributorCorrelationIds`, `ContributorInputIndices`)
- Optional snapshots (`InputSnapshot`, `OutputSnapshot`)
- Optional `Data` (subject to redaction)

### `ILineageCollector`

Collector methods are event-centric:

```csharp
IReadOnlyList<LineageRecord> GetCorrelationHistory(Guid correlationId);
LineageOutcomeReason? GetTerminalReason(Guid correlationId);
IReadOnlyList<LineageRecord> GetAllRecords();
IReadOnlyList<Guid> GetUnresolvedCorrelations();
```

### `ILineageSink`

Item-level sinks receive single events:

```csharp
public interface ILineageSink
{
    Task RecordAsync(LineageRecord record, CancellationToken cancellationToken);
}
```

### `LineagePacket<T>`

`LineagePacket<T>` carries user payload and lineage context through execution.

- `Data`
- `CorrelationId`
- `TraversalPath`
- `LineageRecords`
- `Collect` (sampling decision)

## Event Lifecycle

1. Source creates a packet with a new `CorrelationId`.
2. Mapping strategies append records as nodes process inputs/outputs.
3. Join/aggregate outputs retain contributor lineage through contributor metadata and inherited traversal context.
4. Sink unwrap path emits final events and enforces terminal completeness when enabled.
5. Collector persists events for query and sink export.

## Completeness Guarantees

`LineageOptions` controls completeness behavior:

- `EnsurePerInputTerminalRecord = true` ensures each sampled correlation gets terminal closure.
- `EmitIntermediateNodeRecords = true` emits non-terminal intermediate events for richer traces.
- `EmitBackpressureDropRecords = true` emits explicit terminal records for dropped items.

This removes ambiguity between "no more events yet" and "correlation is complete."

## Mapping Strategy Behavior

Lineage mapping strategies (`StreamingOneToOne`, `Materializing`, `CapAwareMaterializing`, `PositionalStreaming`) decide how output records inherit lineage from inputs.

When lineage mapping is ambiguous (many-to-one / one-to-many), records include contributor metadata and observed cardinality so downstream analysis can reason about provenance without requiring strict one-to-one assumptions.

## Overflow Handling

`MaterializationCap` and `OverflowPolicy` define behavior when materialization limits are reached:

- `Degrade`: fallback to streaming positional mapping and continue.
- `Strict`: throw immediately.
- `WarnContinue`: warn and continue.

The default (`Degrade`) is production-friendly because it preserves execution while reducing memory risk.

## Concurrency Model

Collector state is safe for concurrent writes from parallel node execution:

- correlation-indexed storage
- per-correlation event append ordering
- lock-minimized, thread-safe updates

Parallel queue drops can still be audited through `DroppedByBackpressure` terminal events (when enabled).

## Related Topics

- [Getting Started](./getting-started.md)
- [Configuration](./configuration.md)
- [Performance](./performance.md)
- [Use Cases](./use-cases.md)
