---
title: "Materialization"
description: "Buffer streaming inputs so nodes can restart from the beginning after a failure."
order: 7
---

# Materialization

NPipeline streams data item-by-item through `IAsyncEnumerable<T>`. Once an item has been consumed from a forward-only stream, it's gone - there's no way to "rewind." This creates a problem for node restart: if a node fails mid-stream, how do you replay the items it already processed?

**Materialization** solves this by buffering consumed items in memory so the stream can be replayed from the beginning after a failure.

## Why It's Needed

When a resilience policy returns `RestartNode`, the runtime re-executes the entire node from scratch. Without materialization:

- Items already consumed from the input stream are lost.
- The restart would begin with an empty or partially-consumed stream.
- Data loss would occur silently.

With materialization:

- Items are buffered as they're consumed (up to a configurable cap).
- On restart, the buffered items are replayed to the restarted node.
- No data loss for items within the buffer.

## Configuring Materialization

Set `MaxMaterializedItems` in `PipelineRetryOptions`:

```csharp
builder.WithRetryOptions(options => options with
{
    MaxMaterializedItems = 1000,   // Buffer up to 1000 items
    MaxNodeRestartAttempts = 3     // Allow up to 3 restarts
});
```

## When Materialization Activates

Materialization only occurs when **all three conditions** are met:

1. The node has `.WithResilience(builder)` applied.
2. `MaxNodeRestartAttempts` is greater than 0.
3. `MaxMaterializedItems` is set to a positive number.

If any condition is missing, the `ResilientExecutionStrategy` throws an `InvalidOperationException` at runtime with a diagnostic message explaining what's missing.

> **Tip:** The NPipeline analyzers also check these conditions at build time and produce warnings if a resilience-enabled node is missing materialization configuration.

## The Performance Trade-Off

Materialization trades memory for resilience:

| Without Materialization | With Materialization |
|------------------------|---------------------|
| Zero memory overhead | Buffers up to N items in memory |
| No restart capability | Full restart from buffered items |
| Item-level retry only | Item retry + node restart |
| Streaming remains fully lazy | Initial segment is eagerly buffered |

### Memory Estimation

Each materialized item occupies its object size in memory. For a 1KB record with `MaxMaterializedItems = 1000`, expect ~1MB of buffer per node. Plan accordingly for:

- Large records (images, documents): use a smaller cap.
- Small records (IDs, metrics): a larger cap is safe.
- Multiple resilient nodes: each node has its own buffer.

## How It Works Internally

1. The `ResilientExecutionStrategy` detects that the input is a forward-only stream (`IForwardOnlyDataStream`).
2. It wraps the input in a `CappedReplayableDataStream<T>` that buffers items as they're consumed.
3. If the cap is set, the stream eagerly pre-buffers items to enforce the limit even during successful execution.
4. On node restart, the replayable stream rewinds to the beginning, re-enumerating from the buffer.
5. If the buffer is exhausted (more items than `MaxMaterializedItems`), the cap enforcement throws an exception.

## What Happens When the Cap Is Exceeded

If the input stream contains more items than `MaxMaterializedItems`, the capped stream prevents further buffering. This means:

- Items beyond the cap **cannot** be replayed on restart.
- The pipeline will fail with a clear error if a restart is needed but the buffer is insufficient.

Choose your cap based on:

- Expected batch sizes
- Available memory
- Acceptable data loss window (items beyond the cap cannot be replayed)

## In-Memory Streams Don't Need Materialization

If your input is already in-memory (e.g., `InMemoryDataStream<T>` from a list), it's inherently replayable. Materialization only applies to forward-only async streams. The runtime detects this automatically.

## Configuration Patterns

### Conservative (low memory, limited restart)

```csharp
builder.WithRetryOptions(options => options with
{
    MaxMaterializedItems = 100,
    MaxNodeRestartAttempts = 1
});
```

### Balanced (typical production)

```csharp
builder.WithRetryOptions(options => options with
{
    MaxMaterializedItems = 1000,
    MaxNodeRestartAttempts = 3
});
```

### Aggressive (high tolerance, more memory)

```csharp
builder.WithRetryOptions(options => options with
{
    MaxMaterializedItems = 10000,
    MaxNodeRestartAttempts = 5
});
```

## Complete Example

```csharp
public class StreamingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Enable materialization for node restart support
        builder.WithRetryOptions(options => options with
        {
            MaxItemRetries = 3,
            MaxMaterializedItems = 2000,
            MaxNodeRestartAttempts = 2
        }.WithExponentialBackoffAndFullJitter());

        // Policy that can return RestartNode for severe failures
        builder.AddResiliencePolicy(new RestartOnConnectionLoss());
        builder.AddDeadLetterSink(new BoundedInMemoryDeadLetterSink());

        var source = builder.AddSource<KafkaSource, Event>("events");
        var transform = builder.AddTransform<EnrichEvent, Event, EnrichedEvent>("enrich");
        var sink = builder.AddSink<DatabaseSink, EnrichedEvent>("store");

        // Enable resilience (this activates materialization for streaming input)
        transform.WithResilience(builder);

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

public class RestartOnConnectionLoss : ResiliencePolicyBase
{
    public override Task<ResilienceDecision> DecideNodeFailureAsync(
        NodeDefinition nodeDefinition,
        INode node,
        Exception exception,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Restart the node if the connection dropped
        if (exception is System.Data.Common.DbException)
            return Task.FromResult(ResilienceDecision.RestartNode);

        return Task.FromResult(ResilienceDecision.Fail);
    }
}
```

## Prerequisites Validation

The runtime validates at execution time that resilient nodes have proper materialization configured. If validation fails, you'll see errors like:

```
Node 'enrich' has streaming inputs but MaxMaterializedItems is null (must be > 0).
Restart functionality is disabled for streaming inputs.
Configure: builder.WithRetryOptions(o => o.WithMaxMaterializedItems(1000))
```

```
Node 'enrich' is using ResilientExecutionStrategy but MaxNodeRestartAttempts is 0 (must be > 0).
Restart functionality is disabled.
Configure: builder.WithRetryOptions(o => o.WithMaxNodeRestartAttempts(3))
```

## Next Steps

- [Resilience Policies](resilience-policies.md) - configure policies that return `RestartNode`
- [Circuit Breakers](circuit-breakers.md) - stop restart attempts when failures are persistent
- [Retry Strategies](retry-strategies.md) - control timing between restarts
