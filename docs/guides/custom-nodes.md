---
title: "Custom Nodes"
description: "Write your own source, transform, and sink nodes by extending NPipeline base classes."
order: 4
---

# Custom Nodes

> **Prerequisites:** [Defining Pipelines](defining-pipelines.md), [Key Concepts](../getting-started/key-concepts.md)

When lambda nodes aren't enough - you need state, dependency injection, or complex logic - write a custom [node](../reference/glossary.md#node) class. NPipeline provides base classes for each node type.

## Source Nodes

Extend `SourceNode<TOut>` and override `OpenStream` to produce an `IDataStream<TOut>`:

```csharp
public class SensorSource : SourceNode<SensorReading>
{
    public override IDataStream<SensorReading> OpenStream(
        PipelineContext context, CancellationToken cancellationToken)
    {
        return new DataStream<SensorReading>(
            ReadSensorsAsync(cancellationToken), "sensors");
    }

    private static async IAsyncEnumerable<SensorReading> ReadSensorsAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            yield return await PollSensorAsync(ct);
        }
    }
}
```

Return a `DataStream<T>` wrapping an `IAsyncEnumerable<T>` for lazy streaming. Use `InMemoryDataStream<T>` only for small, bounded collections.

## Transform Nodes

Extend `TransformNode<TIn, TOut>` and override `TransformAsync` to map one item at a time:

```csharp
public class EnrichOrder : TransformNode<Order, EnrichedOrder>
{
    private readonly HttpClient _http;

    public EnrichOrder(HttpClient http) => _http = http;

    public override async ValueTask<EnrichedOrder> TransformAsync(
        Order item, PipelineContext context, CancellationToken cancellationToken)
    {
        var details = await _http.GetFromJsonAsync<Details>(
            $"/orders/{item.Id}", cancellationToken);
        return new EnrichedOrder(item, details!);
    }
}
```

### Synchronous Transforms Allocate Nothing

`TransformAsync` returns `ValueTask<T>`, so a transform that completes synchronously costs no allocation per item:

```csharp
public class UpperCase : TransformNode<string, string>
{
    public override ValueTask<string> TransformAsync(
        string item, PipelineContext context, CancellationToken ct)
        => ValueTask.FromResult(item.ToUpperInvariant());
}
```

There is no separate fast path to opt into. See [Synchronous Fast Paths](../performance/synchronous-fast-paths.md).

## Stream Transform Nodes

When you need access to the full [stream](../reference/glossary.md#stream) - for filtering, windowing, or reshaping - implement `IStreamTransformNode<TIn, TOut>`:

```csharp
public class DeduplicateNode : IStreamTransformNode<Order, Order>
{
    public IExecutionStrategy ExecutionStrategy { get; set; } = new SequentialExecutionStrategy();

    public async IAsyncEnumerable<Order> TransformAsync(
        IAsyncEnumerable<Order> items, PipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var seen = new HashSet<int>();
        await foreach (var order in items.WithCancellation(cancellationToken))
        {
            if (seen.Add(order.Id))
                yield return order;
        }
    }
}
```

Register with `AddStreamTransform`:

```csharp
var dedup = builder.AddStreamTransform<DeduplicateNode, Order, Order>("deduplicate");
```

## Sink Nodes

Extend `SinkNode<TIn>` and override `ConsumeAsync` to process the incoming stream:

```csharp
public class DatabaseSink : SinkNode<Order>, IAsyncDisposable
{
    private readonly IDbConnection _connection;

    public DatabaseSink(IDbConnection connection) => _connection = connection;

    public override async Task ConsumeAsync(
        IDataStream<Order> input, PipelineContext context,
        CancellationToken cancellationToken)
    {
        await foreach (var order in input.WithCancellation(cancellationToken))
        {
            await InsertAsync(order, cancellationToken);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is IAsyncDisposable d)
            await d.DisposeAsync().ConfigureAwait(false);
    }
}
```

> ⚠️ **Warning:** You must consume the `input` parameter in `ConsumeAsync`. The `SinkNodeInputConsumptionAnalyzer` (NP9301) will error if you don't.

## Resource Disposal

Nodes are disposable only if they implement `IAsyncDisposable` or `IDisposable`. If your node holds resources like connections, file handles, or HTTP clients, implement `IAsyncDisposable` (or `IDisposable`) on it; the runtime checks for it and disposes the instance at the end of the run that created it. There is no base implementation to call:

```csharp
public sealed class HttpEnricher : TransformNode<Order, Order>, IAsyncDisposable
{
    private readonly HttpClient _client = new();

    public override ValueTask<Order> TransformAsync(
        Order item, PipelineContext context, CancellationToken cancellationToken) => /* ... */;

    public ValueTask DisposeAsync()
    {
        _client.Dispose();
        return ValueTask.CompletedTask;
    }
}
```

Nodes that hold nothing — most nodes — implement neither and are left alone.

## Choosing the Right Base

| Base Class | Use When |
|-----------|----------|
| `SourceNode<TOut>` | Producing data from an external system |
| `TransformNode<TIn, TOut>` | Mapping one item to one output |
| `IStreamTransformNode<TIn, TOut>` | Filtering, deduplicating, or reshaping streams |
| `SinkNode<TIn>` | Writing data to a final destination |
| `LookupNode<TIn, TKey, TValue, TOut>` | Enriching items from a lookup table |
| `CustomMergeNode<TIn>` | Custom merge logic for multiple upstream streams |

## Next Steps

- [Lambda Nodes](lambda-nodes.md) - lightweight alternative for simple transforms
- [Branching and Merging](branching-and-merging.md) - fan-out and merge patterns
- [Joins and Lookups](joins-and-lookups.md) - combining data from multiple streams
