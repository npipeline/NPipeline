---
title: "Cancellation"
description: "How cooperative cancellation propagates through the pipeline execution system."
order: 7
---

# Cancellation

NPipeline uses .NET's cooperative cancellation model. A `CancellationToken` enters at the runner and propagates through every node, stream, and execution strategy. This page explains the propagation path and what contributors need to know.

## Propagation Path

```mermaid
flowchart TD
    A["PipelineRunner.RunAsync(ct)"] --> B[PipelineContext]
    B --> C[PipelineExecutionOrchestrator]
    C --> D["ISourceNode.OpenStream(context, ct)"]
    C --> E["IExecutionStrategy.ExecuteAsync(..., ct)"]
    C --> F["ISinkNode.ConsumeAsync(input, context, ct)"]
    E --> G["ITransformNode.TransformAsync(item, context, ct)"]
    D --> H["IDataStream<T> enumeration"]
    G --> H
```

### Entry

The token enters through `PipelineRunner.RunAsync()`:

```csharp
await runner.RunAsync<MyPipeline>(context, cancellationToken);
```

If no token is provided, `CancellationToken.None` is used. A context can also carry its own token, set with `PipelineContextConfiguration.WithCancellation(token)`. Cancelling either token stops the run: for the duration of the run, `context.CancellationToken` is linked to both, so nodes that read the context's token observe the runner's token too. When the run ends, `context.CancellationToken` is the context's own token again.

### Distribution

The orchestrator passes the token to every node method:

| Node Type | Method Signature |
|-----------|-----------------|
| Source | `OpenStream(PipelineContext context, CancellationToken cancellationToken)` |
| Transform | `TransformAsync(TIn item, PipelineContext context, CancellationToken cancellationToken)` |
| StreamTransform | `TransformAsync(IAsyncEnumerable<TIn> items, PipelineContext context, CancellationToken cancellationToken)` |
| Sink | `ConsumeAsync(IDataStream<TIn> input, PipelineContext context, CancellationToken cancellationToken)` |

Execution strategies also receive the token:

```csharp
IExecutionStrategy.ExecuteAsync<TIn, TOut>(
    IDataStream<TIn> input,
    ITransformNode<TIn, TOut> node,
    PipelineContext context,
    CancellationToken cancellationToken)
```

## Rules for Contributors

### 1. Always Pass the Token

Every async method that accepts a `CancellationToken` must forward it to child operations:

```csharp
// ✓ Correct: token forwarded
public async ValueTask<Order> TransformAsync(
    RawOrder item, PipelineContext context, CancellationToken ct)
{
    var enriched = await _httpClient.GetAsync(item.Url, ct);
    return Map(item, enriched);
}

// ✗ Wrong: token dropped
public async ValueTask<Order> TransformAsync(
    RawOrder item, PipelineContext context, CancellationToken ct)
{
    var enriched = await _httpClient.GetAsync(item.Url); // missing ct!
    return Map(item, enriched);
}
```

The `CancellationTokenRespectAnalyzer` warns about dropped tokens at build time.

### 2. Use WithCancellation on Async Enumerables

When consuming an `IAsyncEnumerable<T>`, always attach the token:

```csharp
await foreach (var item in input.WithCancellation(ct))
{
    // process item
}
```

Without `.WithCancellation()`, the enumeration ignores cancellation requests and continues until the source is exhausted.

### 3. Check Cancellation in Long Loops

For CPU-bound transforms that process items in a tight loop, periodically check the token:

```csharp
public async ValueTask<Batch<T>> TransformAsync(
    Batch<T> batch, PipelineContext context, CancellationToken ct)
{
    var results = new List<T>(batch.Items.Count);
    foreach (var item in batch.Items)
    {
        ct.ThrowIfCancellationRequested();
        results.Add(Process(item));
    }
    return new Batch<T>(results);
}
```

### 4. Handle OperationCanceledException

The orchestrator catches `OperationCanceledException` at the pipeline level. Individual nodes should **not** catch and suppress cancellation exceptions unless they have specific cleanup logic:

```csharp
// ✓ Correct: let cancellation propagate
public async Task ConsumeAsync(
    IDataStream<Order> input, PipelineContext context, CancellationToken ct)
{
    await foreach (var order in input.WithCancellation(ct))
    {
        await _db.InsertAsync(order, ct);
    }
}

// ✗ Wrong: swallowing cancellation
try
{
    await foreach (var order in input.WithCancellation(ct))
    {
        await _db.InsertAsync(order, ct);
    }
}
catch (OperationCanceledException)
{
    // Silently ignoring cancellation prevents pipeline shutdown
}
```

The `OperationCanceledExceptionAnalyzer` detects swallowed cancellation exceptions.

### 5. Cancellation in Execution Strategies

If you're implementing a custom `IExecutionStrategy`, check cancellation before processing each item and between retry attempts:

```csharp
public async Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(
    IDataStream<TIn> input,
    ITransformNode<TIn, TOut> node,
    PipelineContext context,
    CancellationToken ct)
{
    async IAsyncEnumerable<TOut> Transform(
        [EnumeratorCancellation] CancellationToken token = default)
    {
        await foreach (var item in input.WithCancellation(token))
        {
            token.ThrowIfCancellationRequested();
            yield return await node.TransformAsync(item, context, token);
        }
    }

    return new DataStream<TOut>(Transform(ct), "my-strategy-output");
}
```

## Cancellation and Resilience

Cancelling the pipeline's token is never a failure. None of the resilience layers retries, restarts, skips, or dead-letters work because of it:

- **Item retry (L1):** An `OperationCanceledException` raised while the pipeline's token is cancelled propagates at once. It doesn't count against the circuit breaker, and `RetryClassifier` never classifies it as transient.
- **Node restart (L2):** The restart strategy checks the token before each run of the stream and before each item. A cancellation throws `OperationCanceledException` and never uses a restart, so a cancelled run can't report success with a truncated result set.
- **Node retry (L3):** A cancellation propagates, and the node doesn't execute again.

Every retry and restart delay waits on `PipelineResilienceOptions.Time` with the pipeline's token:

```csharp
await Task.Delay(delay, options.Time, cancellationToken).ConfigureAwait(false);
```

If you cancel during a delay, the wait ends immediately with `OperationCanceledException`. It doesn't wait for the delay to finish. A circuit breaker configured with `BreakerOpenBehavior.Pause` waits the same way, so cancellation also ends a pause at once. For more information, see [The three resilience layers](../error-handling/three-layers.md).

## Next Steps

- [Adding a Node Type](../contributing/adding-a-node-type.md) - implement nodes that properly handle cancellation
- [Execution Model](execution-model.md) - how the orchestrator coordinates cancellation
- [Coding Conventions](../contributing/coding-conventions.md) - analyzer rules that enforce cancellation patterns
