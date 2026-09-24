---
title: "Pipeline Context"
description: "Share state, parameters, and configuration across nodes during pipeline execution."
order: 10
---

# Pipeline Context

> **Prerequisites:** [Defining Pipelines](defining-pipelines.md)

`PipelineContext` is the shared object that every [node](../reference/glossary.md#node) receives during execution. It carries runtime parameters, shared state, cancellation tokens, and framework services.

## Creating a Context

```csharp
// Default context with no configuration
var context = PipelineContext.CreateDefault();

// Context with parameters
var context = new PipelineContext(
    PipelineContextConfiguration.WithParameters(new Dictionary<string, object>
    {
        ["region"] = "us-east-1",
        ["batchDate"] = DateTime.Today
    }));

// Context with cancellation
var context = new PipelineContext(
    PipelineContextConfiguration.WithCancellation(cancellationToken));
```

Pass the context when running:

```csharp
await runner.RunAsync<MyPipeline>(context, cancellationToken);
```

## Three Dictionaries

PipelineContext exposes three `IDictionary<string, object>` collections with different purposes:

| Dictionary | Purpose | Set By | Read By |
| ----------- | --------- | -------- | --------- |
| `Parameters` | Runtime inputs (file paths, dates, config values) | Caller before execution | Nodes during execution |
| `Items` | Node-to-node shared state | Any node during execution | Any downstream node |
| `Properties` | Extension/plugin storage, and hooks the framework reads | You and your extensions | Extensions and the framework (for published hook keys) |

All three belong to you. The framework keeps its own state on the typed members below and never writes to `Items` or `Properties`. It only reads keys that you set deliberately, such as the decorator hooks on `PipelineContextKeys`.

### Using Parameters

Set parameters before running, read them in nodes:

```csharp
// Set before execution
context.Parameters["inputPath"] = "/data/orders.csv";

// Read in a node
public override IDataStream<Order> OpenStream(
    PipelineContext context, CancellationToken ct)
{
    var path = (string)context.Parameters["inputPath"];
    return new DataStream<Order>(ReadCsvAsync(path, ct), "orders");
}
```

### Sharing State Between Nodes

Use `Items` for node-to-node communication:

```csharp
// In a transform node: store a computed value
public override ValueTask<Order> TransformAsync(
    Order item, PipelineContext context, CancellationToken ct)
{
    var count = context.Items.TryGetValue("orderCount", out var c) ? (int)c : 0;
    context.Items["orderCount"] = count + 1;
    return ValueTask.FromResult(item);
}

// In a later sink: read the value
public override async Task ConsumeAsync(
    IDataStream<Order> input, PipelineContext context, CancellationToken ct)
{
    await foreach (var order in input.WithCancellation(ct)) { /* ... */ }
    var total = (int)context.Items["orderCount"];
    Console.WriteLine($"Processed {total} orders");
}
```

> **Thread Safety:** In the `Default` [optimization profile](optimization-profiles.md), `Parameters`, `Items`, and `Properties` are backed by `ConcurrentDictionary` and support concurrent reads and writes. In `HighThroughput` mode, they are plain `Dictionary` instances with zero locking overhead but no thread safety. For complex shared state in parallel execution, use [`IPipelineStateManager`](parallel-execution.md#ipipelinestatemanager).

## Accessing Framework Services

Framework services are grouped into five sub-contexts, each covering one concern. Reach a service through the
sub-context that owns it:

| Sub-context | Holds | Examples |
| ------------- | ------- | ---------- |
| `RunIdentity` | Who this run is | `PipelineId`, `RunId`, `PipelineName`, `PipelineStartTimeUtc` |
| `Observability` | Logging, tracing, metrics | `LoggerFactory`, `Tracer`, `ExecutionObserver`, `ObservabilityFactory` |
| `ExecutionConfiguration` | Resilience and run settings | `Resilience`, `GetResilienceOptions(nodeId)`, `ResiliencePolicy`, `OptimizationProfile`, `IsParallelExecution` |
| `NodeEnvironment` | Per-node execution state | `GetNodeId(node)`, `TryGetNodeId(node, out id)`, `GetNodeStatus(nodeId)`, `EnumerateNodeStatuses()`, `NodeExecutionScopeRegistry`, `DiOwnedNodes` |
| `Lineage` | Lineage sinks and collectors | `LineageSink`, `PipelineLineageSink`, `LineageCollector`, `LineageFactory` |

```csharp
public override ValueTask<Order> TransformAsync(
    Order item, PipelineContext context, CancellationToken ct)
{
    var logger = context.Observability.LoggerFactory.CreateLogger("OrderTransform");
    logger.LogDebug("Run {RunId} processing order {OrderId}", context.RunIdentity.RunId, item.Id);
    return ValueTask.FromResult(item);
}
```

### Get the node ID

To key state, name an activity, or build an error message, a node can retrieve its ID from the node environment:

```csharp
public override ValueTask<Order> TransformAsync(
    Order item, PipelineContext context, CancellationToken ct)
{
    var nodeId = context.NodeEnvironment.GetNodeId(this);
    ...
}
```

The ID is resolved from the instance, so the answer is exact no matter how many nodes are running at once. It costs a
dictionary lookup only when a node asks.

`GetNodeId` throws an exception if the instance is not part of the run. This occurs if a node is tested in isolation or if a single instance is mapped to multiple positions in the graph. Use `TryGetNodeId` where that is a legitimate case, and `RegisterNode(nodeId, node)` to give a node an ID when
driving it outside a pipeline:

```csharp
var node = new MyTransform();
context.NodeEnvironment.RegisterNode("my-node", node);
```

### Node status

The run records how each node finished, eliminating the need to check flags in a dictionary:

```csharp
if (context.NodeEnvironment.GetNodeStatus("enrich") == NodeExecutionStatus.Failed)
{
    // ...
}
```

Nodes that have not finished report `NodeExecutionStatus.Pending` and are absent from `EnumerateNodeStatuses()`.

A few members sit directly on the context because they belong to no single concern:

| Member | Type | Description |
| -------- | ------ | ------------- |
| `CancellationToken` | `CancellationToken` | Pipeline-wide cancellation |
| `Parameters`, `Items`, `Properties` | `IDictionary<string, object>` | The three dictionaries above |
| `DeadLetterSink` | `IDeadLetterSink?` | For routing failed items |
| `ErrorHandlerFactory` | `IErrorHandlerFactory` | Creates error handlers |
| `StateManager`, `StatefulRegistry` | `IPipelineStateManager?`, `IStatefulRegistry?` | Stateful execution services. Assign them here, or supply one for every run with the `ExecutionAnnotationKeys.GlobalStateManager` / `GlobalStatefulRegistry` builder annotation. |

### Stateful node registration

When `StatefulRegistry` is configured, NPipeline registers each node that implements `IStatefulNode` during pipeline setup:

```csharp
public sealed class RunningTotalNode : TransformNode<int, int>, IStatefulNode
{
    private int _total;

    public override ValueTask<int> TransformAsync(
        int item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        _total += item;
        return ValueTask.FromResult(_total);
    }
}
```

Implement the marker explicitly. NPipeline doesn't infer stateful behavior from an interface or type name. If the registry rejects a node or otherwise fails during registration, pipeline setup fails with that exception.

> **Note:** Earlier versions also exposed every one of these as a flat property on `PipelineContext` itself, so
> `context.LoggerFactory` and `context.Observability.LoggerFactory` both worked. The flat forwarders are gone: there is
> now exactly one way to reach each value.

## Configuring the Context

`PipelineContextConfiguration` provides factory methods for common setups:

```csharp
// Combine multiple configurations
var config = new PipelineContextConfiguration(
    Parameters: new Dictionary<string, object> { ["key"] = "value" },
    LoggerFactory: loggerFactory,
    CancellationToken: cancellationToken);

var context = new PipelineContext(config);
```

Available factory methods:

| Method | Purpose |
| -------- | --------- |
| `WithParameters(dict)` | Set runtime parameters |
| `WithCancellation(token)` | Set cancellation token |
| `WithLogging(loggerFactory)` | Configure logging |
| `WithResilience(policy)` | Set resilience policy |
| `WithErrorHandling(deadLetterSink?)` | Configure error handling |
| `WithObservability(loggerFactory?, tracer?)` | Configure observability |

## Next Steps

- [Dependency Injection](dependency-injection.md) - context setup via DI
- [Parallel Execution](parallel-execution.md) - thread safety for shared state
- [Pipeline Composition](pipeline-composition.md) - how context flows to sub-pipelines
