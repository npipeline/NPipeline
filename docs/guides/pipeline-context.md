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
var context = PipelineContext.Default;

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

PipelineContext exposes three `Dictionary<string, object>` collections with different purposes:

| Dictionary | Purpose | Set By | Read By |
|-----------|---------|--------|---------|
| `Parameters` | Runtime inputs (file paths, dates, config values) | Caller before execution | Nodes during execution |
| `Items` | Node-to-node shared state | Any node during execution | Any downstream node |
| `Properties` | Extension/plugin storage | Extensions and framework | Extensions and framework |

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
public override Task<Order> TransformAsync(
    Order item, PipelineContext context, CancellationToken ct)
{
    var count = context.Items.TryGetValue("orderCount", out var c) ? (int)c : 0;
    context.Items["orderCount"] = count + 1;
    return Task.FromResult(item);
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

> ⚠️ **Warning:** `Parameters`, `Items`, and `Properties` are **not thread-safe**. For parallel execution, use concurrent collections or avoid shared state. See [Parallel Execution](parallel-execution.md).

## Accessing Framework Services

PipelineContext also exposes framework services for observability, error handling, and lineage:

| Property | Type | Description |
|----------|------|-------------|
| `CancellationToken` | `CancellationToken` | Pipeline-wide cancellation |
| `PipelineId` | `Guid` | Unique ID for this pipeline definition |
| `RunId` | `Guid` | Unique ID for this execution run |
| `PipelineName` | `string?` | Human-readable name |
| `PipelineStartTimeUtc` | `DateTime` | When execution started |
| `LoggerFactory` | `ILoggerFactory` | For creating loggers in nodes |
| `DeadLetterSink` | `IDeadLetterSink?` | For routing failed items |
| `GlobalRetryOptions` | `PipelineRetryOptions` | Pipeline-wide retry configuration |

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
|--------|---------|
| `WithParameters(dict)` | Set runtime parameters |
| `WithCancellation(token)` | Set cancellation token |
| `WithLogging(loggerFactory)` | Configure logging |
| `WithRetry(retryOptions)` | Set retry configuration |
| `WithResilience(policy)` | Set resilience policy |
| `WithErrorHandling(deadLetterSink?)` | Configure error handling |
| `WithObservability(loggerFactory?, tracer?)` | Configure observability |

## Next Steps

- [Dependency Injection](dependency-injection.md) — context setup via DI
- [Parallel Execution](parallel-execution.md) — thread safety for shared state
- [Pipeline Composition](pipeline-composition.md) — how context flows to sub-pipelines
