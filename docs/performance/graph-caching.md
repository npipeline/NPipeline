---
title: "Graph Caching"
description: "Build a pipeline graph once instead of on every run with the [CacheableGraph] attribute."
order: 5
---

# Graph Caching

> **Prerequisites:** [Defining Pipelines](../guides/defining-pipelines.md)

[Execution plan caching](execution-plan-caching.md) removes the cost of compiling node delegates, but the graph those plans describe is still rebuilt on every `RunAsync`. Building a graph runs your `Define` method, copies the nodes and edges into immutable arrays, constructs two frozen dictionaries, builds a child graph for every composite node, and runs the full validation rule set.

For a long-running pipeline that cost is irrelevant. For a service that runs a short pipeline per request, it is most of the work. Marking the definition with `[CacheableGraph]` moves all of it to the first run:

```csharp
using NPipeline.Attributes;

[CacheableGraph]
public sealed class OrderProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<OrderSource, Order>("orders");
        var validate = builder.AddTransform<ValidateOrder, Order, Order>("validate");
        var sink = builder.AddSink<OrderSink, Order>("write");

        builder.Connect(source, validate).Connect(validate, sink);
    }
}
```

## The Contract

`Define` must ignore the `PipelineContext` it is handed. That context belongs to one run, and the graph it produces will serve every later run.

This is a constraint on `Define` only. Reading the context from inside a **node** is the normal way to parameterise a run, and it keeps working exactly as before - nodes are instantiated fresh per run and receive their own run's context.

Do not mark a definition that branches on the context:

```csharp
// Not cacheable: the graph depends on the run.
public void Define(PipelineBuilder builder, PipelineContext context)
{
    if (context.Parameters.ContainsKey("dryRun"))
        builder.AddSink<NoOpSink, Order>("write");
    else
        builder.AddSink<DatabaseSink, Order>("write");
}
```

Move that decision into a node, or leave the definition unmarked.

## What Is Shared Across Runs

Everything the cached graph holds is shared by every run that uses it:

- resilience policies, dead-letter sinks and lineage sinks supplied to the builder as **instances**
- execution strategies supplied as instances
- global observers, state managers and stateful registries

Supply these as types rather than instances if they hold per-run state. Node instances are never shared - see below.

## When Caching Is Skipped

Two shapes are refused and fall back to a normal rebuild, because reusing them would hand a later run objects the first run has already disposed:

- **The definition registers a node instance.** Every builder overload that takes a node instance rather than a node type does this, including `AddTap(sink)` and the lookup and grouping helpers. This case is reported once per definition type through `Trace`, because the attribute is not buying what it claims to.
- **The run's context supplies preconfigured node instances.** This is the shape tests use to inject fakes, and it makes the graph specific to that run.

A graph is also never shared between two runners using different lineage modules, since lineage adapters are built from the module that will handle them at runtime.

## What Is Not Shared

Node instances are created per run from the cached graph and disposed when the run ends, exactly as they are without caching. A cached graph stores node *types*, never node objects.

## Measured Effect

From `PipelineBuildBenchmarks` (.NET 10, short job), comparing the same definition with and without the attribute:

| Benchmark | Rebuilt | Cached |
|---|---|---|
| Build single-node graph | 5.8 us | 0.8 us |
| Build fan-out graph | 7.9 us | 0.8 us |
| Build 12-node linear graph | 14.3 us | 0.8 us |
| Run empty fan-out pipeline | 32.6 us | 14.8 us |
| Run fan-out pipeline over 100 items | 85.3 us | 75.2 us |

The cached build is flat regardless of graph size, so the larger the graph the more the attribute saves. The gain is a fixed amount per run, so it matters in proportion to how little work each run does.

## Next Steps

- [Execution Plan Caching](execution-plan-caching.md) - reuse compiled node delegates across runs
- [Performance Best Practices](best-practices.md) - broader optimization guidance
