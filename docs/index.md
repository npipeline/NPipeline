---
title: "NPipeline"
description: "High-performance, graph-based streaming data pipeline library for .NET."
order: 1
---

# NPipeline

NPipeline is a .NET library for building high-performance streaming data pipelines. You define a graph of processing nodes — sources that produce data, transforms that modify it, and sinks that consume it — and NPipeline handles execution, error recovery, and data flow between them.

```csharp
// Define the pipeline structure
public class OrderPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<CsvSource, Order>("read-orders");
        var validate = builder.AddTransform<ValidateOrder, Order, Order>("validate");
        var enrich = builder.AddTransform<EnrichOrder, Order, EnrichedOrder>("enrich");
        var save = builder.AddSink<DatabaseSink, EnrichedOrder>("save");

        builder.Connect(source, validate);
        builder.Connect(validate, enrich);
        builder.Connect(enrich, save);
    }
}

// Run it
var runner = PipelineRunner.Create();
await runner.RunAsync<OrderPipeline>();
```

## Why NPipeline?

- **Streaming-first.** Data flows item-by-item through async streams, so you can process files larger than available memory without buffering everything upfront.
- **Graph-based.** Pipelines are directed graphs. Branch, merge, join, and compose sub-pipelines to model complex workflows.
- **Resilient.** Built-in retry strategies, circuit breakers, dead-letter queues, and node restart capabilities handle failures without losing data.
- **Fast.** Zero-allocation fast paths, compiled node factories, and execution plan caching minimize overhead in hot loops.
- **Testable.** Every node is independently testable. In-memory sources and sinks ship out of the box for integration tests.
- **Catches bugs at build time.** [Roslyn analyzers](analyzers/index.md) flag misconfigured retries, blocking calls, silent data loss, and performance anti-patterns before your pipeline ever runs.

## Use Cases

- ETL pipelines (extract from APIs/databases, transform, load elsewhere)
- Real-time event processing from Kafka, RabbitMQ, or Azure Service Bus
- Data validation and cleansing workflows
- File format conversion (CSV → Parquet, JSON → database)
- Batch processing of large datasets with controlled memory usage

## Design Principles

NPipeline's architecture is driven by a small set of non-negotiable principles:

| Principle | What It Means |
|-----------|--------------|
| **Streaming-first** | Data flows item-by-item via `IAsyncEnumerable<T>`. Nothing is buffered unless you explicitly opt in. |
| **Fail-fast defaults** | No items are silently skipped or retried. The default resilience policy returns `Fail` — you opt into recovery. |
| **Zero-allocation hot paths** | The per-item processing loop avoids heap allocations. No LINQ, no closures, `ValueTask<T>` where possible. |
| **Type safety at the graph level** | Typed handles prevent connecting incompatible nodes — caught by the compiler, not at runtime. |
| **Immutable configuration** | All config records are `sealed record` with `init`-only properties. No mutation, no race conditions. |
| **Extension points over modification** | New behavior is added through interfaces (`IExecutionStrategy`, `IResiliencePolicy`, `IDeadLetterSink`), not by modifying core classes. |

→ [Full Design Principles](advanced-topics/design-principles.md)

## Get Started

New to NPipeline? Follow these pages in order:

1. [Installation](getting-started/installation.md) — add the NuGet packages to your project
2. [Your First Pipeline](getting-started/your-first-pipeline.md) — build and run a working pipeline in 10 minutes
3. [Key Concepts](getting-started/key-concepts.md) — understand nodes, streams, and graphs
4. [What Next?](getting-started/what-next.md) — find the right guide for what you want to build

## Next Steps

- [Installation](getting-started/installation.md)
- [Guides](guides/defining-pipelines.md) for specific tasks (branching, batching, joins, etc.)
- [Connectors](connectors/index.md) for reading/writing CSV, databases, message queues, and more
- [Build-Time Analyzers](analyzers/index.md) to see what the analyzers catch
- [Advanced Topics](advanced-topics/index.md) for architecture deep-dives and extensibility
- [Extensions](extensions/index.md) for a full list of available packages
